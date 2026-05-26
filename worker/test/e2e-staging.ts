// Phase 3 end-to-end test against the real staging Cloudflare Worker.
//
// Run manually (or via nightly-e2e.yml) to prove:
//   1. JWT issuer (apps/web) hands out tokens for the staging relay URL
//   2. Both clients successfully upgrade against the live Worker (real TLS,
//      real Cloudflare edge, real Durable Object)
//   3. Messages round-trip in < 500ms each direction
//
// Required env:
//   STAGING_RELAY_URL       e.g. ziva-multiplayer-staging.ziva-multiplayer.workers.dev
//                           (no scheme — script prepends wss://)
//   STAGING_TOKEN_ENDPOINT  e.g. https://staging.ziva.sh
//   E2E_USER_API_KEY        Better-Auth API key for a basic-tier user with
//                           multiplayerEnabled=true.
// Optional:
//   STAGING_BYPASS_TOKEN    Vercel protection-bypass token for staging.ziva.sh.
//                           Required if the deployment is behind Vercel SSO.

import { WebSocket } from "ws";

function envOrThrow(name: string): string {
  const v = process.env[name];
  if (!v || v.length === 0) {
    throw new Error(`${name} not set`);
  }
  return v;
}

const RELAY_HOST = envOrThrow("STAGING_RELAY_URL").replace(/^wss?:\/\//, "");
const TOKEN_ENDPOINT = envOrThrow("STAGING_TOKEN_ENDPOINT").replace(/\/+$/, "");
const API_KEY = envOrThrow("E2E_USER_API_KEY");
const BYPASS = process.env.STAGING_BYPASS_TOKEN ?? "";

interface TokenResponse {
  token: string;
  relay_url: string;
  room_id: string;
  expires_at: number;
  protocol_version: number;
}

interface WelcomeEnvelope {
  type: "welcome";
  peer_id: number;
  protocol_version: 1;
}

async function fetchToken(roomId?: string): Promise<TokenResponse> {
  // Bypass token can be passed either as a query string or a header. Header
  // form is more robust because POST bodies don't survive a redirect.
  const url = `${TOKEN_ENDPOINT}/api/multiplayer/token`;
  const headers: Record<string, string> = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
  };
  if (BYPASS) headers["x-vercel-protection-bypass"] = BYPASS;
  if (BYPASS) headers["x-vercel-set-bypass-cookie"] = "true";

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(roomId ? { room_id: roomId } : {}),
  });
  if (!res.ok) {
    throw new Error(
      `Token endpoint returned ${res.status}: ${await res.text()}`,
    );
  }
  return (await res.json()) as TokenResponse;
}

function openClient(label: string, token: string, roomId: string): Promise<{
  ws: WebSocket;
  welcome: WelcomeEnvelope;
  closed: Promise<{ code: number; reason: string }>;
  next: (timeoutMs?: number) => Promise<string>;
}> {
  const url = `wss://${RELAY_HOST}/r/${encodeURIComponent(roomId)}?token=${encodeURIComponent(token)}&v=1`;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const buf: string[] = [];
    const waiters: ((m: string) => void)[] = [];
    let welcomeResolve: ((env: WelcomeEnvelope) => void) | null = null;
    const welcomeP = new Promise<WelcomeEnvelope>((r) => {
      welcomeResolve = r;
    });

    ws.on("message", (data) => {
      const m = data.toString();
      // First message is always the welcome.
      if (welcomeResolve) {
        let parsed: WelcomeEnvelope;
        try {
          parsed = JSON.parse(m) as WelcomeEnvelope;
        } catch (err) {
          reject(new Error(`${label}: invalid welcome JSON: ${m}`));
          return;
        }
        if (parsed.type !== "welcome") {
          reject(new Error(`${label}: expected welcome, got: ${m}`));
          return;
        }
        welcomeResolve(parsed);
        welcomeResolve = null;
        return;
      }
      const w = waiters.shift();
      if (w) w(m);
      else buf.push(m);
    });

    let resolveClosed!: (info: { code: number; reason: string }) => void;
    const closed = new Promise<{ code: number; reason: string }>((r) => {
      resolveClosed = r;
    });
    ws.on("close", (code, reason) => {
      resolveClosed({ code, reason: reason.toString() });
    });

    ws.on("error", (err) => {
      reject(new Error(`${label}: ws error: ${err.message}`));
    });

    ws.on("open", async () => {
      try {
        const welcome = await welcomeP;
        resolve({
          ws,
          welcome,
          closed,
          next(timeoutMs = 500) {
            return new Promise<string>((resolveNext, rejectNext) => {
              if (buf.length > 0) {
                resolveNext(buf.shift()!);
                return;
              }
              const timer = setTimeout(() => {
                const idx = waiters.indexOf(handler);
                if (idx >= 0) waiters.splice(idx, 1);
                rejectNext(new Error(`${label}.next() timeout after ${timeoutMs}ms`));
              }, timeoutMs);
              const handler = (m: string) => {
                clearTimeout(timer);
                resolveNext(m);
              };
              waiters.push(handler);
            });
          },
        });
      } catch (err) {
        reject(err);
      }
    });
  });
}

async function main() {
  console.log(`[e2e] relay=wss://${RELAY_HOST}`);
  console.log(`[e2e] token endpoint=${TOKEN_ENDPOINT}/api/multiplayer/token`);

  // Step 1: mint a token (lets the server pick a fresh room id), then mint
  // a second token for the same room so both clients land on the same DO.
  const t1 = await fetchToken();
  const roomId = t1.room_id;
  console.log(`[e2e] minted token for room=${roomId}`);
  const t2 = await fetchToken(roomId);
  console.log(`[e2e] minted second token for same room`);

  // Step 2: open both clients in parallel.
  const [a, b] = await Promise.all([
    openClient("A", t1.token, roomId),
    openClient("B", t2.token, roomId),
  ]);
  console.log(`[e2e] both clients connected; peer ids A=${a.welcome.peer_id} B=${b.welcome.peer_id}`);

  // Step 3: A -> B round trip.
  const expectAtoB = JSON.stringify({ type: "data", payload: { ping: 1 } });
  const recvB = b.next(500);
  const t0AB = Date.now();
  a.ws.send(expectAtoB);
  const rawB = await recvB;
  const dtAB = Date.now() - t0AB;
  const envAtoB = JSON.parse(rawB) as { type: string; from: number; payload: { ping: number } };
  if (envAtoB.type !== "data" || envAtoB.from !== a.welcome.peer_id || envAtoB.payload?.ping !== 1) {
    throw new Error(`A->B mismatch: ${rawB}`);
  }
  console.log(`[e2e] A->B latency: ${dtAB}ms`);

  // Step 4: B -> A round trip.
  const expectBtoA = JSON.stringify({ type: "data", payload: { pong: 2 } });
  const recvA = a.next(500);
  const t0BA = Date.now();
  b.ws.send(expectBtoA);
  const rawA = await recvA;
  const dtBA = Date.now() - t0BA;
  const envBtoA = JSON.parse(rawA) as { type: string; from: number; payload: { pong: number } };
  if (envBtoA.type !== "data" || envBtoA.from !== b.welcome.peer_id || envBtoA.payload?.pong !== 2) {
    throw new Error(`B->A mismatch: ${rawA}`);
  }
  console.log(`[e2e] B->A latency: ${dtBA}ms`);

  a.ws.close();
  b.ws.close();
  await Promise.all([a.closed, b.closed]);
  console.log(`[e2e] OK`);
}

main().catch((err) => {
  console.error(`[e2e] FAIL:`, err.message ?? err);
  process.exit(1);
});

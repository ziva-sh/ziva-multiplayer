// End-to-end test against the real staging Cloudflare Worker.
//
// Uses Godot's WebSocketMultiplayerPeer binary protocol — same wire format
// the headless-Godot e2e test exercises but via Bun's `ws` client. Proves:
//   1. JWT issuer (apps/web) hands out tokens for the staging relay URL
//   2. Both clients successfully upgrade against the live Worker (real TLS,
//      real Cloudflare edge, real Durable Object)
//   3. The 4-byte LE peer_id handshake arrives first
//   4. SYS_COMMAND_ADD_PEER announcements fire on both directions
//   5. SYS_COMMAND_RELAY round-trips with sender_id rewritten correctly
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

import {
  NETWORK_COMMAND_SYS,
  SYS_COMMAND_ADD_PEER,
  SYS_COMMAND_RELAY,
} from "../src/proto/v1";

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

interface Client {
  ws: WebSocket;
  peerId: number;
  closed: Promise<{ code: number; reason: string }>;
  next: (timeoutMs?: number) => Promise<Buffer>;
}

function openClient(label: string, token: string, roomId: string): Promise<Client> {
  const url = `wss://${RELAY_HOST}/r/${encodeURIComponent(roomId)}?token=${encodeURIComponent(token)}&v=1`;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const buf: Buffer[] = [];
    const waiters: ((m: Buffer) => void)[] = [];
    let peerIdResolve: ((id: number) => void) | null = null;
    const peerIdP = new Promise<number>((r) => {
      peerIdResolve = r;
    });

    ws.on("message", (data) => {
      // First message is the 4-byte LE peer_id handshake.
      const buffer = Buffer.isBuffer(data)
        ? data
        : Buffer.from(data as ArrayBuffer);
      if (peerIdResolve) {
        if (buffer.byteLength !== 4) {
          reject(
            new Error(
              `${label}: expected 4-byte peer_id handshake, got ${buffer.byteLength} bytes`,
            ),
          );
          return;
        }
        const id = buffer.readInt32LE(0);
        peerIdResolve(id);
        peerIdResolve = null;
        return;
      }
      const w = waiters.shift();
      if (w) w(buffer);
      else buf.push(buffer);
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
        const peerId = await peerIdP;
        resolve({
          ws,
          peerId,
          closed,
          next(timeoutMs = 1000) {
            return new Promise<Buffer>((resolveNext, rejectNext) => {
              if (buf.length > 0) {
                resolveNext(buf.shift()!);
                return;
              }
              const timer = setTimeout(() => {
                const idx = waiters.indexOf(handler);
                if (idx >= 0) waiters.splice(idx, 1);
                rejectNext(
                  new Error(`${label}.next() timeout after ${timeoutMs}ms`),
                );
              }, timeoutMs);
              const handler = (m: Buffer) => {
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

function buildRelayPacket(targetPeer: number, innerPayload: Uint8Array): Uint8Array {
  const out = new Uint8Array(6 + innerPayload.byteLength);
  out[0] = NETWORK_COMMAND_SYS;
  out[1] = SYS_COMMAND_RELAY;
  new DataView(out.buffer).setInt32(2, targetPeer, /* littleEndian */ true);
  out.set(innerPayload, 6);
  return out;
}

function parseRelayPacket(buf: Buffer): { senderId: number; inner: Buffer } {
  if (buf.byteLength < 7) {
    throw new Error(`relay packet too small: ${buf.byteLength}`);
  }
  if ((buf[0] & 0x7) !== NETWORK_COMMAND_SYS || buf[1] !== SYS_COMMAND_RELAY) {
    throw new Error(
      `expected SYS|RELAY header, got cmd=${buf[0] & 0x7} sub=${buf[1]}`,
    );
  }
  const senderId = buf.readInt32LE(2);
  const inner = buf.subarray(6);
  return { senderId, inner };
}

function parseSysPeerPacket(buf: Buffer, expectedSub: number): number {
  if (buf.byteLength < 6) {
    throw new Error(`SYS peer packet too small: ${buf.byteLength}`);
  }
  if ((buf[0] & 0x7) !== NETWORK_COMMAND_SYS) {
    throw new Error(`expected NETWORK_COMMAND_SYS, got cmd ${buf[0] & 0x7}`);
  }
  if (buf[1] !== expectedSub) {
    throw new Error(`expected SYS sub ${expectedSub}, got ${buf[1]}`);
  }
  return buf.readInt32LE(2);
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
  console.log(
    `[e2e] both clients connected; peer ids A=${a.peerId} B=${b.peerId}`,
  );

  // Step 3: each client should receive an ADD_PEER announcement for the
  // other. The order depends on which joined first (the second joiner gets
  // an ADD_PEER for the first; the first gets an ADD_PEER for the second
  // at the moment the second joins).
  const aAdd = await a.next(2000);
  const bAdd = await b.next(2000);
  const aHeardAbout = parseSysPeerPacket(aAdd, SYS_COMMAND_ADD_PEER);
  const bHeardAbout = parseSysPeerPacket(bAdd, SYS_COMMAND_ADD_PEER);
  if (aHeardAbout !== b.peerId) {
    throw new Error(
      `A expected ADD_PEER(${b.peerId}), got ADD_PEER(${aHeardAbout})`,
    );
  }
  if (bHeardAbout !== a.peerId) {
    throw new Error(
      `B expected ADD_PEER(${a.peerId}), got ADD_PEER(${bHeardAbout})`,
    );
  }
  console.log(`[e2e] both peers received ADD_PEER announcements`);

  // Step 4: A -> B RELAY with sender_id rewrite.
  const innerAB = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
  const recvB = b.next(2000);
  const t0AB = Date.now();
  a.ws.send(buildRelayPacket(b.peerId, innerAB));
  const rawB = await recvB;
  const dtAB = Date.now() - t0AB;
  const parsedAB = parseRelayPacket(rawB);
  if (parsedAB.senderId !== a.peerId) {
    throw new Error(
      `A->B sender mismatch: expected ${a.peerId}, got ${parsedAB.senderId}`,
    );
  }
  if (!parsedAB.inner.equals(Buffer.from(innerAB))) {
    throw new Error(`A->B inner payload mismatch: ${parsedAB.inner.toString("hex")}`);
  }
  console.log(`[e2e] A->B RELAY round-trip latency: ${dtAB}ms`);

  // Step 5: B -> A RELAY in the other direction.
  const innerBA = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);
  const recvA = a.next(2000);
  const t0BA = Date.now();
  b.ws.send(buildRelayPacket(a.peerId, innerBA));
  const rawA = await recvA;
  const dtBA = Date.now() - t0BA;
  const parsedBA = parseRelayPacket(rawA);
  if (parsedBA.senderId !== b.peerId) {
    throw new Error(
      `B->A sender mismatch: expected ${b.peerId}, got ${parsedBA.senderId}`,
    );
  }
  if (!parsedBA.inner.equals(Buffer.from(innerBA))) {
    throw new Error(`B->A inner payload mismatch: ${parsedBA.inner.toString("hex")}`);
  }
  console.log(`[e2e] B->A RELAY round-trip latency: ${dtBA}ms`);

  a.ws.close();
  b.ws.close();
  await Promise.all([a.closed, b.closed]);
  console.log(`[e2e] OK`);
}

main().catch((err) => {
  console.error(`[e2e] FAIL:`, err.message ?? err);
  process.exit(1);
});

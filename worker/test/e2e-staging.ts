// End-to-end test against the real staging Cloudflare Worker.
//
// Uses Godot's WebSocketMultiplayerPeer binary protocol — same wire format
// the headless-Godot e2e test exercises but via Bun's `ws` client. Proves:
//   1. Both clients upgrade against the live Worker token-lessly (the Worker's
//      lazy /check against staging apps/web returns allowed for E2E_USER_ID)
//   2. The 4-byte LE peer_id handshake arrives first
//   3. SYS_COMMAND_ADD_PEER announcements fire in both directions
//   4. SYS_COMMAND_RELAY round-trips with sender_id rewritten correctly
//
// Required env:
//   STAGING_RELAY_URL  e.g. ziva-multiplayer-staging.ziva-multiplayer.workers.dev
//                      (no scheme — script prepends wss://)
//   E2E_USER_ID        a dev user id whose /api/multiplayer/check returns
//                      { allowed: true } on staging apps/web.
// Optional:
//   E2E_GAME_ID        game id to report (default "e2e").

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
const USER_ID = envOrThrow("E2E_USER_ID");
const GAME_ID = process.env.E2E_GAME_ID ?? "e2e";

interface Client {
  ws: WebSocket;
  peerId: number;
  // cf-ray on the WS upgrade response. The ...-XXX suffix is the serving colo.
  // Best-effort: node's `ws` exposes it via the 'upgrade' event; bun's `ws`
  // does not implement that event, so this is null under bun (we fall back to
  // /cdn-cgi/trace for the colo there).
  upgradeColo: string | null;
  closed: Promise<{ code: number; reason: string }>;
  next: (timeoutMs?: number) => Promise<Buffer>;
}

function openClient(label: string, roomId: string): Promise<Client> {
  const url = `wss://${RELAY_HOST}/r/${encodeURIComponent(roomId)}?u=${encodeURIComponent(USER_ID)}&g=${encodeURIComponent(GAME_ID)}&v=1`;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    let upgradeColo: string | null = null;
    // 'upgrade' fires on node; under bun this is simply never called.
    ws.on("upgrade", (res: { headers: Record<string, string | string[]> }) => {
      const ray = res.headers["cf-ray"];
      const rayStr = Array.isArray(ray) ? ray[0] : ray;
      const suffix = typeof rayStr === "string" ? rayStr.split("-")[1] : undefined;
      upgradeColo = suffix ?? null;
    });
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
          upgradeColo,
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

// Number of relay round-trips to time. Default 40 — enough for a stable p50/p90
// without hammering the live DO (the Worker invocation budget is shared).
const LATENCY_SAMPLES = Number(process.env.LATENCY_SAMPLES ?? "40");

// Nearest-rank percentile over a copy of the samples (sorted ascending).
// p in [0,100]. Nearest-rank avoids interpolation so every reported number is
// an actually-observed round-trip, not a synthesized average.
function percentile(samples: number[], p: number): number {
  const sorted = [...samples].sort((x, y) => x - y);
  const rank = Math.ceil((p / 100) * sorted.length);
  const idx = Math.min(Math.max(rank - 1, 0), sorted.length - 1);
  return sorted[idx];
}

// One application-level relay round-trip: send SYS|RELAY to `from`'s peer, wait
// for it to arrive at `to`, return the elapsed ms. Verifies sender-id rewrite
// and payload so a corrupted relay can't masquerade as a fast one.
async function relayRoundTrip(
  from: Client,
  to: Client,
  inner: Uint8Array,
): Promise<number> {
  const recv = to.next(2000);
  const t0 = Date.now();
  from.ws.send(buildRelayPacket(to.peerId, inner));
  const raw = await recv;
  const dt = Date.now() - t0;
  const parsed = parseRelayPacket(raw);
  if (parsed.senderId !== from.peerId) {
    throw new Error(
      `relay sender mismatch: expected ${from.peerId}, got ${parsed.senderId}`,
    );
  }
  if (!parsed.inner.equals(Buffer.from(inner))) {
    throw new Error(`relay inner payload mismatch: ${parsed.inner.toString("hex")}`);
  }
  return dt;
}

// Serving colo + measuring-client region, from /cdn-cgi/trace against the SAME
// edge hostname the WS connects to. This is the portable colo source (works
// under bun, which can't read the upgrade's cf-ray). `colo` is the Cloudflare
// PoP terminating the WS; `loc` is the country Cloudflare geo-located us to.
async function fetchEdgeTrace(): Promise<{ colo: string; loc: string }> {
  const res = await fetch(`https://${RELAY_HOST}/cdn-cgi/trace`);
  if (!res.ok) {
    throw new Error(`/cdn-cgi/trace returned ${res.status}`);
  }
  const text = await res.text();
  const kv = new Map(
    text
      .trim()
      .split("\n")
      .map((line) => {
        const eq = line.indexOf("=");
        return [line.slice(0, eq), line.slice(eq + 1)] as [string, string];
      }),
  );
  const colo = kv.get("colo");
  const loc = kv.get("loc");
  if (!colo || !loc) {
    throw new Error(`/cdn-cgi/trace missing colo/loc: ${text}`);
  }
  return { colo, loc };
}

async function main() {
  console.log(`[e2e] relay=wss://${RELAY_HOST}`);

  // Pick a fresh room id so both clients land on the same DO without colliding
  // with any other run.
  const roomId = `e2e-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  console.log(`[e2e] room=${roomId} user=${USER_ID} game=${GAME_ID}`);

  const [a, b] = await Promise.all([
    openClient("A", roomId),
    openClient("B", roomId),
  ]);
  console.log(
    `[e2e] both clients connected; peer ids A=${a.peerId} B=${b.peerId}`,
  );

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

  // Two warm-up round-trips (one each direction) double as the original
  // correctness check: sender-id rewrite + payload integrity both ways. Their
  // timings are discarded so cold-start / first-packet effects don't skew p50.
  await relayRoundTrip(a, b, new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
  await relayRoundTrip(b, a, new Uint8Array([0xca, 0xfe, 0xba, 0xbe]));
  console.log(`[e2e] warm-up round-trips OK (both directions verified)`);

  // The serving colo and our region. cf-ray from the upgrade is the ground
  // truth for which colo terminated THIS socket; /cdn-cgi/trace gives the same
  // colo plus our geo and works under bun (no upgrade event there).
  const trace = await fetchEdgeTrace();
  const coloFromRay = a.upgradeColo ?? b.upgradeColo;
  console.log(
    `[e2e] serving colo=${trace.colo}` +
      (coloFromRay ? ` (cf-ray confirms ${coloFromRay})` : ` (cf-ray unavailable under this runtime)`) +
      ` | measuring-client region=${trace.loc}`,
  );

  // N-sample latency loop. Alternate A->B / B->A so neither direction's queue
  // dominates, and pool both into one distribution (the relay is symmetric).
  const samples: number[] = [];
  const inner = new Uint8Array([0x12, 0x34, 0x56, 0x78]);
  for (let i = 0; i < LATENCY_SAMPLES; i++) {
    const dt = i % 2 === 0 ? await relayRoundTrip(a, b, inner) : await relayRoundTrip(b, a, inner);
    samples.push(dt);
  }
  const p50 = percentile(samples, 50);
  const p90 = percentile(samples, 90);
  const min = Math.min(...samples);
  const max = Math.max(...samples);
  console.log(
    `[e2e] RELAY round-trip over ${samples.length} samples: ` +
      `p50=${p50}ms p90=${p90}ms (min=${min}ms max=${max}ms)`,
  );
  console.log(
    `[e2e] LATENCY-RESULT colo=${trace.colo} region=${trace.loc} ` +
      `p50=${p50} p90=${p90} min=${min} max=${max} n=${samples.length}`,
  );

  a.ws.close();
  b.ws.close();
  await Promise.all([a.closed, b.closed]);
  console.log(`[e2e] OK`);
}

main().catch((err) => {
  console.error(`[e2e] FAIL:`, err.message ?? err);
  process.exit(1);
});

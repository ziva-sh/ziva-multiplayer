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
  closed: Promise<{ code: number; reason: string }>;
  next: (timeoutMs?: number) => Promise<Buffer>;
}

function openClient(label: string, roomId: string): Promise<Client> {
  const url = `wss://${RELAY_HOST}/r/${encodeURIComponent(roomId)}?u=${encodeURIComponent(USER_ID)}&g=${encodeURIComponent(GAME_ID)}&v=1`;
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

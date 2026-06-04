// Throughput / "tickrate" probe against the live staging relay.
//
// Latency (e2e-staging.ts) answers "how long does ONE update take". This answers
// "how many updates/sec can the DO ingest + broadcast" — a different number.
//
// Two paths, because they hit different ceilings in room-do.ts:
//   • RELAY  (Model 1 host-client @rpc/sync): SYS|RELAY fan-out, NO storage.
//     Capped at RATE_LIMIT_MSGS_PER_SEC (1000/s) PER CONNECTION.
//   • STATE  (Model 2b shared-state):         STATE_SET -> assign rev (SQLite
//     UPDATE..RETURNING) -> persist -> broadcast. Handled BEFORE the rate-limit
//     gate, so NO msg cap — bounded by the DO's serial SQLite commit rate.
//
// For each path we fire a pipelined burst as fast as the socket drains and
// measure: effective delivered rate (msgs/sec), drop count, and the latency
// distribution UNDER LOAD (queuing pushes it above the idle ~36ms floor).
//
// Env: STAGING_RELAY_URL, E2E_USER_ID, [E2E_GAME_ID].
import { WebSocket } from "ws";

import {
  STATE_SET,
  STATE_UPDATE,
  NETWORK_COMMAND_SYS,
  SYS_COMMAND_RELAY,
} from "../src/proto/v1";

function envOrThrow(n: string): string {
  const v = process.env[n];
  if (!v) throw new Error(`${n} not set`);
  return v;
}
const RELAY_HOST = envOrThrow("STAGING_RELAY_URL").replace(/^wss?:\/\//, "");
const USER_ID = envOrThrow("E2E_USER_ID");
const GAME_ID = process.env.E2E_GAME_ID ?? "tickrate";

interface Client {
  ws: WebSocket;
  peerId: number;
  onBinary: (cb: (b: Buffer) => void) => void;
}

function openClient(label: string, room: string): Promise<Client> {
  const url = `wss://${RELAY_HOST}/r/${encodeURIComponent(room)}?u=${encodeURIComponent(USER_ID)}&g=${encodeURIComponent(GAME_ID)}&v=1`;
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    let gotId = false;
    let cb: ((b: Buffer) => void) | null = null;
    ws.on("message", (data) => {
      const b = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
      if (!gotId) {
        gotId = true; // first frame = 4-byte peer id handshake
        (ws as unknown as { _pid: number })._pid = b.readInt32LE(0);
        return;
      }
      if (cb) cb(b);
    });
    ws.on("error", (e) => reject(new Error(`${label}: ${e.message}`)));
    ws.on("close", (code, r) => {
      if (code !== 1000 && code !== 1005)
        console.log(
          `[tput] ${label} closed code=${code} reason=${r.toString()}`,
        );
    });
    ws.on("open", () =>
      setTimeout(() => {
        resolve({
          ws,
          peerId: (ws as unknown as { _pid: number })._pid,
          onBinary: (fn) => (cb = fn),
        });
      }, 300),
    );
  });
}

function relayBroadcast(seq: number, sendTs: number): Uint8Array {
  // [SYS][RELAY][i32 target=0][u32 seq][f64 sendTs]
  const out = new Uint8Array(6 + 12);
  out[0] = NETWORK_COMMAND_SYS;
  out[1] = SYS_COMMAND_RELAY;
  const dv = new DataView(out.buffer);
  dv.setInt32(2, 0, true); // broadcast
  dv.setUint32(6, seq, true);
  dv.setFloat64(10, sendTs, true);
  return out;
}
function stateSet(seq: number, sendTs: number): Uint8Array {
  // [STATE_SET][u16 keyLen=1]["k"][u32 seq][f64 sendTs]
  const out = new Uint8Array(1 + 2 + 1 + 12);
  out[0] = STATE_SET;
  const dv = new DataView(out.buffer);
  dv.setUint16(1, 1, true);
  out[3] = 0x6b; // 'k'
  dv.setUint32(4, seq, true);
  dv.setFloat64(8, sendTs, true);
  return out;
}

function pct(a: number[], p: number): number {
  const s = [...a].sort((x, y) => x - y);
  return s[
    Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))
  ];
}

async function drainBurst(opts: {
  name: string;
  count: number;
  build: (seq: number, ts: number) => Uint8Array;
  sender: Client;
  // where the measured echo arrives, and how to read (seq, sendTs) from it
  observer: Client;
  parse: (b: Buffer) => { seq: number; ts: number } | null;
}): Promise<void> {
  const { name, count, build, sender, observer, parse } = opts;
  const lat: number[] = [];
  let received = 0;
  let firstRecv = 0;
  let lastRecv = 0;
  const seen = new Set<number>();
  let done!: () => void;
  const finished = new Promise<void>((r) => (done = r));
  observer.onBinary((b) => {
    const m = parse(b);
    if (!m || seen.has(m.seq)) return;
    seen.add(m.seq);
    received++;
    const now = Date.now();
    if (!firstRecv) firstRecv = now;
    lastRecv = now;
    lat.push(now - m.ts);
    if (received >= count) done();
  });

  const t0 = Date.now();
  for (let i = 0; i < count; i++) sender.ws.send(build(i, Date.now()));
  const sendDone = Date.now();

  // Wait for drain or timeout.
  const timeout = new Promise<void>((r) => setTimeout(r, 15000));
  await Promise.race([finished, timeout]);

  const drainMs = (lastRecv || Date.now()) - t0;
  const rate = received > 0 ? (received / drainMs) * 1000 : 0;
  console.log(
    `[tput] ${name}: sent ${count} in ${sendDone - t0}ms | received ${received}/${count} ` +
      `(${count - received} lost) | drain ${drainMs}ms => ${rate.toFixed(0)} msg/s | ` +
      `latency-under-load p50=${pct(lat, 50)}ms p90=${pct(lat, 90)}ms max=${Math.max(...lat, 0)}ms`,
  );
  console.log(
    `[tput] RESULT ${name} rate=${rate.toFixed(0)} recv=${received} lost=${count - received} p50=${pct(lat, 50)} p90=${pct(lat, 90)}`,
  );
}

// Steady-state: send at a FIXED rate (a real game tickrate) and measure the
// round-trip latency when the DO is NOT backlogged — the number that says "can
// I run my loop at this Hz". Warm-up samples (first 0.5s) are discarded.
async function pacedTest(opts: {
  name: string;
  rateHz: number;
  durationS: number;
  build: (seq: number, ts: number) => Uint8Array;
  sender: Client;
  observer: Client;
  parse: (b: Buffer) => { seq: number; ts: number } | null;
}): Promise<void> {
  const { name, rateHz, durationS, build, sender, observer, parse } = opts;
  const interval = 1000 / rateHz;
  const lat = new Map<number, number>(); // seq -> sendTs
  const rtt: number[] = [];
  let warmupCutoff = 0;
  observer.onBinary((b) => {
    const m = parse(b);
    if (!m || !lat.has(m.seq)) return;
    const dt = Date.now() - lat.get(m.seq)!;
    if (lat.get(m.seq)! >= warmupCutoff) rtt.push(dt);
  });
  const start = Date.now();
  warmupCutoff = start + 500;
  let i = 0;
  while (Date.now() - start < durationS * 1000) {
    const ts = Date.now();
    lat.set(i, ts);
    sender.ws.send(build(i, ts));
    i++;
    const wait = start + i * interval - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  }
  await new Promise((r) => setTimeout(r, 1000)); // drain
  // Expected steady samples = total sent minus those sent during the 0.5s warmup
  // (which are intentionally discarded). Loss = measured well below that.
  const warmupSends = Math.ceil(500 / interval);
  const expected = Math.max(1, i - warmupSends);
  console.log(
    `[tput] ${name} @${rateHz}Hz for ${durationS}s: sent ${i}, steady-state RTT ` +
      `p50=${pct(rtt, 50)}ms p90=${pct(rtt, 90)}ms (n=${rtt.length}/${expected} post-warmup) — ${rtt.length >= expected * 0.95 ? "KEEPS UP" : "BACKLOG/LOSS"}`,
  );
}

async function main() {
  const room = `tput-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  console.log(`[tput] relay=wss://${RELAY_HOST} room=${room} user=${USER_ID}`);
  const w = await openClient("W", room);
  const o = await openClient("O", room);
  console.log(`[tput] connected W=${w.peerId} O=${o.peerId}`);
  // let the ADD_PEER burst settle
  await new Promise((r) => setTimeout(r, 500));

  // --- RELAY path (Model 1). Capped at 1000/s; stay under it (800 burst). ---
  await drainBurst({
    name: "RELAY-800",
    count: 800,
    build: relayBroadcast,
    sender: w,
    observer: o,
    parse: (b) => {
      if (
        b.length < 18 ||
        (b[0] & 0x7) !== NETWORK_COMMAND_SYS ||
        b[1] !== SYS_COMMAND_RELAY
      )
        return null;
      return { seq: b.readUInt32LE(6), ts: b.readDoubleLE(10) };
    },
  });

  await new Promise((r) => setTimeout(r, 1500));

  // --- STATE path (Model 2b). No msg cap; push hard to find the commit ceiling.
  // Measure the echo back at the SENDER (its own STATE_UPDATE), which is the
  // committed-rev signal. 'k' is a single hot key (worst case: every write
  // serializes on the same key's rev + row).
  await drainBurst({
    name: "STATE-2000-hotkey",
    count: 2000,
    build: stateSet,
    sender: w,
    observer: w,
    parse: (b) => {
      if (b.length < 1 || b[0] !== STATE_UPDATE) return null;
      // [STATE_UPDATE][u32 rev][u16 keyLen][key][u32 seq][f64 ts]
      const keyLen = b.readUInt16LE(5);
      const off = 7 + keyLen;
      if (b.length < off + 12) return null;
      return { seq: b.readUInt32LE(off), ts: b.readDoubleLE(off + 4) };
    },
  });

  await new Promise((r) => setTimeout(r, 1500));

  // --- Steady-state at real game tickrates: does it keep up, and at what RTT? ---
  const stateParse = (b: Buffer) => {
    if (b.length < 1 || b[0] !== STATE_UPDATE) return null;
    const keyLen = b.readUInt16LE(5);
    const off = 7 + keyLen;
    if (b.length < off + 12) return null;
    return { seq: b.readUInt32LE(off), ts: b.readDoubleLE(off + 4) };
  };
  await pacedTest({
    name: "STATE",
    rateHz: 20,
    durationS: 3,
    build: stateSet,
    sender: w,
    observer: w,
    parse: stateParse,
  });
  await pacedTest({
    name: "STATE",
    rateHz: 60,
    durationS: 3,
    build: stateSet,
    sender: w,
    observer: w,
    parse: stateParse,
  });

  w.ws.close();
  o.ws.close();
  await new Promise((r) => setTimeout(r, 300));
  console.log("[tput] OK");
  process.exit(0);
}

main().catch((e) => {
  console.error("[tput] FAIL:", e.message ?? e);
  process.exit(1);
});

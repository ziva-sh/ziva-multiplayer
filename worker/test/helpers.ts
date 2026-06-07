// Shared test helpers — /check stubbing and WebSocket plumbing.
//
// The Client connection pattern is intentionally specific: we attach the
// message listener BEFORE calling ws.accept(). If we don't, the runtime can
// deliver buffered server frames (like the assigned-peer-id handshake) before
// the listener is registered and the test will block forever waiting for them.

import { SELF, env, fetchMock } from "cloudflare:test";

import {
  NETWORK_COMMAND_SYS,
  SYS_COMMAND_ADD_PEER,
  SYS_COMMAND_DEL_PEER,
  SYS_COMMAND_RELAY,
} from "../src/proto/v1";

// The Worker's access check fetches `${ZIVA_API_BASE}/api/multiplayer/check`.
// We mock that origin via Miniflare's fetch mock. Reading the origin from the
// same binding the Worker uses guarantees the interceptor and the Worker can
// never disagree.
const CHECK_ORIGIN = env.ZIVA_API_BASE;

function checkPathMatcher(userId: string): (path: string) => boolean {
  // undici matches on path INCLUDING the query string. We match the exact
  // user so concurrent tests with distinct users don't cross-talk.
  return (path: string) =>
    path.startsWith("/api/multiplayer/check") &&
    path.includes(`u=${encodeURIComponent(userId)}`);
}

// Stub the NEXT /check call for `userId` to return {allowed, reason}. One-shot by
// default; multiple registrations for the same user are consumed FIFO so a
// test can script "prime allowed, then refresh not-allowed".
export function stubCheck(
  userId: string,
  allowed: boolean,
  times = 1,
  reason?: string,
): void {
  const body = reason ? { allowed, reason } : { allowed };
  fetchMock
    .get(CHECK_ORIGIN)
    .intercept({ method: "GET", path: checkPathMatcher(userId) })
    .reply(200, body)
    .times(times);
}

// Stub the NEXT /check call for `userId` to fail at the transport layer
// (simulates apps/web being unreachable / 5xx).
export function stubCheckError(userId: string, times = 1): void {
  fetchMock
    .get(CHECK_ORIGIN)
    .intercept({ method: "GET", path: checkPathMatcher(userId) })
    .replyWithError(new Error("simulated check transport failure"))
    .times(times);
}

export interface ConnectOptions {
  rid?: string;
  u?: string;
  g?: string;
  v?: number | string;
  ip?: string;
  // When set, register a one-shot `/check → {allowed:true}` stub for `u`
  // immediately before connecting. Most relay tests just want a working
  // connection and don't care about the check; they leave this on.
  stub?: boolean;
}

export interface Connection {
  ws: WebSocket;
  // Returns the next message as raw bytes. Throws on timeout.
  next(timeoutMs?: number): Promise<Uint8Array>;
  closed: Promise<{ code: number; reason: string }>;
}

export class UpgradeFailure extends Error {
  constructor(
    public status: number,
    public body: string,
  ) {
    super(`upgrade failed: ${status} ${body}`);
  }
}

export async function connect(opts: ConnectOptions = {}): Promise<Connection> {
  const rid = opts.rid ?? "room-test";
  const u = opts.u ?? "user-test";
  const g = opts.g ?? "game-test";
  const v = opts.v ?? 1;
  // Default to a unique IP per call so normal tests never contend on the
  // shared per-IP connection-rate window. Rate-limit tests pin `ip` to make
  // the window accumulate.
  const ip = opts.ip ?? `ip-${crypto.randomUUID()}`;

  if (opts.stub ?? true) {
    stubCheck(u, true);
  }

  const params = new URLSearchParams();
  params.set("u", u);
  params.set("g", g);
  params.set("v", String(v));

  const res = await SELF.fetch(
    `http://relay.test/r/${encodeURIComponent(rid)}?${params.toString()}`,
    {
      headers: { Upgrade: "websocket", "CF-Connecting-IP": ip },
    },
  );

  if (res.status !== 101 || !res.webSocket) {
    throw new UpgradeFailure(res.status, await res.text());
  }

  const ws = res.webSocket;

  // FIFO buffer + waiter queue so callers can `await next()` without races.
  const buf: Uint8Array[] = [];
  const waiters: ((m: Uint8Array) => void)[] = [];
  ws.addEventListener("message", (ev) => {
    // The Workers runtime delivers binary frames as ArrayBuffer; convert
    // text frames (shouldn't happen with the binary protocol) into bytes
    // so the test interface stays consistent.
    const m =
      typeof ev.data === "string"
        ? new TextEncoder().encode(ev.data)
        : new Uint8Array(ev.data as ArrayBuffer);
    const w = waiters.shift();
    if (w) w(m);
    else buf.push(m);
  });

  let resolveClosed!: (info: { code: number; reason: string }) => void;
  const closed = new Promise<{ code: number; reason: string }>((r) => {
    resolveClosed = r;
  });
  ws.addEventListener("close", (ev) => {
    resolveClosed({ code: ev.code, reason: ev.reason });
  });

  ws.accept();

  return {
    ws,
    closed,
    next(timeoutMs = 500) {
      return new Promise<Uint8Array>((resolve, reject) => {
        if (buf.length > 0) {
          resolve(buf.shift()!);
          return;
        }
        const timer = setTimeout(() => {
          const idx = waiters.indexOf(handler);
          if (idx >= 0) waiters.splice(idx, 1);
          reject(new Error(`next() timeout after ${timeoutMs}ms`));
        }, timeoutMs);
        const handler = (m: Uint8Array) => {
          clearTimeout(timer);
          resolve(m);
        };
        waiters.push(handler);
      });
    },
  };
}

// Read the 4-byte handshake (LE int32 = assigned peer_id) off a freshly
// connected socket. Throws if the first frame isn't 4 bytes.
export async function readPeerId(c: Connection): Promise<number> {
  const raw = await c.next(500);
  if (raw.byteLength !== 4) {
    throw new Error(
      `expected 4-byte peer_id handshake, got ${raw.byteLength} bytes`,
    );
  }
  return new DataView(raw.buffer, raw.byteOffset, raw.byteLength).getInt32(
    0,
    /* littleEndian */ true,
  );
}

// Convenience: parse a SYS-with-peer-arg packet (ADD_PEER / DEL_PEER) and
// return the announced peer id. Throws if the frame doesn't match.
export function parseSysPeerPacket(
  frame: Uint8Array,
  expectedSubCommand: number,
): number {
  if (frame.byteLength < 6) {
    throw new Error(`SYS peer packet too small: ${frame.byteLength}`);
  }
  if ((frame[0] & 0x7) !== NETWORK_COMMAND_SYS) {
    throw new Error(
      `expected NETWORK_COMMAND_SYS, got cmd ${frame[0] & 0x7}`,
    );
  }
  if (frame[1] !== expectedSubCommand) {
    throw new Error(
      `expected SYS sub-command ${expectedSubCommand}, got ${frame[1]}`,
    );
  }
  return new DataView(frame.buffer, frame.byteOffset, frame.byteLength).getInt32(
    2,
    /* littleEndian */ true,
  );
}

// Build a binary SYS_COMMAND_RELAY packet the way a Godot client would.
// `targetPeer` is 0 for broadcast, positive for unicast, negative for
// all-except-N. The inner payload is opaque to the relay — tests pass an
// arbitrary byte sequence to assert verbatim forwarding.
export function buildRelayPacket(
  targetPeer: number,
  innerPayload: Uint8Array,
  channel = 0,
): Uint8Array {
  const out = new Uint8Array(6 + innerPayload.byteLength);
  out[0] = NETWORK_COMMAND_SYS | (channel << 5);
  out[1] = SYS_COMMAND_RELAY;
  new DataView(out.buffer).setInt32(2, targetPeer, /* littleEndian */ true);
  out.set(innerPayload, 6);
  return out;
}

// Parse the relay packet the server sends. Returns the sender_id rewritten
// into the header and the inner payload bytes.
export function parseRelayPacket(frame: Uint8Array): {
  senderId: number;
  innerPayload: Uint8Array;
} {
  if (frame.byteLength < 7) {
    throw new Error(`relay packet too small: ${frame.byteLength}`);
  }
  if ((frame[0] & 0x7) !== NETWORK_COMMAND_SYS || frame[1] !== SYS_COMMAND_RELAY) {
    throw new Error(
      `expected SYS|RELAY header, got cmd=${frame[0] & 0x7} sub=${frame[1]}`,
    );
  }
  const senderId = new DataView(
    frame.buffer,
    frame.byteOffset,
    frame.byteLength,
  ).getInt32(2, /* littleEndian */ true);
  return { senderId, innerPayload: frame.subarray(6) };
}

export { SYS_COMMAND_ADD_PEER, SYS_COMMAND_DEL_PEER };

// Shared test helpers — JWT minting and WebSocket plumbing.
//
// The Client connection pattern is intentionally specific: we attach the
// message listener BEFORE calling ws.accept(). If we don't, the runtime can
// deliver buffered server frames (like the assigned-peer-id handshake) before
// the listener is registered and the test will block forever waiting for them.

import { SELF, env } from "cloudflare:test";
import { SignJWT } from "jose";

import {
  NETWORK_COMMAND_SYS,
  SYS_COMMAND_ADD_PEER,
  SYS_COMMAND_DEL_PEER,
  SYS_COMMAND_RELAY,
} from "../src/proto/v1";

export interface ClaimOverrides {
  sub?: string;
  rid?: string;
  tier?: string;
  ttlSeconds?: number;
}

export async function mintJwt(overrides: ClaimOverrides = {}): Promise<string> {
  const secret = new TextEncoder().encode(env.MULTIPLAYER_JWT_SECRET);
  const now = Math.floor(Date.now() / 1000);
  const ttl = overrides.ttlSeconds ?? 15 * 60;
  return await new SignJWT({
    sub: overrides.sub ?? "user-test",
    rid: overrides.rid ?? "room-test",
    tier: overrides.tier ?? "basic",
  })
    .setProtectedHeader({ alg: "HS256" })
    .setIssuedAt(now)
    .setExpirationTime(now + ttl)
    .sign(secret);
}

export interface ConnectOptions {
  rid?: string;
  sub?: string;
  v?: number | string;
  token?: string;
  ttlSeconds?: number;
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
  const sub = opts.sub ?? "user-test";
  const v = opts.v ?? 1;
  const token =
    opts.token ?? (await mintJwt({ rid, sub, ttlSeconds: opts.ttlSeconds }));

  const params = new URLSearchParams();
  if (token !== "") params.set("token", token);
  params.set("v", String(v));

  const res = await SELF.fetch(`http://relay.test/?${params.toString()}`, {
    headers: { Upgrade: "websocket" },
  });

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

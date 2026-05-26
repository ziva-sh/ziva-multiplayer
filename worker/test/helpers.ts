// Shared test helpers — JWT minting and WebSocket plumbing.
//
// The Client connection pattern is intentionally specific: we attach the
// message listener BEFORE calling ws.accept(). If we don't, the runtime can
// deliver buffered server frames (like the welcome envelope) before the
// listener is registered and the test will block forever waiting for them.

import { SELF, env } from "cloudflare:test";
import { SignJWT } from "jose";

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
  next(timeoutMs?: number): Promise<string>;
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
  const buf: string[] = [];
  const waiters: ((m: string) => void)[] = [];
  ws.addEventListener("message", (ev) => {
    const m = typeof ev.data === "string" ? ev.data : "<binary>";
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
      return new Promise<string>((resolve, reject) => {
        if (buf.length > 0) {
          resolve(buf.shift()!);
          return;
        }
        const timer = setTimeout(() => {
          const idx = waiters.indexOf(handler);
          if (idx >= 0) waiters.splice(idx, 1);
          reject(new Error(`next() timeout after ${timeoutMs}ms`));
        }, timeoutMs);
        const handler = (m: string) => {
          clearTimeout(timer);
          resolve(m);
        };
        waiters.push(handler);
      });
    },
  };
}

export interface WelcomeEnvelope {
  type: "welcome";
  peer_id: number;
  protocol_version: 1;
}

// Drains the welcome envelope off a freshly-connected socket. Every test
// that cares about subsequent traffic must drain the welcome first.
export async function readWelcome(c: Connection): Promise<WelcomeEnvelope> {
  const raw = await c.next(500);
  const env = JSON.parse(raw) as WelcomeEnvelope;
  if (env.type !== "welcome") {
    throw new Error(`expected welcome, got: ${raw}`);
  }
  return env;
}

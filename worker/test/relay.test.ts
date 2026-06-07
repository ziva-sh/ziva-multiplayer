// End-to-end tests for the relay against the real Workers runtime.
//
// Two concerns are exercised here:
//   1. The token-less access gate in src/index.ts — a lazy, stale-while-
//      revalidate /check against apps/web (stubbed via Miniflare's fetch mock)
//      plus the ConnRateDO connection-rate limiter.
//   2. The Godot WebSocketMultiplayerPeer binary relay in src/room-do.ts —
//      4-byte LE peer_id handshake, ADD_PEER / DEL_PEER, RELAY sender rewrite.

import { SELF, env, fetchMock, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { CONN_RATE_LIMIT_PER_UG } from "../src/conn-rate-do";
import type { RoomDO } from "../src/room-do";
import {
  SYS_COMMAND_ADD_PEER,
  SYS_COMMAND_DEL_PEER,
  UpgradeFailure,
  buildRelayPacket,
  connect,
  parseRelayPacket,
  parseSysPeerPacket,
  readPeerId,
  stubCheck,
  stubCheckError,
} from "./helpers";

void UpgradeFailure;

const uid = (p: string) => `${p}-${crypto.randomUUID()}`;

beforeEach(() => {
  // fetchMock is deactivated by default; turn it on and make any un-stubbed
  // outbound fetch throw loudly instead of hitting the network.
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  vi.useRealTimers();
});

// ---- access gate: lazy stale-while-revalidate /check ----

describe("access gate", () => {
  it("(a) empty cache + check→allowed connects on the first attempt", async () => {
    const u = uid("user-firsttry");
    // connect() auto-stubs allowed:true for `u`. The very first attempt must
    // succeed — no first-try failure, no retry needed.
    const a = await connect({ rid: uid("room"), u });
    expect(await readPeerId(a)).toBe(2);
    a.ws.close();
  });

  it("(b) fresh cached u connects with ZERO check calls", async () => {
    const u = uid("user-cached");
    // First connect warms the cache (consumes one allowed stub).
    const first = await connect({ rid: uid("room"), u });
    await readPeerId(first);
    first.ws.close();

    // Second connect within the 60s TTL must not fetch at all. We register
    // NO stub (stub:false); if the Worker fetched, disableNetConnect would
    // make it throw and the upgrade would fail.
    const before = fetchMock.pendingInterceptors().length;
    const second = await connect({ rid: uid("room"), u, stub: false });
    expect(await readPeerId(second)).toBe(2);
    // No interceptors were added or consumed.
    expect(fetchMock.pendingInterceptors().length).toBe(before);
    second.ws.close();
  });

  it("(c) expired cached u connects stale AND background-refreshes; once refresh denies, next connect is 1008 not_allowed", async () => {
    const u = uid("user-swr");
    // Warm the cache.
    const first = await connect({ rid: uid("room"), u });
    await readPeerId(first);
    first.ws.close();

    // Jump past the 60s positive TTL so the entry is stale (worker shares the
    // test isolate, so faked system time moves its Date.now()).
    vi.useFakeTimers();
    vi.setSystemTime(Date.now() + 120_000);

    // The stale connect serves last-good immediately AND fires a background
    // /check. Script that refresh to return not-allowed.
    stubCheck(u, false);
    const stale = await connect({ rid: uid("room"), u, stub: false });
    expect(await readPeerId(stale)).toBe(2); // served stale → connected
    stale.ws.close();

    // Let the waitUntil refresh run and drop the now-revoked cache entry.
    await vi.waitFor(
      () => expect(fetchMock.pendingInterceptors().length).toBe(0),
      { timeout: 1000 },
    );
    vi.useRealTimers();

    // Next attempt is a true miss (entry was dropped). Stub the blocking
    // check to deny → 1008 not_allowed.
    stubCheck(u, false);
    const denied = await connect({ rid: uid("room"), u, stub: false });
    const { code, reason } = await denied.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("not_allowed");
  });

  it("(d) true miss + check→not allowed is 1008 not_allowed; with no negative cache a later check→allowed connects", async () => {
    const u = uid("user-flip");

    // First: legacy deny without a reason → closed 1008 with the generic
    // reason. This keeps older apps/web responses compatible.
    stubCheck(u, false);
    const denied = await connect({ rid: uid("room"), u, stub: false });
    {
      const { code, reason } = await denied.closed;
      expect(code).toBe(1008);
      expect(reason).toBe("not_allowed");
    }

    // No negative cache: the next true-miss check is consulted fresh. Allow
    // it → connects immediately.
    stubCheck(u, true);
    const ok = await connect({ rid: uid("room"), u, stub: false });
    expect(await readPeerId(ok)).toBe(2);
    ok.ws.close();
  });

  it("(e) true miss + structured check denial preserves the exact close reason", async () => {
    const cases = [
      "unknown_user",
      "tier_not_allowed",
      "multiplayer_disabled",
      "usage_throttled",
    ];

    for (const accessReason of cases) {
      const u = uid(`user-${accessReason}`);
      stubCheck(u, false, 1, accessReason);
      const denied = await connect({ rid: uid("room"), u, stub: false });
      const { code, reason } = await denied.closed;
      expect(code).toBe(1008);
      expect(reason).toBe(accessReason);
    }
  });

  it("(f) check throws with an existing positive entry → still allowed + console.error; true miss + throw → 1008 check_unavailable", async () => {
    // --- existing positive entry, refresh throws → served last-good ---
    const warm = uid("user-lastgood");
    const first = await connect({ rid: uid("room"), u: warm });
    await readPeerId(first);
    first.ws.close();

    vi.useFakeTimers();
    vi.setSystemTime(Date.now() + 120_000); // make the entry stale

    const errors: string[] = [];
    const origError = console.error;
    console.error = (...args: unknown[]) => {
      errors.push(args.map(String).join(" "));
    };
    try {
      stubCheckError(warm); // the background refresh transport-fails
      const served = await connect({ rid: uid("room"), u: warm, stub: false });
      expect(await readPeerId(served)).toBe(2); // last-good keeps them online
      served.ws.close();

      await vi.waitFor(
        () => expect(fetchMock.pendingInterceptors().length).toBe(0),
        { timeout: 1000 },
      );
      expect(errors.some((l) => l.includes(warm))).toBe(true);
    } finally {
      console.error = origError;
      vi.useRealTimers();
    }

    // --- true miss, blocking check throws → fail CLOSED ---
    const cold = uid("user-coldfail");
    stubCheckError(cold);
    const failed = await connect({ rid: uid("room"), u: cold, stub: false });
    const { code, reason } = await failed.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("check_unavailable");
  });
});

// ---- diagnostic endpoint ----

describe("diagnose", () => {
  it("returns user-facing JSON for a structured access denial", async () => {
    const u = uid("user-disabled");
    stubCheck(u, false, 1, "multiplayer_disabled");

    const res = await SELF.fetch(
      `http://relay.test/diagnose?u=${encodeURIComponent(u)}&g=game-test&v=1`,
      { headers: { "CF-Connecting-IP": `ip-${crypto.randomUUID()}` } },
    );
    const body = (await res.json()) as { ok: boolean; reason: string; message: string };

    expect(res.status).toBe(200);
    expect(res.headers.get("Cache-Control")).toBe("no-store");
    expect(body.ok).toBe(false);
    expect(body.reason).toBe("multiplayer_disabled");
    expect(body.message).toContain("enable multiplayer");
  });

  it("returns ok:true when access is allowed", async () => {
    const u = uid("user-diagnose-ok");
    stubCheck(u, true);

    const res = await SELF.fetch(
      `http://relay.test/diagnose?u=${encodeURIComponent(u)}&g=game-test&v=1`,
      { headers: { "CF-Connecting-IP": `ip-${crypto.randomUUID()}` } },
    );
    const body = (await res.json()) as { ok: boolean; reason: string; message: string };

    expect(res.status).toBe(200);
    expect(body.ok).toBe(true);
    expect(body.reason).toBe("ok");
    expect(body.message).toContain("enabled");
  });

  it("returns missing_params without calling apps/web", async () => {
    const res = await SELF.fetch("http://relay.test/diagnose?u=user-only", {
      headers: { "CF-Connecting-IP": `ip-${crypto.randomUUID()}` },
    });
    const body = (await res.json()) as { ok: boolean; reason: string; message: string };

    expect(res.status).toBe(400);
    expect(body.ok).toBe(false);
    expect(body.reason).toBe("missing_params");
    expect(body.message).toContain("missing Ziva multiplayer settings");
  });

  it("returns check_unavailable when apps/web cannot be reached", async () => {
    const u = uid("user-diagnose-fail");
    stubCheckError(u);

    const errors: string[] = [];
    const origError = console.error;
    console.error = (...args: unknown[]) => {
      errors.push(args.map(String).join(" "));
    };
    try {
      const res = await SELF.fetch(
        `http://relay.test/diagnose?u=${encodeURIComponent(u)}&g=game-test&v=1`,
        { headers: { "CF-Connecting-IP": `ip-${crypto.randomUUID()}` } },
      );
      const body = (await res.json()) as { ok: boolean; reason: string; message: string };

      expect(res.status).toBe(503);
      expect(body.ok).toBe(false);
      expect(body.reason).toBe("check_unavailable");
      expect(body.message).toContain("could not verify");
      expect(errors.some((l) => l.includes(u))).toBe(true);
    } finally {
      console.error = origError;
    }
  });
});

// ---- ConnRateDO connection-rate limiter ----

describe("conn rate limit", () => {
  it("allows N connects from one IP/(u,g) within the window then 1008 rate_limited", async () => {
    const u = uid("user-rate");
    const g = "game-rate";
    const ip = `5.5.5.${Math.floor(Math.random() * 200)}`;
    const rid = uid("room-rate");

    // The per-(u,g) cap is the tighter of the two windows when ip and (u,g)
    // are both pinned. Drive exactly up to the cap — all succeed.
    stubCheck(u, true, CONN_RATE_LIMIT_PER_UG);
    const sockets: WebSocket[] = [];
    for (let i = 0; i < CONN_RATE_LIMIT_PER_UG; i++) {
      const c = await connect({ rid, u, g, ip, stub: false });
      await readPeerId(c);
      sockets.push(c.ws);
    }

    // The (cap+1)th is rejected BEFORE any /check (no stub registered; if it
    // fetched, disableNetConnect would throw a different error).
    const over = await connect({ rid, u, g, ip, stub: false });
    const { code, reason } = await over.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("rate_limited");

    for (const ws of sockets) ws.close();
  });
});

// ---- carried-over relay protocol tests ----
//
// Each connect() uses a unique user id so every attempt is a true-miss that
// consumes exactly its auto-registered allowed stub — no cross-test cache
// bleed, no leftover interceptors.

describe("relay", () => {
  it("assigns peer_id 2 to the first joiner via the 4-byte handshake", async () => {
    const rid = uid("room-handshake");
    const a = await connect({ rid, u: uid("user-a") });
    expect(await readPeerId(a)).toBe(2);
    a.ws.close();
  });

  it("relays a unicast SYS_COMMAND_RELAY from A to B with sender_id rewritten", async () => {
    const rid = uid("room-relay");
    const a = await connect({ rid, u: uid("user-a") });
    const b = await connect({ rid, u: uid("user-b") });

    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    expect(idA).toBe(2);
    expect(idB).toBe(3);

    const aJoinNotice = await a.next(500);
    expect(parseSysPeerPacket(aJoinNotice, SYS_COMMAND_ADD_PEER)).toBe(idB);
    const bExistingPeer = await b.next(500);
    expect(parseSysPeerPacket(bExistingPeer, SYS_COMMAND_ADD_PEER)).toBe(idA);

    const inner = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const recvB = b.next(500);
    const start = Date.now();
    a.ws.send(buildRelayPacket(idB, inner));
    const raw = await recvB;
    expect(Date.now() - start).toBeLessThan(100);

    const parsed = parseRelayPacket(raw);
    expect(parsed.senderId).toBe(idA);
    expect(Array.from(parsed.innerPayload)).toEqual(Array.from(inner));

    a.ws.close();
    b.ws.close();
  });

  it("broadcasts SYS_COMMAND_RELAY target=0 to all peers except sender", async () => {
    const rid = uid("room-broadcast");
    const a = await connect({ rid, u: uid("user-a") });
    const b = await connect({ rid, u: uid("user-b") });
    const c = await connect({ rid, u: uid("user-c") });

    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    const idC = await readPeerId(c);

    await a.next(500);
    await a.next(500);
    await b.next(500);
    await b.next(500);
    await c.next(500);
    await c.next(500);

    const inner = new Uint8Array([0x01, 0x02, 0x03]);
    const recvB = b.next(500);
    const recvC = c.next(500);
    a.ws.send(buildRelayPacket(0, inner));

    const [bFrame, cFrame] = await Promise.all([recvB, recvC]);
    const bp = parseRelayPacket(bFrame);
    const cp = parseRelayPacket(cFrame);
    expect(bp.senderId).toBe(idA);
    expect(cp.senderId).toBe(idA);
    expect(Array.from(bp.innerPayload)).toEqual([0x01, 0x02, 0x03]);
    expect(Array.from(cp.innerPayload)).toEqual([0x01, 0x02, 0x03]);

    a.ws.close();
    b.ws.close();
    c.ws.close();
  });

  it("rejects missing required params (u/g) with close code 1008", async () => {
    // No u/g in the query → the gate closes before any room routing.
    const res = await SELF.fetch(`http://relay.test/r/${uid("room")}?v=1`, {
      headers: { Upgrade: "websocket" },
    });
    expect(res.status).toBe(101);
    const ws = res.webSocket!;
    const closed = new Promise<{ code: number; reason: string }>((r) => {
      ws.addEventListener("close", (ev) => r({ code: ev.code, reason: ev.reason }));
    });
    ws.accept();
    const { code, reason } = await closed;
    expect(code).toBe(1008);
    expect(reason).toBe("missing_params");
  });

  it("rejects unknown protocol version with close code 1008", async () => {
    const c = await connect({ v: 999, u: uid("user-v"), stub: false });
    const { code, reason } = await c.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("unsupported_protocol_version");
  });

  it("rejects 33rd client into a 32-cap room with close code 1008", async () => {
    const rid = uid("room-cap");

    const sockets: WebSocket[] = [];
    for (let i = 0; i < 32; i++) {
      const c = await connect({ rid, u: uid(`user-${i}`) });
      await readPeerId(c);
      sockets.push(c.ws);
    }

    const overflow = await connect({ rid, u: uid("user-overflow") });
    const { code, reason } = await overflow.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("room_full");

    for (const ws of sockets) ws.close();
  });

  it("sends SYS_COMMAND_ADD_PEER to existing peers when a new peer connects", async () => {
    const rid = uid("room-pjoin");
    const a = await connect({ rid, u: uid("pjoin-a") });
    const idA = await readPeerId(a);
    expect(idA).toBe(2);

    const aNext = a.next(500);
    const b = await connect({ rid, u: uid("pjoin-b") });
    const idB = await readPeerId(b);
    expect(idB).toBe(3);

    const aFrame = await aNext;
    expect(parseSysPeerPacket(aFrame, SYS_COMMAND_ADD_PEER)).toBe(idB);

    const bExisting = await b.next(500);
    expect(parseSysPeerPacket(bExisting, SYS_COMMAND_ADD_PEER)).toBe(idA);

    a.ws.close();
    b.ws.close();
  });

  it("broadcasts SYS_COMMAND_DEL_PEER to remaining peers when one disconnects", async () => {
    const rid = uid("room-pleave");
    const a = await connect({ rid, u: uid("pleave-a") });
    const b = await connect({ rid, u: uid("pleave-b") });
    const idA = await readPeerId(a);
    const idB = await readPeerId(b);

    expect(parseSysPeerPacket(await a.next(500), SYS_COMMAND_ADD_PEER)).toBe(idB);
    expect(parseSysPeerPacket(await b.next(500), SYS_COMMAND_ADD_PEER)).toBe(idA);

    const aNext = a.next(500);
    b.ws.close(1000, "bye");
    await b.closed;
    const aFrame = await aNext;
    expect(parseSysPeerPacket(aFrame, SYS_COMMAND_DEL_PEER)).toBe(idB);

    a.ws.close();
  });

  it("assigns peer ids 2, 3, 4 in order (id=1 is the implicit server)", async () => {
    const rid = uid("room-ids");
    const c1 = await connect({ rid, u: uid("u1") });
    expect(await readPeerId(c1)).toBe(2);
    const c2 = await connect({ rid, u: uid("u2") });
    expect(await readPeerId(c2)).toBe(3);
    const c3 = await connect({ rid, u: uid("u3") });
    expect(await readPeerId(c3)).toBe(4);
    c1.ws.close();
    c2.ws.close();
    c3.ws.close();
  });

  it("closes a peer that exceeds the per-second message cap with 1008 within 1s", async () => {
    const rid = uid("room-rate-msg");
    const a = await connect({ rid, u: uid("rate-a") });
    await readPeerId(a);

    const start = Date.now();
    const payload = buildRelayPacket(0, new Uint8Array([0x00]));
    for (let i = 0; i < 1100; i++) {
      try {
        a.ws.send(payload);
      } catch {
        break;
      }
    }

    const { code, reason } = await a.closed;
    const elapsed = Date.now() - start;
    expect(code).toBe(1008);
    expect(reason).toBe("rate_limit_exceeded");
    expect(elapsed).toBeLessThan(1000);
  });

  it("relays correctly after a hibernation-style idle period", async () => {
    const rid = uid("room-hib");
    const a = await connect({ rid, u: uid("hib-a") });
    const b = await connect({ rid, u: uid("hib-b") });
    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    await a.next(500);
    await b.next(500);

    await new Promise((r) => setTimeout(r, 1000));

    const stub = env.ROOM.getByName(rid);
    await runInDurableObject(stub, async (_: RoomDO, state) => {
      expect(state.getWebSockets().length).toBe(2);
    });

    const inner = new Uint8Array([0xaa, 0xbb, 0xcc]);
    const recvB = b.next(500);
    a.ws.send(buildRelayPacket(idB, inner));
    const raw = await recvB;
    const parsed = parseRelayPacket(raw);
    expect(parsed.senderId).toBe(idA);
    expect(Array.from(parsed.innerPayload)).toEqual([0xaa, 0xbb, 0xcc]);

    a.ws.close();
    b.ws.close();
  });

  it("logs room_closed with game_id and peer count returns to 0 on full disconnect", async () => {
    const rid = uid("room-cleanup");
    const g = "game-cleanup";
    const a = await connect({ rid, u: uid("cleanup-a"), g });
    const b = await connect({ rid, u: uid("cleanup-b"), g });
    await readPeerId(a);
    await readPeerId(b);

    const logs: string[] = [];
    const origLog = console.log;
    console.log = (...args: unknown[]) => {
      logs.push(args.map(String).join(" "));
      origLog(...args);
    };

    try {
      a.ws.close(1000, "bye");
      b.ws.close(1000, "bye");
      await a.closed;
      await b.closed;

      await new Promise((r) => setTimeout(r, 100));

      const stub = env.ROOM.getByName(rid);
      await runInDurableObject(stub, async (_inst, state) => {
        expect(state.getWebSockets().length).toBe(0);
      });

      const sawRoomClosed = logs.some((line) =>
        line.includes('"event":"room_closed"'),
      );
      expect(sawRoomClosed).toBe(true);
      // The emit carries game_id now — the close-time peer_left event for this
      // game must include it.
      const sawGameId = logs.some(
        (line) =>
          line.includes('"event":"peer_left"') && line.includes(`"gameId":"${g}"`),
      );
      expect(sawGameId).toBe(true);
    } finally {
      console.log = origLog;
    }
  });
});

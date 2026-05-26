// End-to-end tests for the relay against the real Workers runtime.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import type { RoomDO } from "../src/room-do";
import { UpgradeFailure, connect, readWelcome } from "./helpers";

describe("relay", () => {
  it("broadcasts A -> B within 100 ms with valid JWTs", async () => {
    const rid = `room-broadcast-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "user-a" });
    const b = await connect({ rid, sub: "user-b" });

    expect((await readWelcome(a)).peer_id).toBe(1);
    expect((await readWelcome(b)).peer_id).toBe(2);

    const bData = b.next(500);
    const start = Date.now();
    a.ws.send(JSON.stringify({ type: "data", payload: { hello: "world" } }));
    const raw = await bData;
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(100);

    const envObj = JSON.parse(raw) as Record<string, unknown>;
    expect(envObj.type).toBe("data");
    expect(envObj.from).toBe(1);
    expect(envObj.payload).toEqual({ hello: "world" });

    a.ws.close();
    b.ws.close();
  });

  it("rejects invalid JWT with HTTP 401", async () => {
    await expect(connect({ token: "garbage.not.a.jwt" })).rejects.toMatchObject({
      status: 401,
    });

    // Wrong signature → also 401.
    const goodHeader = btoa(JSON.stringify({ alg: "HS256", typ: "JWT" }))
      .replaceAll("=", "")
      .replaceAll("+", "-")
      .replaceAll("/", "_");
    const payload = btoa(
      JSON.stringify({
        sub: "user-test",
        rid: "room-test",
        tier: "basic",
        iat: Math.floor(Date.now() / 1000),
        exp: Math.floor(Date.now() / 1000) + 60,
      }),
    )
      .replaceAll("=", "")
      .replaceAll("+", "-")
      .replaceAll("/", "_");
    const fakeSig = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const forgedToken = `${goodHeader}.${payload}.${fakeSig}`;
    await expect(connect({ token: forgedToken })).rejects.toMatchObject({
      status: 401,
    });
  });

  it("rejects unknown protocol version with close code 1008", async () => {
    const c = await connect({ v: 999 });
    const { code, reason } = await c.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("unsupported_protocol_version");
  });

  it("rejects 33rd client into a 32-cap room with close code 1008", async () => {
    const rid = `room-cap-${crypto.randomUUID()}`;

    const sockets: WebSocket[] = [];
    for (let i = 0; i < 32; i++) {
      const c = await connect({ rid, sub: `user-${i}` });
      await readWelcome(c);
      sockets.push(c.ws);
    }

    const overflow = await connect({ rid, sub: "user-overflow" });
    const { code, reason } = await overflow.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("room_full");

    for (const ws of sockets) ws.close();
  });

  it("assigns peer ids 1, 2, 3 in order", async () => {
    const rid = `room-ids-${crypto.randomUUID()}`;
    const c1 = await connect({ rid, sub: "u1" });
    expect((await readWelcome(c1)).peer_id).toBe(1);
    const c2 = await connect({ rid, sub: "u2" });
    expect((await readWelcome(c2)).peer_id).toBe(2);
    const c3 = await connect({ rid, sub: "u3" });
    expect((await readWelcome(c3)).peer_id).toBe(3);
    c1.ws.close();
    c2.ws.close();
    c3.ws.close();
  });

  it("closes a peer that sends >60 msg/s with 1008 within 1s", async () => {
    const rid = `room-rate-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "rate-a" });
    await readWelcome(a);

    const start = Date.now();
    const payload = JSON.stringify({ type: "data", payload: "x" });
    for (let i = 0; i < 70; i++) {
      try {
        a.ws.send(payload);
      } catch {
        // Socket closed mid-burst — that's the rate-limit hit.
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
    // The test runner's miniflare doesn't sleep DOs for real, so we exercise
    // the equivalent code path by exercising the hibernation API across a
    // 1s idle: long enough that production would have hibernated, short
    // enough that tests stay fast.
    const rid = `room-hib-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "hib-a" });
    const b = await connect({ rid, sub: "hib-b" });
    await readWelcome(a);
    await readWelcome(b);

    await new Promise((r) => setTimeout(r, 1000));

    // Verify the DO still has both attached sockets after the idle.
    const stub = env.ROOM.getByName(rid);
    await runInDurableObject(stub, async (_: RoomDO, state) => {
      expect(state.getWebSockets().length).toBe(2);
    });

    const bData = b.next(500);
    a.ws.send(JSON.stringify({ type: "data", payload: "post-idle" }));
    const raw = await bData;
    const envObj = JSON.parse(raw) as Record<string, unknown>;
    expect(envObj.type).toBe("data");
    expect(envObj.from).toBe(1);
    expect(envObj.payload).toBe("post-idle");

    a.ws.close();
    b.ws.close();
  });

  it("logs room_closed and peer count returns to 0 on full disconnect", async () => {
    const rid = `room-cleanup-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "cleanup-a" });
    const b = await connect({ rid, sub: "cleanup-b" });
    await readWelcome(a);
    await readWelcome(b);

    // Capture console.log emissions so we can assert "room_closed" fired.
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

      // Give the DO a tick to process webSocketClose.
      await new Promise((r) => setTimeout(r, 100));

      const stub = env.ROOM.getByName(rid);
      await runInDurableObject(stub, async (_inst, state) => {
        expect(state.getWebSockets().length).toBe(0);
      });

      const sawRoomClosed = logs.some((line) =>
        line.includes('"event":"room_closed"'),
      );
      expect(sawRoomClosed).toBe(true);
    } finally {
      console.log = origLog;
    }
  });
});

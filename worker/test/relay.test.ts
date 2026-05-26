// End-to-end tests for the relay against the real Workers runtime.
//
// All assertions speak Godot's WebSocketMultiplayerPeer binary protocol —
// 4-byte LE peer_id handshake, SYS_COMMAND_ADD_PEER / DEL_PEER announcements,
// SYS_COMMAND_RELAY with sender_id rewriting. See src/proto/v1.ts.

import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";

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
} from "./helpers";

void UpgradeFailure;

describe("relay", () => {
  it("assigns peer_id 2 to the first joiner via the 4-byte handshake", async () => {
    const rid = `room-handshake-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "user-a" });

    const idA = await readPeerId(a);
    // Server is the implicit id=1; first real client gets id=2.
    expect(idA).toBe(2);

    a.ws.close();
  });

  it("relays a unicast SYS_COMMAND_RELAY from A to B with sender_id rewritten", async () => {
    const rid = `room-relay-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "user-a" });
    const b = await connect({ rid, sub: "user-b" });

    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    expect(idA).toBe(2);
    expect(idB).toBe(3);

    // Drain the ADD_PEER chatter from join order: A gets ADD_PEER(B); B gets
    // ADD_PEER(A) at join time.
    const aJoinNotice = await a.next(500);
    expect(parseSysPeerPacket(aJoinNotice, SYS_COMMAND_ADD_PEER)).toBe(idB);
    const bExistingPeer = await b.next(500);
    expect(parseSysPeerPacket(bExistingPeer, SYS_COMMAND_ADD_PEER)).toBe(idA);

    // A sends a RELAY targeting B with an opaque inner payload.
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
    const rid = `room-broadcast-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "user-a" });
    const b = await connect({ rid, sub: "user-b" });
    const c = await connect({ rid, sub: "user-c" });

    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    const idC = await readPeerId(c);

    // Drain join chatter. A: ADD_PEER(B), ADD_PEER(C). B: ADD_PEER(A) (its
    // existing-peer notification from join), ADD_PEER(C) (C joining after).
    // C: ADD_PEER(A), ADD_PEER(B) at join time.
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
      await readPeerId(c);
      sockets.push(c.ws);
    }

    const overflow = await connect({ rid, sub: "user-overflow" });
    const { code, reason } = await overflow.closed;
    expect(code).toBe(1008);
    expect(reason).toBe("room_full");

    for (const ws of sockets) ws.close();
  });

  it("sends SYS_COMMAND_ADD_PEER to existing peers when a new peer connects", async () => {
    const rid = `room-pjoin-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "pjoin-a" });
    const idA = await readPeerId(a);
    expect(idA).toBe(2);

    // While a is alone there is no traffic. Once b connects, a should see
    // an ADD_PEER frame announcing b's id.
    const aNext = a.next(500);
    const b = await connect({ rid, sub: "pjoin-b" });
    const idB = await readPeerId(b);
    expect(idB).toBe(3);

    const aFrame = await aNext;
    expect(parseSysPeerPacket(aFrame, SYS_COMMAND_ADD_PEER)).toBe(idB);

    // B should also have received an ADD_PEER announcing A as the existing peer.
    const bExisting = await b.next(500);
    expect(parseSysPeerPacket(bExisting, SYS_COMMAND_ADD_PEER)).toBe(idA);

    a.ws.close();
    b.ws.close();
  });

  it("broadcasts SYS_COMMAND_DEL_PEER to remaining peers when one disconnects", async () => {
    const rid = `room-pleave-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "pleave-a" });
    const b = await connect({ rid, sub: "pleave-b" });
    const idA = await readPeerId(a);
    const idB = await readPeerId(b);

    // Drain a's ADD_PEER(b) and b's ADD_PEER(a).
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
    const rid = `room-ids-${crypto.randomUUID()}`;
    const c1 = await connect({ rid, sub: "u1" });
    expect(await readPeerId(c1)).toBe(2);
    const c2 = await connect({ rid, sub: "u2" });
    expect(await readPeerId(c2)).toBe(3);
    const c3 = await connect({ rid, sub: "u3" });
    expect(await readPeerId(c3)).toBe(4);
    c1.ws.close();
    c2.ws.close();
    c3.ws.close();
  });

  it("closes a peer that sends >60 msg/s with 1008 within 1s", async () => {
    const rid = `room-rate-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "rate-a" });
    await readPeerId(a);

    const start = Date.now();
    // Send a valid RELAY frame at high rate so we exercise the rate limit
    // path, not the unknown-packet drop path.
    const payload = buildRelayPacket(0, new Uint8Array([0x00]));
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
    const idA = await readPeerId(a);
    const idB = await readPeerId(b);
    // Drain join chatter.
    await a.next(500);
    await b.next(500);

    await new Promise((r) => setTimeout(r, 1000));

    // Verify the DO still has both attached sockets after the idle.
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

  it("logs room_closed and peer count returns to 0 on full disconnect", async () => {
    const rid = `room-cleanup-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: "cleanup-a" });
    const b = await connect({ rid, sub: "cleanup-b" });
    await readPeerId(a);
    await readPeerId(b);

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

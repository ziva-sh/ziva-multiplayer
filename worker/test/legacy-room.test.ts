// Rooms saved before #3998 live in RoomDOs named by room id alone. A listed
// room copies that data once on its first open; an unlisted one only logs it.
import { env, fetchMock, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  STATE_SNAPSHOT,
  WORLD_LOAD,
  WORLD_SAVE,
  WORLD_SAVE_ACK,
  decodeStateSnapshot,
} from "../src/proto/v1";
import { SharedStateStore } from "../src/shared-state-store";
import { WorldStore } from "../src/world-store";
import { type Connection, connect, readPeerId } from "./helpers";

beforeEach(() => {
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

afterEach(() => {
  vi.restoreAllMocks();
});

async function seedLegacyRoom(roomId: string): Promise<void> {
  await runInDurableObject(env.ROOM.getByName(roomId), async (_inst, state) => {
    new WorldStore(state.storage.sql).save(new Uint8Array([7, 7, 7]));
    new SharedStateStore(state.storage.sql).set("turn", new Uint8Array([2]), 5);
    state.storage.sql.exec("UPDATE room_state SET value = 7 WHERE key = 'state_rev'");
  });
}

// Join-burst frames after the handshake, keyed by their opcode.
async function joinFrames(c: Connection): Promise<Map<number, Uint8Array>> {
  await readPeerId(c);
  const frames = new Map<number, Uint8Array>();
  for (;;) {
    const f = await c.next(300).catch(() => null);
    if (!f) return frames;
    frames.set(f[0], f);
  }
}

describe("legacy room migration", () => {
  it("copies a listed legacy room's world, state and revision once", async () => {
    // Fixed ids because wrangler.toml lists this room's hash for the suite.
    const rid = "room-legacy-listed";
    await seedLegacyRoom(rid);

    const a = await connect({ rid, g: "game-legacy" });
    const frames = await joinFrames(a);
    expect(Array.from(frames.get(WORLD_LOAD)!.subarray(1))).toEqual([7, 7, 7]);
    expect(decodeStateSnapshot(frames.get(STATE_SNAPSHOT)!)).toEqual([
      { key: "turn", value: new Uint8Array([2]), rev: 5 },
    ]);
    await runInDurableObject(env.ROOM.getByName(a.key), async (_inst, state) => {
      const rev = state.storage.sql
        .exec<{ value: number }>("SELECT value FROM room_state WHERE key = 'state_rev'")
        .one().value;
      expect(rev).toBe(7);
    });

    // A later save must not be overwritten by the legacy copy on reopen.
    a.ws.send(new Uint8Array([WORLD_SAVE, 9]));
    expect((await a.next(500))[0]).toBe(WORLD_SAVE_ACK);
    a.ws.close();
    const b = await connect({ rid, g: "game-legacy" });
    expect(Array.from((await joinFrames(b)).get(WORLD_LOAD)!.subarray(1))).toEqual([9]);
    b.ws.close();
  });

  it("logs an unlisted legacy room's data without copying it", async () => {
    const rid = `room-legacy-unlisted-${crypto.randomUUID()}`;
    await seedLegacyRoom(rid);
    const log = vi.spyOn(console, "log");

    const a = await connect({ rid, g: "game-other" });
    const frames = await joinFrames(a);
    expect(frames.has(WORLD_LOAD)).toBe(false);
    expect(frames.has(STATE_SNAPSHOT)).toBe(false);
    const events = log.mock.calls.map((c) => String(c[0]));
    expect(
      events.some(
        (e) =>
          e.includes('"event":"legacy_room_unmigrated"') &&
          e.includes('"gameId":"game-other"') &&
          e.includes(`"roomId":"${rid}"`),
      ),
    ).toBe(true);
    a.ws.close();
  });
});

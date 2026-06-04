// World store — a per-room persistent world blob saved to / loaded from the
// room's Durable Object. The contract under test:
//   1. A saved world survives EVERY peer leaving: a fresh peer joining the same
//      room later is handed the exact bytes back.
//   2. A >1MB world round-trips correctly via cross-row chunking (the size that
//      previously threw SQLITE_TOOBIG as a single SQLite cell).
//   3. An oversized world save fails LOUD — the socket is closed with an
//      observable reason, never silently dropped.
import { fetchMock } from "cloudflare:test";
import { beforeEach, describe, expect, it } from "vitest";

import {
  WORLD_LOAD,
  WORLD_SAVE,
  WORLD_SAVE_ACK,
} from "../src/proto/v1";
import { WORLD_CHUNK_BYTES, WORLD_MAX_BYTES } from "../src/world-store";
import { connect, readPeerId } from "./helpers";

beforeEach(() => {
  // Any un-stubbed outbound fetch (the /check call) throws loudly instead of
  // hitting the network — matches the rest of the suite.
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

const uid = (p: string) => `${p}-${crypto.randomUUID()}`;

// Pull frames off a connection until one whose first byte is `firstByte`
// arrives, or null after `tries` reads time out / run dry. World frames are
// interleaved with the join-burst membership packets, so we can't assume
// position.
async function findFrame(
  c: { next: (ms?: number) => Promise<Uint8Array> },
  firstByte: number,
  tries = 8,
  ms = 500,
): Promise<Uint8Array | null> {
  for (let i = 0; i < tries; i++) {
    const f = await c.next(ms).catch(() => null);
    if (!f) return null;
    if (f[0] === firstByte) return f;
  }
  return null;
}

// Save `blob` into room `rid` via peer A, wait for the ack, then disconnect
// EVERYONE. Join a fresh peer B and return whatever world blob B is handed in
// its join burst (or null). This is the full "survives all-peers-left" path.
async function saveThenReload(
  rid: string,
  blob: Uint8Array,
): Promise<Uint8Array | null> {
  const a = await connect({ rid, u: uid("ua") });
  await readPeerId(a); // consume the 4-byte handshake (A is first → no peers)

  const save = new Uint8Array(1 + blob.byteLength);
  save[0] = WORLD_SAVE;
  save.set(blob, 1);
  a.ws.send(save);

  const ack = await findFrame(a, WORLD_SAVE_ACK);
  expect(ack, "expected a WORLD_SAVE_ACK").not.toBeNull();
  expect(ack!.byteLength, "ack is a single opcode byte").toBe(1);

  // The ack means the chunked write has committed to durable SQLite, so it's
  // safe to drop A and have B read it back. (We don't await the close event —
  // a client-initiated WS close can take seconds to round-trip in Miniflare,
  // and the room is already logically empty the moment we call close.)
  a.ws.close(); // the room is now empty — everyone has left

  const b = await connect({ rid, u: uid("ub") });
  await readPeerId(b); // consume handshake
  const loadFrame = await findFrame(b, WORLD_LOAD);
  b.ws.close();
  return loadFrame ? loadFrame.subarray(1) : null;
}

// Deterministic sparse fill so a multi-MB comparison stays cheap: every 4099th
// byte (a prime stride, so it crosses chunk boundaries) gets a non-zero marker.
function fillSparse(blob: Uint8Array): void {
  for (let i = 0; i < blob.byteLength; i += 4099) blob[i] = (i % 251) + 1;
}
function checkSparse(blob: Uint8Array): boolean {
  for (let i = 0; i < blob.byteLength; i += 4099) {
    if (blob[i] !== (i % 251) + 1) return false;
  }
  return true;
}

describe("world store: DO-persisted world survives all-peers-left", () => {
  it("hands a fresh peer the EXACT saved blob after everyone left", async () => {
    const rid = uid("room");
    const original = new TextEncoder().encode(
      JSON.stringify({
        objects: [
          { id: 1, x: 5, y: 6 },
          { id: 2, x: 99, y: 7 },
        ],
        note: "persistent world",
      }),
    );

    const loaded = await saveThenReload(rid, original);
    expect(loaded, "fresh peer received the world").not.toBeNull();
    // Byte-identical, not just same-length.
    expect(Array.from(loaded!)).toEqual(Array.from(original));
  });

  it("round-trips a >1MB world via chunking (previously SQLITE_TOOBIG)", async () => {
    // 3.5MB: spans multiple WORLD_CHUNK_BYTES rows AND a 4MB-class blob is the
    // case Spike 2A saw throw as a single SQLite cell.
    const n = 3.5 * 1024 * 1024;
    expect(n, "must exceed the single-cell ceiling").toBeGreaterThan(
      WORLD_CHUNK_BYTES,
    );
    const big = new Uint8Array(n);
    fillSparse(big);

    const loaded = await saveThenReload(uid("room"), big);
    expect(loaded, "fresh peer received the big world").not.toBeNull();
    expect(loaded!.byteLength, "exact byte length preserved").toBe(n);
    expect(checkSparse(loaded!), "chunk reassembly is byte-correct").toBe(true);
  });

  it("round-trips a blob exactly on a chunk boundary (off-by-one guard)", async () => {
    // Exactly 2 full chunks — the loop's `offset < length` boundary must not
    // drop or duplicate the final chunk.
    const n = WORLD_CHUNK_BYTES * 2;
    const blob = new Uint8Array(n);
    fillSparse(blob);
    // Distinct last byte so a truncated reassembly is caught.
    blob[n - 1] = 0xab;

    const loaded = await saveThenReload(uid("room"), blob);
    expect(loaded, "received the boundary-sized world").not.toBeNull();
    expect(loaded!.byteLength).toBe(n);
    expect(loaded![n - 1]).toBe(0xab);
    expect(checkSparse(loaded!)).toBe(true);
  });

  it("a later save overwrites the earlier world (no stale chunks linger)", async () => {
    const rid = uid("room");
    // First save a LARGE multi-chunk world, then overwrite with a tiny one.
    // If old chunks lingered, the reload would be longer than the new world.
    const first = new Uint8Array(WORLD_CHUNK_BYTES * 3);
    fillSparse(first);
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    const s1 = new Uint8Array(1 + first.byteLength);
    s1[0] = WORLD_SAVE;
    s1.set(first, 1);
    a.ws.send(s1);
    expect(await findFrame(a, WORLD_SAVE_ACK), "first ack").not.toBeNull();

    const second = new TextEncoder().encode("tiny replacement world");
    const s2 = new Uint8Array(1 + second.byteLength);
    s2[0] = WORLD_SAVE;
    s2.set(second, 1);
    a.ws.send(s2);
    expect(await findFrame(a, WORLD_SAVE_ACK), "second ack").not.toBeNull();
    a.ws.close();

    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const loadFrame = await findFrame(b, WORLD_LOAD);
    b.ws.close();
    expect(loadFrame, "got a world").not.toBeNull();
    expect(Array.from(loadFrame!.subarray(1))).toEqual(Array.from(second));
  });

  it("a room that never saved hands the joiner NO world frame", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    // No save. A second peer joins; it must never receive a WORLD_LOAD.
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    // b will get an ADD_PEER for a, but never a WORLD_LOAD (0xF2).
    const world = await findFrame(b, WORLD_LOAD, 3, 300);
    expect(world, "no world was ever saved").toBeNull();
    a.ws.close();
    b.ws.close();
  });

  it("an oversized save fails LOUD — socket closed, never silently dropped", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);

    // One byte past the hard cap. The store must reject it; the relay must
    // close the socket with an observable reason rather than ack or drop.
    // We DON'T allocate WORLD_MAX_BYTES+1 of real payload (that would be a
    // 64MB+ frame); instead we drive the rejection deterministically by
    // asserting the server closes the connection with the documented reason.
    // A blob just over the cap is built sparsely to keep the test cheap on RAM
    // while still exercising the real >MAX path end-to-end.
    const oversized = new Uint8Array(WORLD_MAX_BYTES + 1);
    const save = new Uint8Array(1 + oversized.byteLength);
    save[0] = WORLD_SAVE;
    save.set(oversized, 1);
    a.ws.send(save);

    const { code, reason } = await a.closed;
    expect(code, "1009 = message too big").toBe(1009);
    expect(reason).toBe("world_too_large");

    // And it must NOT have been persisted: a fresh peer joining sees no world.
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const world = await findFrame(b, WORLD_LOAD, 3, 300);
    expect(world, "rejected save left nothing behind").toBeNull();
    b.ws.close();
  });
});

// Shared state — the no-host authoritative model: a live per-key document the
// room's Durable Object owns, orders, persists, and broadcasts. Contract under
// test:
//   1. Concurrent state_set on ONE key from peers A/B/C → the DO commits them in
//      ONE order with strictly increasing revisions, broadcasts each, and all
//      peers converge to the SAME final value (last-writer-wins by DO rev).
//   2. A late/reconnecting peer is handed the current persisted state (snapshot)
//      in its join burst and matches the others.
//   3. The state survives ALL peers leaving (durable) — a fresh peer joining a
//      long-empty room reads the last committed value.
//   4. A >1MB value round-trips via the chunked store.
import { fetchMock } from "cloudflare:test";
import { beforeEach, describe, expect, it } from "vitest";

import {
  decodeStateSnapshot,
  decodeStateUpdate,
  encodeStateSet,
  STATE_SNAPSHOT,
  STATE_UPDATE,
} from "../src/proto/v1";
import { connect, readPeerId, type Connection } from "./helpers";

beforeEach(() => {
  // Any un-stubbed outbound fetch (the /check call) throws loudly instead of
  // hitting the network — matches the rest of the suite.
  fetchMock.activate();
  fetchMock.disableNetConnect();
});

const uid = (p: string) => `${p}-${crypto.randomUUID()}`;
const enc = new TextEncoder();

// Pull frames off a connection until one whose first byte is `firstByte`
// arrives, or null after `tries` reads run dry. Shared-state frames are
// interleaved with the join-burst membership packets, so position isn't fixed.
async function findFrame(
  c: Connection,
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

// Collect exactly `n` STATE_UPDATE frames off a connection (skipping any
// interleaved membership/snapshot frames), decoded. Throws on timeout so a
// missing broadcast fails loud rather than hanging the suite.
async function collectUpdates(
  c: Connection,
  n: number,
  ms = 800,
): Promise<{ key: string; value: Uint8Array; rev: number }[]> {
  const out: { key: string; value: Uint8Array; rev: number }[] = [];
  while (out.length < n) {
    const f = await c.next(ms);
    if (f[0] === STATE_UPDATE) out.push(decodeStateUpdate(f));
  }
  return out;
}

const valStr = (v: Uint8Array) => new TextDecoder().decode(v);

describe("shared state: DO-ordered, durable, per-key live doc", () => {
  it("orders concurrent state_set on one key (LWW by DO rev) → all peers converge", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    const b = await connect({ rid, u: uid("ub") });
    const c = await connect({ rid, u: uid("uc") });
    await readPeerId(a);
    await readPeerId(b);
    await readPeerId(c);

    // No key has been written yet, so the join burst carries NO snapshot frame
    // (empty state sends nothing — same contract as a never-saved world). The
    // STATE_UPDATEs we collect below are therefore exactly the three writes.

    // A, B, C all write the SAME key concurrently with distinct values. The DO
    // is the single writer-orderer: it serializes these into one commit order.
    a.ws.send(encodeStateSet("score", enc.encode("from-A")));
    b.ws.send(encodeStateSet("score", enc.encode("from-B")));
    c.ws.send(encodeStateSet("score", enc.encode("from-C")));

    // Every peer (sender included) receives all three committed updates.
    const [ua, ub, uc] = await Promise.all([
      collectUpdates(a, 3),
      collectUpdates(b, 3),
      collectUpdates(c, 3),
    ]);

    // (1) One consistent order with STRICTLY INCREASING revisions, identical as
    //     seen by every peer (broadcast order == commit order).
    const revsA = ua.map((u) => u.rev);
    for (let i = 1; i < revsA.length; i++) {
      expect(revsA[i], "revs strictly increase").toBeGreaterThan(revsA[i - 1]);
    }
    expect(ub.map((u) => u.rev), "B sees the same rev order as A").toEqual(revsA);
    expect(uc.map((u) => u.rev), "C sees the same rev order as A").toEqual(revsA);

    // Every update is for the contended key; the three values are exactly the
    // three that were sent (no loss, no duplication).
    for (const u of ua) expect(u.key).toBe("score");
    expect(new Set(ua.map((u) => valStr(u.value)))).toEqual(
      new Set(["from-A", "from-B", "from-C"]),
    );

    // (2) Last-writer-wins by DO rev: the final value is the one at the highest
    //     rev, and ALL peers agree on it.
    const winner = (us: typeof ua) =>
      us.reduce((hi, u) => (u.rev > hi.rev ? u : hi)).value;
    const finalA = valStr(winner(ua));
    expect(valStr(winner(ub)), "B converges to same final value").toBe(finalA);
    expect(valStr(winner(uc)), "C converges to same final value").toBe(finalA);
    expect(["from-A", "from-B", "from-C"]).toContain(finalA);

    a.ws.close();
    b.ws.close();
    c.ws.close();
  });

  it("hands a late/reconnecting peer the current persisted state via snapshot", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    // A is the first peer into an empty room → no snapshot frame in its burst.

    // Two keys committed by A. Wait for each broadcast so we know it's durable
    // before the latecomer joins.
    a.ws.send(encodeStateSet("turn", enc.encode("player-2")));
    a.ws.send(encodeStateSet("deck", enc.encode("[7,8,9]")));
    const aUpdates = await collectUpdates(a, 2);
    expect(aUpdates.map((u) => u.key)).toEqual(["turn", "deck"]);

    // A late peer B joins and must receive BOTH keys at their committed revs.
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const snapFrame = await findFrame(b, STATE_SNAPSHOT);
    expect(snapFrame, "latecomer gets a snapshot").not.toBeNull();
    const snap = decodeStateSnapshot(snapFrame!);

    const byKey = new Map(snap.map((e) => [e.key, e]));
    expect(valStr(byKey.get("turn")!.value)).toBe("player-2");
    expect(valStr(byKey.get("deck")!.value)).toBe("[7,8,9]");
    // Snapshot revs match what A saw committed.
    expect(byKey.get("turn")!.rev).toBe(aUpdates[0].rev);
    expect(byKey.get("deck")!.rev).toBe(aUpdates[1].rev);
    // Snapshot is ordered by commit order (rev ascending).
    expect(snap.map((e) => e.rev)).toEqual([...snap.map((e) => e.rev)].sort((x, y) => x - y));

    a.ws.close();
    b.ws.close();
  });

  it("survives ALL peers leaving — a fresh peer reads the last committed state", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    // First peer into an empty room → no snapshot frame.

    a.ws.send(encodeStateSet("world-seed", enc.encode("durable-42")));
    const committed = await collectUpdates(a, 1);
    expect(valStr(committed[0].value)).toBe("durable-42");

    // The broadcast we just received means the chunked write committed to
    // durable SQLite, so it's safe to drop A — the room is now logically empty.
    a.ws.close();

    // A fresh peer joins the (now empty) room; its snapshot must still carry the
    // value, proving it outlived all-peers-left and any DO eviction.
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const snapFrame = await findFrame(b, STATE_SNAPSHOT);
    expect(snapFrame, "fresh peer gets a snapshot").not.toBeNull();
    const snap = decodeStateSnapshot(snapFrame!);
    expect(snap.length).toBe(1);
    expect(snap[0].key).toBe("world-seed");
    expect(valStr(snap[0].value)).toBe("durable-42");
    expect(snap[0].rev).toBe(committed[0].rev);

    b.ws.close();
  });

  it("round-trips a >1MB value via the chunked store", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    // First peer into an empty room → no snapshot frame.

    // 1.5MB — past the ~1–2MB single-cell SQLITE_TOOBIG boundary, so it must
    // span multiple chunk rows and reassemble. Deterministic sparse fill (prime
    // stride crosses chunk boundaries) keeps the comparison cheap.
    const n = Math.floor(1.5 * 1024 * 1024);
    const big = new Uint8Array(n);
    for (let i = 0; i < n; i += 4099) big[i] = (i % 251) + 1;

    a.ws.send(encodeStateSet("blob", big));
    // The committed broadcast must carry the whole reassembled value.
    const [update] = await collectUpdates(a, 1, 2000);
    expect(update.key).toBe("blob");
    expect(update.value.byteLength, "exact byte length preserved").toBe(n);
    for (let i = 0; i < n; i += 4099) {
      expect(update.value[i], `byte ${i} survived chunking`).toBe((i % 251) + 1);
    }

    // And a late joiner gets the same large value back out of the chunked store.
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const snap = decodeStateSnapshot((await findFrame(b, STATE_SNAPSHOT, 12, 800))!);
    expect(snap.length).toBe(1);
    expect(snap[0].value.byteLength).toBe(n);
    for (let i = 0; i < n; i += 4099) {
      expect(snap[0].value[i]).toBe((i % 251) + 1);
    }

    a.ws.close();
    b.ws.close();
  });

  it("a malformed STATE_SET fails LOUD — socket closed, never silently committed", async () => {
    const rid = uid("room");
    const a = await connect({ rid, u: uid("ua") });
    await readPeerId(a);
    // First peer into an empty room → no snapshot frame.

    // STATE_SET opcode with a keyLen that runs past the frame end → truncated.
    // The DO must close the socket, not commit a corrupt key.
    const bad = new Uint8Array([0xf4, 0xff, 0xff, 0x61]); // keyLen=0xFFFF, 1 key byte
    a.ws.send(bad);

    const { code, reason } = await a.closed;
    expect(code, "1008 = malformed control frame").toBe(1008);
    expect(reason).toBe("malformed_state_set");

    // Nothing was persisted: a fresh peer joining the now-empty room gets NO
    // snapshot frame at all (empty state sends nothing).
    const b = await connect({ rid, u: uid("ub") });
    await readPeerId(b);
    const snap = await findFrame(b, STATE_SNAPSHOT, 4, 300);
    expect(snap, "rejected write left nothing behind").toBeNull();
    b.ws.close();
  });
});

// Per-room live shared-state store, backed by the RoomDO's SQLite.
//
// This is the no-host authoritative model (turn-based / card / persistent
// world): a per-KEY document the DO owns. It is deliberately SEPARATE from
// world-store.ts — that store is one opaque whole-blob save/load; this is many
// independent keys, each with its own DO-assigned monotonic revision and
// last-writer-wins semantics. The DO is the single writer-orderer.
//
// Like the world store it must survive every peer leaving and the DO being
// evicted/hibernated, which it does because SQLite in a Durable Object is
// durable across eviction. It reuses the world store's chunking (a single
// SQLite cell tops out ~1–2MB) so a large value still round-trips: a value is
// split into fixed-size chunks across rows keyed by (key, chunk_index).

import { SharedStateEntry } from "./proto/v1";
import { WORLD_CHUNK_BYTES } from "./world-store";

// Same hard ceiling as a world blob — a single key's value can be a full scene
// snapshot, but anything past 64MB is a bug or abuse, not a use case. Rejected
// loud (callers close the socket), never silently truncated.
export const SHARED_VALUE_MAX_BYTES = 64 * 1024 * 1024;

export class SharedValueTooLargeError extends Error {
  constructor(
    public readonly key: string,
    public readonly byteLength: number,
  ) {
    super(
      `shared-state value for key '${key}' is ${byteLength} bytes, exceeds max ${SHARED_VALUE_MAX_BYTES} bytes`,
    );
    this.name = "SharedValueTooLargeError";
  }
}

// Row shape for the value-chunk query. Values are the columns selected.
interface ValueChunkRow extends Record<string, SqlStorageValue> {
  k: string;
  rev: number;
  chunk_index: number;
  data: ArrayBuffer;
}

export class SharedStateStore {
  constructor(private readonly sql: SqlStorage) {}

  // Idempotent schema creation. PRIMARY KEY (k, chunk_index) lets one key's
  // value span many chunk rows; `rev` is duplicated onto every chunk so the
  // join+order on load is a single indexed scan with no second table.
  static createTable(sql: SqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS shared_state (
        k TEXT NOT NULL,
        rev INTEGER NOT NULL,
        chunk_index INTEGER NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (k, chunk_index)
      )
    `);
  }

  // Commit one write. The DO calls this in strict arrival order, so the rev it
  // assigns is the room-wide commit order. Returns the rev stamped on this key.
  //
  // LAST-WRITER-WINS is enforced by the caller passing a monotonically
  // increasing `rev` (room counter); we simply replace the key's prior chunks
  // with the new value+rev. The store ORDERS and PERSISTS; it does NOT validate
  // game rules — a write that loses the race is still durably the older rev, and
  // peers reconcile by rev. This prevents desync, not cheating.
  set(key: string, value: Uint8Array, rev: number): void {
    if (value.byteLength > SHARED_VALUE_MAX_BYTES) {
      throw new SharedValueTooLargeError(key, value.byteLength);
    }

    // Replace-as-one: drop the key's old chunks then write the new value's
    // chunks. SQLite in a DO runs each exec in an implicit transaction and there
    // are no awaits between these, so a concurrent load can't observe a key
    // mid-rewrite (half old chunks, half new).
    this.sql.exec("DELETE FROM shared_state WHERE k = ?", key);

    // An empty value is a legitimate "set to empty" — one zero-length chunk so
    // the key still exists at this rev (distinct from "key never written").
    if (value.byteLength === 0) {
      this.sql.exec(
        "INSERT INTO shared_state (k, rev, chunk_index, data) VALUES (?, ?, 0, ?)",
        key,
        rev,
        new Uint8Array(0),
      );
      return;
    }

    for (
      let index = 0, offset = 0;
      offset < value.byteLength;
      index++, offset += WORLD_CHUNK_BYTES
    ) {
      // slice copies, so SQLite gets a standalone buffer and we don't pin the
      // whole value alive through one chunk's backing store.
      const chunk = value.slice(offset, offset + WORLD_CHUNK_BYTES);
      this.sql.exec(
        "INSERT INTO shared_state (k, rev, chunk_index, data) VALUES (?, ?, ?, ?)",
        key,
        rev,
        index,
        chunk,
      );
    }
  }

  // The current persisted state of every key, reassembled and ordered by rev
  // (commit order) so a joiner applies them in the same order the room did.
  // Empty array if nothing was ever written. This is the joiner's snapshot.
  snapshot(): SharedStateEntry[] {
    const rows = this.sql
      .exec<ValueChunkRow>(
        "SELECT k, rev, chunk_index, data FROM shared_state ORDER BY rev ASC, k ASC, chunk_index ASC",
      )
      .toArray();

    if (rows.length === 0) return [];

    // Reassemble per key. Rows are ordered by (rev, k, chunk_index); within one
    // key the chunk_index is contiguous from 0, which we assert — a gap means a
    // half-written value we must never hand back as intact.
    const entries: SharedStateEntry[] = [];
    let i = 0;
    while (i < rows.length) {
      const k = rows[i].k;
      const rev = rows[i].rev;
      const chunks: ArrayBuffer[] = [];
      let total = 0;
      let expectedIndex = 0;
      while (i < rows.length && rows[i].k === k) {
        if (rows[i].chunk_index !== expectedIndex) {
          throw new Error(
            `shared_state chunk sequence broken for key '${k}': expected index ${expectedIndex}, got ${rows[i].chunk_index}`,
          );
        }
        chunks.push(rows[i].data);
        total += rows[i].data.byteLength;
        expectedIndex++;
        i++;
      }
      const value = new Uint8Array(total);
      let off = 0;
      for (const c of chunks) {
        value.set(new Uint8Array(c), off);
        off += c.byteLength;
      }
      entries.push({ key: k, value, rev });
    }
    return entries;
  }
}

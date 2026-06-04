// Per-room persistent world store, backed by the RoomDO's SQLite.
//
// A "world" is one opaque blob the game saves and later loads. It must survive
// every peer leaving and the DO being evicted/hibernated, which it does because
// SQLite storage in a Durable Object is durable across eviction.
//
// Why chunking: a single SQLite cell has a hard ceiling — Spike 2A measured a
// ~1–2MB single-value `SQLITE_TOOBIG` boundary (1MB round-tripped, 4MB threw).
// A "lite MMO" world can exceed that, so we split the blob into fixed-size
// chunks across rows keyed by index and concatenate them back in order on load.
//
// The store holds exactly ONE logical world per room under WORLD_KEY. Saving
// replaces it wholesale (delete old chunks, insert new) inside a transaction so
// a load never observes a half-written world.

// Stay comfortably below the observed ~1–2MB single-cell ceiling. 768 KiB
// leaves margin for SQLite per-row/page overhead while keeping the row count
// low for multi-MB worlds (a 4MB world is 6 chunks).
export const WORLD_CHUNK_BYTES = 768 * 1024;

// Refuse absurd worlds loudly instead of letting them silently blow up SQLite
// or the DO's storage budget. 64MB is far beyond any real Godot scene snapshot;
// crossing it is a bug or abuse, not a use case.
export const WORLD_MAX_BYTES = 64 * 1024 * 1024;

// Single logical world per room. Kept as a column value (not hardcoded into
// SQL) so the schema could hold named worlds later without a migration.
const WORLD_KEY = "world";

export class WorldTooLargeError extends Error {
  constructor(public readonly byteLength: number) {
    super(
      `world blob ${byteLength} bytes exceeds max ${WORLD_MAX_BYTES} bytes`,
    );
    this.name = "WorldTooLargeError";
  }
}

// Row shape for the chunk query. Indexed by string to satisfy SqlStorage's
// `T extends Record<string, SqlStorageValue>` constraint; values are exactly
// the two columns selected.
interface ChunkRow extends Record<string, SqlStorageValue> {
  chunk_index: number;
  data: ArrayBuffer;
}

// `SqlStorage` / `SqlStorageValue` are ambient globals from
// @cloudflare/workers-types; the DO passes `ctx.storage.sql`.
export class WorldStore {
  constructor(private readonly sql: SqlStorage) {}

  // Idempotent schema creation. PRIMARY KEY (key, chunk_index) is what lets one
  // logical world span many rows; chunks load back in index order.
  static createTable(sql: SqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS world_state (
        key TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (key, chunk_index)
      )
    `);
  }

  // Persist `blob` as the room's world, replacing any prior world. Throws
  // WorldTooLargeError above the hard cap — callers must surface that, never
  // swallow it (a silently dropped save corrupts the world invisibly).
  save(blob: Uint8Array): void {
    if (blob.byteLength > WORLD_MAX_BYTES) {
      throw new WorldTooLargeError(blob.byteLength);
    }

    // Replace-as-one: drop the previous world's chunks, then write the new
    // ones. SQLite in a DO has implicit per-`exec` transactions; the delete +
    // inserts run with no awaits between them so a concurrent load can't see a
    // partially-rewritten world.
    this.sql.exec("DELETE FROM world_state WHERE key = ?", WORLD_KEY);

    // An empty world is a legitimate "clear" — zero chunks. A later load
    // returns null, same as a room that never saved.
    for (
      let index = 0, offset = 0;
      offset < blob.byteLength;
      index++, offset += WORLD_CHUNK_BYTES
    ) {
      // subarray is a view; copy so we hand SQLite a standalone buffer and
      // never pin the whole blob alive through one chunk's backing store.
      const chunk = blob.slice(offset, offset + WORLD_CHUNK_BYTES);
      this.sql.exec(
        "INSERT INTO world_state (key, chunk_index, data) VALUES (?, ?, ?)",
        WORLD_KEY,
        index,
        chunk,
      );
    }
  }

  // Reassemble the room's world, or null if none was ever saved (or it was
  // cleared to empty). Concatenates chunks strictly in index order.
  load(): Uint8Array | null {
    const rows = this.sql
      .exec<ChunkRow>(
        "SELECT chunk_index, data FROM world_state WHERE key = ? ORDER BY chunk_index ASC",
        WORLD_KEY,
      )
      .toArray();

    if (rows.length === 0) {
      return null;
    }

    // Fail loud on a gap/duplicate in the index sequence — that means a
    // half-written or corrupted world, which we must never hand back as if it
    // were intact.
    let total = 0;
    for (let i = 0; i < rows.length; i++) {
      if (rows[i].chunk_index !== i) {
        throw new Error(
          `world chunk sequence broken: expected index ${i}, got ${rows[i].chunk_index} (have ${rows.length} chunks)`,
        );
      }
      total += rows[i].data.byteLength;
    }

    const out = new Uint8Array(total);
    let offset = 0;
    for (const row of rows) {
      out.set(new Uint8Array(row.data), offset);
      offset += row.data.byteLength;
    }
    return out;
  }
}

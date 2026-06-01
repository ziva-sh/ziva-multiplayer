// ConnRateDO — fixed-window connection-rate limiter.
//
// The relay has NO negative cache: an unknown user triggers a blocking /check
// and, on failure of an existing entry, serves last-good. That design is only
// safe if raw connection attempts are bounded, otherwise an attacker could
// hammer the upgrade with fresh user ids and force unbounded /check fan-out at
// apps/web. This DO bounds it.
//
// One DO instance per rate key (the Worker shards by key via getByName, so
// each instance owns exactly ONE window) — keyed per-IP and per-(u,g) by the
// caller. State is a single fixed window held in SQLite so it survives
// eviction. A fixed window (not sliding) is deliberate: it's one row, one
// branch, and an attacker gaining at most a 2x burst at a window boundary is
// irrelevant for a coarse abuse cap.

import { DurableObject } from "cloudflare:workers";

// Connection-rate caps. Coarse abuse bounds, NOT per-message rate limiting
// (that lives in the room DO). Consulted once per upgrade. Defined here (not in
// the worker entry) so they can be imported by both index.ts and the test
// suite without the entry module exporting a non-handler value — workerd
// rejects plain named exports from the entrypoint as invalid service bindings.
export const CONN_RATE_WINDOW_MS = 60_000;
export const CONN_RATE_LIMIT_PER_IP = 120;
export const CONN_RATE_LIMIT_PER_UG = 30;

export class ConnRateDO extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS window_state (
          key TEXT PRIMARY KEY,
          window_start INTEGER NOT NULL,
          count INTEGER NOT NULL
        )
      `);
    });
  }

  // Returns true if this attempt is within the cap for the current window,
  // false if the window cap is exceeded. Counts the attempt only when allowed
  // so a blocked caller can't keep inflating the count past the cap.
  async hit(limit: number, windowMs: number): Promise<boolean> {
    const now = Date.now();
    const rows = this.ctx.storage.sql
      .exec<{ window_start: number; count: number }>(
        "SELECT window_start, count FROM window_state WHERE key = 'w'",
      )
      .toArray();

    let windowStart = rows.length > 0 ? rows[0].window_start : 0;
    let count = rows.length > 0 ? rows[0].count : 0;

    if (now - windowStart >= windowMs) {
      windowStart = now;
      count = 0;
    }

    if (count >= limit) {
      // At cap — reject without incrementing.
      this.ctx.storage.sql.exec(
        "INSERT INTO window_state (key, window_start, count) VALUES ('w', ?, ?) " +
          "ON CONFLICT(key) DO UPDATE SET window_start = excluded.window_start, count = excluded.count",
        windowStart,
        count,
      );
      return false;
    }

    count += 1;
    this.ctx.storage.sql.exec(
      "INSERT INTO window_state (key, window_start, count) VALUES ('w', ?, ?) " +
        "ON CONFLICT(key) DO UPDATE SET window_start = excluded.window_start, count = excluded.count",
      windowStart,
      count,
    );
    return true;
  }
}

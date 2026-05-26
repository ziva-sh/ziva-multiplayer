// CapState — caches the set of throttled devUserIds from CAPS_KV.
//
// The Vercel cron writes a marker key per throttled user into CAPS_KV. The
// RoomDO consults this cache per message to decide whether to apply the soft
// 1 KB/s rate limit instead of the regular 32 KB/s limit.
//
// One instance lives per DO. Refreshed lazily: every getThrottled() call that
// finds the cache older than REFRESH_MS triggers an async reload (and the
// caller proceeds against the still-valid old set so we never block message
// handling on KV).
//
// Key shape in CAPS_KV: `throttled:<devUserId>` -> any non-empty value.
// We list with the prefix and collect the suffix as the user id.

const REFRESH_MS = 5 * 60 * 1000;
const KV_PREFIX = "throttled:";

export const THROTTLED_BYTES_PER_SEC = 1024;

export class CapState {
  private throttled = new Set<string>();
  private lastRefreshAt = 0;
  private inflight: Promise<void> | null = null;

  constructor(private kv: KVNamespace) {}

  // Synchronous read against the cached set. May trigger an async refresh.
  isThrottled(devUserId: string): boolean {
    this.maybeRefresh();
    return this.throttled.has(devUserId);
  }

  // Force-refresh from KV. Used on cold start and from the periodic refresh.
  async refresh(): Promise<void> {
    if (this.inflight) {
      await this.inflight;
      return;
    }
    this.inflight = this.doRefresh();
    try {
      await this.inflight;
    } finally {
      this.inflight = null;
    }
  }

  private maybeRefresh(): void {
    const now = Date.now();
    if (now - this.lastRefreshAt < REFRESH_MS) return;
    if (this.inflight) return;
    // Fire-and-forget; next call sees the updated set.
    this.inflight = this.doRefresh();
    this.inflight
      .catch((err) => {
        console.log(
          JSON.stringify({
            event: "cap_state_refresh_error",
            error: String((err as Error).message ?? err),
          }),
        );
      })
      .finally(() => {
        this.inflight = null;
      });
  }

  private async doRefresh(): Promise<void> {
    const next = new Set<string>();
    let cursor: string | undefined;
    do {
      const page = await this.kv.list({ prefix: KV_PREFIX, cursor });
      for (const k of page.keys) {
        const id = k.name.slice(KV_PREFIX.length);
        if (id.length > 0) next.add(id);
      }
      cursor = page.list_complete ? undefined : page.cursor;
    } while (cursor);
    this.throttled = next;
    this.lastRefreshAt = Date.now();
  }
}

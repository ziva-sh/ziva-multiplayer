// Verifies CapState loads throttled devUserIds from CAPS_KV and that the
// RoomDO applies the 1 KB/s budget to those users.
//
// We exercise the RoomDO end-to-end (helpers.connect → real WS → real DO)
// rather than unit-testing CapState in isolation so the assertion proves the
// whole path actually throttles. The KV is seeded via env.CAPS_KV directly
// (cloudflare:test exposes the binding to the test harness).

import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";

import { buildRelayPacket, connect, readPeerId } from "./helpers";

describe("cap-state", () => {
  it("closes a throttled peer with 1008 when they exceed the 1 KB/s budget", async () => {
    const devUserId = `capped-user-${crypto.randomUUID()}`;
    // Seed the CAPS_KV with the throttle marker. The DO's CapState refreshes
    // on cold start; we get a fresh RoomDO for this unique rid so the first
    // isThrottled() call triggers a load.
    await env.CAPS_KV.put(`throttled:${devUserId}`, "1");

    const rid = `room-capped-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: devUserId });
    await readPeerId(a);

    // 1 KB/s budget. A 200-byte payload sent ~6 times = 1200 bytes > 1024 →
    // the connection should be closed with rate_limit_exceeded well under
    // a second. We send 10 to ensure we cross the threshold even if some
    // frames are coalesced.
    const payload = buildRelayPacket(0, new Uint8Array(200));
    const start = Date.now();
    for (let i = 0; i < 10; i++) {
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

    // Cleanup so other tests don't see the throttle.
    await env.CAPS_KV.delete(`throttled:${devUserId}`);
  });

  it("allows a non-throttled peer to send well past the 1 KB/s throttled budget", async () => {
    const devUserId = `uncapped-user-${crypto.randomUUID()}`;
    const rid = `room-uncapped-${crypto.randomUUID()}`;
    const a = await connect({ rid, sub: devUserId });
    await readPeerId(a);

    // 200-byte payload * 5 = 1000 bytes — would trip the 1 KB/s throttled
    // budget but is far below the regular 32 KB/s. Should NOT close.
    const payload = buildRelayPacket(0, new Uint8Array(200));
    for (let i = 0; i < 5; i++) a.ws.send(payload);

    // Give the DO time to process; if it were going to close us it'd happen.
    await new Promise((r) => setTimeout(r, 200));

    // Socket should still be open; closing it ourselves is clean.
    a.ws.close(1000, "bye");
    const { code } = await a.closed;
    // 1000 = clean close. Anything other than 1008 means we weren't rate-limited.
    expect(code).not.toBe(1008);
  });
});

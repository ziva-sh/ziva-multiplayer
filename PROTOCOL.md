# Wire protocol

This document is the source of truth for the relay's wire format. Breaking
changes require a new major version.

## Versioning

Clients select a protocol major via the `?v=<major>` query parameter on the
WebSocket upgrade URL. Unknown majors are rejected at upgrade time with WS
close code `1008` and reason `unsupported_protocol_version`.

Current version: **1**.

## Authentication

There is no token. Clients identify themselves with two public query
parameters on the upgrade URL:

| Param | Type   | Meaning                                                      |
|-------|--------|-------------------------------------------------------------|
| `u`   | string | Dev user id — the Ziva account that owns and is billed.      |
| `g`   | string | Game id — stable per project, scopes connection-rate limits. |

The room id comes from the `/r/<room>` path segment, not a query param.

These values are not secrets; they say *which* account a connection belongs
to. On each upgrade the Worker bounds raw attempts with `ConnRateDO` (per-IP
and per-`(u,g)`), then asks the Ziva web app `GET /api/multiplayer/check?u=<u>`
whether that account may connect (subscribed tier, multiplayer enabled, under
its bandwidth cap). A denied or unavailable check rejects the upgrade with WS
close code `1008` — the WebSocket is accepted then immediately closed, never
fully established. Missing `u`, `g`, or room → `1008` `missing_params`.

## Connection lifecycle

1. Client opens `<relay_url>/r/<room>?u=<user_id>&g=<game_id>&v=1`.
2. Worker checks access (see Authentication) and forwards to the `RoomDO`
   named by the room id.
3. RoomDO assigns the next monotonic `peer_id` (starting at 1) and accepts
   the socket via the Hibernation API.
4. Server sends a `welcome` envelope to the new peer.
5. Peers exchange `data` envelopes until they disconnect.

Limits enforced per room:

- **Room cap**: 32 concurrent connections. The 33rd upgrade is rejected with
  close code `1008` and reason `room_full`.
- **Rate limit**: 60 messages/sec and 32 KiB/sec per connection. Violators
  are closed with `1008` and reason `rate_limit_exceeded`.

## Envelopes (v1)

All messages are JSON for control frames; payloads may be JSON or binary.

### Server → client: `welcome`

```json
{ "type": "welcome", "peer_id": 1, "protocol_version": 1 }
```

Sent once to the newly-connected client. Contains the peer id assigned by
the relay.

### Server → client: `peer_join`

```json
{ "type": "peer_join", "peer_id": 2 }
```

Broadcast to every already-connected peer when a new peer joins. The new
peer itself does not receive this — they learn their own id from `welcome`.

### Server → client: `peer_leave`

```json
{ "type": "peer_leave", "peer_id": 2 }
```

Broadcast to every remaining peer when a peer disconnects (for any reason,
including a server-side cap/rate-limit close). Lets clients keep their
membership view consistent without out-of-band coordination.

### Client → server / server → client: `data`

```json
{ "type": "data", "from": 1, "payload": "<string or base64-encoded binary>" }
```

The server tags every relayed `data` frame with the `from` peer id. Clients
that send a `data` frame without a `from` field (or with the wrong one) have
it overwritten by the server. Broadcast semantics: every other peer in the
room receives the frame; the sender does not echo.

Binary frames are forwarded as-is — the server adds the `from` tag only for
JSON `data` envelopes.

## World persistence (v1)

A Ziva extension on the binary channel: a single per-room **world blob** the
game saves to / loads from the room's Durable Object. It survives every peer
leaving and the DO being evicted/hibernated, because the DO's SQLite storage is
durable across eviction. This is the persistence primitive behind a save that
outlives the session.

These are NOT Godot protocol frames. They are matched on the **whole first
byte**, before any Godot parsing. The opcodes occupy `0xF1`–`0xF3`, whose low
three bits are `1`/`2`/`3` — never `7` (`NETWORK_COMMAND_SYS`) — so a world
frame can never be mistaken for a SYS (relay/auth/add/del) packet on any
transfer channel, and vice-versa.

| Opcode | Byte | Direction       | Frame                | Meaning                                  |
|--------|------|-----------------|----------------------|------------------------------------------|
| `WORLD_SAVE`     | `0xF1` | client → server | `[0xF1][…blob]` | Persist `blob` as this room's world (replaces any prior world). |
| `WORLD_LOAD`     | `0xF2` | server → client | `[0xF2][…blob]` | Here is the stored world. |
| `WORLD_SAVE_ACK` | `0xF3` | server → client | `[0xF3]`        | The save committed to durable storage. |

Semantics:

- **Save**: the client sends `WORLD_SAVE` with the world bytes appended. The
  server persists it and replies `WORLD_SAVE_ACK` once the write has committed.
  A save with an empty blob clears the world. Saves are handled before the
  per-second realtime byte cap so a legitimate multi-MB world isn't throttled;
  their own size ceiling (below) bounds abuse instead.
- **Load**: a stored world is delivered exactly once, unsolicited, in the new
  peer's **join burst** (alongside the `peer_join`/membership packets) — even
  when the room had been empty and the DO evicted in between. A room that never
  saved sends no `WORLD_LOAD`.
- **Chunking**: a single SQLite cell tops out around 1–2 MB. The store splits a
  world into fixed-size chunks (≤ 768 KiB each) keyed by index across rows, and
  concatenates them back in order on load, so multi-MB worlds round-trip
  transparently. The wire frame is always the whole reassembled blob.
- **Size ceiling**: a save larger than **64 MiB** is rejected loud — the server
  closes the connection with `1009 world_too_large` and persists nothing,
  rather than silently dropping a save the game believes succeeded.

## Shared state (v1)

A Ziva extension on the binary channel: a **live per-key document** owned by the
room's Durable Object — the no-host authoritative model for turn-based, card,
and persistent-world games. This is **distinct from both** the Godot relay path
(host-client games never see these frames — the DO handles them itself and never
forwards them) **and** from World persistence above (that is one opaque
whole-blob save/load; this is many independent keys, each carrying its own
DO-assigned revision).

The DO is the **single writer-orderer**. It applies incoming writes in arrival
order, stamps each with a strictly-increasing **revision**, persists it to a
`shared_state` SQLite table, and broadcasts the committed change to every
connected peer. A joiner is handed the full current state. Resolution is
**last-writer-wins per key by the DO-assigned revision**.

> The DO **orders** writes; it does **not** validate game rules. This prevents
> *desync* (everyone converges to one consistent, durable doc), not *cheating*
> (a client that lies about game logic still gets its write committed). Authority
> over what counts as a legal move belongs to the game, not the relay.

These are NOT Godot protocol frames. They are matched on the **whole first
byte**, before any Godot parsing. The opcodes occupy `0xF4`–`0xF6`, whose low
three bits are `4`/`5`/`6` — never `7` (`NETWORK_COMMAND_SYS`) — so a shared-
state frame can never be mistaken for a SYS (relay/auth/add/del) packet on any
transfer channel, and they are also disjoint from the World opcodes `0xF1`–`0xF3`.

| Opcode | Byte | Direction        | Frame |
|--------|------|------------------|-------|
| `STATE_SET`      | `0xF4` | client → DO    | `[0xF4][u16 keyLen][key][value…]` |
| `STATE_UPDATE`   | `0xF5` | DO → all peers | `[0xF5][u32 rev][u16 keyLen][key][value…]` |
| `STATE_SNAPSHOT` | `0xF6` | DO → joiner    | `[0xF6][u32 count]` then per entry `[u32 rev][u16 keyLen][key][u32 valLen][value]` |

All integers are little-endian. `key` is UTF-8 (≤ 1 KiB); `value` is opaque
bytes the relay never interprets. In `STATE_SET`/`STATE_UPDATE` the value is the
frame tail (no length prefix); in `STATE_SNAPSHOT` every field is length-prefixed
because entries are packed back-to-back.

Semantics:

- **Set**: the client sends `STATE_SET` with a key and value. The DO assigns the
  next revision, persists it (last-writer-wins replaces any prior value for that
  key), and broadcasts a `STATE_UPDATE`. Like world saves, `STATE_SET` is handled
  before the per-second realtime byte cap so a large value isn't throttled; its
  own size ceiling bounds abuse instead.
- **Update**: every peer — **including the original sender** — receives the
  committed `STATE_UPDATE` carrying the DO-assigned `rev`. A peer applies an
  update only if its `rev` exceeds the rev it already holds for that key, so all
  peers converge to the same final value regardless of arrival interleaving. The
  sender learns the committed rev/order this way rather than trusting its own
  optimistic write.
- **Snapshot**: on join, when the room has any committed state the DO sends
  exactly one `STATE_SNAPSHOT` with the full current state, ordered by revision
  (commit order), in the join burst — even when the room had been empty and the
  DO evicted in between (the state is durable in SQLite). A room that never wrote
  a key sends **no** snapshot frame, exactly like a never-saved world sends no
  `WORLD_LOAD`; "no snapshot frame" is the joiner's unambiguous "state is empty"
  signal. (This also keeps a pure host-client/relay game's join burst
  byte-identical to a relay that has no shared-state support — those games never
  call `STATE_SET`.)
- **Chunking**: a single SQLite cell tops out around 1–2 MB, so a value is split
  into fixed-size chunks (≤ 768 KiB each) keyed by index across rows and
  reassembled on read — multi-MB values round-trip transparently. The wire frame
  always carries the whole reassembled value.
- **Durability**: the doc survives every peer leaving and the DO being
  evicted/hibernated, because SQLite storage in a Durable Object is durable
  across eviction. A fresh peer joining a long-empty room reads the last
  committed state.
- **Malformed frame**: a structurally invalid `STATE_SET` (truncated, bad key
  length, non-UTF-8 key) is rejected loud — the server closes the connection
  with `1008 malformed_state_set` rather than committing a corrupt write. An
  oversized value closes with `1009 state_value_too_large`.

## Close codes

| Code | Reason                          | Meaning                                            |
|------|---------------------------------|----------------------------------------------------|
| 1000 | normal_closure                  | Peer disconnected cleanly.                         |
| 1008 | unsupported_protocol_version    | `?v` does not match a server-supported major.      |
| 1008 | missing_params                  | Upgrade URL lacks `u`, `g`, or a `/r/<room>` path. |
| 1008 | rate_limited                    | Connection attempts exceeded the per-IP/`(u,g)` cap. |
| 1008 | not_allowed                     | Account check denied (tier, disabled, or capped).  |
| 1008 | check_unavailable               | Access check failed with no last-good (fail closed). |
| 1008 | room_full                       | Room is at its 32-peer cap.                        |
| 1008 | rate_limit_exceeded             | Sender exceeded per-connection message rate limit. |
| 1008 | malformed_state_set             | A `STATE_SET` frame was structurally invalid.      |
| 1009 | world_too_large                 | A `WORLD_SAVE` blob exceeded the 64 MiB ceiling.   |
| 1009 | state_value_too_large           | A `STATE_SET` value exceeded the 64 MiB ceiling.   |
| 1011 | internal_error                  | Server-side bug.                                   |

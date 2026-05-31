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
| 1011 | internal_error                  | Server-side bug.                                   |

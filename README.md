# ziva-multiplayer

Cloudflare Worker + Durable Object relay that powers real-time multiplayer for
Ziva-built games. Gates connection attempts against [`ziva.sh`](https://ziva.sh)
account state and brokers WebSocket messages between peers in the same room.

This repo is intentionally narrow: it implements the wire protocol and the
relay. Token issuance, billing, and game logic live elsewhere.

## Architecture

```
Game client ──ws──► Worker (access gate) ──► RoomDO (broadcast)
                       │
                       ├─► ziva.sh /api/multiplayer/check
                       └─► Analytics Engine (usage metrics)
```

- **Worker** (`worker/src/index.ts`): terminates the WebSocket upgrade,
  checks the owning Ziva account, looks up the `RoomDO` by room id, forwards
  the upgrade, and exposes `/diagnose` for user-facing failure messages.
- **RoomDO** (`worker/src/room-do.ts`): one Durable Object per room. Uses the
  Hibernation API so idle rooms cost nothing. Enforces room cap, per-connection
  rate limit, and broadcasts messages to all peers except the sender.

See [`PROTOCOL.md`](./PROTOCOL.md) for the wire format and version-compat rules.

## Development

```bash
cd worker
bun install
bun test          # vitest against the real Workers runtime
bun run dev       # local wrangler dev
```

The test secret lives in `worker/.dev.vars` (gitignored). Production deploys
read `MULTIPLAYER_JWT_SECRET` from Cloudflare's secret store.

## Security

Report vulnerabilities to security@ziva.sh. See [`SECURITY.md`](./SECURITY.md).

## License

MIT — see [`LICENSE`](./LICENSE).

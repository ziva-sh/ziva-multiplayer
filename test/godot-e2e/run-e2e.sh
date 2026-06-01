#!/usr/bin/env bash
# Headless-Godot e2e canary.
#
# Spawns two `godot --headless -s` processes (host + client) that connect to
# the relay as plain `WebSocketMultiplayerPeer.create_client(url)` instances
# using the credential-less `?u=&g=&v=1` URL. The host fires a `ping` @rpc;
# the client replies `ack`. Both must exit 0 within the script's internal 15s
# deadline.
#
# Required env:
#   ZIVA_USER_ID             developer user id passed as `?u=`
# Optional:
#   ZIVA_RELAY_URL_E2E       ws/wss URL of the relay (defaults to staging).
#                            Use ws://localhost:8787 for local `wrangler dev`.
#   ZIVA_GAME_ID             game id passed as `?g=` (default: g_canary)
#   ZIVA_ROOM_ID             shared room id (default: random canary-<hex>)
#   GODOT                    path to the godot binary (default: `godot`)

set -uo pipefail

RELAY_URL="${ZIVA_RELAY_URL_E2E:-wss://ziva-multiplayer-staging.ziva-multiplayer.workers.dev}"
USER_ID="${ZIVA_USER_ID:-}"
GAME_ID="${ZIVA_GAME_ID:-g_canary}"
GODOT_BIN="${GODOT:-godot}"

# Accept a relay URL with or without scheme — the GH secret historically
# stored the bare hostname for the JSON-protocol e2e script. An explicit
# ws:// (local wrangler dev) is left untouched.
if [[ "$RELAY_URL" != ws://* && "$RELAY_URL" != wss://* ]]; then
    RELAY_URL="wss://$RELAY_URL"
fi

if [[ -z "$USER_ID" ]]; then
    echo "[run-e2e] FAIL: ZIVA_USER_ID not set" >&2
    exit 1
fi

if ! command -v "$GODOT_BIN" >/dev/null 2>&1; then
    echo "[run-e2e] FAIL: '$GODOT_BIN' not on PATH" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Both processes must land on the same RoomDO, which the worker derives from
# the /r/<room> path, so host and client share one room id.
ROOM_ID="${ZIVA_ROOM_ID:-canary-$(openssl rand -hex 4)}"
echo "[run-e2e] room_id=$ROOM_ID user_id=$USER_ID game_id=$GAME_ID relay=$RELAY_URL"

# Spawn host and client in parallel against the same room. Stagger by 200ms
# so the host's WS upgrade lands first — gives deterministic peer_id
# ordering (host=2, client=3) which simplifies log debugging. The protocol
# works in either order.
LOG_HOST="$(mktemp -t ziva-host.XXXXXX.log)"
LOG_CLIENT="$(mktemp -t ziva-client.XXXXXX.log)"
trap 'rm -f "$LOG_HOST" "$LOG_CLIENT"' EXIT

echo "[run-e2e] launching host (logs: $LOG_HOST)"
ZIVA_RELAY_URL="$RELAY_URL" ZIVA_ROOM_ID="$ROOM_ID" ZIVA_USER_ID="$USER_ID" ZIVA_GAME_ID="$GAME_ID" \
    "$GODOT_BIN" --headless --path "$SCRIPT_DIR" res://host.tscn \
    >"$LOG_HOST" 2>&1 &
HOST_PID=$!

sleep 0.2

echo "[run-e2e] launching client (logs: $LOG_CLIENT)"
ZIVA_RELAY_URL="$RELAY_URL" ZIVA_ROOM_ID="$ROOM_ID" ZIVA_USER_ID="$USER_ID" ZIVA_GAME_ID="$GAME_ID" \
    "$GODOT_BIN" --headless --path "$SCRIPT_DIR" res://client.tscn \
    >"$LOG_CLIENT" 2>&1 &
CLIENT_PID=$!

wait "$HOST_PID"
HOST_EXIT=$?
wait "$CLIENT_PID"
CLIENT_EXIT=$?

echo
echo "==================== HOST LOG ===================="
cat "$LOG_HOST"
echo "==================== CLIENT LOG ===================="
cat "$LOG_CLIENT"
echo "==================== EXIT CODES ===================="
echo "host_exit=$HOST_EXIT client_exit=$CLIENT_EXIT"

if [[ "$HOST_EXIT" -eq 0 && "$CLIENT_EXIT" -eq 0 ]]; then
    echo "[run-e2e] PASS"
    exit 0
fi

echo "[run-e2e] FAIL"
exit 1

#!/usr/bin/env bash
# Headless-Godot e2e canary.
#
# Mints two JWTs against the configured token endpoint, then spawns two
# `godot --headless -s` processes (host + client) that connect to the
# staging relay as plain `WebSocketMultiplayerPeer.create_client(url)`
# instances. The host fires a `ping` @rpc; the client replies `ack`.
# Both must exit 0 within the script's internal 15s deadline.
#
# Required env:
#   ZIVA_RELAY_URL_E2E       wss URL of the relay (defaults to staging)
#   ZIVA_TOKEN_ENDPOINT_E2E  https URL of the token issuer (defaults to staging)
#   E2E_USER_API_KEY         Better-Auth API key for a basic-tier user with
#                            multiplayerEnabled=true
# Optional:
#   STAGING_BYPASS_TOKEN     Vercel protection-bypass token if the token
#                            endpoint sits behind Vercel SSO
#   GODOT                    path to the godot binary (default: `godot`)

set -uo pipefail

RELAY_URL="${ZIVA_RELAY_URL_E2E:-wss://ziva-multiplayer-staging.ziva-multiplayer.workers.dev}"
TOKEN_ENDPOINT="${ZIVA_TOKEN_ENDPOINT_E2E:-https://staging.ziva.sh}"
API_KEY="${E2E_USER_API_KEY:-}"
BYPASS="${STAGING_BYPASS_TOKEN:-}"
GODOT_BIN="${GODOT:-godot}"

# Accept a relay URL with or without scheme — the GH secret historically
# stored the bare hostname for the JSON-protocol e2e script.
if [[ "$RELAY_URL" != ws://* && "$RELAY_URL" != wss://* ]]; then
    RELAY_URL="wss://$RELAY_URL"
fi

if [[ -z "$API_KEY" ]]; then
    echo "[run-e2e] FAIL: E2E_USER_API_KEY not set" >&2
    exit 1
fi

if ! command -v "$GODOT_BIN" >/dev/null 2>&1; then
    echo "[run-e2e] FAIL: '$GODOT_BIN' not on PATH" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Mint host token first (lets the server pick a fresh room id), then mint a
# second token for the SAME room id so both clients land on the same DO.
HEADERS=(-H "x-api-key: $API_KEY" -H "Content-Type: application/json")
if [[ -n "$BYPASS" ]]; then
    HEADERS+=(-H "x-vercel-protection-bypass: $BYPASS" -H "x-vercel-set-bypass-cookie: true")
fi

echo "[run-e2e] minting host token against $TOKEN_ENDPOINT"
RESP1="$(curl -sS -X POST "${HEADERS[@]}" -d '{}' "$TOKEN_ENDPOINT/api/multiplayer/token")"
if [[ -z "$RESP1" ]]; then
    echo "[run-e2e] FAIL: empty response from token endpoint" >&2
    exit 1
fi
ROOM_ID="$(echo "$RESP1" | jq -r '.room_id // empty')"
TOKEN_HOST="$(echo "$RESP1" | jq -r '.token // empty')"
if [[ -z "$ROOM_ID" || -z "$TOKEN_HOST" ]]; then
    echo "[run-e2e] FAIL: malformed token response: $RESP1" >&2
    exit 1
fi
echo "[run-e2e] room_id=$ROOM_ID"

echo "[run-e2e] minting client token for same room"
RESP2="$(curl -sS -X POST "${HEADERS[@]}" -d "{\"room_id\":\"$ROOM_ID\"}" "$TOKEN_ENDPOINT/api/multiplayer/token")"
TOKEN_CLIENT="$(echo "$RESP2" | jq -r '.token // empty')"
if [[ -z "$TOKEN_CLIENT" ]]; then
    echo "[run-e2e] FAIL: malformed second token response: $RESP2" >&2
    exit 1
fi

# Spawn host and client in parallel against the same room. Stagger by 200ms
# so the host's WS upgrade lands first — gives deterministic peer_id
# ordering (host=2, client=3) which simplifies log debugging. The protocol
# works in either order.
LOG_HOST="$(mktemp -t ziva-host.XXXXXX.log)"
LOG_CLIENT="$(mktemp -t ziva-client.XXXXXX.log)"
trap 'rm -f "$LOG_HOST" "$LOG_CLIENT"' EXIT

echo "[run-e2e] launching host (logs: $LOG_HOST)"
ZIVA_RELAY_URL="$RELAY_URL" ZIVA_ROOM_ID="$ROOM_ID" ZIVA_TOKEN_HOST="$TOKEN_HOST" \
    "$GODOT_BIN" --headless --path "$SCRIPT_DIR" res://host.tscn \
    >"$LOG_HOST" 2>&1 &
HOST_PID=$!

sleep 0.2

echo "[run-e2e] launching client (logs: $LOG_CLIENT)"
ZIVA_RELAY_URL="$RELAY_URL" ZIVA_ROOM_ID="$ROOM_ID" ZIVA_TOKEN_CLIENT="$TOKEN_CLIENT" \
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

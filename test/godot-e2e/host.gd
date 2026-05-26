extends Node

# Headless Godot host. Connects to the Ziva relay as a plain
# WebSocketMultiplayerPeer client (the relay is the implicit server id=1),
# waits for the other peer to join, fires a `ping` @rpc, then waits for
# `ack` back. PASS = exit 0.
#
# Loaded via host.tscn as the main scene so the @rpc decorator binds to a
# real Node inside the SceneTree (required for RPC NodePath routing).
#
# Required env vars (set by run-e2e.sh):
#   ZIVA_RELAY_URL         wss://<host>
#   ZIVA_ROOM_ID           shared room id (both processes use the same one)
#   ZIVA_TOKEN_HOST        JWT for the host's session

const TIMEOUT_MS := 15000
const ACK_WAIT_MS := 5000

var _peer: WebSocketMultiplayerPeer
var _other_peer_id := 0
var _got_ack := false
var _start_ms := 0
var _ack_deadline_ms := 0

func _ready() -> void:
	_start_ms = Time.get_ticks_msec()
	var relay_url := _env_or_die("ZIVA_RELAY_URL")
	var room_id := _env_or_die("ZIVA_ROOM_ID")
	var token := _env_or_die("ZIVA_TOKEN_HOST")
	var url := "%s/r/%s?token=%s&v=1" % [relay_url, room_id, token]
	print("[host] connecting to %s" % url)

	_peer = WebSocketMultiplayerPeer.new()
	var err := _peer.create_client(url)
	if err != OK:
		_fail("create_client returned %d" % err)
		return
	multiplayer.multiplayer_peer = _peer

	multiplayer.peer_connected.connect(_on_peer_connected)
	multiplayer.peer_disconnected.connect(_on_peer_disconnected)
	multiplayer.connected_to_server.connect(_on_connected_to_server)
	multiplayer.connection_failed.connect(_on_connection_failed)
	multiplayer.server_disconnected.connect(_on_server_disconnected)

func _process(_delta: float) -> void:
	# Cap total runtime so a stuck test exits non-zero instead of hanging CI.
	if Time.get_ticks_msec() - _start_ms > TIMEOUT_MS:
		_fail("timeout after %d ms (other_peer=%d got_ack=%s)" % [TIMEOUT_MS, _other_peer_id, str(_got_ack)])
		return
	if _ack_deadline_ms > 0 and Time.get_ticks_msec() > _ack_deadline_ms and not _got_ack:
		_fail("no ack received within %d ms" % ACK_WAIT_MS)
		return

func _on_connected_to_server() -> void:
	print("[host] connected to relay; my unique_id=%d" % multiplayer.get_unique_id())

func _on_connection_failed() -> void:
	_fail("connection_failed")

func _on_server_disconnected() -> void:
	if _got_ack:
		return
	_fail("server_disconnected before completion")

func _on_peer_connected(id: int) -> void:
	# id=1 is the implicit server (the relay). We care about real peers.
	if id == 1:
		return
	_other_peer_id = id
	print("[host] peer_connected(%d) — sending ping" % id)
	ping.rpc_id(id, "hello")
	_ack_deadline_ms = Time.get_ticks_msec() + ACK_WAIT_MS

func _on_peer_disconnected(id: int) -> void:
	print("[host] peer_disconnected(%d)" % id)

@rpc("any_peer", "call_remote", "reliable")
func ping(_payload: String) -> void:
	# Host shouldn't receive its own ping back — but if it does, log it.
	print("[host] unexpectedly received ping callback")

@rpc("any_peer", "call_remote", "reliable")
func ack(payload: String) -> void:
	print("[host] received ack(payload='%s') from peer %d" % [payload, multiplayer.get_remote_sender_id()])
	if payload == "hello":
		_got_ack = true
		print("[host] PASS")
		get_tree().quit(0)
	else:
		_fail("ack payload mismatch: %s" % payload)

func _env_or_die(name: String) -> String:
	var v := OS.get_environment(name)
	if v.is_empty():
		print("[host] FAIL: env var %s not set" % name)
		get_tree().quit(1)
	return v

func _fail(msg: String) -> void:
	print("[host] FAIL: %s" % msg)
	get_tree().quit(1)

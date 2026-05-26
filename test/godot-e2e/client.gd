extends Node

# Headless Godot client. Connects to the same room as host.gd. The relay
# delivers a `ping` @rpc from the host; the client sends `ack` back. PASS = exit 0.
#
# Loaded via client.tscn as the main scene so the @rpc decorator binds to
# a real Node inside the SceneTree.
#
# Required env vars (set by run-e2e.sh):
#   ZIVA_RELAY_URL         wss://<host>
#   ZIVA_ROOM_ID           shared room id (both processes use the same one)
#   ZIVA_TOKEN_CLIENT      JWT for the client's session

const TIMEOUT_MS := 15000

var _peer: WebSocketMultiplayerPeer
var _got_ping := false
var _start_ms := 0

func _ready() -> void:
	_start_ms = Time.get_ticks_msec()
	var relay_url := _env_or_die("ZIVA_RELAY_URL")
	var room_id := _env_or_die("ZIVA_ROOM_ID")
	var token := _env_or_die("ZIVA_TOKEN_CLIENT")
	var url := "%s/r/%s?token=%s&v=1" % [relay_url, room_id, token]
	print("[client] connecting to %s" % url)

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
		_fail("timeout after %d ms (got_ping=%s)" % [TIMEOUT_MS, str(_got_ping)])
		return

func _on_connected_to_server() -> void:
	print("[client] connected to relay; my unique_id=%d" % multiplayer.get_unique_id())

func _on_connection_failed() -> void:
	_fail("connection_failed")

func _on_server_disconnected() -> void:
	# The host quitting after we ack causes the relay's DEL_PEER which
	# Godot interprets as server_disconnected. If we already PASSed, this
	# is benign; if we haven't, it's a real failure.
	if _got_ping:
		return
	_fail("server_disconnected before completion")

func _on_peer_connected(id: int) -> void:
	if id == 1:
		return
	print("[client] peer_connected(%d)" % id)

func _on_peer_disconnected(id: int) -> void:
	print("[client] peer_disconnected(%d)" % id)

@rpc("any_peer", "call_remote", "reliable")
func ping(payload: String) -> void:
	var sender := multiplayer.get_remote_sender_id()
	print("[client] received ping(payload='%s') from peer %d" % [payload, sender])
	if payload != "hello":
		_fail("ping payload mismatch: %s" % payload)
		return
	_got_ping = true
	ack.rpc_id(sender, "hello")
	# Give the relay a tick to deliver the ack before quitting.
	await get_tree().create_timer(0.5).timeout
	print("[client] PASS")
	get_tree().quit(0)

@rpc("any_peer", "call_remote", "reliable")
func ack(_payload: String) -> void:
	print("[client] unexpectedly received ack")

func _env_or_die(name: String) -> String:
	var v := OS.get_environment(name)
	if v.is_empty():
		print("[client] FAIL: env var %s not set" % name)
		get_tree().quit(1)
	return v

func _fail(msg: String) -> void:
	print("[client] FAIL: %s" % msg)
	get_tree().quit(1)

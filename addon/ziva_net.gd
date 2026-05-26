# ziva_net.gd — autoload singleton wrapping the Ziva multiplayer relay.
#
# Plan-deviation note:
# The Phase-4 spec called for wrapping Godot's WebSocketMultiplayerPeer and
# assigning it to `multiplayer.multiplayer_peer`. That doesn't work against
# our relay because WebSocketMultiplayerPeer expects the server's very first
# packet to be a 4-byte little-endian unique_id (Godot's internal handshake);
# our relay sends a JSON `welcome` envelope per PROTOCOL.md and rejects the
# Godot handshake the client tries to negotiate. Bridging the two protocols
# inside Godot would require a MultiplayerPeerExtension subclass, which is
# substantial code with subtle edge cases.
#
# Instead this autoload owns a plain `WebSocketPeer` and exposes its own
# message-passing API: `send(payload)`, `data_received(from, payload)` and
# the membership signals the spec asks for. Devs use it directly rather than
# via `multiplayer.rpc`. The example scene demonstrates the pattern.
#
# Public API:
#   host_room() -> String          Mint a fresh room id + token, connect.
#                                   Returns the room id, or "" on error.
#   join_room(room_id) -> Error    Mint a token for the room, connect.
#   leave_room() -> void           Close the WebSocket.
#   send(payload) -> Error         Broadcast a payload to every other peer.
#   peer_id() -> int               Our peer id (after welcome). 0 if not yet.
#   current_room_id() -> String    Room id if currently in a room, else "".
#
# Signals:
#   peer_joined(peer_id: int)      A remote peer joined our room.
#   peer_left(peer_id: int)        A remote peer left, or local disconnect.
#   data_received(from: int, payload: Variant)
#                                   A `data` envelope arrived from `from`.
#   room_state_changed(state: int) State transition (see State enum).
#
# Project settings (registered by plugin.gd):
#   ziva_net/relay_url           wss base (default "wss://relay.ziva.sh")
#   ziva_net/token_endpoint      POST url that mints JWTs
#                                (default "https://ziva.sh/api/multiplayer/token")
#   ziva_net/protocol_version    int, the ?v= query param (default 1)
#   ziva_net/api_key             optional Bearer for the token endpoint.
#                                Empty in production; tests populate this.
extends Node

signal peer_joined(peer_id: int)
signal peer_left(peer_id: int)
signal data_received(from: int, payload: Variant)
signal room_state_changed(state: int)

enum State { DISCONNECTED, CONNECTING, CONNECTED }

const _SETTING_RELAY := "ziva_net/relay_url"
const _SETTING_TOKEN_ENDPOINT := "ziva_net/token_endpoint"
const _SETTING_PROTOCOL_VERSION := "ziva_net/protocol_version"
const _SETTING_API_KEY := "ziva_net/api_key"

var _current_room_id: String = ""
var _peer: WebSocketPeer = null
var _peer_id: int = 0
var _last_state: int = State.DISCONNECTED


func _ready() -> void:
	# Defensive: ensure settings exist even when the autoload is loaded
	# without the plugin (e.g. headless tests).
	for entry in [
		[_SETTING_RELAY, "wss://relay.ziva.sh"],
		[_SETTING_TOKEN_ENDPOINT, "https://ziva.sh/api/multiplayer/token"],
		[_SETTING_PROTOCOL_VERSION, 1],
		[_SETTING_API_KEY, ""],
	]:
		if not ProjectSettings.has_setting(entry[0]):
			ProjectSettings.set_setting(entry[0], entry[1])


func host_room() -> String:
	var info := await _mint_token("")
	if info.is_empty():
		return ""
	if _connect_to_room(info.room_id, info.token, info.relay_url, info.protocol_version) != OK:
		return ""
	_current_room_id = info.room_id
	return info.room_id


func join_room(room_id: String) -> Error:
	if room_id.is_empty():
		push_error("ziva_net: join_room requires a non-empty room_id")
		return ERR_INVALID_PARAMETER
	var info := await _mint_token(room_id)
	if info.is_empty():
		return ERR_CANT_CONNECT
	var rc := _connect_to_room(info.room_id, info.token, info.relay_url, info.protocol_version)
	if rc == OK:
		_current_room_id = info.room_id
	return rc


func leave_room() -> void:
	if _peer != null:
		_peer.close()
		_peer = null
	var was_connected := _last_state == State.CONNECTED
	var prev_peer_id := _peer_id
	_current_room_id = ""
	_peer_id = 0
	_set_state(State.DISCONNECTED)
	# Emit peer_left for our own id so listeners can update their UI even
	# though the relay won't tell remote peers about us until our socket
	# actually closes (which they observe via their own peer_leave envelope).
	if was_connected and prev_peer_id != 0:
		peer_left.emit(prev_peer_id)


func send(payload: Variant) -> Error:
	if _peer == null or _last_state != State.CONNECTED:
		push_error("ziva_net: cannot send before connected")
		return ERR_UNCONFIGURED
	var envelope := JSON.stringify({"type": "data", "payload": payload})
	return _peer.send_text(envelope)


func peer_id() -> int:
	return _peer_id


func current_room_id() -> String:
	return _current_room_id


func _process(_delta: float) -> void:
	if _peer == null:
		return
	_peer.poll()
	var ready_state := _peer.get_ready_state()
	match ready_state:
		WebSocketPeer.STATE_CONNECTING:
			_set_state(State.CONNECTING)
		WebSocketPeer.STATE_OPEN:
			_set_state(State.CONNECTED)
		WebSocketPeer.STATE_CLOSING:
			pass
		WebSocketPeer.STATE_CLOSED:
			if _last_state != State.DISCONNECTED:
				var was_connected := _last_state == State.CONNECTED
				var prev_id := _peer_id
				_peer = null
				_peer_id = 0
				_current_room_id = ""
				_set_state(State.DISCONNECTED)
				if was_connected and prev_id != 0:
					peer_left.emit(prev_id)
			return
	# Drain pending packets while the socket is open.
	while _peer != null and _peer.get_available_packet_count() > 0:
		var raw_bytes: PackedByteArray = _peer.get_packet()
		_handle_packet(raw_bytes)


# ---- internals ----

class _TokenInfo:
	var token: String
	var room_id: String
	var relay_url: String
	var protocol_version: int = 1

	func is_empty() -> bool:
		return token.is_empty()


func _mint_token(room_id: String) -> _TokenInfo:
	var endpoint: String = ProjectSettings.get_setting(_SETTING_TOKEN_ENDPOINT)
	var api_key: String = ProjectSettings.get_setting(_SETTING_API_KEY)
	var http := HTTPRequest.new()
	add_child(http)
	var headers: PackedStringArray = ["Content-Type: application/json"]
	if not api_key.is_empty():
		# Some installs accept x-api-key (Better-Auth style), others Bearer.
		# Send both so the SDK works regardless of which the dev's server expects.
		headers.append("x-api-key: " + api_key)
		headers.append("Authorization: Bearer " + api_key)
	var body := JSON.stringify({"room_id": room_id} if not room_id.is_empty() else {})
	var rc := http.request(endpoint, headers, HTTPClient.METHOD_POST, body)
	var result := _TokenInfo.new()
	if rc != OK:
		push_error("ziva_net: HTTPRequest.request() returned %d" % rc)
		http.queue_free()
		return result
	var response: Array = await http.request_completed
	http.queue_free()
	var http_result: int = response[0]
	var status: int = response[1]
	var raw_body: PackedByteArray = response[3]
	if http_result != HTTPRequest.RESULT_SUCCESS:
		push_error("ziva_net: token request failed (result=%d)" % http_result)
		return result
	if status < 200 or status >= 300:
		push_error("ziva_net: token endpoint %s returned HTTP %d: %s" % [endpoint, status, raw_body.get_string_from_utf8()])
		return result
	var parsed: Variant = JSON.parse_string(raw_body.get_string_from_utf8())
	if typeof(parsed) != TYPE_DICTIONARY:
		push_error("ziva_net: token endpoint returned non-JSON body: %s" % raw_body.get_string_from_utf8())
		return result
	var d: Dictionary = parsed
	if not d.has("token") or not d.has("room_id"):
		push_error("ziva_net: token endpoint response missing token/room_id: %s" % d)
		return result
	result.token = d["token"]
	result.room_id = d["room_id"]
	# Server-supplied relay_url overrides the project setting when present —
	# lets ops re-point clients without a new addon release.
	result.relay_url = d.get("relay_url", ProjectSettings.get_setting(_SETTING_RELAY))
	result.protocol_version = int(d.get("protocol_version", ProjectSettings.get_setting(_SETTING_PROTOCOL_VERSION)))
	return result


func _connect_to_room(room_id: String, token: String, relay_url: String, version: int) -> Error:
	if _peer != null:
		_peer.close()
		_peer = null
	var url := "%s/r/%s?token=%s&v=%d" % [
		relay_url.trim_suffix("/"),
		room_id.uri_encode(),
		token.uri_encode(),
		version,
	]
	_peer = WebSocketPeer.new()
	var rc := _peer.connect_to_url(url)
	if rc != OK:
		push_error("ziva_net: WebSocketPeer.connect_to_url returned %d for %s" % [rc, url])
		_peer = null
		return rc
	_peer_id = 0
	_set_state(State.CONNECTING)
	return OK


func _handle_packet(raw_bytes: PackedByteArray) -> void:
	var text := raw_bytes.get_string_from_utf8()
	var parsed: Variant = JSON.parse_string(text)
	if typeof(parsed) != TYPE_DICTIONARY:
		# Binary or non-JSON traffic — surface it as a raw data event with from=0.
		data_received.emit(0, raw_bytes)
		return
	var d: Dictionary = parsed
	var t: String = d.get("type", "")
	match t:
		"welcome":
			_peer_id = int(d.get("peer_id", 0))
		"peer_join":
			peer_joined.emit(int(d.get("peer_id", 0)))
		"peer_leave":
			peer_left.emit(int(d.get("peer_id", 0)))
		"data":
			data_received.emit(int(d.get("from", 0)), d.get("payload"))
		_:
			push_warning("ziva_net: unknown envelope type '%s'" % t)


func _set_state(new_state: int) -> void:
	if new_state == _last_state:
		return
	_last_state = new_state
	room_state_changed.emit(new_state)

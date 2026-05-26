# example_room.gd — minimal multiplayer room demo.
#
# Shows the full ziva_net lifecycle:
#   1. Click [Host] to mint a room. Becomes peer_id=1.
#   2. Share the room id; another client clicks [Join] with that id.
#   3. Click [Say Hi] to broadcast "hi" to every other peer in the room.
extends Control

@onready var host_btn: Button = $V/HostBtn
@onready var join_btn: Button = $V/Join/JoinBtn
@onready var leave_btn: Button = $V/LeaveBtn
@onready var hi_btn: Button = $V/HiBtn
@onready var room_id_edit: LineEdit = $V/Join/RoomIdEdit
@onready var status_label: Label = $V/Status

var _peers: Dictionary[int, bool] = {}


func _ready() -> void:
	host_btn.pressed.connect(_on_host)
	join_btn.pressed.connect(_on_join)
	leave_btn.pressed.connect(_on_leave)
	hi_btn.pressed.connect(_on_say_hi)
	ZivaNet.peer_joined.connect(_on_peer_joined)
	ZivaNet.peer_left.connect(_on_peer_left)
	ZivaNet.data_received.connect(_on_data)
	ZivaNet.room_state_changed.connect(_refresh_status)
	_refresh_status()


func _refresh_status(_state: int = -1) -> void:
	status_label.text = "room=%s id=%d peers=%d" % [
		ZivaNet.current_room_id(), ZivaNet.peer_id(), _peers.size() + 1,
	]


func _on_peer_joined(id: int) -> void:
	_peers[id] = true
	_refresh_status()


func _on_peer_left(id: int) -> void:
	_peers.erase(id)
	_refresh_status()


func _on_data(from: int, payload: Variant) -> void:
	print("[example_room] data from peer %d: %s" % [from, payload])


func _on_host() -> void:
	var rid: String = await ZivaNet.host_room()
	if rid.is_empty():
		status_label.text = "host failed (see console)"
		return
	room_id_edit.text = rid


func _on_join() -> void:
	var rid := room_id_edit.text.strip_edges()
	if rid.is_empty():
		status_label.text = "enter a room id first"
		return
	var err: Error = await ZivaNet.join_room(rid)
	if err != OK:
		status_label.text = "join failed: %d" % err


func _on_leave() -> void:
	_peers.clear()
	ZivaNet.leave_room()


func _on_say_hi() -> void:
	ZivaNet.send({"event": "hi", "from": ZivaNet.peer_id()})

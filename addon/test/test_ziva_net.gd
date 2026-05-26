# test_ziva_net.gd — Phase-4 acceptance test for the ziva_net addon.
#
# Architecture: one parent Godot process orchestrates two child Godot
# processes via OS.create_process. The host child connects with host_room(),
# the join child connects with join_room(<host's room id>). They communicate
# via three temp files in /tmp/ziva_net_test/:
#   room_id.txt   host writes the room id once host_room() returns
#   host.result   host writes "OK" / "FAIL: <msg>" before exit
#   join.result   join writes "OK" / "FAIL: <msg>" before exit
# Files are used (rather than stdout pipes) because OS.create_process()
# returns only a pid and OS.execute() blocks, which would serialise the
# children. Filesystem mtime is finer-grained than Godot's process polling.
#
# The test verifies:
#   1. Both children connect successfully to the staging worker via tokens
#      minted by the local dev token endpoint.
#   2. The join child receives a data payload sent by the host.
#   3. After the host calls leave_room(), the join child observes a
#      `peer_left` signal (delivered by the relay's `peer_leave` envelope —
#      see PROTOCOL.md and worker/src/room-do.ts).
#
# Required env:
#   E2E_USER_API_KEY  seeded basic-tier API key
# Optional env overrides:
#   ZIVA_RELAY_URL          default staging worker
#   ZIVA_TOKEN_ENDPOINT     default http://localhost:3000/api/multiplayer/token
#
# Usage:
#   godot --headless -s addon/test/test_ziva_net.gd
# Internal subprocess modes (spawned by the orchestrator):
#   godot --headless -s addon/test/test_ziva_net.gd -- --role=host
#   godot --headless -s addon/test/test_ziva_net.gd -- --role=join --room=<id>
extends SceneTree

const TMP := "/tmp/ziva_net_test"
const TOKEN_ENDPOINT_DEFAULT := "http://localhost:3000/api/multiplayer/token"
const RELAY_URL_DEFAULT := "wss://ziva-multiplayer-staging.ziva-multiplayer.workers.dev"
const ORCHESTRATOR_TIMEOUT_SECONDS := 45
const HOST_PAYLOAD := "hello-from-host"


func _init() -> void:
	# _init runs before Godot's per-frame TLS cert load. Defer the real work
	# so HTTPS calls succeed on the first frame.
	call_deferred("_dispatch")


func _dispatch() -> void:
	var args := _parse_args()
	match args.get("role", "orchestrator"):
		"orchestrator":
			_run_orchestrator()
		"host":
			_run_host()
		"join":
			_run_join(args.get("room", ""))
		_:
			push_error("unknown role: %s" % args.get("role"))
			quit(2)


func _parse_args() -> Dictionary:
	# Godot returns args after "--" via get_cmdline_user_args().
	var result: Dictionary = {}
	for a in OS.get_cmdline_user_args():
		var s: String = a
		if s.begins_with("--role="):
			result["role"] = s.substr(7)
		elif s.begins_with("--room="):
			result["room"] = s.substr(7)
	return result


# ============================================================================
# Orchestrator
# ============================================================================

func _run_orchestrator() -> void:
	var api_key := OS.get_environment("E2E_USER_API_KEY")
	if api_key.is_empty():
		push_error("E2E_USER_API_KEY env var is required")
		quit(1)
		return
	DirAccess.make_dir_recursive_absolute(TMP)
	for f in ["room_id.txt", "host.result", "join.result"]:
		var p := "%s/%s" % [TMP, f]
		if FileAccess.file_exists(p):
			DirAccess.remove_absolute(p)

	var godot := OS.get_executable_path()
	var script := "addon/test/test_ziva_net.gd"
	var common_args := PackedStringArray(["--headless", "-s", script, "--"])

	prints("[orchestrator] starting host child")
	var host_args := common_args.duplicate()
	host_args.append("--role=host")
	var host_pid := OS.create_process(godot, host_args)
	if host_pid <= 0:
		push_error("failed to spawn host child (pid=%d)" % host_pid)
		quit(1)
		return

	# Wait up to 15s for host to publish its room id.
	var room_id := ""
	var t0 := Time.get_ticks_msec()
	while Time.get_ticks_msec() - t0 < 15_000:
		if FileAccess.file_exists("%s/room_id.txt" % TMP):
			room_id = FileAccess.get_file_as_string("%s/room_id.txt" % TMP).strip_edges()
			if not room_id.is_empty():
				break
		await create_timer(0.1).timeout
	if room_id.is_empty():
		push_error("[orchestrator] host failed to publish room id within 15s")
		OS.kill(host_pid)
		quit(1)
		return
	prints("[orchestrator] host published room=%s" % room_id)

	prints("[orchestrator] starting join child for room=%s" % room_id)
	var join_args := common_args.duplicate()
	join_args.append("--role=join")
	join_args.append("--room=%s" % room_id)
	var join_pid := OS.create_process(godot, join_args)
	if join_pid <= 0:
		push_error("failed to spawn join child (pid=%d)" % join_pid)
		OS.kill(host_pid)
		quit(1)
		return

	# Wait for both children to exit.
	var deadline := Time.get_ticks_msec() + ORCHESTRATOR_TIMEOUT_SECONDS * 1000
	var host_running := true
	var join_running := true
	while (host_running or join_running) and Time.get_ticks_msec() < deadline:
		if host_running and not OS.is_process_running(host_pid):
			host_running = false
		if join_running and not OS.is_process_running(join_pid):
			join_running = false
		await create_timer(0.1).timeout
	if host_running:
		push_error("[orchestrator] host child timed out, killing")
		OS.kill(host_pid)
	if join_running:
		push_error("[orchestrator] join child timed out, killing")
		OS.kill(join_pid)

	var host_result := _read_result("host.result")
	var join_result := _read_result("join.result")
	prints("[orchestrator] host result: %s" % host_result)
	prints("[orchestrator] join result: %s" % join_result)
	var ok := host_result == "OK" and join_result == "OK" and not host_running and not join_running
	prints("[orchestrator] FINAL: %s" % ("PASS" if ok else "FAIL"))
	quit(0 if ok else 1)


func _read_result(filename: String) -> String:
	var p := "%s/%s" % [TMP, filename]
	if not FileAccess.file_exists(p):
		return "(no result file)"
	return FileAccess.get_file_as_string(p).strip_edges()


# ============================================================================
# Host child
# ============================================================================

func _run_host() -> void:
	var net := _instantiate_zivanet()
	if net == null:
		_write_result("host.result", "FAIL: could not instantiate ZivaNet")
		quit(1)
		return

	var got_peer_joined := [false]
	net.peer_joined.connect(func(id: int):
		prints("[host] peer_joined: %d" % id)
		got_peer_joined[0] = true
	)

	prints("[host] minting room")
	var room_id: String = await net.host_room()
	if room_id.is_empty():
		_write_result("host.result", "FAIL: host_room returned empty")
		quit(1)
		return
	prints("[host] room=%s" % room_id)
	var f := FileAccess.open("%s/room_id.txt" % TMP, FileAccess.WRITE)
	f.store_string(room_id)
	f.close()

	# Wait until our peer is in the CONNECTED state before sending.
	var connect_deadline := Time.get_ticks_msec() + 10_000
	while Time.get_ticks_msec() < connect_deadline and net._last_state != net.State.CONNECTED:
		await create_timer(0.05).timeout
	if net._last_state != net.State.CONNECTED:
		_write_result("host.result", "FAIL: host did not reach CONNECTED state")
		quit(1)
		return
	prints("[host] CONNECTED, peer_id=%d" % net.peer_id())

	# Wait for peer_joined (joiner connects + relay broadcasts peer_join).
	if not await _await_flag(got_peer_joined, 20_000):
		_write_result("host.result", "FAIL: timed out waiting for peer_joined")
		quit(1)
		return

	# Broadcast our hello payload.
	prints("[host] sending payload")
	var send_rc: int = net.send(HOST_PAYLOAD)
	if send_rc != OK:
		_write_result("host.result", "FAIL: send returned %d" % send_rc)
		quit(1)
		return

	# Give the joiner a moment to receive before we close.
	await create_timer(1.5).timeout

	prints("[host] leaving room")
	net.leave_room()

	# Give the join child time to observe our disconnect and write its result.
	await create_timer(3.0).timeout

	_write_result("host.result", "OK")
	quit(0)


# ============================================================================
# Join child
# ============================================================================

func _run_join(room_id: String) -> void:
	if room_id.is_empty():
		_write_result("join.result", "FAIL: --room missing")
		quit(1)
		return
	var net := _instantiate_zivanet()
	if net == null:
		_write_result("join.result", "FAIL: could not instantiate ZivaNet")
		quit(1)
		return

	var got_peer_left := [false]
	net.peer_left.connect(func(id: int):
		prints("[join] peer_left: %d" % id)
		got_peer_left[0] = true
	)

	var got_payload := [false]
	net.data_received.connect(func(from: int, payload: Variant):
		prints("[join] data_received from=%d payload=%s" % [from, payload])
		if typeof(payload) == TYPE_STRING and String(payload) == HOST_PAYLOAD:
			got_payload[0] = true
	)

	prints("[join] joining room=%s" % room_id)
	var err: Error = await net.join_room(room_id)
	if err != OK:
		_write_result("join.result", "FAIL: join_room returned %d" % err)
		quit(1)
		return

	# Wait until connected so peer_id() is populated.
	var connect_deadline := Time.get_ticks_msec() + 10_000
	while Time.get_ticks_msec() < connect_deadline and net._last_state != net.State.CONNECTED:
		await create_timer(0.05).timeout
	if net._last_state != net.State.CONNECTED:
		_write_result("join.result", "FAIL: join did not reach CONNECTED state")
		quit(1)
		return
	prints("[join] CONNECTED, peer_id=%d" % net.peer_id())

	# Wait for the host's data payload (up to 15s).
	if not await _await_flag(got_payload, 15_000):
		_write_result("join.result", "FAIL: did not receive host payload")
		quit(1)
		return

	# Wait for peer_left signal (host leaves, relay broadcasts peer_leave).
	if not await _await_flag(got_peer_left, 15_000):
		_write_result("join.result", "FAIL: did not receive peer_left signal")
		quit(1)
		return

	_write_result("join.result", "OK")
	quit(0)


# ============================================================================
# Helpers
# ============================================================================

# Manually load the autoload script as a regular Node so the test runs
# without a project.godot. Same code, just instantiated by hand instead of
# by Godot's autoload bootstrap.
func _instantiate_zivanet() -> Node:
	var script: GDScript = load("res://addon/ziva_net.gd")
	if script == null:
		push_error("could not load res://addon/ziva_net.gd")
		return null
	var node: Node = script.new()
	node.name = "ZivaNet"
	root.add_child(node)
	# Test-time overrides — these mirror the project settings the plugin
	# normally registers, but pointed at staging + the seeded API key.
	var relay := OS.get_environment("ZIVA_RELAY_URL")
	if relay.is_empty():
		relay = RELAY_URL_DEFAULT
	var endpoint := OS.get_environment("ZIVA_TOKEN_ENDPOINT")
	if endpoint.is_empty():
		endpoint = TOKEN_ENDPOINT_DEFAULT
	ProjectSettings.set_setting("ziva_net/relay_url", relay)
	ProjectSettings.set_setting("ziva_net/token_endpoint", endpoint)
	ProjectSettings.set_setting("ziva_net/protocol_version", 1)
	ProjectSettings.set_setting("ziva_net/api_key", OS.get_environment("E2E_USER_API_KEY"))
	return node


func _await_flag(flag_ref: Array, timeout_ms: int) -> bool:
	# flag_ref is a single-element Array used as a mutable closure capture.
	var deadline := Time.get_ticks_msec() + timeout_ms
	while Time.get_ticks_msec() < deadline:
		if flag_ref[0]:
			return true
		await create_timer(0.05).timeout
	return flag_ref[0]


func _write_result(filename: String, text: String) -> void:
	var f := FileAccess.open("%s/%s" % [TMP, filename], FileAccess.WRITE)
	if f == null:
		push_error("failed to open %s for write" % filename)
		return
	f.store_string(text)
	f.close()

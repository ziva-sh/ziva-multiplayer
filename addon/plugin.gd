@tool
extends EditorPlugin

const AUTOLOAD_NAME := "ZivaNet"
const AUTOLOAD_PATH := "res://addons/ziva_net/ziva_net.gd"


func _enter_tree() -> void:
	_register_project_settings()
	add_autoload_singleton(AUTOLOAD_NAME, AUTOLOAD_PATH)


func _exit_tree() -> void:
	remove_autoload_singleton(AUTOLOAD_NAME)


func _register_project_settings() -> void:
	_define_setting("ziva_net/relay_url", TYPE_STRING, "wss://relay.ziva.sh")
	_define_setting("ziva_net/token_endpoint", TYPE_STRING, "https://ziva.sh/api/multiplayer/token")
	_define_setting("ziva_net/protocol_version", TYPE_INT, 1)
	_define_setting("ziva_net/api_key", TYPE_STRING, "")


func _define_setting(name: String, type: int, default_value: Variant) -> void:
	if not ProjectSettings.has_setting(name):
		ProjectSettings.set_setting(name, default_value)
	ProjectSettings.set_initial_value(name, default_value)
	ProjectSettings.add_property_info({
		"name": name,
		"type": type,
		"hint": PROPERTY_HINT_NONE,
	})

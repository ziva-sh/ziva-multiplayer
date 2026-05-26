// Godot WebSocketMultiplayerPeer wire protocol — binary.
//
// The Worker speaks the same on-wire protocol as a Godot SceneMultiplayer
// host so vanilla Godot clients (`WebSocketMultiplayerPeer.create_client`)
// can connect and exchange `@rpc` calls with no addon. Reference:
//   modules/websocket/websocket_multiplayer_peer.cpp (handshake)
//   modules/multiplayer/scene_multiplayer.cpp (RELAY / ADD_PEER / DEL_PEER)
//
// Layout summary:
//   Handshake: server sends 4 raw bytes = LE int32 peer_id (must be >= 2).
//   After handshake every packet starts with a command byte whose low 3 bits
//   are the NETWORK_COMMAND_*. We only care about NETWORK_COMMAND_SYS — the
//   high bits encode the transfer channel which is irrelevant over WebSocket
//   (TCP is reliable+ordered) and we forward them verbatim.

export const PROTOCOL_VERSION = 1;

// NETWORK_COMMAND_* — low 3 bits of the command byte (CMD_MASK = 0x7).
export const NETWORK_COMMAND_REMOTE_CALL = 0;
export const NETWORK_COMMAND_SIMPLIFY_PATH = 1;
export const NETWORK_COMMAND_CONFIRM_PATH = 2;
export const NETWORK_COMMAND_RAW = 3;
export const NETWORK_COMMAND_SPAWN = 4;
export const NETWORK_COMMAND_DESPAWN = 5;
export const NETWORK_COMMAND_SYNC = 6;
export const NETWORK_COMMAND_SYS = 7;
export const CMD_MASK = 0x7;

// SYS_COMMAND_* — second byte when command is NETWORK_COMMAND_SYS.
export const SYS_COMMAND_AUTH = 0;
export const SYS_COMMAND_ADD_PEER = 1;
export const SYS_COMMAND_DEL_PEER = 2;
export const SYS_COMMAND_RELAY = 3;

// SYS commands carry [cmd, sub_cmd, int32 LE arg] = 6 bytes (RELAY adds a
// trailing inner-packet payload after that).
export const SYS_CMD_SIZE = 6;

// First server-to-client packet: 4 raw bytes = LE int32 of the assigned id.
// Source: websocket_multiplayer_peer.cpp ~line 235 — `unique_id = *((int32_t *)in_buffer)`.
export function assignedPeerId(id: number): Uint8Array {
  const buf = new Uint8Array(4);
  new DataView(buf.buffer).setInt32(0, id, /* littleEndian */ true);
  return buf;
}

function sysWithPeerArg(subCommand: number, peerId: number): Uint8Array {
  const buf = new Uint8Array(SYS_CMD_SIZE);
  buf[0] = NETWORK_COMMAND_SYS;
  buf[1] = subCommand;
  new DataView(buf.buffer).setInt32(2, peerId, /* littleEndian */ true);
  return buf;
}

export function addPeerPacket(peerId: number): Uint8Array {
  return sysWithPeerArg(SYS_COMMAND_ADD_PEER, peerId);
}

export function delPeerPacket(peerId: number): Uint8Array {
  return sysWithPeerArg(SYS_COMMAND_DEL_PEER, peerId);
}

// Check whether an incoming binary frame is `NETWORK_COMMAND_SYS | SYS_COMMAND_RELAY`.
// Ignores the channel bits in the command byte's high 5 bits.
export function isRelayPacket(view: Uint8Array): boolean {
  return (
    view.byteLength >= SYS_CMD_SIZE + 1 &&
    (view[0] & CMD_MASK) === NETWORK_COMMAND_SYS &&
    view[1] === SYS_COMMAND_RELAY
  );
}

// Check whether an incoming binary frame is a SYS_COMMAND_AUTH packet.
// Used to pass-through application-level auth handshakes unchanged.
export function isAuthPacket(view: Uint8Array): boolean {
  return (
    view.byteLength >= 2 &&
    (view[0] & CMD_MASK) === NETWORK_COMMAND_SYS &&
    view[1] === SYS_COMMAND_AUTH
  );
}

export interface RelayHeader {
  // 0 = broadcast to all (except sender), positive N = unicast to peer N,
  // -N = broadcast to all (except sender) excluding peer N. Source:
  // scene_multiplayer.cpp lines 321-340.
  targetPeer: number;
  // The packet bytes that follow the relay header (inner SceneMultiplayer
  // packet — application-level RPC / SYNC / etc). Worker treats as opaque.
  innerPayload: Uint8Array;
}

export function parseRelayHeader(buffer: ArrayBuffer | Uint8Array): RelayHeader {
  const view =
    buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  if (view.byteLength < SYS_CMD_SIZE + 1) {
    throw new Error(
      `relay packet too small: ${view.byteLength} bytes (need at least ${SYS_CMD_SIZE + 1})`,
    );
  }
  const dv = new DataView(view.buffer, view.byteOffset, view.byteLength);
  const targetPeer = dv.getInt32(2, /* littleEndian */ true);
  const innerPayload = view.subarray(SYS_CMD_SIZE);
  return { targetPeer, innerPayload };
}

// Build the relay packet the server sends to its targets. The on-wire layout
// here mirrors what a Godot host would write at scene_multiplayer.cpp:313-318:
//   [0] NETWORK_COMMAND_SYS (channel bits inherited from incoming packet)
//   [1] SYS_COMMAND_RELAY
//   [2..5] int32 LE = ORIGINAL sender's peer_id (so the receiver knows who sent it)
//   [6..]  inner payload from the incoming packet (opaque)
//
// We preserve the original command byte's high bits (channel) so the
// receiver's transfer-channel routing keeps working.
export function rewriteRelaySender(
  incoming: ArrayBuffer | Uint8Array,
  senderId: number,
): Uint8Array {
  const src =
    incoming instanceof Uint8Array ? incoming : new Uint8Array(incoming);
  if (src.byteLength < SYS_CMD_SIZE + 1) {
    throw new Error(
      `relay packet too small for sender rewrite: ${src.byteLength} bytes`,
    );
  }
  // Copy so we never mutate the caller's buffer (Cloudflare reuses).
  const out = new Uint8Array(src.byteLength);
  out.set(src);
  out[0] = src[0]; // preserve channel bits + SYS command
  out[1] = SYS_COMMAND_RELAY;
  new DataView(out.buffer).setInt32(2, senderId, /* littleEndian */ true);
  // Inner payload (bytes 6..) is unchanged — opaque to us.
  return out;
}

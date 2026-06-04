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

// ---- Ziva relay control frames (NOT part of Godot's protocol) ----
//
// These are a Ziva extension: a per-room persistent world blob the game saves
// to / loads from the room's Durable Object (survives "everyone left"). They
// ride the same binary WebSocket channel as the Godot traffic, so the first
// byte must be unambiguous against every byte a real Godot client can send.
//
// A Godot command byte is `cmd | (channel << 5)` — cmd in the low 3 bits
// (0..7), channel in the high 5 bits. The maximum legal value is therefore
// `7 | (31 << 5) == 0xFF`, BUT the largest *meaningful* command byte a Godot
// client emits is a SYS packet (cmd == 7) on some channel: `7 | (ch << 5)`,
// whose low 3 bits are always 7. We deliberately pick control bytes in the
// 0xF1..0xF3 window whose low 3 bits are 1/2/3 — i.e. they are never SYS — so
// `(byte & CMD_MASK) === NETWORK_COMMAND_SYS` can never be true for them and
// the relay's SYS routing (relay/auth/add/del) cannot collide with these.
// The relay matches these on the WHOLE byte before any Godot parsing.
export const WORLD_SAVE = 0xf1; // client → server: [0xF1][...blob]  (save a world)
export const WORLD_LOAD = 0xf2; // server → client: [0xF2][...blob]  (here is the world)
export const WORLD_SAVE_ACK = 0xf3; // server → client: [0xF3]         (save committed)

// True iff `firstByte` is one of the Ziva control opcodes above. Used to keep
// the collision invariant in one place: a SYS command byte (low 3 bits == 7)
// can never satisfy this, so world frames and Godot SYS frames are disjoint.
export function isWorldControlByte(firstByte: number): boolean {
  return (
    firstByte === WORLD_SAVE ||
    firstByte === WORLD_LOAD ||
    firstByte === WORLD_SAVE_ACK
  );
}

// ---- Ziva shared-state control frames (NOT part of Godot's protocol) ----
//
// A live per-key shared document the relay's Durable Object owns, orders, and
// persists — the no-host authoritative model (turn-based / card / persistent
// world). This is DISTINCT from both the Godot WSMP relay path (host-client
// games never see these frames; the DO handles them itself, never forwards
// them) and from the world-store whole-blob save/load: this is per-key live
// state with a DO-assigned monotonic revision, not one opaque snapshot.
//
// The DO is the single writer-orderer. It ORDERS writes (so peers can't desync)
// but does NOT validate game rules — it prevents desync, not cheating. A client
// that lies about game logic still gets a consistent, ordered, durable doc;
// authority over *what is a legal move* belongs to the game, not the relay.
//
// Opcode choice mirrors the world frames: a Godot command byte's low 3 bits are
// the NETWORK_COMMAND_* (max meaningful is SYS == 7). We pick 0xF4/0xF5/0xF6,
// whose low 3 bits are 4/5/6 — never 7 — so `(byte & CMD_MASK) === SYS` is
// impossible for them and they can't collide with relay/auth/add/del routing.
// They are also distinct from the world opcodes 0xF1–0xF3. The relay matches
// these on the WHOLE first byte, before any Godot parsing.
export const STATE_SET = 0xf4; // client → DO:   [0xF4][u16 keyLen][key][value]
export const STATE_UPDATE = 0xf5; // DO → all:    [0xF5][u32 rev][u16 keyLen][key][value]
export const STATE_SNAPSHOT = 0xf6; // DO → joiner: [0xF6][u32 count]{ [u32 rev][u16 keyLen][key][u32 valLen][value] }*

// True iff `firstByte` is one of the shared-state opcodes above. Kept parallel
// to isWorldControlByte so the disjoint-from-SYS invariant lives in one place
// per opcode family. A SYS byte (low 3 bits == 7) can never satisfy this.
export function isSharedStateControlByte(firstByte: number): boolean {
  return (
    firstByte === STATE_SET ||
    firstByte === STATE_UPDATE ||
    firstByte === STATE_SNAPSHOT
  );
}

// Max key length so a malformed/hostile frame can't claim a 4GB key. Keys are
// short identifiers ("score", "player:2:hp"); 1 KiB is generous headroom.
export const STATE_KEY_MAX_BYTES = 1024;

// A single shared-state write/update on the wire. `value` is opaque bytes the
// relay never interprets (the game owns its meaning).
export interface SharedStateFrame {
  key: string;
  value: Uint8Array;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

// Encode a client→DO STATE_SET frame: [0xF4][u16 LE keyLen][key bytes][value].
// keyLen is u16 so it can't be confused with the value; the value runs to the
// end of the frame (no length prefix needed — it's the tail).
export function encodeStateSet(key: string, value: Uint8Array): Uint8Array {
  const keyBytes = textEncoder.encode(key);
  if (keyBytes.byteLength === 0) {
    throw new Error("STATE_SET key must be non-empty");
  }
  if (keyBytes.byteLength > STATE_KEY_MAX_BYTES) {
    throw new Error(
      `STATE_SET key ${keyBytes.byteLength} bytes exceeds max ${STATE_KEY_MAX_BYTES}`,
    );
  }
  const out = new Uint8Array(1 + 2 + keyBytes.byteLength + value.byteLength);
  out[0] = STATE_SET;
  new DataView(out.buffer).setUint16(1, keyBytes.byteLength, /* LE */ true);
  out.set(keyBytes, 3);
  out.set(value, 3 + keyBytes.byteLength);
  return out;
}

// Decode a STATE_SET frame. Throws (never returns a partial/garbage result) on
// any structural violation so a malformed frame fails loud + debuggable rather
// than committing a corrupt key/value.
export function decodeStateSet(frame: Uint8Array): SharedStateFrame {
  if (frame.byteLength < 3) {
    throw new Error(
      `STATE_SET frame too small: ${frame.byteLength} bytes (need ≥3 for opcode+keyLen)`,
    );
  }
  if (frame[0] !== STATE_SET) {
    throw new Error(
      `STATE_SET frame has wrong opcode 0x${frame[0].toString(16)}`,
    );
  }
  const dv = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  const keyLen = dv.getUint16(1, /* LE */ true);
  if (keyLen === 0) {
    throw new Error("STATE_SET key length is zero");
  }
  if (keyLen > STATE_KEY_MAX_BYTES) {
    throw new Error(
      `STATE_SET key length ${keyLen} exceeds max ${STATE_KEY_MAX_BYTES}`,
    );
  }
  if (frame.byteLength < 3 + keyLen) {
    throw new Error(
      `STATE_SET frame truncated: keyLen ${keyLen} but only ${frame.byteLength - 3} bytes after header`,
    );
  }
  // fatal:true decoder throws on invalid UTF-8 — a non-text key is a bug.
  const key = textDecoder.decode(frame.subarray(3, 3 + keyLen));
  // Copy the value out: the incoming frame's backing buffer is reused by the
  // runtime, and we persist this asynchronously.
  const value = frame.slice(3 + keyLen);
  return { key, value };
}

// Encode a DO→all STATE_UPDATE broadcast:
//   [0xF5][u32 LE rev][u16 LE keyLen][key bytes][value tail]
// The committed `rev` is what lets every peer (incl. the original sender) apply
// last-writer-wins consistently: a peer ignores an update whose rev is ≤ the
// rev it already applied for that key.
export function encodeStateUpdate(
  key: string,
  value: Uint8Array,
  rev: number,
): Uint8Array {
  const keyBytes = textEncoder.encode(key);
  const out = new Uint8Array(1 + 4 + 2 + keyBytes.byteLength + value.byteLength);
  const dv = new DataView(out.buffer);
  out[0] = STATE_UPDATE;
  dv.setUint32(1, rev, /* LE */ true);
  dv.setUint16(5, keyBytes.byteLength, /* LE */ true);
  out.set(keyBytes, 7);
  out.set(value, 7 + keyBytes.byteLength);
  return out;
}

// Decode a STATE_UPDATE frame (used by clients/tests). Throws on malformed.
export function decodeStateUpdate(
  frame: Uint8Array,
): SharedStateFrame & { rev: number } {
  if (frame.byteLength < 7) {
    throw new Error(
      `STATE_UPDATE frame too small: ${frame.byteLength} bytes (need ≥7)`,
    );
  }
  if (frame[0] !== STATE_UPDATE) {
    throw new Error(
      `STATE_UPDATE frame has wrong opcode 0x${frame[0].toString(16)}`,
    );
  }
  const dv = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  const rev = dv.getUint32(1, /* LE */ true);
  const keyLen = dv.getUint16(5, /* LE */ true);
  if (frame.byteLength < 7 + keyLen) {
    throw new Error(
      `STATE_UPDATE frame truncated: keyLen ${keyLen} but only ${frame.byteLength - 7} bytes after header`,
    );
  }
  const key = textDecoder.decode(frame.subarray(7, 7 + keyLen));
  const value = frame.slice(7 + keyLen);
  return { key, value, rev };
}

// One key's current state as handed to a joiner inside a STATE_SNAPSHOT.
export interface SharedStateEntry {
  key: string;
  value: Uint8Array;
  rev: number;
}

// Encode the full-state STATE_SNAPSHOT handed to a joiner:
//   [0xF6][u32 LE count]  then per entry:
//     [u32 LE rev][u16 LE keyLen][key bytes][u32 LE valLen][value bytes]
// Unlike STATE_UPDATE, every field here is length-prefixed (including the
// value) because multiple entries are packed back-to-back in one frame.
export function encodeStateSnapshot(entries: SharedStateEntry[]): Uint8Array {
  let total = 1 + 4; // opcode + count
  const keyByteList: Uint8Array[] = [];
  for (const e of entries) {
    const kb = textEncoder.encode(e.key);
    keyByteList.push(kb);
    total += 4 + 2 + kb.byteLength + 4 + e.value.byteLength;
  }
  const out = new Uint8Array(total);
  const dv = new DataView(out.buffer);
  out[0] = STATE_SNAPSHOT;
  dv.setUint32(1, entries.length, /* LE */ true);
  let off = 5;
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    const kb = keyByteList[i];
    dv.setUint32(off, e.rev, /* LE */ true);
    off += 4;
    dv.setUint16(off, kb.byteLength, /* LE */ true);
    off += 2;
    out.set(kb, off);
    off += kb.byteLength;
    dv.setUint32(off, e.value.byteLength, /* LE */ true);
    off += 4;
    out.set(e.value, off);
    off += e.value.byteLength;
  }
  return out;
}

// Decode a STATE_SNAPSHOT frame (used by clients/tests). Throws on any
// structural inconsistency — a truncated snapshot must never be read as if a
// missing key simply didn't exist.
export function decodeStateSnapshot(frame: Uint8Array): SharedStateEntry[] {
  if (frame.byteLength < 5) {
    throw new Error(
      `STATE_SNAPSHOT frame too small: ${frame.byteLength} bytes (need ≥5)`,
    );
  }
  if (frame[0] !== STATE_SNAPSHOT) {
    throw new Error(
      `STATE_SNAPSHOT frame has wrong opcode 0x${frame[0].toString(16)}`,
    );
  }
  const dv = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
  const count = dv.getUint32(1, /* LE */ true);
  const entries: SharedStateEntry[] = [];
  let off = 5;
  for (let i = 0; i < count; i++) {
    if (off + 6 > frame.byteLength) {
      throw new Error(
        `STATE_SNAPSHOT truncated reading entry ${i} header at offset ${off}`,
      );
    }
    const rev = dv.getUint32(off, /* LE */ true);
    off += 4;
    const keyLen = dv.getUint16(off, /* LE */ true);
    off += 2;
    if (off + keyLen + 4 > frame.byteLength) {
      throw new Error(
        `STATE_SNAPSHOT truncated reading key/valLen for entry ${i}`,
      );
    }
    const key = textDecoder.decode(frame.subarray(off, off + keyLen));
    off += keyLen;
    const valLen = dv.getUint32(off, /* LE */ true);
    off += 4;
    if (off + valLen > frame.byteLength) {
      throw new Error(
        `STATE_SNAPSHOT truncated reading value (${valLen} bytes) for entry ${i}`,
      );
    }
    const value = frame.slice(off, off + valLen);
    off += valLen;
    entries.push({ key, value, rev });
  }
  return entries;
}

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

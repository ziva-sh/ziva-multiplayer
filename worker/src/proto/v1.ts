// v1 wire protocol.
//
// Control frames are JSON. Binary frames pass through unmodified.
// See PROTOCOL.md at the repo root for the full spec.

export const PROTOCOL_VERSION = 1;

export interface WelcomeEnvelope {
  type: "welcome";
  peer_id: number;
  protocol_version: 1;
}

export interface DataEnvelope {
  type: "data";
  from: number;
  payload: unknown;
}

export type ServerEnvelope = WelcomeEnvelope | DataEnvelope;

export function welcome(peerId: number): string {
  const env: WelcomeEnvelope = {
    type: "welcome",
    peer_id: peerId,
    protocol_version: PROTOCOL_VERSION,
  };
  return JSON.stringify(env);
}

// Tag an inbound JSON `data` frame with the sender's peer id, then re-encode.
// Returns the raw text unchanged if it isn't a JSON `data` envelope — the
// relay still forwards it (callers decided JSON shape on their own).
export function tagSender(raw: string, peerId: number): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return raw;
  }
  if (
    typeof parsed === "object" &&
    parsed !== null &&
    (parsed as { type?: unknown }).type === "data"
  ) {
    (parsed as DataEnvelope).from = peerId;
    return JSON.stringify(parsed);
  }
  return raw;
}

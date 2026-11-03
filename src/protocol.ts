/**
 * MindooDB JVM bridge wire protocol.
 *
 * The bridge reuses the shape of the MindooDB App Bridge (see
 * `mindoodb-app-sdk/src/types.ts`) but adapts it for a WebSocket transport:
 *
 * - Control / RPC / events travel as JSON text frames.
 * - Attachment bytes travel as binary WebSocket frames prefixed with a small
 *   header so multiple streams multiplex over one connection:
 *
 *       byte[0]    = 0x01              (binary chunk marker)
 *       byte[1..4] = uint32 streamId  (big-endian)
 *       byte[5..8] = uint32 seq       (big-endian)
 *       byte[9..]  = payload bytes
 *
 * One WebSocket connection carries exactly one user session. Multiple users =
 * multiple connections (each backed by an isolated Node child process).
 */

export const PROTOCOL = "mindoodb-bridge" as const;

/** Marker byte for a framed binary attachment chunk. */
export const BINARY_CHUNK_MARKER = 0x01;

/** Fixed binary header length: marker(1) + streamId(4) + seq(4). */
export const BINARY_HEADER_LENGTH = 9;

// ---------------------------------------------------------------------------
// JSON frames
// ---------------------------------------------------------------------------

export interface RpcRequest {
  protocol: typeof PROTOCOL;
  kind: "request";
  id: string;
  /** Present on every frame after the session is open; `null` for `hello`/`session.open`. */
  sessionToken?: string | null;
  method: string;
  params: unknown;
}

export interface RpcSuccess {
  protocol: typeof PROTOCOL;
  kind: "success";
  id: string;
  result: unknown;
}

export interface BridgeErrorPayload {
  code: string;
  message: string;
}

export interface RpcError {
  protocol: typeof PROTOCOL;
  kind: "error";
  id: string;
  error: BridgeErrorPayload;
}

/** Host -> client push (live query results, view change notifications, changefeed). */
export interface EventFrame {
  protocol: typeof PROTOCOL;
  kind: "event";
  subscriptionId: string;
  event: string;
  payload: unknown;
}

/** Host -> client: an attachment read stream was opened and metadata is known. */
export interface StreamOpenFrame {
  protocol: typeof PROTOCOL;
  kind: "stream-open";
  streamId: number;
  meta: { size?: number; mimeType?: string; fileName?: string };
}

/** Flow-control credit (either direction): allow the peer to send N more chunks. */
export interface StreamCreditFrame {
  protocol: typeof PROTOCOL;
  kind: "stream-credit";
  streamId: number;
  credits: number;
}

/** A stream finished successfully (no more binary frames will follow). */
export interface StreamEndFrame {
  protocol: typeof PROTOCOL;
  kind: "stream-end";
  streamId: number;
}

/** A stream failed. */
export interface StreamErrorFrame {
  protocol: typeof PROTOCOL;
  kind: "stream-error";
  streamId: number;
  error: BridgeErrorPayload;
}

export type ClientFrame = RpcRequest;

export type ServerFrame =
  | RpcSuccess
  | RpcError
  | EventFrame
  | StreamOpenFrame
  | StreamCreditFrame
  | StreamEndFrame
  | StreamErrorFrame;

export type AnyJsonFrame = ClientFrame | ServerFrame;

// ---------------------------------------------------------------------------
// Method names (backend-relevant subset of the App Bridge surface)
// ---------------------------------------------------------------------------

export const Methods = {
  /** Connection auth: `{ bootstrapToken }` -> `{ ok: true }`. */
  hello: "hello",

  // Bootstrap provisioning (no session; bootstrap-token authorized).
  tenantCreate: "tenant.create",
  identityCreateUser: "identity.createUser",
  identityCreateJoinRequest: "identity.createJoinRequest",
  identityJoinTenant: "identity.joinTenant",

  sessionOpen: "session.open",
  sessionClose: "session.close",
  sessionRefresh: "session.refresh",
  sessionAwaitIndexReady: "session.awaitIndexReady",
  sessionListDatabases: "session.listDatabases",
  sessionOpenDatabase: "session.openDatabase",
  sessionSetServerUrl: "session.setServerUrl",
  sessionPublishToServer: "session.publishToServer",

  tenantApproveJoinRequest: "tenant.approveJoinRequest",

  keybagCreateDocKey: "keybag.createDocKey",
  keybagListKeys: "keybag.listKeys",
  keybagPersist: "keybag.persist",

  docGet: "documents.get",
  docList: "documents.list",
  docHeadCursor: "documents.getHeadCursor",
  docCreate: "documents.create",
  docCreateMany: "documents.createMany",
  docUpdate: "documents.update",
  docDelete: "documents.delete",
  docDeleteMany: "documents.deleteMany",
  docListDeleted: "documents.listDeleted",
  docUndelete: "documents.undelete",
  docListHistory: "documents.listHistory",
  docGetAtRevision: "documents.getAtRevision",

  query: "db.query",
  liveQueryStart: "db.liveQuery.start",
  liveQueryStop: "db.liveQuery.stop",

  fulltextGetSetup: "fulltext.getSetup",
  fulltextSetSetup: "fulltext.setSetup",

  viewOpenNavigator: "view.openNavigator",
  viewCreateNavigator: "view.createNavigator",
  navCall: "nav.call",
  navSubscribe: "nav.subscribe",
  navUnsubscribe: "nav.unsubscribe",
  navDispose: "nav.dispose",

  attachList: "attachments.list",
  attachRemove: "attachments.remove",
  attachOpenRead: "attachments.openReadStream",
  attachOpenWrite: "attachments.openWriteStream",
  attachCloseWrite: "attachments.closeWriteStream",
  attachAbort: "attachments.abortStream",
  streamCreditRpc: "stream.credit",

  changesIterateSince: "changes.iterateSince",
} as const;

export type MethodName = (typeof Methods)[keyof typeof Methods];

// ---------------------------------------------------------------------------
// Stable error codes
// ---------------------------------------------------------------------------

export const ErrorCodes = {
  NotAuthorized: "NotAuthorized",
  SessionClosed: "SessionClosed",
  NotFound: "NotFound",
  Conflict: "Conflict",
  IndexNotReady: "IndexNotReady",
  BadRequest: "BadRequest",
  FulltextNotEnabled: "FulltextNotEnabled",
  Internal: "Internal",
} as const;

export type ErrorCode = (typeof ErrorCodes)[keyof typeof ErrorCodes];

export class BridgeError extends Error {
  constructor(
    public readonly code: ErrorCode,
    message: string,
  ) {
    super(message);
    this.name = "BridgeError";
  }
}

// ---------------------------------------------------------------------------
// Binary framing helpers
// ---------------------------------------------------------------------------

/** Encode a binary attachment chunk with its multiplexing header. */
export function encodeBinaryChunk(streamId: number, seq: number, payload: Uint8Array): Buffer {
  const out = Buffer.allocUnsafe(BINARY_HEADER_LENGTH + payload.length);
  out[0] = BINARY_CHUNK_MARKER;
  out.writeUInt32BE(streamId >>> 0, 1);
  out.writeUInt32BE(seq >>> 0, 5);
  Buffer.from(payload.buffer, payload.byteOffset, payload.byteLength).copy(out, BINARY_HEADER_LENGTH);
  return out;
}

export interface DecodedBinaryChunk {
  streamId: number;
  seq: number;
  payload: Buffer;
}

/** Decode a framed binary attachment chunk; returns null if the header is invalid. */
export function decodeBinaryChunk(data: Buffer): DecodedBinaryChunk | null {
  if (data.length < BINARY_HEADER_LENGTH || data[0] !== BINARY_CHUNK_MARKER) {
    return null;
  }
  return {
    streamId: data.readUInt32BE(1),
    seq: data.readUInt32BE(5),
    payload: data.subarray(BINARY_HEADER_LENGTH),
  };
}

export function makeSuccess(id: string, result: unknown): RpcSuccess {
  return { protocol: PROTOCOL, kind: "success", id, result };
}

export function makeError(id: string, code: ErrorCode, message: string): RpcError {
  return { protocol: PROTOCOL, kind: "error", id, error: { code, message } };
}

export function makeEvent(subscriptionId: string, event: string, payload: unknown): EventFrame {
  return { protocol: PROTOCOL, kind: "event", subscriptionId, event, payload };
}

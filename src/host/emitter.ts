import type { ErrorCode } from "../protocol";

/**
 * Sink the dispatcher uses to push async traffic (events + attachment stream
 * frames) back toward the client. Implemented by the session child, which
 * forwards to the supervisor over IPC and on to the WebSocket.
 */
export interface Emitter {
  sendEvent(subscriptionId: string, event: string, payload: unknown): void;
  sendStreamOpen(streamId: number, meta: { size?: number; mimeType?: string; fileName?: string }): void;
  sendStreamCredit(streamId: number, credits: number): void;
  sendStreamEnd(streamId: number): void;
  sendStreamError(streamId: number, code: ErrorCode, message: string): void;
  sendBinaryChunk(streamId: number, seq: number, payload: Uint8Array): void;
}

import { BridgeError, ErrorCodes } from "../protocol";
import type { Emitter } from "./emitter";
import { projectAttachment, type AttachmentInfoDTO } from "./projections";

/** Default flow-control window (number of in-flight chunks). */
const DEFAULT_CREDITS = 16;

interface StreamDoc {
  getAttachments(): Array<{ attachmentId: string; fileName: string; mimeType: string; size?: number }>;
  getHeads(): string[];
  streamAttachment(attachmentId: string, startOffset?: number): AsyncGenerator<Uint8Array>;
}

interface StreamDb {
  getDocument(docId: string): Promise<StreamDoc>;
  changeDoc(doc: StreamDoc, fn: (d: StreamStreamableDoc) => void | Promise<void>): Promise<void>;
}

interface StreamStreamableDoc extends StreamDoc {
  addAttachmentStream(
    dataStream: AsyncIterable<Uint8Array>,
    fileName: string,
    mimeType: string,
  ): Promise<{ attachmentId: string; fileName: string; mimeType: string; size?: number }>;
  removeAttachment(attachmentId: string): Promise<void>;
}

class ReadStream {
  credits = 0;
  seq = 0;
  done = false;
  private pumping = false;

  constructor(
    readonly streamId: number,
    private readonly gen: AsyncGenerator<Uint8Array>,
    private readonly emitter: Emitter,
    private readonly onFinished: () => void,
  ) {}

  grantCredits(n: number): void {
    this.credits += Math.max(0, n);
    void this.pump();
  }

  private async pump(): Promise<void> {
    if (this.pumping || this.done) return;
    this.pumping = true;
    try {
      while (this.credits > 0 && !this.done) {
        const next = await this.gen.next();
        if (next.done) {
          this.done = true;
          this.emitter.sendStreamEnd(this.streamId);
          this.onFinished();
          break;
        }
        this.emitter.sendBinaryChunk(this.streamId, this.seq++, next.value);
        this.credits--;
      }
    } catch (err) {
      this.done = true;
      this.emitter.sendStreamError(this.streamId, ErrorCodes.Internal, String((err as Error)?.message ?? err));
      this.onFinished();
    } finally {
      this.pumping = false;
    }
  }

  async abort(): Promise<void> {
    this.done = true;
    try {
      await this.gen.return?.(undefined);
    } catch {
      /* ignore */
    }
  }
}

class WriteStream {
  readonly chunks: Array<{ seq: number; payload: Uint8Array }> = [];
  ended = false;

  constructor(
    readonly streamId: number,
    readonly dbId: string,
    readonly docId: string,
    readonly fileName: string,
    readonly mimeType: string,
  ) {}

  addChunk(seq: number, payload: Uint8Array): void {
    if (this.ended) throw new BridgeError(ErrorCodes.BadRequest, "Write stream already closed");
    this.chunks.push({ seq, payload });
  }

  orderedIterable(): AsyncIterable<Uint8Array> {
    const ordered = [...this.chunks].sort((a, b) => a.seq - b.seq).map((c) => c.payload);
    return {
      async *[Symbol.asyncIterator]() {
        for (const chunk of ordered) yield chunk;
      },
    };
  }
}

/** Owns attachment read/write streams for one user session. */
export class StreamRegistry {
  private nextId = 1;
  private readonly reads = new Map<number, ReadStream>();
  private readonly writes = new Map<number, WriteStream>();

  constructor(
    private readonly emitter: Emitter,
    private readonly getDb: (dbId: string) => StreamDb,
  ) {}

  private resolveAttachmentId(
    doc: StreamDoc,
    ref: { attachmentId?: string; fileName?: string },
  ): { attachmentId: string; mimeType: string; size: number } | null {
    const attachments = doc.getAttachments();
    const match = ref.attachmentId
      ? attachments.find((a) => a.attachmentId === ref.attachmentId)
      : attachments.find((a) => a.fileName === ref.fileName);
    if (!match) return null;
    return { attachmentId: match.attachmentId, mimeType: match.mimeType, size: match.size ?? 0 };
  }

  async openRead(params: {
    dbId: string;
    docId: string;
    fileName?: string;
    attachmentId?: string;
    startOffset?: number;
  }): Promise<{ streamId: number; size: number; mimeType: string }> {
    const db = this.getDb(params.dbId);
    const doc = await db.getDocument(params.docId);
    const resolved = this.resolveAttachmentId(doc, params);
    if (!resolved) {
      throw new BridgeError(ErrorCodes.NotFound, `Attachment not found on ${params.docId}`);
    }
    const streamId = this.nextId++;
    const gen = doc.streamAttachment(resolved.attachmentId, params.startOffset ?? 0);
    const stream = new ReadStream(streamId, gen, this.emitter, () => this.reads.delete(streamId));
    this.reads.set(streamId, stream);
    // Prime the flow with an initial credit window so slow clients still start.
    stream.grantCredits(DEFAULT_CREDITS);
    return { streamId, size: resolved.size, mimeType: resolved.mimeType };
  }

  grantReadCredits(streamId: number, credits: number): void {
    this.reads.get(streamId)?.grantCredits(credits);
  }

  async openWrite(params: {
    dbId: string;
    docId: string;
    fileName: string;
    mimeType: string;
  }): Promise<{ streamId: number; credits: number }> {
    // Ensure the target document exists before accepting bytes.
    await this.getDb(params.dbId).getDocument(params.docId);
    const streamId = this.nextId++;
    this.writes.set(
      streamId,
      new WriteStream(streamId, params.dbId, params.docId, params.fileName, params.mimeType),
    );
    return { streamId, credits: DEFAULT_CREDITS };
  }

  acceptWriteChunk(streamId: number, seq: number, payload: Uint8Array): void {
    const stream = this.writes.get(streamId);
    if (!stream) return;
    stream.addChunk(seq, payload);
    // Replenish one credit per accepted chunk to keep the window full.
    this.emitter.sendStreamCredit(streamId, 1);
  }

  async closeWrite(streamId: number): Promise<{ attachment: AttachmentInfoDTO; heads: string[] }> {
    const stream = this.writes.get(streamId);
    if (!stream) throw new BridgeError(ErrorCodes.NotFound, `Unknown write stream: ${streamId}`);
    stream.ended = true;
    try {
      const db = this.getDb(stream.dbId);
      const doc = await db.getDocument(stream.docId);
      let ref: { attachmentId: string; fileName: string; mimeType: string; size?: number } | undefined;
      await db.changeDoc(doc, async (d) => {
        ref = await d.addAttachmentStream(stream.orderedIterable(), stream.fileName, stream.mimeType);
      });
      if (!ref) throw new BridgeError(ErrorCodes.Internal, "addAttachmentStream returned no reference");
      return { attachment: projectAttachment(ref as never), heads: doc.getHeads() };
    } finally {
      this.writes.delete(streamId);
    }
  }

  async abort(streamId: number): Promise<void> {
    const read = this.reads.get(streamId);
    if (read) {
      await read.abort();
      this.reads.delete(streamId);
    }
    this.writes.delete(streamId);
  }

  async removeAttachment(dbId: string, docId: string, ref: { attachmentId?: string; fileName?: string }): Promise<{ heads: string[] }> {
    const db = this.getDb(dbId);
    const doc = await db.getDocument(docId);
    const resolved = this.resolveAttachmentId(doc, ref);
    if (!resolved) throw new BridgeError(ErrorCodes.NotFound, `Attachment not found on ${docId}`);
    await db.changeDoc(doc, async (d) => {
      await (d as StreamStreamableDoc).removeAttachment(resolved.attachmentId);
    });
    return { heads: doc.getHeads() };
  }

  disposeAll(): void {
    for (const stream of this.reads.values()) void stream.abort();
    this.reads.clear();
    this.writes.clear();
  }
}

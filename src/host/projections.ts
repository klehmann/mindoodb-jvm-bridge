import type { AttachmentReference, MindooDoc } from "mindoodb";

export interface AttachmentInfoDTO {
  attachmentId: string;
  fileName: string;
  mimeType: string;
  size: number;
}

export interface DocumentDTO {
  id: string;
  data: Record<string, unknown>;
  heads: string[];
  attachments: AttachmentInfoDTO[];
  isDeleted: boolean;
}

export function projectAttachment(ref: AttachmentReference): AttachmentInfoDTO {
  return {
    attachmentId: ref.attachmentId,
    fileName: ref.fileName,
    mimeType: ref.mimeType,
    size: (ref as unknown as { size?: number }).size ?? 0,
  };
}

export function projectDocument(doc: MindooDoc): DocumentDTO {
  return {
    id: doc.getId(),
    data: toPlainJson(doc.getData()) as Record<string, unknown>,
    heads: doc.getHeads(),
    attachments: doc.getAttachments().map(projectAttachment),
    isDeleted: doc.isDeleted(),
  };
}

export interface ViewEntryDTO {
  position: string;
  level: number;
  isCategory: boolean;
  isDocument: boolean;
  origin: string | null;
  docId: string | null;
  categoryValue: unknown;
  columnValues: Record<string, unknown>;
  childCount: number;
  descendantCount: number;
  expanded: boolean;
}

/** Minimal shape of a core VirtualViewEntryData the projection reads. */
interface EntryLike {
  getPositionStr(): string;
  getLevel(): number;
  isCategory(): boolean;
  isDocument(): boolean;
  getCategoryValue(): unknown;
  getColumnValues(): Record<string, unknown>;
  getChildCount(): number;
  getDescendantCount(): number;
  origin?: string;
  docId?: string;
}

export function projectViewEntry(entry: EntryLike, expanded: boolean): ViewEntryDTO {
  return {
    position: entry.getPositionStr(),
    level: entry.getLevel(),
    isCategory: entry.isCategory(),
    isDocument: entry.isDocument(),
    origin: entry.origin ?? null,
    docId: entry.docId ?? null,
    categoryValue: toPlainJson(entry.getCategoryValue()),
    columnValues: toPlainJson(entry.getColumnValues()) as Record<string, unknown>,
    childCount: entry.getChildCount(),
    descendantCount: entry.getDescendantCount(),
    expanded,
  };
}

/**
 * Deep-copy a materialized document payload into plain JSON.
 *
 * MindooDoc.getData() may hand back Automerge-proxied objects, typed arrays,
 * or Dates; JSON.parse(JSON.stringify(...)) after a normalize pass yields a
 * structure the JSON transport can serialize deterministically.
 */
export function toPlainJson(value: unknown, seen = new WeakSet<object>()): unknown {
  if (value === null || typeof value !== "object") {
    return value;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
  }
  if (seen.has(value)) {
    return null;
  }
  seen.add(value);
  if (Array.isArray(value)) {
    return value.map((entry) => toPlainJson(entry, seen));
  }
  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    out[key] = toPlainJson(entry, seen);
  }
  return out;
}

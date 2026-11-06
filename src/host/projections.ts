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

/** Field types that differ from what the plain JSON suggests (strings are text, numbers plain numbers). */
const NOTABLE_FIELD_TYPES = new Set(["atomic", "richText", "counter", "timestamp", "bytes"]);

/** A JSON Pointer (RFC 6901) for a document path, e.g. `/items/0/status`. */
export function jsonPointer(path: Array<string | number>): string {
  return path.map((segment) => "/" + String(segment).replace(/~/g, "~0").replace(/\//g, "~1")).join("");
}

/**
 * The type of every leaf whose plain JSON hides it, keyed by JSON Pointer:
 * atomic strings, rich text, counters, timestamps and bytes. A client needs
 * them to turn an edited JSON copy into matching patches (text splices for
 * text, a whole new value for atomic strings, increments for counters).
 * Plain text, numbers, booleans and null are left out.
 */
export function projectFieldTypes(doc: MindooDoc): Record<string, string> {
  const out: Record<string, string> = {};
  const getFieldType = (doc as unknown as { getFieldType?: (path: Array<string | number>) => string | undefined }).getFieldType;
  if (typeof getFieldType !== "function") return out;
  const walk = (value: unknown, path: Array<string | number>): void => {
    if (path.length > 0) {
      const isContainer =
        value !== null && typeof value === "object" && !(value instanceof Date) && !(value instanceof Uint8Array);
      if (!isContainer) {
        const type = getFieldType.call(doc, path);
        if (type && NOTABLE_FIELD_TYPES.has(type)) out[jsonPointer(path)] = type;
        return;
      }
    }
    if (Array.isArray(value)) value.forEach((entry, index) => walk(entry, [...path, index]));
    else if (value !== null && typeof value === "object") {
      for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
        if (path.length === 0 && key.startsWith("_")) continue;
        walk(entry, [...path, key]);
      }
    }
  };
  walk(doc.getData(), []);
  return out;
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
  // `seen` holds the ancestors only: a cycle becomes null, but the same object
  // reached twice (e.g. one joined customer shared by several rows) is copied twice.
  if (seen.has(value)) {
    return null;
  }
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      return value.map((entry) => toPlainJson(entry, seen));
    }
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      out[key] = toPlainJson(entry, seen);
    }
    return out;
  } finally {
    seen.delete(value);
  }
}

/** A changefeed item on the wire: `{ doc, cursor }` or `{ meta, cursor }`. */
export function toChangeDto(item: unknown, metadataOnly: boolean): unknown {
  if (metadataOnly) {
    const m = item as { docId: string; lastModified: number; isDeleted: boolean; cursor: unknown };
    return {
      meta: { docId: m.docId, lastModified: m.lastModified, isDeleted: m.isDeleted },
      cursor: toPlainJson(m.cursor),
    };
  }
  const c = item as { doc: unknown; cursor: unknown };
  return { doc: projectDocument(c.doc as never), cursor: toPlainJson(c.cursor) };
}

import { createHash } from "node:crypto";
import { join } from "node:path";

/** Root directory that holds every user's isolated environment. */
export function dataRoot(): string {
  return process.env.MINDOODB_BRIDGE_DATA ?? "./.mindoodb-bridge-data";
}

/** Filesystem-safe hash of a username (used as the per-user folder name). */
export function userDirName(username: string): string {
  return createHash("sha256").update(username, "utf-8").digest("hex").slice(0, 32);
}

/** `<root>/<tenantId>/<userHash>` — the per-user environment root. */
export function userEnvDir(root: string, tenantId: string, username: string): string {
  return join(root, sanitizeSegment(tenantId), userDirName(username));
}

/** On-disk content-addressed store base path for a user environment. */
export function userStoreBasePath(envDir: string): string {
  return join(envDir, "stores");
}

/** Identity bundle file path for a user environment. */
export function identityFilePath(envDir: string): string {
  return join(envDir, "identity.json");
}

function sanitizeSegment(value: string): string {
  return value.replace(/[^A-Za-z0-9._-]/g, "_");
}

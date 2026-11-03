import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import type { PrivateUserId } from "mindoodb";

/**
 * Persisted, password-locked identity for one user in one tenant.
 *
 * `user` carries the user's PrivateUserId whose private keys are already
 * password-encrypted (EncryptedPrivateKey). `keyBagBlobBase64` is the output
 * of `KeyBag.save()` (self-encrypted with the user's encryption key), which
 * `KeyBag.load()` restores. Nothing in this file is readable without the
 * user's password.
 */
export interface IdentityBundle {
  version: 1;
  tenantId: string;
  username: string;
  adminSigningPublicKey: string;
  adminEncryptionPublicKey: string;
  user: PrivateUserId;
  keyBagBlobBase64: string;
  /** Optional sync server URL. When present, the session runs a background sync loop. */
  serverUrl?: string;
  /**
   * Optional admin PrivateUserId (password-locked keys) persisted at tenant
   * creation so the same machine can approve join requests without a separate
   * admin identity file.
   */
  adminUser?: PrivateUserId;
}

export function readIdentityBundle(path: string): IdentityBundle {
  const raw = readFileSync(path, "utf-8");
  const parsed = JSON.parse(raw) as IdentityBundle;
  if (parsed.version !== 1) {
    throw new Error(`Unsupported identity bundle version: ${(parsed as { version?: unknown }).version}`);
  }
  return parsed;
}

export function writeIdentityBundle(path: string, bundle: IdentityBundle): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(bundle, null, 2), { encoding: "utf-8", mode: 0o600 });
}

export function identityBundleExists(path: string): boolean {
  return existsSync(path);
}

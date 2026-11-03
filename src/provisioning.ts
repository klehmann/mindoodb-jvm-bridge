/**
 * Reusable provisioning primitives.
 *
 * These bootstrap identity/tenant state *outside* of any open session, so they
 * are shared by both the `provision.ts` CLI and the supervisor's bootstrap RPCs.
 * Nothing here requires a session token; callers must already hold the bootstrap
 * token.
 */
import {
  BaseMindooTenantFactory,
  decodeMindooURI,
  generateTenantId,
  isMindooURI,
  type PrivateUserId,
} from "mindoodb";
import { hostname } from "node:os";
import { NodeCryptoAdapter } from "mindoodb/node";
import { ScopedOnDiskStoreFactory } from "./scopedStoreFactory";
import { writeIdentityBundle, type IdentityBundle } from "./identityStore";
import { userEnvDir, userStoreBasePath, identityFilePath } from "./paths";
import { bytesToBase64 } from "./bytes";
import { pullSystemDatabases } from "./syncLoop";

export interface ProvisionTenantInput {
  dataRoot: string;
  /**
   * Omit to get a random, time-sortable id like Haven's (`generateTenantId`),
   * so tenants from different machines never collide on a shared server.
   */
  tenantId?: string;
  /** Human-readable name, stored encrypted in the directory's `tenantsetup` document. */
  tenantLabel?: string;
  adminName: string;
  adminPassword: string;
  userName: string;
  userPassword: string;
  databases?: string[];
  serverUrl?: string;
}

export interface ProvisionTenantResult {
  tenantId: string;
  tenantLabel: string | null;
  /** The app user's username (what `session.open` expects as `userId`). */
  userId: string;
  /** The admin user's username. */
  adminUserId: string;
  databases: string[];
  envDir: string;
  identityFile: string;
  /**
   * Admin PrivateUserId (password-locked). Needed for approveJoinRequest /
   * publishToServer when the caller is not already holding admin credentials.
   */
  adminIdentity: PrivateUserId;
}

/**
 * Create a tenant + admin + app user and write a password-locked identity
 * bundle to the per-user environment directory, so a later `session.open` can
 * reopen it. Returns non-secret metadata plus the admin identity.
 */
export async function provisionTenant(input: ProvisionTenantInput): Promise<ProvisionTenantResult> {
  const databases = input.databases ?? [];
  const tenantId = input.tenantId?.trim() || generateTenantId();
  const tenantLabel = input.tenantLabel?.trim() || undefined;
  const envDir = userEnvDir(input.dataRoot, tenantId, input.userName);
  const basePath = userStoreBasePath(envDir);

  const crypto = new NodeCryptoAdapter();
  const factory = new BaseMindooTenantFactory(new ScopedOnDiskStoreFactory(basePath), crypto);

  const result = await factory.createTenant({
    tenantId,
    adminName: input.adminName,
    adminPassword: input.adminPassword,
    userName: input.userName,
    userPassword: input.userPassword,
    ...(tenantLabel ? { tenantLabel } : {}),
  });

  for (const dbId of databases) {
    await result.tenant.openDB(dbId);
  }

  const keyBagBlob = await result.keyBag.save();
  const bundle: IdentityBundle = {
    version: 1,
    tenantId,
    username: input.userName,
    adminSigningPublicKey: result.adminUser.userSigningKeyPair.publicKey,
    adminEncryptionPublicKey: result.adminUser.userEncryptionKeyPair.publicKey,
    user: result.appUser,
    keyBagBlobBase64: bytesToBase64(keyBagBlob),
    serverUrl: input.serverUrl,
    adminUser: result.adminUser,
  };
  const identityFile = identityFilePath(envDir);
  writeIdentityBundle(identityFile, bundle);

  return {
    tenantId,
    tenantLabel: tenantLabel ?? null,
    userId: input.userName,
    adminUserId: result.adminUser.username,
    databases,
    envDir,
    identityFile,
    adminIdentity: result.adminUser,
  };
}

export interface CreateUserInput {
  dataRoot: string;
  username: string;
  password: string;
}

export interface CreateUserResult {
  username: string;
  signingPublicKey: string;
  encryptionPublicKey: string;
  identity: PrivateUserId;
}

export async function createUser(input: CreateUserInput): Promise<CreateUserResult> {
  const crypto = new NodeCryptoAdapter();
  const factory = new BaseMindooTenantFactory(new ScopedOnDiskStoreFactory(input.dataRoot), crypto);
  const identity = await factory.createUserId(input.username, input.password);
  // The person-bound User-Key goes into join requests; without it an admin
  // cannot approve (MindooDB 0.0.8x) and "default" cannot be wrapped to the user.
  await factory.ensureUserKeyPair(identity, input.password);
  return {
    username: identity.username,
    signingPublicKey: identity.userSigningKeyPair.publicKey,
    encryptionPublicKey: identity.userEncryptionKeyPair.publicKey,
    identity,
  };
}

export interface CreateJoinRequestInput {
  dataRoot: string;
  identity: PrivateUserId;
  /** Device label shown to the approving admin; defaults to the host name. */
  label?: string;
  format?: "object" | "uri";
}

export async function createJoinRequest(input: CreateJoinRequestInput): Promise<{
  format: "object" | "uri";
  joinRequest: unknown;
}> {
  const crypto = new NodeCryptoAdapter();
  const factory = new BaseMindooTenantFactory(new ScopedOnDiskStoreFactory(input.dataRoot), crypto);
  const format = input.format ?? "uri";
  // The approving admin needs a device label (from the request or its own
  // options); name the machine so the grant is recognizable in the directory.
  const label = input.label?.trim() || `JVM bridge (${hostname()})`;
  if (format === "uri") {
    return {
      format,
      joinRequest: factory.createJoinRequest(input.identity, { format: "uri", label }),
    };
  }
  return {
    format,
    joinRequest: factory.createJoinRequest(input.identity, { format: "object", label }),
  };
}

export interface JoinTenantInput {
  dataRoot: string;
  joinResponse: unknown;
  user: PrivateUserId;
  password: string;
  /** Required only for legacy v2 password-wrapped join responses. */
  sharePassword?: string;
  databases?: string[];
  serverUrl?: string;
}

export interface JoinTenantProvisionResult {
  tenantId: string;
  /** The label the admin's response carried (the live label is in the directory). */
  tenantLabel: string | null;
  userId: string;
  databases: string[];
  envDir: string;
  identityFile: string;
  serverUrl: string | null;
}

/**
 * Join an existing tenant using a JoinResponse (object or URI), then persist an
 * identity bundle for the joining user so `session.open` can reopen it.
 */
export async function joinTenantAndPersist(input: JoinTenantInput): Promise<JoinTenantProvisionResult> {
  const databases = input.databases ?? [];
  // Resolve tenantId from the response when possible for path layout; joinTenant
  // also validates. For URI strings we open under a temp path then re-key — but
  // JoinResponse always carries tenantId after decode inside joinTenant. Parse
  // lightly for the env dir.
  const tenantId = extractTenantId(input.joinResponse);
  if (!tenantId) {
    throw new Error("joinResponse must include tenantId (object) or be an mdb://join-response URI");
  }

  const envDir = userEnvDir(input.dataRoot, tenantId, input.user.username);
  const basePath = userStoreBasePath(envDir);
  const crypto = new NodeCryptoAdapter();
  const factory = new BaseMindooTenantFactory(new ScopedOnDiskStoreFactory(basePath), crypto);

  const result = await factory.joinTenant(input.joinResponse as never, {
    user: input.user,
    password: input.password,
    ...(input.sharePassword ? { sharePassword: input.sharePassword } : {}),
  });

  // Pull admin public keys + optional serverUrl from the (decoded) response.
  const responseMeta = extractJoinResponseMeta(input.joinResponse);
  const serverUrl = input.serverUrl ?? responseMeta.serverUrl;

  // The grant is only on the server until the directory is pulled.
  if (serverUrl) {
    await pullSystemDatabases(result.tenant, serverUrl);
  }
  for (const dbId of databases) {
    await result.tenant.openDB(dbId);
  }

  const keyBagBlob = await result.keyBag.save();
  const bundle: IdentityBundle = {
    version: 1,
    tenantId,
    username: input.user.username,
    adminSigningPublicKey: responseMeta.adminSigningPublicKey,
    adminEncryptionPublicKey: responseMeta.adminEncryptionPublicKey,
    user: input.user,
    keyBagBlobBase64: bytesToBase64(keyBagBlob),
    serverUrl,
  };
  const identityFile = identityFilePath(envDir);
  writeIdentityBundle(identityFile, bundle);

  return {
    tenantId,
    tenantLabel: responseMeta.tenantLabel ?? null,
    userId: input.user.username,
    databases,
    envDir,
    identityFile,
    serverUrl: serverUrl ?? null,
  };
}

function extractTenantId(joinResponse: unknown): string | null {
  if (typeof joinResponse === "string") {
    if (!isMindooURI(joinResponse)) return null;
    try {
      const decoded = decodeMindooURI<{ tenantId?: string }>(joinResponse);
      return decoded.payload?.tenantId ?? null;
    } catch {
      return null;
    }
  }
  if (joinResponse && typeof joinResponse === "object" && "tenantId" in joinResponse) {
    return String((joinResponse as { tenantId: string }).tenantId);
  }
  return null;
}

function extractJoinResponseMeta(joinResponse: unknown): {
  adminSigningPublicKey: string;
  adminEncryptionPublicKey: string;
  serverUrl?: string;
  tenantLabel?: string;
} {
  let payload: {
    adminSigningPublicKey?: string;
    adminEncryptionPublicKey?: string;
    serverUrl?: string;
    tenantLabel?: string;
  };
  if (typeof joinResponse === "string") {
    payload = decodeMindooURI(joinResponse).payload as typeof payload;
  } else {
    payload = joinResponse as typeof payload;
  }
  if (!payload.adminSigningPublicKey || !payload.adminEncryptionPublicKey) {
    throw new Error("joinResponse missing admin public keys");
  }
  return {
    adminSigningPublicKey: payload.adminSigningPublicKey,
    adminEncryptionPublicKey: payload.adminEncryptionPublicKey,
    serverUrl: payload.serverUrl,
    tenantLabel: typeof payload.tenantLabel === "string" ? payload.tenantLabel : undefined,
  };
}

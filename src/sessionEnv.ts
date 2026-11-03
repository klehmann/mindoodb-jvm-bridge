import {
  BaseMindooDB,
  BaseMindooTenant,
  BaseMindooTenantFactory,
  KeyBag,
  StoreKind,
  type MindooDB,
  type MindooTenant,
  type PrivateUserId,
} from "mindoodb";
import { NodeCryptoAdapter } from "mindoodb/node";
import { ScopedOnDiskStoreFactory } from "./scopedStoreFactory";
import {
  writeIdentityBundle,
  type IdentityBundle,
} from "./identityStore";
import { base64ToBytes, bytesToBase64 } from "./bytes";
import { BridgeError, ErrorCodes } from "./protocol";
import { log } from "./logging";

export type DatabaseMode = "local" | "remote";

export interface OpenDatabaseResult {
  dbId: string;
  mode: DatabaseMode;
  databases: string[];
}

/**
 * Mutable per-session environment: open tenant, KeyBag, identity path, and
 * locally/remotely opened databases.
 */
export class SessionEnv {
  private readonly dbs = new Map<string, MindooDB>();
  private readonly dbModes = new Map<string, DatabaseMode>();
  private _serverUrl: string | undefined;
  private serverUrlListener: ((url: string | undefined) => void) | null = null;

  constructor(
    readonly tenant: MindooTenant,
    readonly factory: BaseMindooTenantFactory,
    readonly keyBag: KeyBag,
    readonly bundle: IdentityBundle,
    readonly identityFile: string,
    readonly password: string,
    serverUrl: string | undefined,
  ) {
    this._serverUrl = serverUrl;
  }

  get serverUrl(): string | undefined {
    return this._serverUrl;
  }

  get tenantId(): string {
    return this.bundle.tenantId;
  }

  get currentUser(): PrivateUserId {
    return this.bundle.user;
  }

  /** Notify SyncLoop when the sync peer URL changes. */
  onServerUrlChange(listener: (url: string | undefined) => void): void {
    this.serverUrlListener = listener;
  }

  getDb(id: string): MindooDB {
    const db = this.dbs.get(id);
    if (!db) throw new BridgeError(ErrorCodes.BadRequest, `Database "${id}" is not open in this session`);
    return db;
  }

  hasDb(id: string): boolean {
    return this.dbs.has(id);
  }

  databaseIds(): string[] {
    return [...this.dbs.keys()];
  }

  getDatabaseMode(id: string): DatabaseMode | undefined {
    return this.dbModes.get(id);
  }

  /** Local-primary databases only (eligible for SyncLoop pull/push). */
  localDatabaseIds(): string[] {
    return this.databaseIds().filter((id) => this.dbModes.get(id) === "local");
  }

  async openDatabase(dbId: string, mode: DatabaseMode = "local"): Promise<OpenDatabaseResult> {
    if (!dbId) throw new BridgeError(ErrorCodes.BadRequest, "Missing dbId");
    if (mode !== "local" && mode !== "remote") {
      throw new BridgeError(ErrorCodes.BadRequest, `Invalid database mode: ${mode}`);
    }

    const existing = this.dbs.get(dbId);
    if (existing) {
      const existingMode = this.dbModes.get(dbId) ?? "local";
      if (existingMode !== mode) {
        throw new BridgeError(
          ErrorCodes.BadRequest,
          `Database "${dbId}" is already open in ${existingMode} mode`,
        );
      }
      return { dbId, mode, databases: this.databaseIds() };
    }

    if (mode === "local") {
      const db = await this.tenant.openDB(dbId);
      this.dbs.set(dbId, db);
      this.dbModes.set(dbId, "local");
      log.info("sessionEnv", `opened database "${dbId}" (local)`);
    } else {
      if (!this._serverUrl) {
        throw new BridgeError(
          ErrorCodes.BadRequest,
          "Remote database open requires a serverUrl (call session.setServerUrl or publishToServer first)",
        );
      }
      const db = await this.openRemoteDatabase(dbId, this._serverUrl);
      this.dbs.set(dbId, db);
      this.dbModes.set(dbId, "remote");
      log.info("sessionEnv", `opened database "${dbId}" (remote-primary)`);
    }

    return { dbId, mode, databases: this.databaseIds() };
  }

  /**
   * Direct-remote MindooDB: primary docs + attachments stores are
   * ClientNetworkContentAddressedStore instances (slow; every CAS op is remoted).
   */
  private async openRemoteDatabase(dbId: string, serverUrl: string): Promise<MindooDB> {
    const docStore = await this.tenant.connectToServer(serverUrl, dbId, StoreKind.docs);
    const attachmentStore = await this.tenant.connectToServer(serverUrl, dbId, StoreKind.attachments);
    const baseTenant = this.tenant as BaseMindooTenant;
    const db = new BaseMindooDB(baseTenant, docStore, attachmentStore);
    await db.initialize();
    return db;
  }

  async persistIdentity(): Promise<void> {
    const keyBagBlob = await this.keyBag.save();
    const next: IdentityBundle = {
      ...this.bundle,
      keyBagBlobBase64: bytesToBase64(keyBagBlob),
      serverUrl: this._serverUrl,
    };
    // Keep the in-memory bundle in sync with what we write.
    this.bundle.keyBagBlobBase64 = next.keyBagBlobBase64;
    this.bundle.serverUrl = next.serverUrl;
    writeIdentityBundle(this.identityFile, next);
  }

  async setServerUrl(url: string | null | undefined): Promise<{ serverUrl: string | null }> {
    const normalized = url ? url.replace(/\/$/, "") : undefined;
    this._serverUrl = normalized;
    this.bundle.serverUrl = normalized;
    await this.persistIdentity();
    this.serverUrlListener?.(normalized);
    return { serverUrl: normalized ?? null };
  }

  async close(): Promise<void> {
    this.dbs.clear();
    this.dbModes.clear();
  }
}

/**
 * Open a user's tenant + databases from a password-locked identity bundle on an
 * isolated on-disk replica. The password unlocks the KeyBag and is used only
 * here; the caller must discard it afterwards.
 */
export async function openSessionEnv(params: {
  bundle: IdentityBundle;
  password: string;
  databases: string[];
  basePath: string;
  identityFile: string;
}): Promise<SessionEnv> {
  const crypto = new NodeCryptoAdapter();
  const storeFactory = new ScopedOnDiskStoreFactory(params.basePath);
  const factory = new BaseMindooTenantFactory(storeFactory, crypto);

  const userEncKey = params.bundle.user.userEncryptionKeyPair.privateKey;
  const keyBag = new KeyBag(userEncKey, params.password, crypto);
  await keyBag.load(base64ToBytes(params.bundle.keyBagBlobBase64));

  const tenant = await factory.openTenant(
    params.bundle.tenantId,
    params.bundle.adminSigningPublicKey,
    params.bundle.adminEncryptionPublicKey,
    params.bundle.user,
    params.password,
    keyBag,
  );

  const env = new SessionEnv(
    tenant,
    factory,
    keyBag,
    params.bundle,
    params.identityFile,
    params.password,
    params.bundle.serverUrl,
  );

  for (const id of params.databases) {
    await env.openDatabase(id, "local");
  }

  return env;
}

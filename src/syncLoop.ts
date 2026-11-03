import {
  StoreKind,
  type ContentAddressedStore,
  type MindooDB,
  type MindooTenant,
  type SyncOptions,
  type SyncProgress,
  type SyncResult,
} from "mindoodb";
import type { SessionEnv } from "./sessionEnv";
import { log } from "./logging";

export type SyncDirection = "both" | "pull" | "push";

export interface DatabaseSyncResult {
  dbId: string;
  pulledEntries: number;
  pushedEntries: number;
  pushedBytes: number;
  rejectedEntries: number;
}

export interface RefreshResult {
  /** True when at least one entry was pulled from the server. */
  pulled: boolean;
  /** Kept for wire compatibility; same as `pulled` (pulled data is merged right away). */
  reconciled: boolean;
  /** True when at least one entry was pushed to the server. */
  pushed: boolean;
  databases: DatabaseSyncResult[];
}

/** One `sync-progress` event: MindooDB's SyncProgress for one database, store and direction. */
export interface SyncProgressEvent {
  dbId: string;
  direction: "pull" | "push";
  store: "docs" | "attachments";
  phase: SyncProgress["phase"];
  message: string;
  scannedEntries: number;
  transferredEntries: number;
  transferredBytes: number;
  totalSourceEntries: number | null;
  /** True for the periodic background sync, false for refresh/publish/approve. */
  background: boolean;
}

/** Tenant databases every replica tracks, like Haven does (grants, key wraps, user keys). */
export const SYSTEM_DATABASES = ["directory", "userdirectory"];

/**
 * Pull `directory` and `userdirectory` from the server. A user that just joined
 * (or got a new grant) cannot open any other database before the admin-signed
 * grant is in the local directory.
 */
export async function pullSystemDatabases(tenant: MindooTenant, serverUrl: string): Promise<void> {
  for (const dbId of SYSTEM_DATABASES) {
    const db = await tenant.openDB(dbId, dbId === "directory" ? { adminOnlyDb: true } : undefined);
    for (const kind of [StoreKind.docs, StoreKind.attachments]) {
      await db.pullChangesFrom(await tenant.connectToServer(serverUrl, dbId, kind), { storeKind: kind });
    }
    await db.syncStoreChanges();
  }
}

/**
 * Owns the background sync for a session: periodically pushes local changes and
 * pulls remote ones for the tenant's system databases and every open
 * **local-primary** database, docs and attachments stores alike. Best-effort in
 * the background (a failed cycle is logged and retried on the next tick);
 * {@link refresh} reports errors to the caller. No-op when there is no
 * `serverUrl`. Skips remote-primary databases (already on the server).
 */
export class SyncLoop {
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;
  /** Serializes background ticks and explicit refreshes so two syncs never overlap. */
  private queue: Promise<unknown> = Promise.resolve();
  private readonly remotes = new Map<string, ContentAddressedStore>();
  private readonly progressWatchers = new Map<string, (event: SyncProgressEvent) => void>();

  constructor(
    private readonly env: SessionEnv,
    private readonly intervalMs: number = 5000,
  ) {
    this.env.onServerUrlChange((url) => {
      this.remotes.clear();
      if (url) {
        // No immediate tick: publishToServer still has to push the directory,
        // and a sync before that is refused by the server (USER_REVOKED).
        if (!this.timer && !this.stopped) this.start({ immediate: false });
      } else {
        this.pauseTimer();
      }
    });
  }

  watchProgress(id: string, listener: (event: SyncProgressEvent) => void): void {
    this.progressWatchers.set(id, listener);
  }

  unwatchProgress(id: string): void {
    this.progressWatchers.delete(id);
  }

  /**
   * Run one push or pull and report its progress to the watchers. MindooDB
   * skips the final "complete" event when there was nothing to do, so one is
   * sent from the result then: every call ends with "complete".
   */
  private async tracked(
    dbId: string,
    direction: "pull" | "push",
    kind: StoreKind,
    background: boolean,
    run: (onProgress: ((p: SyncProgress) => void) | undefined) => Promise<SyncResult>,
  ): Promise<SyncResult> {
    if (this.progressWatchers.size === 0) return run(undefined);
    let completed = false;
    const emit = (p: SyncProgress): void => {
      if (p.phase === "complete") completed = true;
      const event: SyncProgressEvent = {
        dbId,
        direction,
        store: kind === StoreKind.attachments ? "attachments" : "docs",
        phase: p.phase,
        message: p.message,
        scannedEntries: p.scannedEntries,
        transferredEntries: p.transferredEntries,
        transferredBytes: p.transferredBytes ?? 0,
        totalSourceEntries: p.totalSourceEntries ?? null,
        background,
      };
      for (const listener of this.progressWatchers.values()) listener(event);
    };
    const result = await run(emit);
    if (!completed) {
      emit({
        phase: "complete",
        message: `${direction === "pull" ? "Pull" : "Push"} complete: ${result.transferredEntries} entries`,
        transferredEntries: result.transferredEntries,
        transferredBytes: result.transferredBytes,
        scannedEntries: result.scannedEntries,
      } as SyncProgress);
    }
    return result;
  }

  start(options: { immediate?: boolean } = {}): void {
    if (!this.env.serverUrl || this.timer || this.stopped) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
    if (options.immediate !== false) void this.tick();
  }

  private pauseTimer(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private serialized<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.queue.then(fn, fn);
    this.queue = run.catch(() => undefined);
    return run;
  }

  private async tick(): Promise<void> {
    if (this.stopped) return;
    await this.serialized(async () => {
      for (const dbId of this.syncedDatabaseIds()) {
        await this.syncDb(dbId, "both", true).catch((err) => log.warn("syncLoop", `sync ${dbId} failed`, err));
      }
    });
  }

  private syncedDatabaseIds(): string[] {
    return [...new Set([...SYSTEM_DATABASES, ...this.env.localDatabaseIds()])];
  }

  /**
   * Sync now, one database or the system databases plus every open local one.
   * Errors propagate to the caller.
   */
  async refresh(dbId?: string, direction: SyncDirection = "both"): Promise<RefreshResult> {
    if (dbId && this.env.getDatabaseMode(dbId) === "remote") {
      return { pulled: false, reconciled: false, pushed: false, databases: [] };
    }
    const ids = dbId ? [dbId] : this.syncedDatabaseIds();
    return this.serialized(async () => {
      const databases: DatabaseSyncResult[] = [];
      for (const id of ids) {
        const r = await this.syncDb(id, direction);
        if (r) databases.push(r);
      }
      const pulled = databases.some((d) => d.pulledEntries > 0);
      return { pulled, reconciled: pulled, pushed: databases.some((d) => d.pushedEntries > 0), databases };
    });
  }

  /**
   * Push a tenant database right away, optionally authenticated as another
   * identity (bootstrap pushes of admin-signed directory entries).
   */
  async pushNow(dbId: string, options?: Pick<SyncOptions, "networkAuthOverride">): Promise<DatabaseSyncResult> {
    return this.serialized(async () => {
      const db = await this.env.openTenantDb(dbId);
      const result = emptyResult(dbId);
      for (const kind of [StoreKind.docs, StoreKind.attachments]) {
        const remote = await this.env.tenant.connectToServer(this.env.serverUrl!, dbId, kind);
        addPush(
          result,
          await this.tracked(dbId, "push", kind, false, (onProgress) =>
            db.pushChangesTo(remote, { ...options, storeKind: kind, onProgress }),
          ),
        );
      }
      return result;
    });
  }

  private async syncDb(dbId: string, direction: SyncDirection, background = false): Promise<DatabaseSyncResult | null> {
    if (!this.env.serverUrl) {
      return null;
    }
    if (this.env.getDatabaseMode(dbId) === "remote") {
      return null;
    }
    const db: MindooDB = await this.env.openTenantDb(dbId);
    const result = emptyResult(dbId);
    for (const kind of [StoreKind.docs, StoreKind.attachments]) {
      const remote = await this.getRemote(dbId, kind);
      if (direction !== "pull") {
        addPush(
          result,
          await this.tracked(dbId, "push", kind, background, (onProgress) =>
            db.pushChangesTo(remote, { storeKind: kind, onProgress }),
          ),
        );
      }
      if (direction !== "push") {
        const pull = await this.tracked(dbId, "pull", kind, background, (onProgress) =>
          db.pullChangesFrom(remote, { storeKind: kind, onProgress }),
        );
        result.pulledEntries += pull.transferredEntries;
      }
    }
    if (result.pulledEntries > 0) {
      await db.syncStoreChanges();
    }
    return result;
  }

  private async getRemote(dbId: string, kind: StoreKind): Promise<ContentAddressedStore> {
    const key = `${dbId}\u0000${kind}`;
    let remote = this.remotes.get(key);
    if (!remote) {
      remote = await this.env.tenant.connectToServer(this.env.serverUrl!, dbId, kind);
      this.remotes.set(key, remote);
    }
    return remote;
  }

  stop(): void {
    this.stopped = true;
    this.pauseTimer();
    this.progressWatchers.clear();
    this.remotes.clear();
  }
}

function emptyResult(dbId: string): DatabaseSyncResult {
  return { dbId, pulledEntries: 0, pushedEntries: 0, pushedBytes: 0, rejectedEntries: 0 };
}

function addPush(result: DatabaseSyncResult, push: SyncResult): void {
  result.pushedEntries += push.transferredEntries;
  result.pushedBytes += push.transferredBytes;
  const rejected = push.rejectedEntries?.length ?? 0;
  if (rejected > 0) {
    log.warn("syncLoop", `${result.dbId}: server rejected ${rejected} entries`);
  }
  result.rejectedEntries += rejected;
}

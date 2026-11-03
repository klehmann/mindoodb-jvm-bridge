import type { ContentAddressedStore } from "mindoodb";
import type { SessionEnv } from "./sessionEnv";
import { log } from "./logging";

export interface RefreshResult {
  pulled: boolean;
  reconciled: boolean;
}

/**
 * Owns the background sync for a session: periodically pulls remote changes,
 * merges them into open **local-primary** documents, and pushes local changes.
 * Best-effort — a failed cycle is logged and retried on the next tick. No-op
 * when there is no `serverUrl`. Skips remote-primary databases (already on the
 * server).
 */
export class SyncLoop {
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;
  private cycleInFlight = false;
  private readonly remotes = new Map<string, ContentAddressedStore>();

  constructor(
    private readonly env: SessionEnv,
    private readonly intervalMs: number = 5000,
  ) {
    this.env.onServerUrlChange((url) => {
      this.remotes.clear();
      if (url) {
        if (!this.timer && !this.stopped) this.start();
      } else {
        this.pauseTimer();
      }
    });
  }

  start(): void {
    if (!this.env.serverUrl || this.timer || this.stopped) return;
    this.timer = setInterval(() => {
      void this.tick();
    }, this.intervalMs);
    void this.tick();
  }

  private pauseTimer(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async tick(): Promise<void> {
    if (this.cycleInFlight || this.stopped) return;
    this.cycleInFlight = true;
    try {
      for (const dbId of this.env.localDatabaseIds()) {
        await this.syncDb(dbId).catch((err) => log.warn("syncLoop", `sync ${dbId} failed`, err));
      }
    } finally {
      this.cycleInFlight = false;
    }
  }

  /** Force a sync now for one local database (or all local) and report whether data merged. */
  async refresh(dbId?: string): Promise<RefreshResult> {
    const ids = dbId ? [dbId] : this.env.localDatabaseIds();
    let pulled = false;
    let reconciled = false;
    for (const id of ids) {
      if (this.env.getDatabaseMode(id) === "remote") {
        continue;
      }
      const r = await this.syncDb(id);
      pulled = pulled || r.pulled;
      reconciled = reconciled || r.reconciled;
    }
    return { pulled, reconciled };
  }

  private async syncDb(dbId: string): Promise<RefreshResult> {
    if (!this.env.serverUrl || !this.env.hasDb(dbId)) {
      return { pulled: false, reconciled: false };
    }
    if (this.env.getDatabaseMode(dbId) === "remote") {
      return { pulled: false, reconciled: false };
    }
    const db = this.env.getDb(dbId);
    const remote = await this.getRemote(dbId);
    const pull = await db.pullChangesFrom(remote);
    await db.syncStoreChanges();
    const dbAny = db as unknown as {
      pushChangesTo?: (r: ContentAddressedStore) => Promise<unknown>;
    };
    if (typeof dbAny.pushChangesTo === "function") {
      await dbAny.pushChangesTo(remote);
    }
    const pulledCount = (pull as unknown as { pulled?: number; entriesPulled?: number }) ?? {};
    const pulled = Boolean(pulledCount.pulled ?? pulledCount.entriesPulled ?? 0);
    return { pulled, reconciled: pulled };
  }

  private async getRemote(dbId: string): Promise<ContentAddressedStore> {
    let remote = this.remotes.get(dbId);
    if (!remote) {
      remote = await this.env.tenant.connectToServer(this.env.serverUrl!, dbId);
      this.remotes.set(dbId, remote);
    }
    return remote;
  }

  stop(): void {
    this.stopped = true;
    this.pauseTimer();
    this.remotes.clear();
  }
}

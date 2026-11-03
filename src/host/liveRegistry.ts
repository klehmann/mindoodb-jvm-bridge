import type { Emitter } from "./emitter";
import { toChangeDto, toPlainJson } from "./projections";
import { log } from "../logging";

interface LiveDb {
  query?: (query: unknown, options?: unknown) => Promise<unknown>;
  queryLive?: (
    query: unknown,
    onResult: (result: unknown) => void,
    options?: { onError?: (error: unknown) => void } & Record<string, unknown>,
  ) => { unsubscribe(): void; refresh(): void };
  addChangeListener?: (listener: () => void) => () => void;
  iterateChangesSince: (cursor: unknown, options?: unknown) => AsyncGenerator<{ doc: unknown; cursor: unknown }>;
  iterateChangeMetadataSince: (cursor: unknown, options?: unknown) => AsyncGenerator<{ cursor: unknown }>;
  getLatestChangeCursor?: () => unknown;
}

interface Subscription {
  cancel(): void;
}

/**
 * Owns the push subscriptions for one user session: live queries (query-result
 * events), view-change notifications, and changefeed streaming. Each is tied to
 * a subscriptionId and cancelled individually or all at once on session close.
 */
export class LiveRegistry {
  private readonly subs = new Map<string, Subscription>();

  constructor(
    private readonly emitter: Emitter,
    private readonly getDb: (dbId: string) => LiveDb,
  ) {}

  startQuery(subscriptionId: string, dbId: string, query: unknown, options?: unknown): void {
    const db = this.getDb(dbId);
    if (typeof db.queryLive === "function") {
      const handle = db.queryLive(
        query,
        (result) => this.emitter.sendEvent(subscriptionId, "query-result", toPlainJson(result)),
        {
          ...(options as Record<string, unknown>),
          onError: (error: unknown) =>
            this.emitter.sendEvent(subscriptionId, "query-error", { message: String((error as Error)?.message ?? error) }),
        },
      );
      this.subs.set(subscriptionId, { cancel: () => handle.unsubscribe() });
      return;
    }
    // Fallback: re-run query on change events / interval and emit on change.
    this.startPolledQuery(subscriptionId, db, query, options);
  }

  private startPolledQuery(subscriptionId: string, db: LiveDb, query: unknown, options?: unknown): void {
    let lastJson = "";
    let disposed = false;
    const run = async (): Promise<void> => {
      if (disposed || typeof db.query !== "function") return;
      try {
        const result = await db.query(query, options);
        const json = JSON.stringify(toPlainJson(result));
        if (json !== lastJson) {
          lastJson = json;
          this.emitter.sendEvent(subscriptionId, "query-result", JSON.parse(json));
        }
      } catch (err) {
        this.emitter.sendEvent(subscriptionId, "query-error", { message: String((err as Error)?.message ?? err) });
      }
    };
    const unsub = db.addChangeListener?.(() => void run());
    const timer = unsub ? null : setInterval(() => void run(), 1500);
    void run();
    this.subs.set(subscriptionId, {
      cancel: () => {
        disposed = true;
        unsub?.();
        if (timer) clearInterval(timer);
      },
    });
  }

  /** View-change subscription: fire a lightweight event whenever the db changes. */
  startViewWatch(subscriptionId: string, dbIds: string[], refresh?: () => Promise<void>): void {
    let disposed = false;
    // Coalesce bursts: one refresh in flight, at most one queued behind it.
    let running: Promise<void> | null = null;
    let again = false;
    const onChange = (): void => {
      if (disposed) return;
      if (running) {
        again = true;
        return;
      }
      running = (async () => {
        do {
          again = false;
          try {
            await refresh?.();
          } catch (err) {
            log.warn("liveRegistry", "view refresh failed", err);
          }
          if (disposed) return;
          this.emitter.sendEvent(subscriptionId, "view-changed", {});
        } while (again && !disposed);
      })().finally(() => {
        running = null;
      });
    };
    const unsubs = dbIds.map((dbId) => this.getDb(dbId).addChangeListener?.(onChange));
    this.subs.set(subscriptionId, {
      cancel: () => {
        disposed = true;
        for (const unsub of unsubs) unsub?.();
      },
    });
  }

  /** Changefeed: stream every changed document from `cursor`, then follow live. */
  startChanges(
    subscriptionId: string,
    dbId: string,
    cursor: unknown,
    options: { idPrefix?: string; metadataOnly?: boolean } = {},
  ): void {
    const db = this.getDb(dbId);
    let disposed = false;
    let wake: (() => void) | null = null;
    const unsub = db.addChangeListener?.(() => wake?.());

    const waitForChange = (): Promise<void> =>
      new Promise<void>((resolve) => {
        wake = resolve;
        // Even without a change listener, poll periodically.
        if (!unsub) setTimeout(resolve, 1500);
      });

    const loop = async (): Promise<void> => {
      let current = cursor;
      let reportedCaughtUp = false;
      while (!disposed) {
        try {
          let advanced = false;
          const iterOptions = { idPrefix: options.idPrefix };
          const items = options.metadataOnly
            ? db.iterateChangeMetadataSince(current, iterOptions)
            : db.iterateChangesSince(current, iterOptions);
          for await (const item of items) {
            if (disposed) return;
            current = item.cursor;
            advanced = true;
            this.emitter.sendEvent(subscriptionId, "change", toChangeDto(item, Boolean(options.metadataOnly)));
          }
          // Once after the replay, then whenever a pass found nothing new.
          if (!reportedCaughtUp || !advanced) {
            reportedCaughtUp = true;
            this.emitter.sendEvent(subscriptionId, "caught-up", { cursor: toPlainJson(current) });
          }
        } catch (err) {
          log.warn("liveRegistry", "changefeed error", err);
          this.emitter.sendEvent(subscriptionId, "change-error", {
            message: String((err as Error)?.message ?? err),
          });
        }
        if (disposed) return;
        await waitForChange();
      }
    };

    void loop();
    this.subs.set(subscriptionId, {
      cancel: () => {
        disposed = true;
        unsub?.();
        wake?.();
      },
    });
  }

  stop(subscriptionId: string): void {
    const sub = this.subs.get(subscriptionId);
    if (!sub) return;
    sub.cancel();
    this.subs.delete(subscriptionId);
  }

  disposeAll(): void {
    for (const id of [...this.subs.keys()]) this.stop(id);
  }
}

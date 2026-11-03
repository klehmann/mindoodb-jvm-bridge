import { VirtualViewFactory, type EncryptedPrivateKey, type PrivateUserId } from "mindoodb";
import { BridgeError, ErrorCodes, Methods } from "../protocol";
import type { DatabaseMode, SessionEnv } from "../sessionEnv";
import type { SyncLoop } from "../syncLoop";
import type { Emitter } from "./emitter";
import { NavigatorRegistry, type CoreNavigator } from "./navigatorRegistry";
import { StreamRegistry } from "./streamRegistry";
import { LiveRegistry } from "./liveRegistry";
import { projectDocument, toPlainJson } from "./projections";

type UpdateOp =
  | { op: "set"; path: string[]; value: unknown }
  | { op: "unset"; path: string[] }
  | { op: "merge"; value: Record<string, unknown> }
  | { op: "setText"; path: string[]; value: string };

interface Params {
  [key: string]: unknown;
}

/**
 * Headless port of the App Bridge host dispatch. Maps bridge method names to
 * the MindooDB core API for one already-open user session. Reads and writes are
 * synchronous request/response; attachment bytes and push updates flow through
 * the {@link Emitter}.
 */
export class Dispatcher {
  private readonly navReg = new NavigatorRegistry();
  private readonly streamReg: StreamRegistry;
  private readonly liveReg: LiveRegistry;

  constructor(
    private readonly env: SessionEnv,
    private readonly syncLoop: SyncLoop,
    private readonly emitter: Emitter,
  ) {
    this.streamReg = new StreamRegistry(emitter, (dbId) => this.db(dbId) as never);
    this.liveReg = new LiveRegistry(emitter, (dbId) => this.db(dbId) as never);
  }

  private db(dbId: string): ReturnType<SessionEnv["getDb"]> {
    if (!dbId) throw new BridgeError(ErrorCodes.BadRequest, "Missing 'db' parameter");
    return this.env.getDb(dbId);
  }

  /** Handle an inbound stream-credit frame for a read stream. */
  onStreamCredit(streamId: number, credits: number): void {
    this.streamReg.grantReadCredits(streamId, credits);
  }

  /** Handle an inbound binary chunk for a write stream. */
  onBinaryChunk(streamId: number, seq: number, payload: Uint8Array): void {
    this.streamReg.acceptWriteChunk(streamId, seq, payload);
  }

  dispose(): void {
    this.navReg.disposeAll();
    this.streamReg.disposeAll();
    this.liveReg.disposeAll();
  }

  async dispatch(method: string, rawParams: unknown): Promise<unknown> {
    const p = (rawParams ?? {}) as Params;
    switch (method) {
      case Methods.sessionRefresh:
        return this.syncLoop.refresh(p.db as string | undefined);
      case Methods.sessionAwaitIndexReady:
        return this.awaitIndexReady(p.db as string | undefined);
      case Methods.sessionListDatabases:
        return { databases: this.env.databaseIds() };
      case Methods.sessionOpenDatabase:
        return this.env.openDatabase(
          p.dbId as string,
          ((p.mode as string | undefined) ?? "local") as DatabaseMode,
        );
      case Methods.sessionSetServerUrl:
        return this.env.setServerUrl(p.serverUrl as string | null | undefined);
      case Methods.sessionPublishToServer:
        return this.publishToServer(p);
      case Methods.tenantApproveJoinRequest:
        return this.approveJoinRequest(p);
      case Methods.keybagCreateDocKey:
        return this.createDocKey(p);
      case Methods.keybagListKeys:
        return this.listKeys();
      case Methods.keybagPersist:
        await this.env.persistIdentity();
        return { ok: true };

      case Methods.docGet:
        return projectDocument(await this.db(p.db as string).getDocument(p.docId as string));
      case Methods.docList:
        return { ids: await this.db(p.db as string).getAllDocumentIds(p.options as never) };
      case Methods.docHeadCursor:
        return { cursor: toPlainJson(this.db(p.db as string).getLatestChangeCursor?.() ?? null) };
      case Methods.docCreate:
        return projectDocument(await this.db(p.db as string).createDocument(p.options as never));
      case Methods.docCreateMany: {
        const docs = await this.db(p.db as string).createDocuments((p.inputs as never[]) ?? []);
        return { documents: docs.map(projectDocument) };
      }
      case Methods.docUpdate:
        return this.updateDocument(p);
      case Methods.docDelete:
        await this.db(p.db as string).deleteDocument(p.docId as string, p.options as never);
        return { ok: true };
      case Methods.docUndelete:
        await this.db(p.db as string).undeleteDocument(p.docId as string, p.options as never);
        return { ok: true };
      case Methods.docListHistory:
        return this.listHistory(p);
      case Methods.docGetAtRevision: {
        const doc = await this.db(p.db as string).getDocumentAtTimestamp(
          p.docId as string,
          Number(p.timestamp),
        );
        return { document: doc ? projectDocument(doc) : null };
      }

      case Methods.query:
        return toPlainJson(await this.requireQuery(p.db as string)(p.query, p.options));

      case Methods.liveQueryStart:
        this.liveReg.startQuery(p.subscriptionId as string, p.db as string, p.query, p.options);
        return { ok: true };
      case Methods.liveQueryStop:
        this.liveReg.stop(p.subscriptionId as string);
        return { ok: true };

      case Methods.fulltextGetSetup:
        return { config: toPlainJson((await this.db(p.db as string).getFulltextSetup?.()) ?? null) };
      case Methods.fulltextSetSetup:
        await this.db(p.db as string).setFulltextSetup?.(p.config as never);
        return { ok: true };

      case Methods.viewOpenNavigator:
      case Methods.viewCreateNavigator:
        return this.openNavigator(p);
      case Methods.navCall:
        return { result: await this.navReg.call(p.navigatorId as string, p.method as string, (p.args as unknown[]) ?? []) };
      case Methods.navSubscribe: {
        const entry = this.navReg.get(p.navigatorId as string);
        this.liveReg.startViewWatch(p.subscriptionId as string, entry.dbId);
        this.navReg.setUnsubscribe(p.navigatorId as string, () => this.liveReg.stop(p.subscriptionId as string));
        return { ok: true };
      }
      case Methods.navUnsubscribe:
        this.liveReg.stop(p.subscriptionId as string);
        return { ok: true };
      case Methods.navDispose:
        this.navReg.dispose(p.navigatorId as string);
        return { ok: true };

      case Methods.attachList: {
        const doc = await this.db(p.db as string).getDocument(p.docId as string);
        return { attachments: projectDocument(doc).attachments };
      }
      case Methods.attachRemove:
        return this.streamReg.removeAttachment(p.db as string, p.docId as string, {
          fileName: p.fileName as string | undefined,
          attachmentId: p.attachmentId as string | undefined,
        });
      case Methods.attachOpenRead:
        return this.streamReg.openRead({
          dbId: p.db as string,
          docId: p.docId as string,
          fileName: p.fileName as string | undefined,
          attachmentId: p.attachmentId as string | undefined,
          startOffset: p.startOffset as number | undefined,
        });
      case Methods.attachOpenWrite:
        return this.streamReg.openWrite({
          dbId: p.db as string,
          docId: p.docId as string,
          fileName: p.fileName as string,
          mimeType: p.mimeType as string,
        });
      case Methods.attachCloseWrite:
        return this.streamReg.closeWrite(Number(p.streamId));
      case Methods.attachAbort:
        await this.streamReg.abort(Number(p.streamId));
        return { ok: true };
      case Methods.streamCreditRpc:
        this.streamReg.grantReadCredits(Number(p.streamId), Number(p.credits));
        return { ok: true };

      case Methods.changesIterateSince:
        this.liveReg.startChanges(p.subscriptionId as string, p.db as string, p.cursor ?? null);
        return { ok: true };

      default:
        throw new BridgeError(ErrorCodes.BadRequest, `Unknown method: ${method}`);
    }
  }

  // ----- helpers -----

  private requireQuery(dbId: string): (query: unknown, options?: unknown) => Promise<unknown> {
    const db = this.db(dbId) as unknown as {
      query?: (q: unknown, o?: unknown) => Promise<unknown>;
    };
    if (typeof db.query !== "function") {
      throw new BridgeError(ErrorCodes.BadRequest, "Ad-hoc query is not available on this database");
    }
    return db.query.bind(db);
  }

  private async awaitIndexReady(dbId?: string): Promise<{ ready: boolean }> {
    const ids = dbId ? [dbId] : this.env.databaseIds();
    for (const id of ids) {
      const db = this.db(id) as unknown as { getSummaryStore?: (c?: unknown) => unknown };
      // Materializing the summary store forces the changefeed-backed index to
      // exist; queries then answer from it. Best-effort - ignore if absent.
      try {
        db.getSummaryStore?.();
      } catch {
        /* summary not configured; queries will report coverage gaps */
      }
    }
    return { ready: true };
  }

  private async updateDocument(p: Params): Promise<{ heads: string[]; conflictExpected?: boolean }> {
    const db = this.db(p.db as string);
    const doc = await db.getDocument(p.docId as string);
    const baseHeads = p.baseHeads as string[] | undefined;
    if (baseHeads && !headsEqual(baseHeads, doc.getHeads())) {
      // Optimistic concurrency: the caller edited a stale version. MindooDB/
      // Automerge still merges, but we surface the divergence so the client can
      // decide whether to re-read.
      // (We proceed with the merge rather than rejecting - CRDT semantics.)
    }
    const ops = (p.operations as UpdateOp[]) ?? [];
    await db.changeDoc(doc, (mutable) => {
      const data = mutable.getData() as Record<string, unknown>;
      for (const op of ops) applyOp(data, op);
    });
    return {
      heads: doc.getHeads(),
      conflictExpected: baseHeads ? !headsEqual(baseHeads, doc.getHeads()) : undefined,
    };
  }

  private async listHistory(p: Params): Promise<{ history: unknown[] }> {
    const db = this.db(p.db as string) as unknown as {
      iterateDocumentHistory?: (docId: string) => AsyncGenerator<{
        doc: { getData(): unknown; getHeads(): string[]; isDeleted(): boolean };
        changeCreatedAt: number;
        changeCreatedByPublicKey?: string;
      }>;
    };
    if (typeof db.iterateDocumentHistory !== "function") {
      throw new BridgeError(ErrorCodes.BadRequest, "Document history is not available on this database");
    }
    const history: unknown[] = [];
    for await (const item of db.iterateDocumentHistory(p.docId as string)) {
      history.push({
        data: toPlainJson(item.doc.getData()),
        heads: item.doc.getHeads(),
        isDeleted: item.doc.isDeleted(),
        changeCreatedAt: item.changeCreatedAt,
        changeCreatedByPublicKey: item.changeCreatedByPublicKey ?? null,
      });
    }
    return { history };
  }

  private async openNavigator(p: Params): Promise<{ navigatorId: string; columns?: unknown }> {
    const dbId = p.db as string;
    const db = this.db(dbId) as unknown as {
      queryView?: (definition: unknown, options?: unknown) => Promise<unknown>;
    };
    if (typeof db.queryView !== "function") {
      throw new BridgeError(ErrorCodes.BadRequest, "Virtual views are not available on this database");
    }
    const view = await db.queryView(p.definition, p.options);
    const nav = buildNavigator(view, p.navigatorOptions as NavigatorOptions | undefined);
    const navigatorId = this.navReg.register(nav, dbId);
    return { navigatorId };
  }

  private async createDocKey(p: Params): Promise<{ keyId: string; keys: string[] }> {
    const keyId = p.keyId as string;
    if (!keyId) throw new BridgeError(ErrorCodes.BadRequest, "keybag.createDocKey requires keyId");
    await this.env.keyBag.createDocKey(this.env.tenantId, keyId);
    await this.env.persistIdentity();
    return { keyId, keys: await this.env.keyBag.listKeys() };
  }

  private async listKeys(): Promise<{ keys: string[]; details: unknown[] }> {
    const keys = await this.env.keyBag.listKeys();
    const details = await this.env.keyBag.listKeyDetails();
    return { keys, details: toPlainJson(details) as unknown[] };
  }

  private async approveJoinRequest(p: Params): Promise<{ format: string; joinResponse: unknown }> {
    const joinRequest = p.joinRequest;
    if (joinRequest == null) {
      throw new BridgeError(ErrorCodes.BadRequest, "tenant.approveJoinRequest requires joinRequest");
    }
    const adminPassword = p.adminPassword as string | undefined;
    if (!adminPassword) {
      throw new BridgeError(
        ErrorCodes.BadRequest,
        "tenant.approveJoinRequest requires adminPassword",
      );
    }

    // Prefer an explicit adminSigningKey; otherwise use adminUser from the
    // identity bundle written at tenant.create.
    let adminSigningKey = p.adminSigningKey as EncryptedPrivateKey | undefined;
    if (!adminSigningKey) {
      const adminUser =
        (p.adminIdentity as PrivateUserId | undefined) ?? this.env.bundle.adminUser;
      if (!adminUser) {
        throw new BridgeError(
          ErrorCodes.BadRequest,
          "tenant.approveJoinRequest requires adminSigningKey or adminIdentity (or a bundle with adminUser)",
        );
      }
      adminSigningKey = adminUser.userSigningKeyPair.privateKey;
    }

    const format = (p.format as "object" | "uri" | undefined) ?? "uri";
    const joinResponse = await this.env.tenant.approveJoinRequest(joinRequest as never, {
      adminSigningKey,
      adminPassword,
      sharePassword: p.sharePassword as string | undefined,
      serverUrl: (p.serverUrl as string | undefined) ?? this.env.serverUrl,
      adminUsername: p.adminUsername as string | undefined,
      sharedDocKeyIds: p.sharedDocKeyIds as string[] | undefined,
      label: p.label as string | undefined,
      format,
    } as never);
    return { format, joinResponse };
  }

  private async publishToServer(p: Params): Promise<{ ok: true; serverUrl: string }> {
    const serverUrl = p.serverUrl as string | undefined;
    if (!serverUrl) {
      throw new BridgeError(ErrorCodes.BadRequest, "session.publishToServer requires serverUrl");
    }
    const systemAdminUser = p.systemAdminUser as PrivateUserId | undefined;
    const systemAdminPassword = p.systemAdminPassword as string | undefined;
    if (!systemAdminUser || !systemAdminPassword) {
      throw new BridgeError(
        ErrorCodes.BadRequest,
        "session.publishToServer requires systemAdminUser and systemAdminPassword",
      );
    }

    await this.env.tenant.publishToServer(serverUrl, {
      systemAdminUser,
      systemAdminPassword,
      adminUsername: (p.adminUsername as string | undefined) ?? this.env.bundle.adminUser?.username,
      registerUsers: p.registerUsers as never,
    });

    // Bootstrap-push the directory so grants land before regular users sync.
    try {
      const directory = await this.env.tenant.openDB("directory");
      const remote = await this.env.tenant.connectToServer(serverUrl, "directory");
      const adminUser = this.env.bundle.adminUser;
      const pushOpts =
        adminUser && p.adminPassword
          ? {
              networkAuthOverride: {
                user: adminUser,
                password: p.adminPassword as string,
              },
            }
          : undefined;
      const dbAny = directory as unknown as {
        pushChangesTo?: (r: unknown, opts?: unknown) => Promise<unknown>;
      };
      if (typeof dbAny.pushChangesTo === "function") {
        await dbAny.pushChangesTo(remote, pushOpts);
      }
    } catch (err) {
      // Registration succeeded; directory bootstrap is best-effort.
      // Callers can still sync later via session.refresh.
    }

    await this.env.setServerUrl(serverUrl);
    this.syncLoop.start();
    return { ok: true, serverUrl: serverUrl.replace(/\/$/, "") };
  }
}

interface NavigatorOptions {
  expandLevel?: number;
  expandAll?: boolean;
  hideEmptyCategories?: boolean;
}

/** A VirtualView (or ephemeral wrapper exposing `.view`). */
function resolveView(view: unknown): unknown {
  const wrapper = view as { view?: unknown; getView?: () => unknown };
  if (wrapper && typeof wrapper.getView === "function") return wrapper.getView();
  if (wrapper && wrapper.view) return wrapper.view;
  return view;
}

function buildNavigator(view: unknown, options?: NavigatorOptions): CoreNavigator {
  const resolved = resolveView(view);
  const builder = VirtualViewFactory.createNavigator(resolved as never) as unknown as {
    hideEmptyCategories?: () => unknown;
    expandAll?: () => unknown;
    expandToLevel?: (n: number) => unknown;
    build: () => CoreNavigator;
  };
  if (options?.hideEmptyCategories) builder.hideEmptyCategories?.();
  if (options?.expandAll) builder.expandAll?.();
  else if (typeof options?.expandLevel === "number") builder.expandToLevel?.(options.expandLevel);
  return builder.build();
}

function headsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((h) => set.has(h));
}

function applyOp(data: Record<string, unknown>, op: UpdateOp): void {
  switch (op.op) {
    case "set":
    case "setText":
      setNested(data, op.path, op.value);
      break;
    case "unset":
      unsetNested(data, op.path);
      break;
    case "merge":
      for (const [key, value] of Object.entries(op.value)) data[key] = value as unknown;
      break;
  }
}

function setNested(root: Record<string, unknown>, path: string[], value: unknown): void {
  if (path.length === 0) return;
  let node = root;
  for (let i = 0; i < path.length - 1; i++) {
    const key = path[i];
    if (typeof node[key] !== "object" || node[key] === null) node[key] = {};
    node = node[key] as Record<string, unknown>;
  }
  node[path[path.length - 1]] = value;
}

function unsetNested(root: Record<string, unknown>, path: string[]): void {
  if (path.length === 0) return;
  let node = root;
  for (let i = 0; i < path.length - 1; i++) {
    const key = path[i];
    if (typeof node[key] !== "object" || node[key] === null) return;
    node = node[key] as Record<string, unknown>;
  }
  delete node[path[path.length - 1]];
}

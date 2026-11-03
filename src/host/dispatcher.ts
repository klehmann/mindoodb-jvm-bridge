import {
  USER_DIRECTORY_DB_ID,
  VirtualViewFactory,
  type EncryptedPrivateKey,
  type MindooDocumentUpdate,
  type MindooJsonPatch,
  type MindooRichTextMaterializeValue,
  type MindooRichTextPatch,
  type MindooRichTextStep,
  type MindooRichTextStepPatch,
  type MindooTextCursorPosition,
  type PrivateUserId,
} from "mindoodb";
import { BridgeError, ErrorCodes, Methods } from "../protocol";
import type { DatabaseMode, SessionEnv } from "../sessionEnv";
import type { DatabaseSyncResult, SyncDirection, SyncLoop } from "../syncLoop";
import type { Emitter } from "./emitter";
import { NavigatorRegistry, type CoreNavigator } from "./navigatorRegistry";
import { StreamRegistry } from "./streamRegistry";
import { LiveRegistry } from "./liveRegistry";
import { projectDocument, toPlainJson } from "./projections";

type Path = Array<string | number>;

/**
 * One `documents.update` operation. Values may carry MindooDB typed values
 * (`{ "$mindoo": "atomic" | "counter" | "timestamp", value }`); a plain string
 * is collaborative text, as everywhere in MindooDB.
 */
type UpdateOp =
  | { op: "set"; path: Path; value: unknown }
  | { op: "unset"; path: Path }
  | { op: "merge"; value: Record<string, unknown> }
  | { op: "setText"; path: Path; value: string }
  | { op: "textSplice"; path: Path; index: number; deleteCount: number; insert?: string }
  | { op: "increment"; path: Path; delta: number }
  | { op: "listInsert"; path: Path; index: number; values: unknown[] }
  | { op: "listDelete"; path: Path; index: number; deleteCount: number }
  | { op: "textMark"; path: Path; index: number; length: number; marks: Record<string, MindooRichTextMaterializeValue> }
  | { op: "textUnmark"; path: Path; index: number; length: number; names: string[] }
  | ({ op: "richText" } & Omit<MindooRichTextPatch, "baseHeads">)
  | { op: "richTextSteps"; path: Path; steps: MindooRichTextStep[] };

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
        this.requireServerUrl("session.refresh");
        return this.syncLoop.refresh(p.db as string | undefined, (p.direction as SyncDirection | undefined) ?? "both");
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
        return this.createDocument(p);
      case Methods.docCreateMany: {
        const docs = await this.db(p.db as string).createDocuments((p.inputs as never[]) ?? []);
        return { documents: docs.map(projectDocument) };
      }
      case Methods.docDeleteMany:
        await this.db(p.db as string).deleteDocuments((p.docIds as string[]) ?? [], p.options as never);
        return { ok: true };
      case Methods.docListDeleted:
        return { ids: await this.db(p.db as string).getDeletedDocumentIds(p.options as never) };
      case Methods.docUpdate:
        return this.updateDocument(p);
      case Methods.docDelete:
        await this.db(p.db as string).deleteDocument(p.docId as string, p.options as never);
        return { ok: true };
      case Methods.docUndelete:
        await this.db(p.db as string).undeleteDocument(p.docId as string, p.options as never);
        return { ok: true };
      case Methods.docGetRichText: {
        const db = this.db(p.db as string);
        return toPlainJson(await db.getRichTextSnapshot(await db.getDocument(p.docId as string), p.path as Path));
      }
      case Methods.docGetTextCursors: {
        const db = this.db(p.db as string);
        return db.getTextCursors(
          await db.getDocument(p.docId as string),
          p.path as Path,
          (p.positions as MindooTextCursorPosition[]) ?? [],
          p.heads ? { heads: p.heads as string[] } : undefined,
        );
      }
      case Methods.docResolveTextCursors: {
        const db = this.db(p.db as string);
        return db.resolveTextCursors(
          await db.getDocument(p.docId as string),
          p.path as Path,
          (p.cursors as string[]) ?? [],
          p.heads ? { heads: p.heads as string[] } : undefined,
        );
      }
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
        const navigatorId = p.navigatorId as string;
        const subscriptionId = p.subscriptionId as string;
        const entry = this.navReg.get(navigatorId);
        // Keep the view itself current, then tell the client to re-read.
        const unbind = entry.view?.bindTo?.();
        this.liveReg.startViewWatch(subscriptionId, entry.dbId, () => this.navReg.refresh(navigatorId));
        this.navReg.setUnsubscribe(navigatorId, () => {
          this.liveReg.stop(subscriptionId);
          unbind?.();
        });
        return { ok: true };
      }
      case Methods.navUnsubscribe:
        if (p.navigatorId) this.navReg.clearUnsubscribe(p.navigatorId as string);
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

  private requireServerUrl(method: string): string {
    const url = this.env.serverUrl;
    if (!url) {
      throw new BridgeError(ErrorCodes.BadRequest, `${method} needs a serverUrl (session.setServerUrl or session.publishToServer)`);
    }
    return url;
  }

  /**
   * MindooDB refuses `initialValues` together with a custom `id` (the
   * deterministic seed change of a convergent id cannot carry them), so those
   * are written in a follow-up change.
   */
  private async createDocument(p: Params): Promise<unknown> {
    const db = this.db(p.db as string);
    const options = { ...((p.options as Record<string, unknown> | undefined) ?? {}) };
    const initialValues = options.initialValues as Record<string, unknown> | undefined;
    if (options.id && !options.assumeUniqueId && initialValues && Object.keys(initialValues).length > 0) {
      delete options.initialValues;
      const doc = await db.createDocument(options as never);
      const result = await db.applyDocumentUpdate(doc, { set: initialValues });
      return projectDocument(result.doc);
    }
    return projectDocument(await db.createDocument(options as never));
  }

  /**
   * Applies the operations as one change. With `baseHeads` the change is made
   * against that version and merged into the current one, so concurrent edits
   * of other fields survive and edits of the same field merge like any CRDT
   * change; `conflictExpected` tells the caller that the document had moved on.
   */
  private async updateDocument(p: Params): Promise<{ heads: string[]; conflictExpected?: boolean }> {
    const db = this.db(p.db as string);
    const doc = await db.getDocument(p.docId as string);
    const headsBefore = doc.getHeads();
    const baseHeads = p.baseHeads as string[] | undefined;
    const ops = (p.operations as UpdateOp[]) ?? [];
    const update = toDocumentUpdate(ops, baseHeads && baseHeads.length > 0 ? baseHeads : undefined);
    const result = await db.applyDocumentUpdate(doc, update, p.options as never);
    return {
      heads: result.heads,
      conflictExpected: baseHeads ? !headsEqual(baseHeads, headsBefore) : undefined,
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
    const view = (await db.queryView(p.definition, p.options)) as EphemeralView;
    const nav = buildNavigator(view, p.navigatorOptions as NavigatorOptions | undefined);
    const navigatorId = this.navReg.register(nav, dbId, view);
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

  private async approveJoinRequest(
    p: Params,
  ): Promise<{ format: string; joinResponse: unknown; pushed: DatabaseSyncResult[] }> {
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
    const adminUser = (p.adminIdentity as PrivateUserId | undefined) ?? this.env.bundle.adminUser;
    let adminSigningKey = p.adminSigningKey as EncryptedPrivateKey | undefined;
    if (!adminSigningKey) {
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
      serverUrl: (p.serverUrl as string | undefined) ?? this.env.serverUrl,
      adminUsername: (p.adminUsername as string | undefined) ?? adminUser?.username,
      sharedDocKeyIds: p.sharedDocKeyIds as string[] | undefined,
      label: p.label as string | undefined,
      username: p.username as string | undefined,
      tenantLabel: p.tenantLabel as string | undefined,
      format,
    } as never);

    // The grant lives in the admin-signed directory: the new member can only
    // sync once it is on the server.
    const pushed = this.env.serverUrl ? await this.pushSystemDatabases(adminUser, adminPassword) : [];
    return { format, joinResponse, pushed };
  }

  /** Push `directory` and `userdirectory`, authenticated as the tenant admin when known. */
  private async pushSystemDatabases(adminUser: PrivateUserId | undefined, adminPassword: string | undefined) {
    const networkAuthOverride = adminUser && adminPassword ? { user: adminUser, password: adminPassword } : undefined;
    const pushed: DatabaseSyncResult[] = [];
    for (const dbId of ["directory", USER_DIRECTORY_DB_ID]) {
      pushed.push(await this.syncLoop.pushNow(dbId, { networkAuthOverride }));
    }
    return pushed;
  }

  /**
   * Register the tenant on a MindooDB server and push its directory, as Haven
   * does. `systemAdminUser`/`systemAdminPassword` are needed unless the server
   * lets anyone register tenants (capability `POST:/system/tenants/*`).
   */
  private async publishToServer(p: Params): Promise<{ ok: true; serverUrl: string; pushed: DatabaseSyncResult[] }> {
    const serverUrl = (p.serverUrl as string | undefined)?.replace(/\/$/, "");
    if (!serverUrl) {
      throw new BridgeError(ErrorCodes.BadRequest, "session.publishToServer requires serverUrl");
    }
    const systemAdminUser = p.systemAdminUser as PrivateUserId | undefined;
    const systemAdminPassword = p.systemAdminPassword as string | undefined;
    if (Boolean(systemAdminUser) !== Boolean(systemAdminPassword)) {
      throw new BridgeError(
        ErrorCodes.BadRequest,
        "session.publishToServer needs systemAdminUser and systemAdminPassword together",
      );
    }
    const adminUser = (p.adminIdentity as PrivateUserId | undefined) ?? this.env.bundle.adminUser;

    // A server that lets any signed principal register tenants accepts the
    // tenant admin in place of a system admin.
    const adminPassword = p.adminPassword as string | undefined;
    const auth = systemAdminUser
      ? { systemAdminUser, systemAdminPassword }
      : adminUser && adminPassword
        ? { systemAdminUser: adminUser, systemAdminPassword: adminPassword }
        : {};
    await this.env.tenant.publishToServer(serverUrl, {
      ...auth,
      adminUsername: (p.adminUsername as string | undefined) ?? adminUser?.username,
      registerUsers: p.registerUsers as never,
    });

    await this.env.setServerUrl(serverUrl);
    // Without the directory (grants) and userdirectory (User-Key wraps) on the
    // server, nobody - not even this user - can sync the tenant.
    const pushed = await this.pushSystemDatabases(adminUser, adminPassword);
    this.syncLoop.start();
    return { ok: true, serverUrl, pushed };
  }
}

interface NavigatorOptions {
  expandLevel?: number;
  expandAll?: boolean;
  hideEmptyCategories?: boolean;
  categoriesOnly?: boolean;
  documentsOnly?: boolean;
  fromCategory?: string;
}

/** The `EphemeralSummaryView` returned by `MindooDB.queryView`. */
export interface EphemeralView {
  getView(): unknown;
  update(options?: unknown): Promise<void>;
  bindTo?(): () => void;
  dispose?(): void;
}

function buildNavigator(view: EphemeralView, options?: NavigatorOptions): CoreNavigator {
  const builder = VirtualViewFactory.createNavigator(view.getView() as never);
  if (options?.hideEmptyCategories) builder.hideEmptyCategories();
  if (options?.categoriesOnly) builder.categoriesOnly();
  if (options?.documentsOnly) builder.documentsOnly();
  if (options?.fromCategory) builder.fromCategory(options.fromCategory);
  const nav = builder.build() as unknown as CoreNavigator;
  // Expansion lives on the navigator (not the builder) since MindooDB 0.0.8x.
  if (options?.expandAll) nav.expandAll();
  else if (typeof options?.expandLevel === "number") nav.expandToLevel(options.expandLevel);
  return nav;
}

function headsEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const set = new Set(a);
  return b.every((h) => set.has(h));
}

/**
 * One `applyDocumentUpdate` from the operations: JSON ops become one JSON
 * patch, rich-text ops one rich-text (step) patch each, all at `baseHeads`.
 */
function toDocumentUpdate(ops: UpdateOp[], baseHeads: string[] | undefined): MindooDocumentUpdate {
  const patch: Required<Omit<MindooJsonPatch, "baseHeads">> = {
    set: [],
    unset: [],
    listInsert: [],
    listDelete: [],
    textSplice: [],
    textMark: [],
    textUnmark: [],
    counterIncrement: [],
  };
  const richText: MindooRichTextPatch[] = [];
  const richTextSteps: MindooRichTextStepPatch[] = [];
  for (const op of ops) {
    switch (op.op) {
      case "set":
      case "setText":
        patch.set.push({ path: op.path, value: op.value });
        break;
      case "merge":
        for (const [key, value] of Object.entries(op.value)) patch.set.push({ path: [key], value });
        break;
      case "unset":
        patch.unset.push({ path: op.path });
        break;
      case "textSplice":
        patch.textSplice.push({ path: op.path, index: op.index, deleteCount: op.deleteCount, insert: op.insert });
        break;
      case "increment":
        patch.counterIncrement.push({ path: op.path, delta: op.delta });
        break;
      case "listInsert":
        patch.listInsert.push({ path: op.path, index: op.index, values: op.values });
        break;
      case "listDelete":
        patch.listDelete.push({ path: op.path, index: op.index, deleteCount: op.deleteCount });
        break;
      case "textMark":
        patch.textMark.push({ path: op.path, index: op.index, length: op.length, marks: op.marks });
        break;
      case "textUnmark":
        patch.textUnmark.push({ path: op.path, index: op.index, length: op.length, names: op.names });
        break;
      case "richText": {
        const { op: _op, ...rest } = op;
        richText.push({ ...rest, baseHeads });
        break;
      }
      case "richTextSteps":
        richTextSteps.push({ path: op.path, steps: op.steps, baseHeads });
        break;
      default:
        throw new BridgeError(ErrorCodes.BadRequest, `Unknown update op: ${(op as { op?: unknown }).op}`);
    }
  }
  const update: MindooDocumentUpdate = {};
  if (Object.values(patch).some((list) => list.length > 0) || (richText.length === 0 && richTextSteps.length === 0)) {
    update.json = { ...patch, baseHeads };
  }
  if (richText.length > 0) update.richText = richText;
  if (richTextSteps.length > 0) update.richTextSteps = richTextSteps;
  return update;
}

// End-to-end over the real bridge, frame by frame like the Kotlin client: local
// tenant, CRUD, typed values, attachments, query, full-text, views, changefeed,
// publish, join, two-user sync (docs + attachments), remote-primary databases.
//
// Usage: node scripts/e2e/server-sync.mjs <emptyDataRoot> <serverUrl>
// The server must let any signed user register tenants (see README "Tests").
import { startSidecar, Conn, assert } from "./client.mjs";

const dataRoot = process.argv[2];
const serverUrl = process.argv[3];
const tenantId = "kmp" + Date.now().toString(36);
const failures = [];

async function step(name, fn) {
  console.log("## " + name);
  try { return await fn(); } catch (e) { console.log("  FAIL -", e.message); failures.push(`${name}: ${e.message}`); }
}

const f = (path) => ({ kind: "field", path });
const l = (value) => ({ kind: "literal", value });
const op = (o, ...args) => ({ kind: "operation", op: o, args });

const sidecar = await startSidecar(dataRoot);
try {
  const boot = await new Conn(sidecar.hs).connect();
  const created = await step("tenant.create", () => boot.call("tenant.create", {
    tenantId, adminName: "cn=admin/o=kmp", adminPassword: "admin-pw",
    userName: "cn=alice/o=kmp", userPassword: "alice-pw", databases: ["notes"],
  }));
  boot.close();

  const alice = await new Conn(sidecar.hs).connect();
  await alice.open({ tenantId, userId: "cn=alice/o=kmp", password: "alice-pw", databases: ["notes"] });

  let doc;
  await step("CRUD", async () => {
    doc = await alice.call("documents.create", { db: "notes", options: { initialValues: { type: "note", title: "Hello", category: "a" } } });
    assert(doc.data.title === "Hello", "create returns data");
    const upd = await alice.call("documents.update", { db: "notes", docId: doc.id, baseHeads: doc.heads, operations: [
      { op: "set", path: ["title"], value: "Hello (edited)" },
      { op: "set", path: ["meta", "n"], value: 3 },
      { op: "merge", value: { tags: ["kotlin", "mindoodb"] } },
      { op: "setText", path: ["body"], value: "Some body text" },
    ] });
    assert(Array.isArray(upd.heads) && upd.heads.length > 0, "update returns heads");
    const re = await alice.call("documents.get", { db: "notes", docId: doc.id });
    assert(re.data.title === "Hello (edited)" && re.data.meta?.n === 3 && re.data.tags?.length === 2, "update applied");
    assert(re.data.body === "Some body text", "setText applied");
    const stale = await alice.call("documents.update", { db: "notes", docId: doc.id, baseHeads: doc.heads, operations: [{ op: "set", path: ["x"], value: 1 }] });
    assert(stale.conflictExpected === true, "stale baseHeads flags conflictExpected");
    const many = await alice.call("documents.createMany", { db: "notes", inputs: [
      { initialValues: { type: "note", title: "B", category: "b" } },
      { initialValues: { type: "note", title: "C", category: "a" } },
    ] });
    assert(many.documents.length === 2, "createMany");
    const custom = await alice.call("documents.create", { db: "notes", options: { id: "settings_main", initialValues: { type: "settings" } } });
    assert(custom.id === "settings_main", "create with custom id");
    const ids = await alice.call("documents.list", { db: "notes" });
    assert(custom.data.type === "settings", "custom id keeps initial values");
    assert(ids.ids.length === 4, "list returns 4 ids, got " + ids.ids.length);
    // typed values + increments
    const typed = await alice.call("documents.update", { db: "notes", docId: custom.id, operations: [
      { op: "set", path: ["status"], value: { $mindoo: "atomic", value: "open" } },
      { op: "set", path: ["hits"], value: { $mindoo: "counter", value: 1 } },
    ] });
    await alice.call("documents.update", { db: "notes", docId: custom.id, operations: [{ op: "increment", path: ["hits"], delta: 4 }] });
    const t2 = await alice.call("documents.get", { db: "notes", docId: custom.id });
    assert(t2.data.status === "open" && t2.data.hits === 5, "atomic + counter + increment: " + JSON.stringify(t2.data));
    // two concurrent edits from the same base merge
    const base = await alice.call("documents.get", { db: "notes", docId: doc.id });
    await alice.call("documents.update", { db: "notes", docId: doc.id, baseHeads: base.heads, operations: [{ op: "set", path: ["a"], value: 1 }] });
    const r2 = await alice.call("documents.update", { db: "notes", docId: doc.id, baseHeads: base.heads, operations: [{ op: "set", path: ["b"], value: 2 }] });
    const merged = await alice.call("documents.get", { db: "notes", docId: doc.id });
    assert(merged.data.a === 1 && merged.data.b === 2 && r2.conflictExpected === true, "baseHeads edits merge");
    await alice.call("documents.delete", { db: "notes", docId: many.documents[0].id });
    const del = await alice.call("documents.get", { db: "notes", docId: many.documents[0].id }).catch((e) => ({ err: e.message }));
    console.log("    get deleted ->", JSON.stringify(del).slice(0, 160));
    const ids2 = await alice.call("documents.list", { db: "notes" });
    assert(ids2.ids.length === 3, "deleted doc not listed");
    await alice.call("documents.undelete", { db: "notes", docId: many.documents[0].id });
    const ids3 = await alice.call("documents.list", { db: "notes" });
    assert(ids3.ids.length === 4, "undelete restores");
    const hist = await alice.call("documents.listHistory", { db: "notes", docId: doc.id });
    assert(hist.history.length >= 2, "history has entries: " + hist.history.length);
    const at = await alice.call("documents.getAtRevision", { db: "notes", docId: doc.id, timestamp: hist.history[0].changeCreatedAt });
    assert(at.document?.data?.title === "Hello", "getAtRevision first revision");
    const missing = await alice.call("documents.get", { db: "notes", docId: "does_not_exist" }).catch((e) => e);
    console.log("    missing doc ->", missing.code, missing.message);
    const dm = await alice.call("documents.createMany", { db: "notes", inputs: [{ initialValues: { type: "tmp" } }, { initialValues: { type: "tmp" } }] });
    await alice.call("documents.deleteMany", { db: "notes", docIds: dm.documents.map((d) => d.id) });
    const deleted = await alice.call("documents.listDeleted", { db: "notes" });
    assert(dm.documents.every((d) => deleted.ids.includes(d.id)), "deleteMany + listDeleted");
  });

  let rt;
  await step("rich text, marks, cursors", async () => {
    rt = await alice.call("documents.create", { db: "notes", options: { initialValues: { type: "page" } } });
    await alice.call("documents.update", { db: "notes", docId: rt.id, operations: [{ op: "richText", path: ["content"], spans: [
      { type: "block", value: { type: "paragraph" } },
      { type: "text", value: "Hello " },
      { type: "text", value: "world", marks: { bold: true } },
    ] }] });
    let snap = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    console.log("    spans:", JSON.stringify(snap.spans));
    assert(snap.spans.some((sp) => sp.type === "text" && sp.value === "world" && sp.marks?.bold === true), "richText patch + snapshot keeps marks");
    const plain = await alice.call("documents.get", { db: "notes", docId: rt.id });
    console.log("    plain content:", JSON.stringify(plain.data.content));
    // positional step at the heads of the snapshot
    await alice.call("documents.update", { db: "notes", docId: rt.id, baseHeads: snap.heads, operations: [
      { op: "richTextSteps", path: ["content"], steps: [{ type: "splice", index: 7, deleteCount: 0, insert: "big ", marks: [{ index: 7, length: 3, marks: { italic: true } }] }] },
    ] });
    snap = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    console.log("    after steps:", JSON.stringify(snap.spans));
    assert(snap.spans.some((sp) => sp.type === "text" && sp.value.includes("big") && sp.marks?.italic === true), "richTextSteps insert with mark");
    // marks via JSON patch on plain collaborative text
    await alice.call("documents.update", { db: "notes", docId: rt.id, operations: [{ op: "setText", path: ["title"], value: "Marked title" }] });
    await alice.call("documents.update", { db: "notes", docId: rt.id, operations: [{ op: "textMark", path: ["title"], index: 0, length: 6, marks: { bold: true } }] });
    let t = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["title"] });
    assert(t.spans.some((sp) => sp.value === "Marked" && sp.marks?.bold === true), "textMark");
    await alice.call("documents.update", { db: "notes", docId: rt.id, operations: [{ op: "textUnmark", path: ["title"], index: 0, length: 6, names: ["bold"] }] });
    t = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["title"] });
    assert(!t.spans.some((sp) => sp.marks?.bold), "textUnmark: " + JSON.stringify(t.spans));
    // cursors survive an insert before them
    const c = await alice.call("documents.getTextCursors", { db: "notes", docId: rt.id, path: ["title"], positions: [7, "end"] });
    await alice.call("documents.update", { db: "notes", docId: rt.id, operations: [{ op: "textSplice", path: ["title"], index: 0, deleteCount: 0, insert: "My " }] });
    const r = await alice.call("documents.resolveTextCursors", { db: "notes", docId: rt.id, path: ["title"], cursors: c.cursors });
    assert(r.positions[0] === 10 && r.positions[1] === 15, "cursors move with the text: " + JSON.stringify(r.positions));
    const old = await alice.call("documents.resolveTextCursors", { db: "notes", docId: rt.id, path: ["title"], cursors: c.cursors, heads: c.heads });
    assert(old.positions[0] === 7, "cursor at old heads");
  });

  await step("attachments", async () => {
    const big = Buffer.alloc(3 * 1024 * 1024 + 17);
    for (let i = 0; i < big.length; i++) big[i] = (i * 31) & 0xff;
    const up = await alice.upload("notes", doc.id, "big.bin", "application/octet-stream", big);
    assert(up.attachment.fileName === "big.bin" && up.attachment.size === big.length, "upload size " + up.attachment.size);
    const list = await alice.call("attachments.list", { db: "notes", docId: doc.id });
    assert(list.attachments.length === 1, "list attachments");
    const back = await alice.readAttachment("notes", doc.id, "big.bin");
    assert(back.equals(big), "download equals upload (" + back.length + ")");
    await alice.call("attachments.remove", { db: "notes", docId: doc.id, fileName: "big.bin" });
    const list2 = await alice.call("attachments.list", { db: "notes", docId: doc.id });
    assert(list2.attachments.length === 0, "remove attachment");
  });

  await step("query", async () => {
    await alice.call("session.awaitIndexReady", { db: "notes" });
    const r = await alice.call("db.query", { db: "notes", query: { filter: op("eq", f("type"), l("note")), sortBy: [{ field: "title", direction: "ascending" }] } });
    console.log("    query keys:", Object.keys(r), "rows:", (r.rows ?? r.results ?? []).length);
    const rows = r.rows ?? r.results ?? r.documents ?? [];
    assert(rows.length === 3, "query returns 3 notes");
  });

  await step("fulltext", async () => {
    const before = await alice.call("fulltext.getSetup", { db: "notes" });
    console.log("    setup:", JSON.stringify(before));
    await alice.call("fulltext.setSetup", { db: "notes", config: { enabled: true, fields: ["title", "body"] } });
    const r = await alice.call("db.query", { db: "notes", query: { text: { query: "body" } } });
    const rows = r.rows ?? [];
    assert(rows.length === 1, "fulltext finds one doc, got " + rows.length + " " + JSON.stringify(r).slice(0, 200));
  });

  await step("virtual view", async () => {
    const nav = await alice.call("view.openNavigator", { db: "notes", definition: {
      filter: op("eq", f("type"), l("note")),
      columns: [
        { name: "category", title: "category", isCategory: true, sorting: "ascending", expression: f("category") },
        { name: "title", title: "title", isCategory: false, isHidden: false, expression: f("title") },
      ],
    }, navigatorOptions: { expandAll: true } });
    const rows = await alice.call("nav.call", { navigatorId: nav.navigatorId, method: "readForward", args: [100, 0] });
    console.log("    rows:", rows.result.map((e) => `${e.level}:${e.isCategory ? "cat " + e.categoryValue : e.columnValues.title}`).join(" | "));
    assert(rows.result.filter((e) => e.isCategory).length === 2, "2 categories");
    assert(rows.result.filter((e) => e.isDocument).length === 3, "3 docs");
    // live view: subscribe, change, expect view-changed
    let changed = false;
    alice.subs.set("vw1", (ev) => { if (ev === "view-changed") changed = true; });
    await alice.call("nav.subscribe", { navigatorId: nav.navigatorId, subscriptionId: "vw1" });
    await alice.call("documents.create", { db: "notes", options: { initialValues: { type: "note", title: "D", category: "c" } } });
    await new Promise((r) => setTimeout(r, 1500));
    assert(changed, "view-changed event");
    await alice.call("nav.call", { navigatorId: nav.navigatorId, method: "expandAll", args: [] });
    const rows2 = await alice.call("nav.call", { navigatorId: nav.navigatorId, method: "readForward", args: [100, 0] });
    assert(rows2.result.filter((e) => e.isDocument).length === 4, "navigator sees new doc after change (got " + rows2.result.filter((e) => e.isDocument).length + ")");
    await alice.call("nav.dispose", { navigatorId: nav.navigatorId });
  });

  await step("changefeed + live query", async () => {
    const changes = [];
    let caughtUp = false;
    alice.subs.set("cf1", (ev, p) => { if (ev === "change") changes.push(p); if (ev === "caught-up") caughtUp = true; });
    await alice.call("changes.iterateSince", { db: "notes", subscriptionId: "cf1", cursor: null });
    const lq = [];
    alice.subs.set("lq1", (ev, p) => lq.push([ev, p]));
    await alice.call("db.liveQuery.start", { db: "notes", subscriptionId: "lq1", query: { filter: op("eq", f("type"), l("note")) } });
    await new Promise((r) => setTimeout(r, 1500));
    const n0 = changes.length;
    assert(n0 >= 6, "changefeed replay " + n0);
    assert(caughtUp, "caught-up event");
    await alice.call("documents.create", { db: "notes", options: { initialValues: { type: "note", title: "E", category: "c" } } });
    await new Promise((r) => setTimeout(r, 2000));
    assert(changes.length > n0, "changefeed follows live");
    const lastRows = lq.filter(([e]) => e === "query-result").at(-1)?.[1]?.rows ?? [];
    assert(lastRows.length === 5, "live query re-emitted with 5 rows, got " + lastRows.length + " " + JSON.stringify(lq.map(([e]) => e)));
    await alice.call("db.liveQuery.stop", { subscriptionId: "cf1" });
    await alice.call("db.liveQuery.stop", { subscriptionId: "lq1" });
  });

  await step("incremental reads (pages, metadata, count, idPrefix)", async () => {
    for (let i = 0; i < 5; i++) {
      await alice.call("documents.create", { db: "notes", options: { idPrefix: "inv", initialValues: { type: "invoice", n: i } } });
    }
    const total = (await alice.call("changes.count", { db: "notes", cursor: null })).count;
    const inv = (await alice.call("changes.count", { db: "notes", cursor: null, idPrefix: "inv" })).count;
    assert(inv === 5 && total > inv, `count all=${total}, inv=${inv}`);
    // page through invoices, 2 at a time, keeping only the cursor
    let cursor = null, seen = [], pages = 0, done = false;
    while (!done) {
      const page = await alice.call("changes.read", { db: "notes", cursor, limit: 2, idPrefix: "inv" });
      seen.push(...page.changes.map((c) => c.doc.data.n));
      cursor = page.cursor; done = page.done; pages++;
    }
    assert(seen.length === 5 && pages === 3, `paged ${pages} pages: ${seen}`);
    const empty = await alice.call("changes.read", { db: "notes", cursor, idPrefix: "inv" });
    assert(empty.changes.length === 0 && empty.done, "nothing new after the saved cursor");
    // a change after the saved cursor shows up, once
    const first = (await alice.call("changes.read", { db: "notes", cursor: null, limit: 1, idPrefix: "inv" })).changes[0];
    await alice.call("documents.update", { db: "notes", docId: first.doc.id, operations: [{ op: "set", path: ["paid"], value: true }] });
    const next = await alice.call("changes.read", { db: "notes", cursor, idPrefix: "inv" });
    assert(next.changes.length === 1 && next.changes[0].doc.data.paid === true, "resume from saved cursor");
    const meta = await alice.call("changes.read", { db: "notes", cursor: null, metadataOnly: true, limit: 100 });
    assert(meta.changes.every((c) => c.meta?.docId && !c.doc) && meta.changes.length === total, "metadata-only pages: " + meta.changes.length);
    assert((await alice.call("changes.count", { db: "notes", cursor: meta.cursor })).count === 0, "count after head is 0");
    const metaEvents = [];
    alice.subs.set("cfm", (ev, p) => { if (ev === "change") metaEvents.push(p); });
    await alice.call("changes.iterateSince", { db: "notes", subscriptionId: "cfm", cursor: meta.cursor, metadataOnly: true, idPrefix: "inv" });
    await alice.call("documents.update", { db: "notes", docId: first.doc.id, operations: [{ op: "set", path: ["paid"], value: false }] });
    await new Promise((r) => setTimeout(r, 1500));
    await alice.call("db.liveQuery.stop", { subscriptionId: "cfm" });
    assert(metaEvents.length === 1 && metaEvents[0].meta.docId === first.doc.id, "live metadata changefeed with idPrefix");
  });

  await step("keybag", async () => {
    const k = await alice.call("keybag.createDocKey", { keyId: "finance" });
    assert(k.keys.includes("finance") || JSON.stringify(k).includes("finance"), "createDocKey");
    const enc = await alice.call("documents.create", { db: "notes", options: { decryptionKeyId: "finance", initialValues: { type: "secret", amount: 5 } } }).catch((e) => e);
    console.log("    encrypted create ->", enc.message ?? enc.id);
  });

  // ---------- server ----------
  await step("publishToServer (no system admin, open server)", async () => {
    const r = await alice.call("session.publishToServer", { serverUrl, adminPassword: "admin-pw" });
    console.log("    ->", JSON.stringify(r));
  });
  await step("refresh (push+pull) alice", async () => {
    const r = await alice.call("session.refresh", {});
    console.log("    ->", JSON.stringify(r));
  });

  const bootB = await new Conn(sidecar.hs).connect();
  const bob = await step("bob creates user + join request", async () => {
    const u = await bootB.call("identity.createUser", { username: "cn=bob/o=kmp", password: "bob-pw" });
    const jr = await bootB.call("identity.createJoinRequest", { identity: u.identity });
    return { u, jr };
  });
  const approved = await step("alice approves", () => alice.call("tenant.approveJoinRequest", {
    joinRequest: bob.jr.joinRequest, adminPassword: "admin-pw", adminIdentity: created.adminIdentity, serverUrl,
  }));
  console.log("    approve pushed:", JSON.stringify(approved?.pushed));
  await step("alice refresh after approval (push directory grant)", async () => {
    console.log("    ->", JSON.stringify(await alice.call("session.refresh", {})));
  });
  const joined = await step("bob joins", () => bootB.call("identity.joinTenant", {
    joinResponse: approved.joinResponse, user: bob.u.identity, password: "bob-pw", databases: ["notes"],
  }));
  console.log("    joined:", JSON.stringify(joined));
  bootB.close();

  const bobC = await new Conn(sidecar.hs).connect();
  await step("bob session + pull", async () => {
    await bobC.open({ tenantId, userId: "cn=bob/o=kmp", password: "bob-pw", databases: ["notes"] });
    const r = await bobC.call("session.refresh", {});
    console.log("    refresh ->", JSON.stringify(r));
    const got = await bobC.call("documents.get", { db: "notes", docId: doc.id });
    assert(got.data.title === "Hello (edited)", "bob sees alice's doc");
  });
  await step("bob writes, alice pulls", async () => {
    await bobC.call("documents.update", { db: "notes", docId: doc.id, operations: [{ op: "set", path: ["byBob"], value: true }] });
    await bobC.call("session.refresh", {});
    await alice.call("session.refresh", {});
    const got = await alice.call("documents.get", { db: "notes", docId: doc.id });
    assert(got.data.byBob === true, "alice sees bob's change");
  });
  await step("concurrent rich text edits merge", async () => {
    await bobC.call("session.refresh", { db: "notes", direction: "pull" });
    const base = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    const bobBase = await bobC.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    assert(JSON.stringify(bobBase.spans) === JSON.stringify(base.spans), "bob sees alice's rich text");
    // both edit offline from the same base: alice marks "Hello", bob appends text
    await alice.call("documents.update", { db: "notes", docId: rt.id, baseHeads: base.heads, operations: [{ op: "textMark", path: ["content"], index: 1, length: 5, marks: { underline: true } }] });
    await bobC.call("documents.update", { db: "notes", docId: rt.id, baseHeads: bobBase.heads, operations: [
      { op: "richTextSteps", path: ["content"], steps: [{ type: "splice", index: 16, deleteCount: 0, insert: "!" }] },
    ] });
    await alice.call("session.refresh", { db: "notes" });
    await bobC.call("session.refresh", { db: "notes" });
    await alice.call("session.refresh", { db: "notes" });
    const a = await alice.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    const b = await bobC.call("documents.getRichText", { db: "notes", docId: rt.id, path: ["content"] });
    console.log("    merged:", JSON.stringify(a.spans));
    assert(JSON.stringify(a.spans) === JSON.stringify(b.spans), "both converge");
    assert(a.spans.some((sp) => sp.type === "text" && sp.value === "Hello" && sp.marks?.underline) && a.spans.some((sp) => sp.type === "text" && sp.value.includes("!")), "mark and insert both kept");
  });
  await step("attachment sync", async () => {
    const bytes = Buffer.from("attachment from bob ".repeat(5000));
    await bobC.upload("notes", doc.id, "bob.txt", "text/plain", bytes);
    const pr = await bobC.call("session.refresh", { direction: "push" });
    console.log("    bob push ->", JSON.stringify(pr.databases.find((d) => d.dbId === "notes")));
    const r = await alice.call("session.refresh", { db: "notes", direction: "pull" });
    assert(r.pulled, "alice pulled entries");
    const back = await alice.readAttachment("notes", doc.id, "bob.txt");
    assert(back.equals(bytes), "alice reads bob's attachment");
  });
  await step("remote-primary database", async () => {
    await bobC.call("session.openDatabase", { dbId: "notes2", mode: "remote" });
    const d = await bobC.call("documents.create", { db: "notes2", options: { initialValues: { type: "remote" } } });
    const got = await bobC.call("documents.get", { db: "notes2", docId: d.id });
    assert(got.data.type === "remote", "remote-primary create + get");
    await alice.call("session.openDatabase", { dbId: "notes2", mode: "remote" });
    const seen = await alice.call("documents.get", { db: "notes2", docId: d.id });
    assert(seen.data.type === "remote", "other user reads remote-primary doc without sync");
  });

  bobC.close();
  alice.close();
} finally {
  sidecar.stop();
}
console.log("\nFAILURES:", failures.length ? "\n- " + failures.join("\n- ") : "none");
process.exit(failures.length ? 1 : 0);

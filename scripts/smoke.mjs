import { spawn, spawnSync } from "node:child_process";
import { readFileSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import WebSocket from "ws";

const here = dirname(fileURLToPath(import.meta.url));
const pkg = dirname(here);
const dataRoot = join(pkg, ".smoke-data");
rmSync(dataRoot, { recursive: true, force: true });

const TENANT = "acme";
const USER = "cn=alice/o=acme";
const PW = "correct horse battery staple";
const DB = "notes";

function log(...a) { console.log("[smoke]", ...a); }

// 1. Provision
log("provisioning tenant…");
const prov = spawnSync("node", [
  join(pkg, "dist/provision.js"),
  "--data", dataRoot,
  "--tenant", TENANT,
  "--admin-name", "cn=admin/o=acme",
  "--admin-password", "adminpw",
  "--user-name", USER,
  "--user-password", PW,
  "--db", DB,
], { encoding: "utf-8" });
if (prov.status !== 0) { console.error(prov.stderr); process.exit(1); }
log("provisioned:", prov.stdout.trim());

// 2. Start supervisor
const handshakePath = join(dataRoot, "handshake.json");
const sup = spawn("node", [join(pkg, "dist/index.js")], {
  env: { ...process.env, MINDOODB_BRIDGE_DATA: dataRoot, MINDOODB_BRIDGE_HANDSHAKE: handshakePath },
  stdio: ["inherit", "pipe", "inherit"],
});
await new Promise((resolve) => sup.stdout.on("data", (d) => { if (String(d).includes("ready")) resolve(); }));
const hs = JSON.parse(readFileSync(handshakePath, "utf-8"));
log("supervisor ready on", hs.port);

// 3. Connect
const ws = new WebSocket(`ws://${hs.host}:${hs.port}`);
await new Promise((r, j) => { ws.on("open", r); ws.on("error", j); });

let idc = 0;
const pending = new Map();
const events = [];
const binChunks = [];
ws.on("message", (data, isBinary) => {
  if (isBinary) { binChunks.push(Buffer.from(data)); return; }
  const msg = JSON.parse(data.toString());
  if (msg.kind === "success") pending.get(msg.id)?.resolve(msg.result);
  else if (msg.kind === "error") pending.get(msg.id)?.reject(new Error(`${msg.error.code}: ${msg.error.message}`));
  else if (msg.kind === "event") events.push(msg);
  else if (msg.kind === "stream-end") pending.get("read-end")?.resolve();
  if (msg.kind === "success" || msg.kind === "error") pending.delete(msg.id);
});
let sessionToken = null;
function call(method, params) {
  const id = String(++idc);
  const frame = { protocol: "mindoodb-bridge", kind: "request", id, method, params, sessionToken };
  return new Promise((resolve, reject) => { pending.set(id, { resolve, reject }); ws.send(JSON.stringify(frame)); });
}

try {
  await call("hello", { bootstrapToken: hs.bootstrapToken });
  log("hello ok");

  const session = await call("session.open", { tenantId: TENANT, userId: USER, password: PW, databases: [DB] });
  sessionToken = session.sessionToken;
  log("session opened:", session.databases);

  const created = await call("documents.create", { db: DB, options: { initialValues: { type: "note", title: "Hello", body: "world" } } });
  log("created doc:", created.id, "heads:", created.heads.length);

  await call("documents.update", { db: DB, docId: created.id, baseHeads: created.heads, operations: [
    { op: "set", path: ["title"], value: "Hello (edited)" },
    { op: "merge", value: { tags: ["a", "b"] } },
  ] });
  const got = await call("documents.get", { db: DB, docId: created.id });
  log("after update, title:", got.data.title, "tags:", JSON.stringify(got.data.tags));
  if (got.data.title !== "Hello (edited)") throw new Error("update did not persist");

  const list = await call("documents.list", { db: DB });
  log("doc count:", list.ids.length);

  // Attachment upload
  const payload = Buffer.from("A".repeat(500000)); // ~500KB
  const openW = await call("attachments.openWriteStream", { db: DB, docId: created.id, fileName: "big.txt", mimeType: "text/plain" });
  const CHUNK = 64 * 1024;
  let seq = 0;
  for (let off = 0; off < payload.length; off += CHUNK) {
    const slice = payload.subarray(off, off + CHUNK);
    const header = Buffer.alloc(9); header[0] = 0x01; header.writeUInt32BE(openW.streamId, 1); header.writeUInt32BE(seq++, 5);
    ws.send(Buffer.concat([header, slice]), { binary: true });
  }
  const closed = await call("attachments.closeWriteStream", { streamId: openW.streamId });
  log("attachment stored:", closed.attachment.fileName, closed.attachment.size, "bytes");
  if (closed.attachment.size !== payload.length) throw new Error(`size mismatch ${closed.attachment.size} != ${payload.length}`);

  // Attachment download
  binChunks.length = 0;
  const openR = await call("attachments.openReadStream", { db: DB, docId: created.id, fileName: "big.txt" });
  log("read stream size:", openR.size);
  await new Promise((resolve) => { pending.set("read-end", { resolve }); });
  const received = Buffer.concat(binChunks.map((b) => b.subarray(9)));
  log("downloaded", received.length, "bytes");
  if (received.length !== payload.length || !received.equals(payload)) throw new Error("attachment round-trip mismatch");

  // Query (may require summary coverage)
  try {
    const q = await call("db.query", { db: DB, query: { filter: { op: "eq", field: "type", value: "note" } } });
    log("query rows:", q.rows?.length, "total:", q.total);
  } catch (e) {
    log("query note:", e.message);
  }

  log("ALL SMOKE CHECKS PASSED");
  process.exitCode = 0;
} catch (err) {
  console.error("[smoke] FAILED:", err);
  process.exitCode = 1;
} finally {
  try { await call("session.close", {}); } catch {}
  ws.close();
  sup.kill("SIGTERM");
  setTimeout(() => { rmSync(dataRoot, { recursive: true, force: true }); process.exit(process.exitCode ?? 0); }, 500);
}

// Minimal bridge client mirroring the Kotlin RpcClient, for protocol-level E2E.
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";

const BRIDGE = fileURLToPath(new URL("../..", import.meta.url)).replace(/\/$/, "");
const require = createRequire(BRIDGE + "/package.json");
const WebSocket = require("ws");

export async function startSidecar(dataRoot) {
  mkdirSync(dataRoot, { recursive: true });
  const child = spawn("node", [BRIDGE + "/dist/index.js"], {
    env: { ...process.env, MINDOODB_BRIDGE_DATA: dataRoot },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stderr.on("data", (d) => process.env.BRIDGE_LOG && process.stderr.write("[sidecar] " + d));
  const ready = await new Promise((resolve, reject) => {
    let buf = "";
    child.stdout.on("data", (d) => {
      buf += d;
      const line = buf.split("\n").find((l) => l.includes('"ready"'));
      if (line) resolve(JSON.parse(line));
    });
    child.on("exit", (c) => reject(new Error("sidecar exited " + c)));
  });
  const { readFileSync } = await import("node:fs");
  const hs = JSON.parse(readFileSync(ready.handshakePath, "utf-8"));
  return { child, hs, stop: () => child.kill("SIGTERM") };
}

export class Conn {
  constructor(hs) {
    this.hs = hs;
    this.nextId = 1;
    this.pending = new Map();
    this.subs = new Map();
    this.reads = new Map();
    this.writeCredits = new Map();
    this.token = null;
  }
  async connect() {
    this.ws = new WebSocket(`ws://${this.hs.host}:${this.hs.port}`);
    await new Promise((r, j) => { this.ws.on("open", r); this.ws.on("error", j); });
    this.ws.on("message", (data, isBinary) => {
      if (isBinary) {
        const b = Buffer.from(data);
        const streamId = b.readUInt32BE(1);
        if (!this.reads.has(streamId)) { this.reads.set(streamId, { chunks: [] }); this.earlyChunks = (this.earlyChunks ?? 0) + 1; }
        this.reads.get(streamId).chunks.push(b.subarray(9));
        return;
      }
      const m = JSON.parse(String(data));
      if (m.kind === "success" || m.kind === "error") {
        const p = this.pending.get(m.id);
        this.pending.delete(m.id);
        if (m.kind === "success") p?.resolve(m.result);
        else { const e = new Error(`${m.error.code}: ${m.error.message}`); e.code = m.error.code; p?.reject(e); }
      } else if (m.kind === "event") {
        this.subs.get(m.subscriptionId)?.(m.event, m.payload);
      } else if (m.kind === "stream-end") {
        const r = this.reads.get(m.streamId); if (r?.done) r.done(); else if (r) r.ended = true; else this.reads.set(m.streamId, { chunks: [], ended: true });
      } else if (m.kind === "stream-error") {
        this.reads.get(m.streamId)?.fail(new Error(m.error.message));
      } else if (m.kind === "stream-credit") {
        this.writeCredits.set(m.streamId, (this.writeCredits.get(m.streamId) ?? 0) + m.credits);
      }
    });
    await this.call("hello", { bootstrapToken: this.hs.bootstrapToken });
    return this;
  }
  call(method, params = {}, timeoutMs = 120000) {
    const id = String(this.nextId++);
    this.ws.send(JSON.stringify({ protocol: "mindoodb-bridge", kind: "request", id, method, params, sessionToken: this.token }));
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("timeout " + method)), timeoutMs);
      this.pending.set(id, { resolve: (v) => { clearTimeout(t); resolve(v); }, reject: (e) => { clearTimeout(t); reject(e); } });
    });
  }
  async open(params) {
    const r = await this.call("session.open", params);
    this.token = r.sessionToken;
    return r;
  }
  async readAttachment(db, docId, fileName) {
    const chunks = [];
    let done, fail;
    const finished = new Promise((r, j) => { done = r; fail = j; });
    const open = await this.call("attachments.openReadStream", { db, docId, fileName });
    // Chunks may arrive before we register; register immediately after open.
    const entry = this.reads.get(open.streamId) ?? { chunks };
    chunks.push(...entry.chunks); entry.chunks = chunks; entry.done = done; entry.fail = fail;
    if (entry.ended) done();
    this.reads.set(open.streamId, entry);
    const timer = setInterval(() => {
      this.ws.send(JSON.stringify({ protocol: "mindoodb-bridge", kind: "stream-credit", streamId: open.streamId, credits: 16, sessionToken: this.token }));
    }, 20);
    try { await finished; } finally { clearInterval(timer); }
    return Buffer.concat(chunks);
  }
  async upload(db, docId, fileName, mimeType, bytes, chunkSize = 64 * 1024) {
    const open = await this.call("attachments.openWriteStream", { db, docId, fileName, mimeType });
    this.writeCredits.set(open.streamId, open.credits);
    let seq = 0;
    for (let off = 0; off < bytes.length; off += chunkSize) {
      while ((this.writeCredits.get(open.streamId) ?? 0) <= 0) await new Promise((r) => setTimeout(r, 5));
      this.writeCredits.set(open.streamId, this.writeCredits.get(open.streamId) - 1);
      const payload = bytes.subarray(off, off + chunkSize);
      const out = Buffer.alloc(9 + payload.length);
      out[0] = 1; out.writeUInt32BE(open.streamId, 1); out.writeUInt32BE(seq++, 5); payload.copy(out, 9);
      this.ws.send(out);
    }
    return this.call("attachments.closeWriteStream", { streamId: open.streamId });
  }
  close() { this.ws.close(); }
}

export function assert(cond, msg) {
  if (!cond) throw new Error("ASSERT: " + msg);
  console.log("  ok -", msg);
}

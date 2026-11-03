import { timingSafeEqual } from "node:crypto";
import { fork, type ChildProcess } from "node:child_process";
import { join } from "node:path";
import { WebSocketServer, type WebSocket } from "ws";
import { randomUUID } from "node:crypto";
import {
  BridgeError,
  ErrorCodes,
  Methods,
  PROTOCOL,
  makeError,
  makeEvent,
  makeSuccess,
  type ErrorCode,
  type RpcRequest,
} from "./protocol";
import {
  createJoinRequest,
  createUser,
  joinTenantAndPersist,
  provisionTenant,
} from "./provisioning";
import type { PrivateUserId } from "mindoodb";
import {
  defaultHandshakePath,
  generateBootstrapToken,
  writeHandshakeFile,
  type HandshakeInfo,
} from "./handshake";
import { dataRoot as defaultDataRoot } from "./paths";
import { log } from "./logging";

export interface SupervisorOptions {
  host?: string;
  port?: number;
  handshakePath?: string;
  dataRoot?: string;
  /** Close a session connection after this many ms of inactivity. */
  idleMs?: number;
  /** Max session.open calls allowed per rolling minute across all connections. */
  maxOpenPerMinute?: number;
}

export interface SupervisorHandle {
  host: string;
  port: number;
  bootstrapToken: string;
  handshakePath: string;
  close(): Promise<void>;
}

interface Conn {
  ws: WebSocket;
  authed: boolean;
  child: ChildProcess | null;
  sessionToken: string | null;
  sessionId: string | null;
  pendingOpenReqId: string | null;
  running: boolean;
  lastActivity: number;
}

export async function startSupervisor(options: SupervisorOptions = {}): Promise<SupervisorHandle> {
  const host = options.host ?? "127.0.0.1";
  const dataRoot = options.dataRoot ?? defaultDataRoot();
  const handshakePath = options.handshakePath ?? defaultHandshakePath();
  const idleMs = options.idleMs ?? 15 * 60 * 1000;
  const maxOpenPerMinute = options.maxOpenPerMinute ?? 30;
  const bootstrapToken = generateBootstrapToken();
  const childModule = join(__dirname, "session-child.js");

  const openTimestamps: number[] = [];
  const conns = new Set<Conn>();

  const wss = new WebSocketServer({ host, port: options.port ?? 0 });

  await new Promise<void>((resolve, reject) => {
    wss.once("listening", () => resolve());
    wss.once("error", reject);
  });

  const address = wss.address();
  const port = typeof address === "object" && address ? address.port : 0;

  const info: HandshakeInfo = { version: 1, host, port, bootstrapToken, pid: process.pid };
  writeHandshakeFile(handshakePath, info);
  log.info("supervisor", `listening on ws://${host}:${port}, handshake at ${handshakePath}`);

  function send(conn: Conn, frame: unknown): void {
    if (conn.ws.readyState === conn.ws.OPEN) conn.ws.send(JSON.stringify(frame));
  }

  function killChild(conn: Conn): void {
    if (conn.child) {
      try {
        conn.child.send({ t: "close" });
      } catch {
        /* ignore */
      }
      const child = conn.child;
      conn.child = null;
      setTimeout(() => {
        if (!child.killed) child.kill("SIGKILL");
      }, 2000);
    }
  }

  function allowOpen(): boolean {
    const now = Date.now();
    while (openTimestamps.length && now - openTimestamps[0] > 60_000) openTimestamps.shift();
    if (openTimestamps.length >= maxOpenPerMinute) return false;
    openTimestamps.push(now);
    return true;
  }

  function handleProvision(conn: Conn, req: RpcRequest): void {
    if (!allowOpen()) {
      send(conn, makeError(req.id, ErrorCodes.NotAuthorized, "Too many provisioning attempts; try again later"));
      return;
    }
    const run = async (): Promise<unknown> => {
      if (req.method === Methods.tenantCreate) {
        const p = (req.params ?? {}) as {
          tenantId?: string;
          adminName?: string;
          adminPassword?: string;
          userName?: string;
          userPassword?: string;
          databases?: string[];
          serverUrl?: string;
          tenantLabel?: string;
        };
        if (!p.adminName || !p.adminPassword || !p.userName || !p.userPassword) {
          throw new BridgeError(
            ErrorCodes.BadRequest,
            "tenant.create requires adminName, adminPassword, userName, userPassword",
          );
        }
        return provisionTenant({
          dataRoot,
          tenantId: p.tenantId,
          tenantLabel: p.tenantLabel,
          adminName: p.adminName,
          adminPassword: p.adminPassword,
          userName: p.userName,
          userPassword: p.userPassword,
          databases: p.databases ?? [],
          serverUrl: p.serverUrl,
        });
      }
      if (req.method === Methods.identityCreateUser) {
        const p = (req.params ?? {}) as { username?: string; password?: string };
        if (!p.username || !p.password) {
          throw new BridgeError(ErrorCodes.BadRequest, "identity.createUser requires username and password");
        }
        return createUser({ dataRoot, username: p.username, password: p.password });
      }
      if (req.method === Methods.identityCreateJoinRequest) {
        const p = (req.params ?? {}) as {
          identity?: PrivateUserId;
          label?: string;
          format?: "object" | "uri";
        };
        if (!p.identity) {
          throw new BridgeError(ErrorCodes.BadRequest, "identity.createJoinRequest requires identity");
        }
        return createJoinRequest({
          dataRoot,
          identity: p.identity,
          label: p.label,
          format: p.format,
        });
      }
      if (req.method === Methods.identityJoinTenant) {
        const p = (req.params ?? {}) as {
          joinResponse?: unknown;
          user?: PrivateUserId;
          password?: string;
          sharePassword?: string;
          databases?: string[];
          serverUrl?: string;
        };
        if (!p.joinResponse || !p.user || !p.password) {
          throw new BridgeError(
            ErrorCodes.BadRequest,
            "identity.joinTenant requires joinResponse, user, password",
          );
        }
        return joinTenantAndPersist({
          dataRoot,
          joinResponse: p.joinResponse,
          user: p.user,
          password: p.password,
          sharePassword: p.sharePassword,
          databases: p.databases ?? [],
          serverUrl: p.serverUrl,
        });
      }
      throw new BridgeError(ErrorCodes.BadRequest, `Unknown bootstrap method: ${req.method}`);
    };
    run()
      .then((result) => send(conn, makeSuccess(req.id, result)))
      .catch((err) => {
        const code: ErrorCode = err instanceof BridgeError ? err.code : ErrorCodes.Internal;
        send(conn, makeError(req.id, code, String((err as Error)?.message ?? err)));
      });
  }

  function spawnChild(conn: Conn, req: RpcRequest): void {
    if (!allowOpen()) {
      send(conn, makeError(req.id, ErrorCodes.NotAuthorized, "Too many session.open attempts; try again later"));
      return;
    }
    const sessionId = randomUUID();
    const sessionToken = generateBootstrapToken();
    const child = fork(childModule, [], {
      serialization: "advanced",
      env: { ...process.env, MINDOODB_BRIDGE_DATA: dataRoot },
      stdio: ["inherit", "inherit", "inherit", "ipc"],
    });
    conn.child = child;
    conn.pendingOpenReqId = req.id;
    conn.sessionId = sessionId;

    child.on("message", (raw: Record<string, unknown>) => onChildMessage(conn, raw));
    child.on("exit", (code) => onChildExit(conn, code));
    child.on("error", (err) => log.error("supervisor", "child error", err));

    child.send({
      t: "open",
      reqId: req.id,
      params: req.params,
      sessionId,
      sessionToken,
      dataRoot,
    });
  }

  function onChildMessage(conn: Conn, raw: Record<string, unknown>): void {
    switch (raw.t) {
      case "opened": {
        const result = raw.result as { sessionToken: string };
        conn.sessionToken = result.sessionToken;
        conn.running = true;
        send(conn, makeSuccess(String(raw.reqId), raw.result));
        break;
      }
      case "open-error": {
        const error = raw.error as { code: string; message: string };
        send(conn, makeError(String(raw.reqId), error.code as never, error.message));
        killChild(conn);
        break;
      }
      case "out-text":
        send(conn, raw.msg);
        break;
      case "out-bin":
        if (conn.ws.readyState === conn.ws.OPEN) conn.ws.send(raw.buf as Buffer, { binary: true });
        break;
      case "closed":
        conn.running = false;
        break;
    }
  }

  function onChildExit(conn: Conn, code: number | null): void {
    conn.child = null;
    if (conn.running && conn.ws.readyState === conn.ws.OPEN) {
      // Unexpected crash of a live session. Keys were only in the child's
      // memory, so we cannot silently restart; tell the client to re-open.
      send(conn, makeEvent(conn.sessionId ?? "session", "session-crashed", { code }));
      conn.ws.close(1011, "session process exited");
    }
    conn.running = false;
    conn.sessionToken = null;
  }

  function touch(conn: Conn): void {
    conn.lastActivity = Date.now();
  }

  function handleText(conn: Conn, data: string): void {
    let msg: RpcRequest;
    try {
      msg = JSON.parse(data) as RpcRequest;
    } catch {
      return;
    }
    if (msg.protocol !== PROTOCOL) return;
    touch(conn);

    if (!conn.authed) {
      if (msg.kind === "request" && msg.method === Methods.hello) {
        const provided = (msg.params as { bootstrapToken?: string })?.bootstrapToken ?? "";
        if (safeEqual(provided, bootstrapToken)) {
          conn.authed = true;
          send(conn, makeSuccess(msg.id, { ok: true }));
        } else {
          send(conn, makeError(msg.id, ErrorCodes.NotAuthorized, "Invalid bootstrap token"));
          conn.ws.close(1008, "unauthorized");
        }
      } else {
        conn.ws.close(1008, "handshake required");
      }
      return;
    }

    if (msg.kind !== "request") {
      // stream-credit and other control frames route straight to the child.
      if (conn.child) conn.child.send({ t: "in-text", msg });
      return;
    }

    if (
      msg.method === Methods.tenantCreate ||
      msg.method === Methods.identityCreateUser ||
      msg.method === Methods.identityCreateJoinRequest ||
      msg.method === Methods.identityJoinTenant
    ) {
      // Bootstrap provisioning runs in the supervisor (WebCrypto keygen is
      // async/threadpool-backed, so it does not block the event loop) and needs
      // no session token or child process.
      handleProvision(conn, msg);
      return;
    }

    if (msg.method === Methods.sessionOpen) {
      if (conn.child) {
        send(conn, makeError(msg.id, ErrorCodes.BadRequest, "A session is already open on this connection"));
        return;
      }
      spawnChild(conn, msg);
      return;
    }

    if (!conn.child || !conn.running) {
      send(conn, makeError(msg.id, ErrorCodes.SessionClosed, "No open session on this connection"));
      return;
    }
    if (msg.sessionToken !== conn.sessionToken) {
      send(conn, makeError(msg.id, ErrorCodes.NotAuthorized, "Invalid or missing session token"));
      return;
    }
    if (msg.method === Methods.sessionClose) {
      conn.running = false;
      killChild(conn);
      send(conn, makeSuccess(msg.id, { ok: true }));
      return;
    }
    conn.child.send({ t: "in-text", msg });
  }

  function handleBinary(conn: Conn, data: Buffer): void {
    if (!conn.authed || !conn.child || !conn.running) return;
    touch(conn);
    conn.child.send({ t: "in-bin", buf: data });
  }

  wss.on("connection", (ws, request) => {
    // Loopback-only defense in depth: reject non-local peers.
    const remote = request.socket.remoteAddress ?? "";
    if (!isLoopback(remote)) {
      ws.close(1008, "loopback only");
      return;
    }
    const conn: Conn = {
      ws,
      authed: false,
      child: null,
      sessionToken: null,
      sessionId: null,
      pendingOpenReqId: null,
      running: false,
      lastActivity: Date.now(),
    };
    conns.add(conn);

    ws.on("message", (data: Buffer, isBinary: boolean) => {
      if (isBinary) handleBinary(conn, data);
      else handleText(conn, data.toString("utf-8"));
    });
    ws.on("close", () => {
      killChild(conn);
      conns.delete(conn);
    });
    ws.on("error", (err) => log.warn("supervisor", "ws error", err));
  });

  const sweep = setInterval(() => {
    const now = Date.now();
    for (const conn of conns) {
      if (conn.running && now - conn.lastActivity > idleMs) {
        log.info("supervisor", "closing idle session");
        killChild(conn);
        conn.ws.close(1000, "idle timeout");
      }
    }
  }, 30_000);

  return {
    host,
    port,
    bootstrapToken,
    handshakePath,
    async close(): Promise<void> {
      clearInterval(sweep);
      for (const conn of conns) killChild(conn);
      await new Promise<void>((resolve) => wss.close(() => resolve()));
    },
  };
}

function safeEqual(a: string, b: string): boolean {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

function isLoopback(address: string): boolean {
  return (
    address === "127.0.0.1" ||
    address === "::1" ||
    address === "::ffff:127.0.0.1" ||
    address.startsWith("127.")
  );
}

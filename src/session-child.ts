/**
 * Session child process.
 *
 * Forked once per user session by the supervisor. Opens the user's isolated
 * MindooDB environment (holding the decrypted KeyBag only in this process's
 * memory) and runs the dispatcher. Communicates with the supervisor over the
 * Node IPC channel using advanced (structured-clone) serialization so binary
 * Buffers pass through untouched.
 */
import {
  Methods,
  decodeBinaryChunk,
  encodeBinaryChunk,
  makeError,
  makeEvent,
  makeSuccess,
  BridgeError,
  ErrorCodes,
  PROTOCOL,
  type ErrorCode,
  type RpcRequest,
  type ServerFrame,
} from "./protocol";
import type { Emitter } from "./host/emitter";
import { Dispatcher } from "./host/dispatcher";
import { openSessionEnv, type SessionEnv } from "./sessionEnv";
import { SyncLoop } from "./syncLoop";
import { readIdentityBundle, type IdentityBundle } from "./identityStore";
import { userEnvDir, userStoreBasePath, identityFilePath } from "./paths";
import { log } from "./logging";

interface OpenParams {
  tenantId: string;
  userId: string;
  password: string;
  databases?: string[];
  identityBundle?: IdentityBundle;
}

type FromSupervisor =
  | { t: "open"; reqId: string; params: OpenParams; sessionId: string; sessionToken: string; dataRoot: string }
  | { t: "in-text"; msg: RpcRequest | { kind: string; streamId?: number; credits?: number } }
  | { t: "in-bin"; buf: Buffer }
  | { t: "close" };

function toSupervisor(msg: unknown): void {
  process.send?.(msg);
}

const emitter: Emitter = {
  sendEvent(subscriptionId, event, payload) {
    toSupervisor({ t: "out-text", msg: makeEvent(subscriptionId, event, payload) });
  },
  sendStreamOpen(streamId, meta) {
    toSupervisor({ t: "out-text", msg: { protocol: PROTOCOL, kind: "stream-open", streamId, meta } satisfies ServerFrame });
  },
  sendStreamCredit(streamId, credits) {
    toSupervisor({ t: "out-text", msg: { protocol: PROTOCOL, kind: "stream-credit", streamId, credits } satisfies ServerFrame });
  },
  sendStreamEnd(streamId) {
    toSupervisor({ t: "out-text", msg: { protocol: PROTOCOL, kind: "stream-end", streamId } satisfies ServerFrame });
  },
  sendStreamError(streamId, code, message) {
    toSupervisor({ t: "out-text", msg: { protocol: PROTOCOL, kind: "stream-error", streamId, error: { code, message } } satisfies ServerFrame });
  },
  sendBinaryChunk(streamId, seq, payload) {
    toSupervisor({ t: "out-bin", buf: encodeBinaryChunk(streamId, seq, payload) });
  },
};

let env: SessionEnv | null = null;
let syncLoop: SyncLoop | null = null;
let dispatcher: Dispatcher | null = null;

async function handleOpen(m: Extract<FromSupervisor, { t: "open" }>): Promise<void> {
  try {
    process.env.MINDOODB_BRIDGE_DATA = m.dataRoot;
    const envDir = userEnvDir(m.dataRoot, m.params.tenantId, m.params.userId);
    const bundle = m.params.identityBundle ?? readIdentityBundle(identityFilePath(envDir));
    env = await openSessionEnv({
      bundle,
      password: m.params.password,
      databases: m.params.databases ?? [],
      basePath: userStoreBasePath(envDir),
      identityFile: identityFilePath(envDir),
    });
    syncLoop = new SyncLoop(env);
    // First background sync after one interval, so a client can subscribe to
    // sync progress (or run refresh itself) before the first big pull starts.
    syncLoop.start({ immediate: false });
    dispatcher = new Dispatcher(env, syncLoop, emitter);
    toSupervisor({
      t: "opened",
      reqId: m.reqId,
      result: {
        sessionId: m.sessionId,
        sessionToken: m.sessionToken,
        databases: env.databaseIds(),
      },
    });
    log.info("session-child", `session ${m.sessionId} opened for tenant ${m.params.tenantId}`);
  } catch (err) {
    const message = String((err as Error)?.message ?? err);
    log.error("session-child", "open failed", message);
    toSupervisor({ t: "open-error", reqId: m.reqId, error: { code: ErrorCodes.NotAuthorized, message } });
    process.exit(1);
  }
}

async function handleRequest(req: RpcRequest): Promise<void> {
  if (!dispatcher) {
    toSupervisor({ t: "out-text", msg: makeError(req.id, ErrorCodes.SessionClosed, "Session not open") });
    return;
  }
  try {
    const result = await dispatcher.dispatch(req.method, req.params);
    toSupervisor({ t: "out-text", msg: makeSuccess(req.id, result) });
  } catch (err) {
    if (err instanceof BridgeError) {
      toSupervisor({ t: "out-text", msg: makeError(req.id, err.code, err.message) });
    } else {
      toSupervisor({ t: "out-text", msg: makeError(req.id, errorCodeFor(err), String((err as Error)?.message ?? err)) });
    }
  }
}

/** Map MindooDB's error classes to the bridge's stable error codes. */
function errorCodeFor(err: unknown): ErrorCode {
  const e = err as { name?: string; code?: string } | null;
  switch (e?.name) {
    case "DocumentNotFoundError":
    case "DocumentDeletedError":
      return ErrorCodes.NotFound;
    case "AccessDeniedError":
    case "SymmetricKeyNotFoundError":
      return ErrorCodes.NotAuthorized;
    case "MindooQueryError":
      return e.code === "fulltext-not-enabled" ? ErrorCodes.FulltextNotEnabled : ErrorCodes.BadRequest;
    default:
      return ErrorCodes.Internal;
  }
}

async function shutdown(): Promise<void> {
  dispatcher?.dispose();
  syncLoop?.stop();
  await env?.close();
  toSupervisor({ t: "closed" });
  process.exit(0);
}

process.on("message", (raw: FromSupervisor) => {
  switch (raw.t) {
    case "open":
      void handleOpen(raw);
      break;
    case "in-text": {
      const msg = raw.msg as RpcRequest & { kind: string; streamId?: number; credits?: number };
      if (msg.kind === "request") {
        if (msg.method === Methods.sessionClose) {
          void shutdown();
        } else {
          void handleRequest(msg);
        }
      } else if (msg.kind === "stream-credit" && dispatcher) {
        dispatcher.onStreamCredit(Number(msg.streamId), Number(msg.credits));
      }
      break;
    }
    case "in-bin": {
      const decoded = decodeBinaryChunk(Buffer.from(raw.buf));
      if (decoded && dispatcher) {
        dispatcher.onBinaryChunk(decoded.streamId, decoded.seq, decoded.payload);
      }
      break;
    }
    case "close":
      void shutdown();
      break;
  }
});

process.on("uncaughtException", (err) => {
  log.error("session-child", "uncaughtException", err);
  process.exit(1);
});

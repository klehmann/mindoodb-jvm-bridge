# mindoodb-jvm-bridge

Headless Node **sidecar** that runs the real `mindoodb` engine and exposes it to
a JVM/Kotlin backend over a **loopback WebSocket**.

MindooDB is end-to-end encrypted and local-first: encryption keys live on the
client. A full Java/Kotlin port would be expensive, so this package keeps the
TypeScript engine in Node and lets the JVM drive it through a thin, authenticated
bridge. The matching Kotlin client is
[`mindoodb-kmp-client`](../mindoodb-kmp-client).

```
JVM / Kotlin client ──► loopback WebSocket ──► Node supervisor ──fork──► session child
                                                              (mindoodb + decrypted KeyBag)
```

## Why a sidecar

- The engine stays in TypeScript (`mindoodb` / `mindoodb/node`).
- Each user session gets its **own child process**, so decrypted keys and
  on-disk replicas stay isolated.
- The JVM never sees raw KeyBag material — only JSON-RPC results and framed
  attachment bytes.

## Architecture

| Piece | Role |
| ----- | ---- |
| **Supervisor** (`src/supervisor.ts`) | Loopback WebSocket server, bootstrap-token auth, process-per-session spawn/route/reap |
| **Session child** (`src/session-child.ts`) | Opens one user’s tenant, holds the unlocked KeyBag, runs the dispatcher + sync loop |
| **Dispatcher** (`src/host/dispatcher.ts`) | Maps wire methods to mindoodb core (docs, query, views, attachments, keybag, join, publish, …) |
| **Provisioning** (`src/provisioning.ts` + CLI) | Create tenant/user / join flows that write password-locked identity bundles |

Data layout (default `$MINDOODB_BRIDGE_DATA`):

```
<dataRoot>/
  handshake.json                          # mode 600 — host, port, bootstrapToken
  <tenantId>/<hash(userId)>/
    identity.json                         # password-locked PrivateUserId + KeyBag blob
    stores/                               # on-disk content-addressed replica
```

## Wire protocol

The full request/response envelope, binary attachment framing, method catalogue,
bootstrap RPCs, and security model are documented in:

**[PROTOCOL.md](./PROTOCOL.md)**

Summary:

- Control / RPC / events → JSON text frames (`protocol: "mindoodb-bridge"`).
- Attachments → binary frames with credit-based flow control.
- One WebSocket connection = one user session = one child process.
- After `hello` (bootstrap token), you may provision identities without a
  session, or call `session.open` to fork a child.

## Requirements

- Node.js `>=22.13.0`
- Workspace packages: `mindoodb`, `ws` (and `mindoodb-app-sdk` for types)

## Build & run

From the workspace root:

```bash
pnpm --filter mindoodb-jvm-bridge install
pnpm --filter mindoodb-jvm-bridge build    # → dist/index.js, dist/provision.js
pnpm --filter mindoodb-jvm-bridge start    # supervisor on loopback
```

Or from this directory:

```bash
pnpm build
pnpm start          # node dist/index.js
pnpm dev            # tsx src/index.ts
pnpm provision -- --data ./data --tenant acme ...   # see src/provision.ts
```

Environment:

| Variable | Purpose |
| -------- | ------- |
| `MINDOODB_BRIDGE_DATA` | Data root (default `./.mindoodb-bridge-data`) |
| `MINDOODB_BRIDGE_HANDSHAKE` | Handshake file path (default `<data>/handshake.json`) |

On listen, the supervisor writes the handshake file and prints a ready line on
stdout so a launcher (e.g. `Sidecar.start` in the Kotlin client) can discover
`host` / `port` / `bootstrapToken`.

## What the bridge exposes

**Bootstrap (no session, after `hello`):**

- `tenant.create` — local tenant + admin + app user + identity bundle
- `identity.createUser` / `createJoinRequest` / `joinTenant`

**Session (per-user child):**

- Documents: get/list/create/update/delete (single and batch)/undelete/history;
  updates are JSON patches applied against `baseHeads`, with MindooDB typed
  values (atomic strings, counters, timestamps)
- Query, live query, full-text setup
- Virtual-view navigators that stay current while subscribed
- Attachments: streamed read/write with flow control
- Changefeed + live events
- KeyBag: `createDocKey` / `listKeys` / persist
- `session.openDatabase` — `local` (on-disk) or `remote` (primary
  `ClientNetworkContentAddressedStore`; intentionally slow)
- `session.setServerUrl` / `session.refresh` / `session.publishToServer`; sync
  covers `directory`, `userdirectory` and every open local database, docs and
  attachments
- `tenant.approveJoinRequest`

See [PROTOCOL.md](./PROTOCOL.md) for parameter shapes and error codes.

## Tests

`scripts/e2e/server-sync.mjs` drives the built sidecar frame by frame, like the
Kotlin client, against a real MindooDB server: tenant, CRUD, typed values,
attachments, query, full-text, views, changefeed, publish, join and two-user sync.
It needs a server that lets any signed user register tenants:

```bash
# one-time server setup (from the workspace root)
MINDOODB_SERVER_PASSWORD=test node mindoodb/dist/node/server/serverinit.js \
  --name cn=testserver/o=test --skip-admin -d /tmp/mdb-server
cat > /tmp/mdb-server/config.json <<'JSON'
{ "capabilities": { "POST:/system/tenants/*": [{ "username": "*", "publicsignkey": "*" }] } }
JSON
MINDOODB_SERVER_PASSWORD=test node mindoodb/dist/node/server/server.js -d /tmp/mdb-server -p 1761

# in mindoodb-jvm-bridge
pnpm build && pnpm e2e -- "$(mktemp -d)" http://127.0.0.1:1761
```

The Kotlin client's `ServerSyncIntegrationTest` runs the same flow against
`MINDOODB_TEST_SERVER_URL`.

## Kotlin client

Prefer driving the bridge from
[`mindoodb-kmp-client`](../mindoodb-kmp-client) (`MindooDBBridge`,
`MindooDBSession`, `Sidecar`). That module’s README has a quick-start and API
map; this package is the Node half of the same system.

## Security

- Binds to loopback and rejects non-local peers.
- Mode-`600` handshake file carries a random bootstrap token.
- User password unlocks the KeyBag once in the child, then is discarded.
- Per-session token required on every post-open frame; idle timeout reaps the
  child.
- Rate-limited `session.open` and bootstrap provisioning.

Details: [PROTOCOL.md §7](./PROTOCOL.md#7-security-summary).

## License

Apache-2.0 — Mindoo GmbH.

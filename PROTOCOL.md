# MindooDB JVM Bridge — Wire Protocol

The bridge lets a JVM/Kotlin backend drive a real MindooDB engine that runs in a
Node sidecar. It reuses the shape of the MindooDB **App Bridge**
(`mindoodb-app-sdk/src/types.ts`) — same request/response envelope, same method
semantics — but rides a **loopback WebSocket** instead of a `MessagePort`.

- **Transport:** WebSocket on `127.0.0.1`, random port.
- **One connection = one user session.** Each connection is backed by its own
  Node **child process** that holds the decrypted KeyBag in isolated memory.
  Multiple users ⇒ multiple connections.
- **Control / RPC / events:** JSON **text** frames.
- **Attachment bytes:** **binary** frames, so a large upload/download never
  blocks the JSON control channel.

## 1. Bootstrap / handshake

The supervisor writes a mode-`600` handshake file (default
`$MINDOODB_BRIDGE_DATA/handshake.json`):

```json
{ "version": 1, "host": "127.0.0.1", "port": 51734, "bootstrapToken": "…", "pid": 12345 }
```

The client reads it, opens a WebSocket to `ws://host:port`, and sends `hello`
**before anything else**:

```json
{ "protocol": "mindoodb-bridge", "kind": "request", "id": "1", "method": "hello",
  "params": { "bootstrapToken": "…" } }
```

An invalid token closes the socket (`1008`). The bootstrap token authorizes the
*connection*; the per-session token (below) authorizes *frames*.

### Bootstrap provisioning (no session)

After `hello`, a connection may create tenants and user identities **without**
opening a session (bootstrap-token authorized, rate-limited alongside
`session.open`). These run in the supervisor — WebCrypto keygen is
threadpool-backed, so it does not block the event loop — and never fork a child:

- `tenant.create` — params
  `{ tenantId, adminName, adminPassword, userName, userPassword, databases?, serverUrl? }`.
  Creates a tenant + admin + app user, materializes each database, and writes a
  password-locked identity bundle to disk. Returns
  `{ tenantId, userId, adminUserId, databases, envDir, identityFile, adminIdentity }`.
  The caller can then `session.open` with `userId` + `password`. `adminIdentity`
  is the admin `PrivateUserId` (password-locked) for join approval.
- `identity.createUser` — params `{ username, password }`. Creates a standalone
  password-locked identity with its person-bound User-Key (MindooDB needs it to
  approve a join request). Returns
  `{ username, signingPublicKey, encryptionPublicKey, identity }`, where
  `identity` is the full `PrivateUserId` (private keys are already
  password-encrypted).
- `identity.createJoinRequest` — params `{ identity, label?, format? }`
  (`format` defaults to `"uri"`, `label` names the device for the approving
  admin and defaults to `JVM bridge (<hostname>)`). Returns `{ format, joinRequest }`.
- `identity.joinTenant` — params
  `{ joinResponse, user, password, sharePassword?, databases?, serverUrl? }`.
  Imports doc keys (v3: RSA-wrapped to the requesting device; v2: `sharePassword`),
  pulls `directory` + `userdirectory` from the server (response `serverUrl` or
  the parameter) so the grant is local, opens `databases`, writes an identity
  bundle, returns
  `{ tenantId, userId, databases, envDir, identityFile, serverUrl }`.

## 2. JSON envelope

All text frames carry `"protocol": "mindoodb-bridge"`.

| kind            | direction | fields |
| --------------- | --------- | ------ |
| `request`       | C → S     | `id`, `method`, `params`, `sessionToken?` |
| `success`       | S → C     | `id`, `result` |
| `error`         | S → C     | `id`, `error: { code, message }` |
| `event`         | S → C     | `subscriptionId`, `event`, `payload` |
| `stream-open`   | S → C     | `streamId`, `meta: { size?, mimeType?, fileName? }` |
| `stream-credit` | both      | `streamId`, `credits` |
| `stream-end`    | S → C     | `streamId` |
| `stream-error`  | S → C     | `streamId`, `error` |

`sessionToken` is `null`/absent for `hello` and `session.open`; **required** on
every frame afterwards and validated against the connection's session.

### Error codes

`NotAuthorized`, `SessionClosed`, `NotFound`, `Conflict`, `IndexNotReady`,
`BadRequest`, `FulltextNotEnabled`, `Internal`.

MindooDB errors map as: document not found / deleted → `NotFound`; access
denied / missing key → `NotAuthorized`; query errors → `BadRequest`
(`FulltextNotEnabled` for a text search without index); anything else →
`Internal`.

## 3. Opening a session

```json
{ "kind": "request", "id": "2", "method": "session.open",
  "params": { "tenantId": "acme", "userId": "cn=alice", "password": "…",
              "databases": ["notes"] } }
```

The supervisor forks a child, which opens the tenant on an isolated on-disk
replica at `<dataRoot>/<tenantId>/<hash(userId)>/stores` and unlocks the KeyBag
with `password` (used once, then discarded). Success:

```json
{ "kind": "success", "id": "2",
  "result": { "sessionId": "…", "sessionToken": "…", "databases": ["notes"] } }
```

Close with `session.close` (or just drop the connection); the child is reaped.

### Session lifecycle helpers

- `session.openDatabase` `{ dbId, mode: "local"|"remote" }` — open another DB.
  `local` uses the on-disk replica (default; eligible for sync). `remote` builds
  a `BaseMindooDB` whose primary docs+attachments stores are
  `ClientNetworkContentAddressedStore` (every CAS op is remoted; requires
  `serverUrl`). Remote-primary DBs are skipped by the sync loop.
- `session.setServerUrl` `{ serverUrl }` — persist sync peer URL (or `null` to
  clear) and start/stop the background sync loop (every 5 s).
- `session.refresh` `{ db?, direction?: "both"|"pull"|"push" }` — sync now: one
  database, or `directory`, `userdirectory` and every open local database; docs
  and attachments stores. Returns `{ pulled, reconciled, pushed, databases:
  [{ dbId, pulledEntries, pushedEntries, pushedBytes, rejectedEntries }] }`.
  `BadRequest` without a server URL; sync errors are returned, not swallowed.
- `session.publishToServer`
  `{ serverUrl, systemAdminUser?, systemAdminPassword?, adminPassword?, adminUsername?, adminIdentity?, registerUsers? }`
  — register the tenant on a MindooDB sync server, `setServerUrl`, then push
  `directory` and `userdirectory` as the tenant admin (`adminIdentity` or the
  bundle's admin + `adminPassword`). Without system-admin credentials the tenant
  admin signs the registration, which servers accept when any signed user may
  create tenants. Returns `{ ok, serverUrl, pushed: [...] }`.
- `tenant.approveJoinRequest`
  `{ joinRequest, adminPassword, adminIdentity?, adminSigningKey?, label?, username?, tenantLabel?, sharedDocKeyIds?, serverUrl?, format? }`
  — admin approves a join; default `format` is `"uri"`. Responses wrap doc keys
  to the requester's encryption public key (`sharePassword` is ignored). `label`
  overrides the device label, `username` registers the device under an existing
  person. With a server URL the grant is pushed (`directory` + `userdirectory`)
  right away. Returns `{ format, joinResponse, pushed }`.
- `keybag.createDocKey` `{ keyId }`, `keybag.listKeys`, `keybag.persist` —
  create/list/re-save KeyBag doc keys (create auto-persists the identity bundle).

## 4. Method catalogue (backend subset)

Session: `session.refresh`, `session.awaitIndexReady`, `session.listDatabases`,
`session.openDatabase`, `session.setServerUrl`, `session.publishToServer`.

Documents: `documents.get`, `documents.list`, `documents.getHeadCursor`,
`documents.create`, `documents.createMany`, `documents.update`,
`documents.delete`, `documents.deleteMany` (`{ db, docIds }`),
`documents.listDeleted`, `documents.undelete`, `documents.listHistory`,
`documents.getAtRevision`.

`documents.create` `options` are MindooDB `CreateOptions` (`id`, `idPrefix`,
`assumeUniqueId`, `decryptionKeyId`, `initialValues`, ...). With a custom `id`,
`initialValues` are written in a second change (MindooDB keeps the first change
of a convergent ID fixed).

`documents.update` params: `{ db, docId, baseHeads?, operations: UpdateOp[], options? }`
where `UpdateOp` is `{op:"set",path,value}`, `{op:"unset",path}`,
`{op:"merge",value}`, `{op:"setText",path,value}`,
`{op:"textSplice",path,index,deleteCount,insert?}`, `{op:"increment",path,delta}`,
`{op:"listInsert",path,index,values}`, `{op:"listDelete",path,index,deleteCount}`,
`{op:"textMark",path,index,length,marks}`, `{op:"textUnmark",path,index,length,names}`,
`{op:"richText",path,spans|spansSequence,updateSpansConfig?}` (replace a rich-text
field's spans; MindooDB diffs them, creates the field when missing) or
`{op:"richTextSteps",path,steps:[{type:"splice",index,deleteCount,insert?,marks?:[{index,length,marks}]}]}`.
All operations go into one `applyDocumentUpdate`: the JSON ops as one JSON patch
applied by group (set, unset, list, text, counter, marks), not in list order;
each rich-text op as its own rich-text patch, all at the same `baseHeads`. Values may be MindooDB
typed values: `{"$mindoo":"atomic","value":"open"}` (a string concurrent
changes cannot mix), `{"$mindoo":"counter","value":0}`,
`{"$mindoo":"timestamp","value":<ms>}`; a plain string is collaborative text.

Rich text and cursors:
- `documents.getRichText` `{ db, docId, path }` → `{ path, heads, spans }`, spans
  `{type:"text",value,marks?}` / `{type:"block",value}`. `documents.get` returns
  only the plain string (a block is `\uFFFC`, marks are dropped). Use `heads` as
  `baseHeads` of the next edit.
- `documents.getTextCursors` `{ db, docId, path, positions: (number|"start"|"end")[], heads? }`
  → `{ path, heads, cursors }`: opaque strings that keep pointing at the same
  character while text changes (store them as atomic strings).
- `documents.resolveTextCursors` `{ db, docId, path, cursors, heads? }` →
  `{ path, heads, positions }`, current or at `heads`.
With `baseHeads` the patch is made against that version and merged into the
current one; `conflictExpected` flags that the document had moved on. Returns
`{ heads, conflictExpected? }`.

Query / views: `db.query`, `db.liveQuery.start` / `db.liveQuery.stop`,
`view.openNavigator` / `view.createNavigator`, `nav.call`, `nav.subscribe` /
`nav.unsubscribe`, `nav.dispose`. `nav.call` params:
`{ navigatorId, method, args }` where `method` ∈ `getCurrentEntry`,
`gotoFirst|gotoLast|gotoNext|gotoPrev|gotoParent|gotoFirstChild`,
`expandAll|collapseAll|expandToLevel|expand|collapse`,
`gotoNextSibling|gotoPrevSibling|gotoLastChild|gotoPos`,
`expandPos|collapsePos`, `select|deselect|selectAll|deselectAll|isSelected`,
`readForward|readBackward|readSelected` (paging: `args = [limit, offset]`),
`refresh` (apply document changes to the view).

`view.openNavigator` params: `{ db, definition, options?, navigatorOptions? }`
with `navigatorOptions` `{ expandAll?, expandLevel?, hideEmptyCategories?,
categoriesOnly?, documentsOnly?, fromCategory? }`. While `nav.subscribe` is
active the view follows the database and `view-changed` fires after each
update; without it, call `refresh`. `nav.dispose` releases the view.

Full-text: `fulltext.getSetup`, `fulltext.setSetup`. Full-text search is a
`text` clause on `db.query` (adds `textScore` per row).

Changefeed: `changes.iterateSince` (`{ db, subscriptionId, cursor }`) streams
`change` events, then `caught-up`, then follows live.

Attachments: `attachments.list`, `attachments.remove`,
`attachments.openReadStream`, `attachments.openWriteStream`,
`attachments.closeWriteStream`, `attachments.abortStream`.

## 5. Binary attachment framing

Binary WebSocket frames carry one attachment chunk:

```
byte[0]    = 0x01              // chunk marker
byte[1..4] = uint32 streamId   // big-endian
byte[5..8] = uint32 seq        // big-endian, per-stream, 0-based
byte[9..]  = payload bytes
```

**Download (read):**

1. `attachments.openReadStream {db, docId, fileName|attachmentId, startOffset?}`
   → `{ streamId, size, mimeType }`.
2. Host streams binary chunks up to the current credit window; client tops up
   credits with `stream-credit {streamId, credits}`.
3. Host sends `stream-end` when finished (or `stream-error`).

**Upload (write):**

1. `attachments.openWriteStream {db, docId, fileName, mimeType}`
   → `{ streamId, credits }` (initial window).
2. Client sends binary chunks within its credit window; host replenishes with
   `stream-credit` per accepted chunk.
3. `attachments.closeWriteStream {streamId}` finalizes via `changeDoc` +
   `addAttachmentStream` → `{ attachment, heads }`. `attachments.abortStream`
   discards.

Flow control is **credit-based** in both directions so neither peer can
overrun the other's memory.

## 6. Events

`event` frames are keyed by the `subscriptionId` the client supplied when
starting the subscription:

- live query: `query-result` (payload = `MindooQueryResult`), `query-error`.
- navigator watch: `view-changed`.
- changefeed: `change` (`{ doc, cursor }`), `caught-up` (after the replay and
  whenever a pass finds nothing new), `change-error`.
- session lifecycle: `session-crashed` (child exited unexpectedly; re-open).

## 7. Security summary

Loopback bind + peer check; mode-`600` bootstrap-token handshake file;
credentials used once to unlock the KeyBag then discarded; per-session,
memory-only, idle-expiring `sessionToken` required on every frame;
rate-limited `session.open`; one child process per session for OS-level memory
isolation of decrypted keys; child killed on `session.close`, idle timeout,
disconnect, or parent exit.

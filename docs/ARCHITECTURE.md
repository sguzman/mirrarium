# Architecture

## System boundary

Mirrarium is deliberately split into a thin Chromium extension and a durable local Rust system.

The extension owns browser observation. Rust owns persistence, indexing, integrity, corpus construction, and replay/revalidation decisions.

```text
ChatGPT tab
  -> MV3 service worker
  -> Chromium DevTools Protocol
  -> Native Messaging
  -> mirrariumd
  -> request ledger + content-addressed stores
  -> rebuildable derived corpus
```

## Capture path

For an attached ChatGPT tab the extension records CDP request/response metadata and waits for `Network.loadingFinished`.

Outbound JSON and URL-encoded request bodies are captured through CDP as private-only evidence. The extension redacts credential-like fields before transport; the Rust daemon re-parses and re-redacts the body in bounded memory before any request-body bytes are allowed onto disk. Opaque, multipart, oversized, auth/session, or otherwise unsupported request bodies are recorded as suppressed rather than archived unsafely. Multipart/upload requests still retain safe structural provenance: content type, whether CDP reports post data, post-data entry count, and declared Content-Length when available. Raw multipart entries and file bytes are never copied into Mirrarium storage at this stage.

For response bodies the extension calls `Network.getResponseBody`, converts the decoded body to raw bytes, and transports those bytes to `mirrariumd` as ordered base64 chunks. Chunks are deliberately far below Chromium's native-messaging message limit.

The daemon writes each capture to a private temporary incoming area, verifies chunk ordering, hashes bytes incrementally with SHA-256, and atomically moves completed bodies into a content-addressed store.

A failed body read still creates a ledger record but does not create a complete body object.

Debugger attachment is asynchronous. The extension tracks pending per-tab setup separately from completed attachments; unsupported navigation or tab closure cancels in-progress setup, and setup rechecks the actual top-level tab URL before becoming active. A canceled partial setup detaches its debugger session before any supported-tab retry. Completed detaches are tracked as per-tab asynchronous operations so rapid out-and-back navigations cannot race reattachment against the old session's teardown. Fetch-paused requests that arrive during setup are passed to the network unchanged, not replayed or conditionally revalidated; those policies begin only after the attachment is origin-verified. This closes the window where a tab might leave ChatGPT before it enters the completed-attachment set.

## Evidence model

Raw observations are evidence and are content-addressed.

Derived records are interpretations and must remain rebuildable from raw evidence.

The first derived adapters are protocol-agnostic SSE reconstruction plus conservative ChatGPT-shaped conversation extraction. Captured `text/event-stream` responses are reparsed from immutable CAS objects into `derived/corpus.sqlite3`, preserving capture provenance, event order, event names, and event data. JSON validity is indexed without discarding non-JSON frames such as `[DONE]`.

Conversation derivation stores observations first. JSON conversation snapshots, message observations, and stream-delta reconstructions retain their source capture IDs and URLs. A read-only canonical view is then computed from the newest immutable JSON snapshot: when ChatGPT supplies a mapping plus `current_node`, Mirrarium walks that node's parent chain and returns only the selected branch. Other branch observations remain intact in the evidence corpus. Stream reconstructions stay separate unless a safe message linkage is known.

## Public/private boundary

Physical object namespaces are:

```text
public/objects/
private/objects/
unknown/objects/
```

The SQLite ledger is sensitive metadata and lives at the protected Mirrarium data root.

Deduplication is scoped by storage class. A hash observed as private never points at a public object, even if the bytes happen to be identical.

Classification is intentionally conservative:

- known versioned/static ChatGPT resources become public;
- non-static ChatGPT-origin traffic becomes private;
- traffic outside understood ChatGPT/static origins remains unknown.

On Unix the data root, private store, incoming area, database, and private objects are permission-hardened. The derived corpus directory is rejected if symlinked (including dangling symlinks) before rebuild or export uses it. The rebuild's directory permission hardening uses a verified open directory handle; the shared `.rebuild.lock` used by rebuild/export must be a real regular file, checked against the opened handle before permissions are applied through that handle. Published `corpus.sqlite3` must itself be a regular, non-symlink file for read-only inspection; publishing a new corpus refuses pre-existing `-wal`/`-shm` sidecars, including dangling symbolic links. Private CAS payloads and in-flight private response staging use authenticated XChaCha20-Poly1305 envelopes, while the authoritative raw ledger and rebuildable derived corpus use SQLCipher with domain-separated keys derived from Mirrarium's master key.

## Extension installation safety

An extension update builds a staged tree and activates it inside a stable user-level install root. Existing destination directories must be real directories, never symlinks, and a recursive preflight rejects nested redirects before any replacement. Activation rechecks directory entries before writing; extension uninstall also refuses redirected roots. The personal Edge profile remains outside the automated QA boundary.

## Replay boundary

Recording does not imply replay permission.

Every request class begins as network-only. Replay is an explicit policy granted only after the resource semantics are understood and tested.

Mutations are not historical-cache candidates.

## QA boundary

The browser integration test launches Playwright's Chromium with:

- an unpacked Mirrarium extension;
- a disposable Chromium user-data directory;
- a disposable HOME/configuration tree;
- a native-host manifest installed by the same CLI path used for normal deployment, pointed only at the disposable test profile;
- a deterministic local TLS fixture mapped to `chatgpt.com`;
- a disposable Mirrarium data directory.

The test never launches or reads Microsoft Edge.

## Current implementation state

Implemented:

- MV3 extension loading and stable unpacked extension identity;
- CDP attachment restricted to ChatGPT origins;
- request/response metadata observation;
- private WebSocket JSON-frame observation using CDP socket creation/handshake/frame events, with sanitized socket URLs and headers, stable per-socket lifecycle identity plus monotonic transport sequence, `WS_SEND` / `WS_RECV` direction, encrypted CAS persistence, credential-key redaction, a mirrored 1 MiB extension/archive payload ceiling, and no replay path; corpus rebuild promotes only verified archived JSON frames into dedicated lifecycle/frame tables ordered by explicit transport sequence while suppressed frames remain raw-only evidence;
- private EventSource message observation using CDP `eventSourceMessageReceived`, stored as canonical single-event `text/event-stream` fragments with `SSE_RECV` direction, stable lifecycle identity plus monotonic transport sequence, sanitized URL/provenance, encrypted CAS persistence, JSON-field redaction, a mirrored 1 MiB extension/archive payload ceiling, separate stats, and no replay path; corpus rebuild derives those fragments into dedicated lifecycle/event tables ordered by explicit transport sequence, retaining source capture/hash and skipped-derivation reasons while keeping them out of generic whole-response SSE tables;
- Chromium reconnect proof for EventSource: each HTTP reconnect leg receives a fresh lifecycle and resets transport sequence to zero; because Chromium omits `Last-Event-ID` from the base `requestWillBeSent` header set, Mirrarium narrowly consumes that one header from `requestWillBeSentExtraInfo` for EventSource requests only, preserving sanitized reconnect provenance without importing the broader sensitive ExtraInfo header surface; derived corpus streams keep reconnect legs separate and add an optional predecessor edge only when Last-Event-ID resolves uniquely to an earlier lifecycle on the same sanitized source URL;
- read-only whole-derived-corpus verification covering SQLite integrity/foreign keys, whole-response SSE counts/sequences/JSON flags, conversation/message/stream/attachment raw-source provenance, attachment download hash/byte agreement, WebSocket/EventSource stream semantics, skipped/derived overlap, reconnect-edge validity, and transport source capture/hash/URL/direction agreement with the authoritative raw ledger, with isolated Chromium proof against the real encrypted fixture-derived corpus;
- direct bounded CLI/API inspection for already-derived completed whole-response SSE captures and their ordered event rows, preserving source hash/privacy provenance, event names, raw data text, JSON validity, and terminal markers without adding new capture semantics;
- direct bounded inspection of WebSocket/EventSource skipped-derivation rows, including sanitized source identity and exact reason, so ambiguous or malformed transport provenance remains explainable without promoting it into trusted lifecycle views;
- stable random raw-archive identity stored and verified inside the authoritative ledger, exposed through a versioned export-source contract so downstream sync state can be bound to the correct Mirrarium archive without changing conversation/sync v1 payloads;
- source-bound sync checkpoints combining archive identity, published manifest fingerprint, and the exact sync-state under one pinned corpus generation, allowing downstream consumers to persist a producer-generated recovery object after matching it against the delta they applied;
- raw-snapshot freshness watermarking for the published derived corpus, with a schema-checked export-status surface that distinguishes ordinary post-rebuild capture growth from raw-ledger rewind/lineage damage;
- versioned conversation JSONL export for downstream local consumers such as Chatarium, composed read-only from derived evidence/canonical/revision/attachment views with source-capture provenance preserved; canonical ambiguity stays explicit in warnings while structural canonical CAS/JSON/database failures abort export instead of being laundered into a valid record; full export, source-bound sync transactions, metadata-only sync plans, and plan-bound batch fetches pin one corpus generation per invocation so concurrent rebuilds cannot mix a single command's published evidence while normal raw capture continues; indexed contiguous fetch ranges let consumers stage large plans in smaller slices, each of which validates the entire plan and checks the current generation before emitting any private content;
- deterministic incremental-sync index JSONL exposing conversation ID plus exact v1 record SHA-256, with the same generation pin and a selective `export-one` path so consumers can fetch only changed conversations and detect deletions from a complete successful index; corpus schema v5 materializes and rebuild-verifies those exact hashes before publication, so manifest/index/no-change delta paths avoid rebuilding every full conversation while all full-record emission recomputes and checks the hash against the materialized identity; a compact manifest fingerprints the exact complete index bytes so unchanged generations can be rejected before transferring the index, while optional expected-record hashes plus a final manifest recheck make multi-command synchronization race-safe across concurrent corpus rebuilds;
- schema-versioned one-command delta synchronization for downstream local consumers: a stdin ID→record-hash sync state is compared against one pinned corpus generation and returns that generation's authoritative manifest, changed/new full conversation records, and explicit deleted IDs in one atomic consumer transaction, avoiding multi-process rebuild-race choreography;
- crash-safe derived-corpus rebuild publication: a single OS-locked rebuild writer pins one coherent read transaction over the live WAL-backed raw ledger, constructs an encrypted rollback-journal staging generation from that snapshot, verifies it against the same raw snapshot, syncs it, and atomically replaces the published corpus only after success; normal failures and stale hard-crash staging artifacts cannot destroy the last good corpus, with Chromium proof using deliberate raw-CAS corruption and simulated stale staging plus a WAL concurrency regression proving later writer appends do not leak into an in-progress rebuild generation;
- native-host process-kill recovery proof over the SQLCipher/WAL raw store: a fully committed capture survives daemon death, an in-flight capture leaves only purgeable staging, and the restarted writer accepts a new capture without duplicating or losing the committed evidence;
- extension-native transport binds all fragments of each in-flight capture and each cache lookup to the original native port: host failure abandons that incomplete transfer rather than splicing its chunks into a new process, while later captures automatically reconnect; errors associated with a capture invalidate its pending transaction even when the host survives; an isolated Chromium native-host kill test proves a fresh sanitized POST is archived after restart. Intermediate writes now receive `capture_message_ack` with an explicit stage and chunk sequence; the extension matches these to a bounded in-memory set of message identities (never payload bodies). These acknowledgments confirm only host processing, not durable storage. A distinct `capture_committed` receipt is emitted only after the synchronous full-durability WAL transaction succeeds; generic native `Ack` and Chrome `postMessage` acceptance are **not** commit receipts. The extension retains each terminal delivery as unconfirmed until that receipt arrives, the port dies, the host rejects it, or a bounded timeout elapses (without logging private capture IDs). Durable per-fragment journaling and replay of interrupted/unacknowledged captures remain unimplemented. True crash-resumable, exactly-once delivery is a separate [protocol-design issue](https://github.com/sguzman/mirrarium/issues/1); no plaintext private-data spool is authorized by this implementation;
- sanitized transport provenance: request/response headers, frame and loader identity, initiator, wall-clock timing, response protocol, and browser-cache/service-worker/prefetch signals;
- redirect-chain preservation with stable lifecycle IDs, hop numbers, and sanitized previous-URL provenance, including Chromium proof that POST→307→POST keeps the sanitized request body associated with both hops while POST→303→GET does not leak the prior body onto the GET hop;
- mirrored ordinary-body resource ceilings: request and response bodies are capped at 16 MiB in both the extension and Rust archive; oversized responses are suppressed before `Network.getResponseBody` when Chromium's transferred size already proves overflow and are rechecked by actual decoded size otherwise, while oversized requests are rejected before sanitizer/chunk transport; archive-side enforcement guarantees no partial oversized body reaches CAS;
- outbound JSON/form request-body capture with extension + daemon secret filtering;
- metadata-only multipart/upload provenance without raw form/file archival;
- completed response-body extraction;
- ordered chunk transport over a persistent Native Messaging port;
- Rust native host framing and typed protocol;
- SHA-256 content-addressed storage;
- privacy-scoped deduplication;
- versioned XChaCha20-Poly1305 encryption for private CAS payloads while preserving plaintext SHA-256 object identity;
- `MIRRPV02` framed XChaCha20-Poly1305 staging for private response bodies, encrypting every body chunk before disk write and allowing unchanged private responses to move from encrypted staging directly into CAS;
- structured private JSON/SSE sanitization by in-memory V2 decryption followed by encrypted V2 rewrite only when redaction changes logical bytes;
- transparent `MIRRPV01`/legacy-plaintext compatibility plus idempotent verified in-place private-CAS migration;
- private encryption status/key-location CLI and isolated Chromium proof that raw private CAS files contain no known fixture plaintext;
- SQLite request ledger;
- body-read failure recording, including Chromium proof that a POST whose response fails after headers preserves sanitized request evidence and response metadata without installing incomplete response bytes into CAS;
- protocol-aware no-body handling for HEAD and 1xx/204/205/304 responses, recorded as intentional suppression without calling `Network.getResponseBody`;
- Unix permission hardening;
- CLI-readable JSON store statistics;
- whole raw-ledger verification begins with SQLCipher/SQLite integrity and foreign-key checks plus explicit required-table/column validation, then applies bidirectional object-reference invariants: capture/request-body hashes must resolve to the correctly classified indexed CAS object with matching logical bytes, and every indexed object must be referenced by at least one capture/request body; structural damage, FK violations, schema loss, and indexed-but-unreferenced rows fail verification;
- non-destructive filesystem CAS orphan auditing remains separate: `verify` reports well-formed unindexed object files and stored bytes without failing, while malformed CAS-tree entries are errors; orphan files are never deleted automatically;
- symlink-safe CAS root checks on both privacy-class and `objects` path components: writable startup preflights them before creating the store, while read-only verification and manual prune reject directory-root symlinks (including dangling ones) and never traverse an external target;
- incoming-staging root is also required to be a real directory: writable startup and abandoned-part cleanup refuse redirected paths before touching any staged evidence, and read-only maintenance rejects dangling or live directory symlinks instead of traversing external files;
- writer-lock control path is required to be a regular non-symlink file, checked both before and after opening with Unix device/inode agreement; permission hardening applies to the open descriptor, not a path that could have been redirected;
- explicit orphan-CAS pruning is manual and exclusive: `maintenance prune-orphans` acquires the raw writer lock, requires a fully clean raw verification result except informational orphan totals, rescans and matches those totals before deletion, removes only well-formed unindexed CAS files, syncs touched directories, and never mutates `.incoming` or migration-recovery evidence;
- local ChatGPT-shaped fixture traffic;
- Playwright/Chromium end-to-end capture test;
- rebuildable SSE event derivation into a separate corpus database;
- conversation snapshots and message observations from ChatGPT-shaped JSON;
- reconstructed stream text from conversation-tagged SSE deltas;
- read-only conversation discovery and evidence inspection through the CLI;
- versioned conversation JSONL interoperability export with a pinned corpus generation and record-at-a-time CLI streaming, preserving deterministic conversation-ID ordering without archive-sized export buffering;
- additive interoperability discovery generations: capabilities v1/v2 and schema bundles v1/v2/v3 remain frozen; capabilities v2 plus bundle v3 advertise negotiation, metadata planning, and full plan-bound batch fetch, while capabilities v3 plus bundle v4 additionally advertise ranged plan-bound fetch; old wire contracts and consumer request schemas remain unchanged;
- exact raw-capture inspection by durable `capture_id`, with recent listings exposing the same ID so every derived provenance key can be resolved without raw SQL; explicit response/request body modes re-read only verified CAS objects, transparently decrypt private storage, preserve hash/byte checks, and encode non-text payloads as base64;
- branch-aware canonical conversation views computed from immutable snapshot evidence;
- per-event stream message revision history with explicit message/parent identity;
- revision-aware canonical merging: exact-ID prefix refinement plus unambiguous exact-parent tail extension;
- temporal gating that prevents stream evidence older than the canonical snapshot from rewriting it;
- explicit derived-corpus schema versioning with rebuild-required failure semantics;
- SQLCipher encryption for the rebuildable derived corpus using a domain-separated key derived from the Mirrarium master key;
- Chromium proof that the derived corpus is not a plaintext SQLite file and does not expose known fixture conversation plaintext at rest;
- SQLCipher encryption for new authoritative raw ledgers from birth with a separate domain-derived key, WAL `synchronous=FULL`, directory-synced fresh-store initialization, synced CAS-before-ledger ordering, and single-transaction capture/request-body publication;
- verified legacy raw-ledger migration via WAL checkpoint + `sqlcipher_export`, schema/row-count/user-version/integrity checks, fsync/directory-sync at staging/install/rollback boundaries, atomic replacement, and interruption recovery; private-object migration uses the same synced staged-replacement discipline;
- read-only ledger inspection paths for CLI/cache/corpus/daemon stats so the browser's native host remains the sole long-lived writer, with a bounded 250 ms SQLite/SQLCipher busy timeout so brief writer lock windows do not immediately surface as read failures;
- Chromium proof that encrypted raw-ledger capture, changed private-response persistence, and subsequent revalidation remain live while read-only inspection runs concurrently;
- exclusive per-data-root writer locking for writable raw-store lifetimes, with read-only inspection left concurrent;
- startup purge of abandoned hashed capture/request/object-migration part files while preserving raw-ledger migration recovery artifacts;
- read-only `.incoming` maintenance reporting that distinguishes live in-flight parts from abandoned crash remnants via the writer lock, reports ledger recovery artifacts separately, and surfaces unexpected entries without opening incomplete payloads;
- live-safe encrypted migration preflight: already-encrypted stores report a read-only no-op, while real legacy migrations still require exclusive ownership;
- structured attachment observations and captured-download correlation by sanitized URL identity;
- immutable public-resource replay candidate inventory and policy auditing;
- explicit separation between immutable/stable public evidence, currently supported replay scope, and safe host/path expansion candidates with byte totals;
- aggregate public replay-coverage audit by host, so new static origins can be justified by observed eligible bytes rather than guessed;
- evidence-ranked cache opportunity summary that identifies the largest unsupported public host/private MIME family only when captured bytes justify expansion;
- verified public-CAS replay lookup with URL/type restrictions, 16 MiB ceiling, path/length/SHA-256 validation;
- chunked native-host replay transport kept below Chrome's per-message host-to-extension limit;
- fail-open CDP Fetch replay for exact query-free ChatGPT `/_next/static/` scripts, stylesheets, images, and fonts;
- exact-host immutable replay for query-free `cdn.oaistatic.com` static resources with matching Rust/extension/telemetry policy gates;
- Chromium end-to-end proof that replay still works with browser cache disabled and avoids origin requests;
- privacy-bounded durable replay outcome telemetry with hit/miss/error/timeout/fulfillment buckets and replayed-byte totals;
- Chromium cold-to-warm lifecycle proof covering initial misses, warm hits, saved bytes, and zero healthy-path replay errors;
- concurrent Chromium stress proof with browser cache disabled: 16 immutable static replays execute locally while 32 private POST captures write concurrently, with no origin fallback and zero replay lookup/timeout/fulfillment errors;
- private JSON/HTML read inventory with volatility, ETag/Last-Modified, cache-control, redacted-identity detection, and conservative revalidation-candidate classification;
- aggregate private cache-coverage audit by resource/MIME family, including validator/no-store coverage, current-policy bytes, and potential expansion bytes without creating a second private URL ledger;
- verified private-CAS lookup with auth/no-store/sensitive-query safety gates, 16 MiB ceiling, path/length/SHA-256 validation, and dedicated chunked native messaging;
- response-stage conditional revalidation for exact ChatGPT `/backend-api/` JSON GET identities plus private HTML `Document` navigations, including non-sensitive query-bearing identities: validators go to the real origin, verified local bytes are substituted only on origin `304`, safe origin 304 headers are preserved on reconstruction, and changed `200` bodies pass through and replace the next revalidation basis;
- sensitive/redacted query keys remain excluded before private lookup;
- Chromium proof with browser cache disabled covering unchanged JSON v1, changed v2, capture of v2, subsequent v2 revalidation, exact query-bearing revalidation, and top-level private HTML document revalidation;
- aggregate-only private revalidation telemetry with not-modified/refresh/fulfillment-error counts and avoided private body bytes, without a second private-URL ledger;
- user-level native-host install/status/uninstall tooling for Edge, Chromium, Chrome, and Chrome for Testing;
- stable user-level unpacked-extension install/status/uninstall tooling with source validation, symlink rejection, fsynced staging, durable atomic initial installation, live-safe synced per-file replacement with the manifest committed last, directory-synced uninstall, and Chromium e2e execution from the installed copy rather than the build tree.
- deterministic compiled-extension build IDs, durably atomically published install/runtime state, durably published Native Messaging manifests, native-host reporting of the running build, and CLI `reload_required` detection when installed files are newer than the active unpacked extension;
- Chromium hot-update proof: replace the stable installed extension while the browser remains open, preserve the extension ID/path, observe the old worker remain active until a browser-level unpacked-extension reload, then observe the new build become active without re-adding the extension.

Next:

- cautiously expand conditional revalidation to additional private MIME/route families only when `cache private-coverage` shows meaningful validator-backed bytes and stable semantics;
- cautiously evaluate additional already-classified exact public static hosts only when `cache public-coverage` shows meaningful immutable/stable expansion bytes;
- continue evaluating browser transport classes only where Chromium exposes stable payload/provenance evidence that can be sanitized and encrypted without weakening the raw-evidence boundary; EventSource and WebSocket are now captured, while WebTransport currently exposes lifecycle events but no comparable frame-payload event in the CDP Network surface used here;
- optional signed/package distribution if a future browser deployment path can preserve the current stable extension identity and native-host contract without adding service dependencies.

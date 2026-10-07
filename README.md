# Mirrarium

Mirrarium is a Chromium-first local recorder, cache, and corpus builder for ChatGPT.

Its job is to observe normal ChatGPT use, preserve the site and account data the browser already receives, build a durable local corpus from that evidence, and serve specifically approved reads from local storage through conservative replay and revalidation policies.

## Core rule

**Capture first. Understand second. Replay third.**

Mirrarium starts as a passive recorder. It must become trustworthy before it is allowed to change ChatGPT network behavior.

## Browser and QA contract

- The extension targets Chromium Manifest V3.
- Playwright using its bundled Chromium build is the canonical automated QA environment.
- Automated tests use disposable Chromium profiles.
- Automated tooling must never launch, attach to, inspect, modify, or reuse Salvador's personal Microsoft Edge profile.
- Edge is a deployment target for normal personal use, not a development or QA sandbox.
- Normal tests do not require a real ChatGPT account.
- Live-site tests, if introduced, must be explicit/manual and still run in isolated Chromium.

## Architecture

```text
chatgpt.com
    |
    v
Chromium extension
    |
    | CDP capture / page observation
    v
Native Messaging
    |
    v
mirrariumd (Rust)
    |
    +-- request ledger / SQLite
    +-- content-addressed raw store
    +-- private store
    +-- derived corpus
    +-- replay / revalidation policy
```

The browser extension stays thin. Durable storage, indexing, corpus construction, integrity, and replay/revalidation policy belong in Rust.

## Storage boundaries

Mirrarium keeps three distinct conceptual stores:

- `public/`: ChatGPT client resources such as JS, CSS, fonts, images, HTML, manifests, and deployment artifacts.
- `private/`: account- or conversation-derived material such as conversation responses, messages, streams, metadata, and attachments.
- `derived/`: rebuildable structured representations and indexes derived from immutable raw evidence.

Raw evidence is preserved. Derived representations may be deleted and rebuilt as parsers improve. The current derived corpus retains conversation snapshots, message observations, SSE events, and reconstructed stream text with source-capture provenance rather than destructively collapsing them into one transcript.

Reusable authentication secrets are not archival data and must not be persisted. Credential-bearing auth/session bodies are suppressed, credential-like query parameters are redacted before URLs are written to the ledger, request bodies are sanitized before persistence, and sensitive request/response headers are redacted before transport provenance is stored.

Mirrarium also distinguishes protocol-level absence from capture failure. HEAD responses and 1xx/204/205/304 statuses are recorded as `suppressed:no_response_body_expected` without calling `Network.getResponseBody`, so legitimate no-body responses do not inflate body-read error telemetry. A response that genuinely fails after headers is kept as failure evidence instead: request evidence and response metadata are retained, but incomplete response bytes never become a CAS object.

WebSocket JSON text frames on ChatGPT hosts are captured as private raw evidence through the same encrypted CAS pipeline. Sent and received frames use `WS_SEND` / `WS_RECV`, share a stable socket lifecycle ID, persist an explicit monotonic `transport_sequence`, preserve sanitized handshake provenance when Chromium exposes it, and redact credential-like JSON fields before persistence. JSON text frames are capped at 1 MiB both before extension-side parsing and again at the Rust archive boundary; oversized frames are observed as `suppressed:websocket_text_frame_too_large` but never enter CAS. Non-JSON text, binary, auth-endpoint, continuation/control-frame payloads are not archived as trusted content. WebSocket evidence is capture-only and is never replayed. `mirrarium stats` reports WebSocket frame count, archived frame bytes, errors, and suppressed frames separately. `corpus rebuild` promotes only archived JSON frames into sequence-aware WebSocket stream/frame tables; suppressed plain-text, binary, oversized, or otherwise untrusted frames remain raw evidence and are not treated as structured corpus content.

Long-lived browser `EventSource` traffic is also observable message-by-message through Chromium CDP rather than waiting for the HTTP stream to close. Mirrarium records each ChatGPT EventSource message as private `SSE_RECV` / `EventSourceMessage` raw evidence using a canonical single-event `text/event-stream` fragment, with a stable lifecycle ID plus explicit monotonic `transport_sequence`, the same structured secret redaction, encrypted CAS persistence, and mirrored 1 MiB extension/archive ceiling. EventSource reconnects remain separate transport lifecycles: each leg restarts `transport_sequence` at zero, while Chromium's reconnect `Last-Event-ID` is retained from the narrow `requestWillBeSentExtraInfo` surface without importing the broader ExtraInfo header set. During `corpus rebuild`, Mirrarium may add a `reconnect_from_lifecycle_id` edge when that Last-Event-ID matches an event from exactly one earlier lifecycle with the same sanitized source URL; missing or ambiguous matches stay unlinked and streams are never silently merged. EventSource message fragments are excluded from generic whole-response SSE derivation so they cannot double-count a later completed stream. `corpus rebuild` instead builds a dedicated sequence-aware EventSource view keyed by lifecycle ID plus `transport_sequence`, retaining source capture/hash, event name, event ID, data, and JSON validity. Missing or ambiguous transport identity is recorded in a derived skipped-capture table rather than guessed. `mirrarium stats` reports EventSource message count, archived bytes, errors, and suppressions separately.

## Inspection CLI

The Rust CLI reads the same local store as the native host:

```bash
mirrarium stats
mirrarium captures 20
mirrarium verify
mirrarium maintenance incoming
mirrarium privacy status
mirrarium privacy migrate
mirrarium cache opportunities
mirrarium cache stats
mirrarium cache replay-stats
mirrarium cache revalidation-stats
mirrarium cache candidates [limit]
mirrarium cache public-coverage
mirrarium cache private-reads [limit]
mirrarium cache private-coverage
mirrarium corpus rebuild
mirrarium corpus stats
mirrarium corpus verify
mirrarium corpus conversations 50
mirrarium corpus conversation <conversation-id>
mirrarium corpus canonical <conversation-id>
mirrarium corpus attachments [conversation-id] [limit]
mirrarium corpus websocket-streams [limit]
mirrarium corpus websocket-frames <lifecycle-id> [limit]
mirrarium corpus eventsource-streams [limit]
mirrarium corpus eventsource-events <lifecycle-id> [limit]
mirrarium corpus stream-revisions <conversation-id> [limit]
mirrarium extension install [source-dir]
mirrarium extension status
mirrarium extension uninstall
mirrarium native-host install
mirrarium native-host status
```

`verify` re-hashes every indexed content-addressed object, checks its class/path and logical byte count, and cross-checks every capture response hash plus every request-body hash/byte count against the authoritative `objects` table. Private CAS objects are transparently decrypted before verification. It also scans the CAS trees for unindexed crash residue: well-formed orphan objects are reported separately by count and stored-on-disk bytes without failing verification, while malformed paths, files, symlinks, or unexpected nesting are reported as verification errors. Mirrarium never deletes orphan CAS objects automatically.

`privacy status` reports the private-CAS key path, whether the key exists, and encrypted/legacy/missing private-object counts. New private response bodies and captured request bodies are stored as versioned XChaCha20-Poly1305 envelopes; the CAS key remains the SHA-256 of the decrypted logical bytes, so deduplication and provenance identities do not change. Existing plaintext private objects remain readable for backward compatibility until migrated.

`privacy migrate` verifies each legacy private object against its indexed logical byte count and SHA-256, then atomically replaces it with an encrypted envelope. The command is idempotent. The default key lives outside the data archive under `XDG_CONFIG_HOME/mirrarium/private.key` or `~/.config/mirrarium/private.key`; `MIRRARIUM_PRIVATE_KEY_FILE` can override it. **Back up the key separately. Losing it makes encrypted private CAS objects unrecoverable.**

Persisted private CAS payloads, the authoritative raw ledger, and the rebuildable derived corpus database are encrypted at rest. New raw ledgers are SQLCipher-encrypted from birth with a ledger-specific key derived from Mirrarium's master key. Existing ordinary-SQLite ledgers remain readable until `privacy migrate`; migration checkpoints the plaintext WAL, exports into a fresh keyed SQLCipher database, verifies schema, row counts, user version, and integrity, atomically installs the encrypted ledger, and preserves recovery evidence on failure. Read-only CLI/cache/corpus inspection never opens a second ledger writer.

The derived corpus likewise uses SQLCipher with its own domain-separated key. `corpus rebuild` pins one coherent read transaction over the live WAL-backed raw ledger, builds a fully encrypted candidate at `derived/.corpus.sqlite3.rebuild` from that single snapshot, derives and verifies it against the same snapshot, syncs it to disk, and only then atomically publishes it as `derived/corpus.sqlite3`. The previous published corpus remains readable if derivation, verification, or raw-CAS access fails. Normal failures clean the staging generation; a hard-crash staging artifact is discarded automatically by the next rebuild. A dedicated OS rebuild lock prevents two rebuild writers from sharing the staging path while leaving read-only corpus inspection available. Neither database can be opened as ordinary SQLite without the Mirrarium key.

Private response staging is encrypted from the first body chunk. Normal private captures create a versioned `MIRRPV02` framed XChaCha20-Poly1305 stream in `.incoming`; each incoming plaintext chunk is authenticated and encrypted before it reaches the file. JSON/SSE sanitization decrypts that stream only in memory, and any rewritten sanitized body is emitted as a fresh encrypted V2 stream before CAS installation. Unchanged private responses can be renamed directly from encrypted staging into private CAS without a plaintext rewrite. Older/request-body `MIRRPV01` envelopes remain readable and deduplicate against V2 by the same plaintext SHA-256 identity.

In steady-state operation Mirrarium no longer intentionally stores private response payloads, raw-ledger contents, or derived-corpus contents as plaintext on disk. The exception is migration of an already-plaintext legacy ledger: the original plaintext database may be retained temporarily as a rollback backup while the new SQLCipher database is verified, and an interrupted migration deliberately preserves recovery evidence until the next migration recovery pass. Key material must therefore be backed up separately from the archive.

Writable raw-store access is single-writer per data root. The native host holds an exclusive `.writer.lock` for its writable lifetime; read-only CLI/cache/corpus/status operations remain concurrent. On writable startup, abandoned hashed response/request/object-migration `.part` files are purged before capture resumes, while the named raw-ledger migration target/backup files are preserved for explicit migration recovery. `mirrarium maintenance incoming` exposes this boundary without mutating it: hashed part files are reported as in-flight when a writer owns the lock and abandoned otherwise, ledger-recovery artifacts are counted separately, and unexpected files/non-files are surfaced without reading or decrypting incomplete payloads. A fully encrypted `privacy migrate` therefore remains a read-only no-op even while the browser daemon is live; an actual legacy migration still requires the exclusive writer lock.

`cache opportunities` combines public/private coverage with runtime savings and, when evidence exists, names the largest currently unsupported public host and private MIME/resource family plus the policy gate blocking each. A null lead means captured evidence does not justify widening that side yet.

`cache stats` and `cache candidates` audit the public replay surface. A candidate must be a successful public GET with a stored body, a static resource type, explicit `Cache-Control: immutable`, and exactly one observed body hash for its URL. Evidence eligibility is distinct from replay policy: candidates now report whether the current exact-host/path policy actually permits replay, whether they are safe scope-expansion candidates, and why a byte-stable asset remains outside policy. Replay is stricter still: v1 fulfills exact query-free ChatGPT `/_next/static/` script, stylesheet, image, and font URLs plus exact query-free `cdn.oaistatic.com` static resources. Bodies are capped at 16 MiB, the public CAS path, byte count, and SHA-256 are re-verified before serving, and every miss, timeout, corruption, ambiguity, or native-host error fails open to the network.

`cache public-coverage` aggregates that evidence by host, reporting observed captures, unique bytes, immutable/stable bytes, bytes already replay-supported, and bytes blocked only by current host/path scope. This is the required evidence view before widening public replay to another host.

Verified hits are streamed from the native host in sub-1-MiB messages and fulfilled through CDP Fetch. The Chromium integration test disables the browser's ordinary cache and verifies Mirrarium's replay marker while proving the ChatGPT and `cdn.oaistatic.com` origins receive no additional requests for replayed static resources. A dedicated stress pass also replays 16 immutable static objects while 32 private POST captures are being written concurrently; all static objects execute locally with zero origin fallback and zero replay lookup/timeout/fulfillment errors.

`cache replay-stats` reports the durable cold/warm replay lifecycle: attempts, successful hits, misses, lookup errors, local lookup timeouts, fulfillment failures, and bytes actually replayed. Telemetry accepts only the same public static replay scope, so it cannot become a ledger for private or dynamic request URLs.

`cache private-reads` audits captured private JSON and HTML GETs and reports observation count, body-hash volatility, latest validator metadata, cache-control, and revalidation eligibility. `cache private-coverage` is the aggregate policy-planning view: it groups all captured private GET bodies by resource/MIME family and reports total bytes, validator coverage, `no-store`, bytes already handled by current revalidation policy, and validator-backed bytes that could justify a future MIME-family expansion. It adds no new private URL ledger. For exact ChatGPT `/backend-api/` JSON GET identities and validator-backed private HTML document navigations with a verified private CAS object and ETag or Last-Modified, Mirrarium performs conditional network revalidation. Query strings are allowed when every query key is non-sensitive and the stored identity was not redacted, so paginated/filter reads such as `?offset=0&limit=2` can participate without permitting token/signature/auth query identities. Mirrarium injects the validator, lets the origin decide freshness, substitutes the verified local body only after a real `304 Not Modified`, preserves safe origin 304 headers on the reconstructed `200`, and passes a changed `200` through normally so the new representation is captured. Missing validators, `no-store`, redacted/sensitive identity, auth paths, unsupported origins, and non-GET traffic remain network-only. Any widening of private revalidation should be driven by `cache private-coverage` rather than MIME guessing.

The Chromium test disables the browser's own HTTP cache and proves both data and document lifecycles: cached JSON v1 -> origin 304 -> local v1; origin changes to v2 -> network 200 v2 wins and is captured; the next read validates v2 -> origin 304 -> local v2. A separate top-level navigation reload proves validator-backed private HTML can likewise receive an origin 304 and be reconstructed from verified private CAS bytes as a normal 200 document. A separate paginated-query fixture proves the same path for an exact non-sensitive query identity. Revalidated local responses remain observations in the raw/corpus history rather than erasing prior evidence.

`cache revalidation-stats` reports aggregate private revalidation outcomes without duplicating private URLs into the telemetry table: total candidate responses, origin `304` reuse, fresh `200` updates, fulfillment errors, and private response-body bytes avoided by successful 304 substitution.

`corpus conversations` lists observed conversation identities with snapshot/message/stream counts. `corpus conversation` returns the evidence for one identity: source-tagged message observations and stream reconstructions. `corpus canonical` computes a read-only transcript from the newest JSON snapshot, following ChatGPT's `current_node` parent chain when mapping data is present. Unselected branches remain available through the evidence command, and unlinked stream text is never silently spliced into the transcript.

`corpus websocket-streams` lists derived socket lifecycles containing trusted archived JSON frames; `corpus websocket-frames` returns those frames in explicit transport-sequence order with sent/received direction, source capture/hash provenance, and sanitized JSON text. Suppressed raw socket frames are intentionally absent from this derived view.

`corpus eventsource-streams` lists sequence-aware derived EventSource lifecycles; `corpus eventsource-events` returns the ordered message evidence for one lifecycle, including source capture/hash, transport sequence, event name/ID, data, and JSON-validity status. EventSource fragments remain separate from generic whole-response SSE tables to prevent duplicate derivation.

`corpus attachments` lists attachment observations extracted from structured JSON and any captured download bodies correlated to them. Correlation requires the same sanitized URL identity; signed query credentials are redacted before persistence and are never treated as durable attachment identity.

`corpus stream-revisions` exposes per-event full-message stream evidence with explicit message and parent IDs. Canonicalization uses that evidence conservatively: exact message IDs may refine prefix-compatible snapshot text, and a streamed child may extend the canonical tail only when its explicit parent is the current tail and there is exactly one child candidate. Branches, conflicting revisions, older-than-snapshot streams, and otherwise ambiguous evidence are preserved but never guessed into the canonical transcript.

The derived corpus is explicitly schema-versioned. When its rebuildable schema changes, readers fail with a direct instruction to run `mirrarium corpus rebuild` rather than leaking low-level SQLite column errors. `corpus verify` is a read-only integrity pass over the encrypted derived database: it runs SQLite integrity and foreign-key checks; validates whole-response SSE counts, sequence shape, and JSON markers; cross-checks conversation snapshots, message observations, stream revisions/reconstructions, attachment observations, and captured downloads against the authoritative raw ledger; and also verifies WebSocket/EventSource stream counts, transport semantics, skipped/derived separation, reconnect edges, JSON consistency, and raw capture/hash/URL/direction provenance.

## Linux extension install/update

Build the extension, then copy it into Mirrarium's stable user-level install directory:

```bash
pnpm build:extension
mirrarium extension install
```

The default source is `./extension/dist`. The installed copy lives at `$XDG_DATA_HOME/mirrarium/extension` or `~/.local/share/mirrarium/extension`; `MIRRARIUM_EXTENSION_SOURCE` and `MIRRARIUM_EXTENSION_DIR` override those paths. Installation validates the MV3 manifest/background worker, rejects symlinks, and stages a self-contained copy beside the destination. Initial install atomically moves that tree into place. Updates keep the already-loaded root directory continuously present, atomically replace staged files inside it, commit `manifest.json` last, and then remove obsolete files; Chromium never sees its unpacked-extension root disappear.

Use `mirrarium extension status` to inspect the installed copy and `mirrarium extension uninstall` to remove it. For unpacked Edge/Chromium development installs, load this **stable installed directory** once. Future `mirrarium extension install` runs update that same path rather than requiring a different build-tree location.

Each build carries a deterministic manifest `version_name` derived from the compiled worker. Installation publishes that build ID atomically and durably under the user's Mirrarium config, and the running service worker reports its own build ID through an equivalently synced runtime-state file in the native host. Native Messaging manifests are also synced before install reports success. `mirrarium extension install` and `mirrarium extension status` compare the installed and observed running builds and report `reload_required`.

The install directory and extension ID stay stable across updates, so the extension never needs to be removed and re-added. Unpacked Chromium/Edge still requires a browser-level extension reload to import changed files from disk: when `reload_required` is true, click **Reload** for Mirrarium in `edge://extensions` (or restart Edge). Mirrarium deliberately does not loop on `chrome.runtime.reload()` trying to hot-swap its own unpacked package. Automated QA performs the equivalent browser-level reload only inside the disposable Chromium profile.

## Linux native-host install

For normal Edge use, build or install `mirrarium` and `mirrariumd` side by side, then run:

```bash
mirrarium native-host install
```

Edge is the default browser target. The command installs only Mirrarium's user-level Native Messaging manifest and resolves `mirrariumd` to an absolute executable path. It does not inspect browser history, cookies, sessions, or other profile contents.

Use `mirrarium native-host status` to inspect whether the manifest exists and `mirrarium native-host uninstall` to remove only that manifest. Supported browser names are `edge`, `chromium`, `chrome`, and `chrome-for-testing`. An explicit daemon path may be supplied after the browser name when `mirrariumd` is not next to the CLI. `MIRRARIUM_BROWSER_USER_DATA_DIR` overrides the browser user-data root for non-default or disposable profiles.

The data root is resolved from `MIRRARIUM_DATA_DIR`, then `XDG_DATA_HOME/mirrarium`, then `~/.local/share/mirrarium`.

## Historical MVP: Passive Recorder

The original recorder-only milestone was defined by the following conditions and is now complete:

1. an unpacked MV3 extension runs in isolated Chromium;
2. it activates only for supported ChatGPT origins;
3. it can attach Chromium DevTools Protocol network instrumentation;
4. captured transactions reach a Rust native host;
5. response bodies are content-addressed and deduplicated;
6. request metadata is indexed durably;
7. public/private classification exists;
8. obvious reusable credentials are filtered;
9. streaming responses are preserved;
10. conversation-related traffic can be identified;
11. capture survives navigation and reload;
12. CLI inspection makes the capture auditable;
13. Playwright exercises the pipeline against deterministic fixtures;
14. response substitution was intentionally still disabled at that milestone.

Mirrarium has since moved beyond this recorder-only baseline: immutable public replay and validator-backed private revalidation are implemented and covered by isolated Chromium integration tests.

## Replay safety

Unknown traffic always defaults to **network + record**.

Mutations such as sending messages, editing, deleting, uploading, or changing account state are never satisfied from historical cache entries.

Replay is granted only to explicitly classified safe resources, beginning with immutable/versioned assets and expanding only when captured evidence and tests justify the semantics.

## Relationship to Chatarium

Mirrarium observes and preserves the real ChatGPT site's ecology.

Chatarium owns the local conversation ecology.

A future bridge may let Chatarium consume Mirrarium's accumulated private corpus without independently crawling account history.

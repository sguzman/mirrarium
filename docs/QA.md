# QA contract

## Canonical browser

Automated QA uses **Playwright's bundled Chromium**, explicitly selected with the `chromium` channel.

Microsoft Edge and branded Google Chrome are not automated test browsers for this repository.

Playwright's Chromium channel supports extension testing in the real new-headless browser, so the canonical suite does not require Xvfb or a desktop session.

## Personal Edge prohibition

Automation must never:

- launch the user's Microsoft Edge binary;
- attach DevTools/CDP to the user's Edge instance;
- point Chromium or Playwright at an Edge user-data directory;
- copy cookies, Local Storage, IndexedDB, passwords, extensions, or session state from Edge;
- use the user's normal ChatGPT login as a prerequisite for routine tests.

This is a hard safety boundary, not a preference.

## Origin boundary

Mirrarium may attach only while the tab's top-level URL is a supported ChatGPT origin.

If a monitored tab navigates away from ChatGPT, the extension must immediately detach its debugger session and discard that tab's pending capture state.

ChatGPT subresources may originate from separate static/CDN hosts; they are observable only because the top-level tab remains an explicitly supported ChatGPT tab.

## Isolation

Every browser automation run gets:

- a disposable Chromium user-data directory;
- a disposable HOME and Chromium configuration tree;
- a disposable native-host manifest;
- a disposable Mirrarium data root.

The installed-extension lifecycle test may use Chromium's browser-level unpacked-extension reload inside that disposable profile to apply changed files at the stable install path. It must never use the user's personal Edge instance to perform or validate an extension reload.

The integration fixture runs locally but is resolved inside Chromium as `https://chatgpt.com`. This exercises the production origin gate without granting Mirrarium access to arbitrary localhost pages.

The fixture deliberately exercises the current production boundaries rather than only the recorder MVP. It includes:

- immutable ChatGPT and `cdn.oaistatic.com` static assets for cold/warm replay;
- concurrent static replay while private POST captures are writing;
- private JSON, query-bearing JSON, and top-level HTML conditional revalidation;
- duplicate private bodies at distinct URLs for privacy-scoped deduplication;
- JSON request-body redaction plus metadata-only multipart upload capture;
- 302, POST→307→POST, and POST→303→GET redirect semantics;
- HEAD/204 no-body responses and a response that fails after headers;
- streaming SSE plus long-lived EventSource messages and reconnects;
- WebSocket sent/received JSON, plain-text, binary, and oversized-frame boundaries;
- encrypted raw ledger/private CAS/derived corpus checks, including FULL-synchronous raw-ledger durability, CAS-before-ledger publication, live-writer `mirrarium verify` with response/request-body object-reference validation and zero orphan/malformed CAS expectations;
- Rust filesystem regressions that reject symlinked CAS privacy-class/object roots during writable startup, read-only verification, and manual orphan-prune preflight, without touching external target files;
- Rust incoming-staging regressions proving both redirected and dangling `.incoming` symlinks fail startup/maintenance without purging external hash-shaped `.part` files;
- Rust writer-lock regressions proving symlink/dangling-link rejection during acquisition and maintenance probing without changing or chmodding foreign lock-target contents;
- Rust corpus generation-lock regressions proving rebuild and export refuse symlinked or dangling `.rebuild.lock` entries without modifying or chmodding external targets;
- Rust derived-corpus-root regressions proving redirected `derived/` paths are refused before locking and directory permission hardening, preserving outside files and permission modes;
- Rust published-corpus regressions proving symlinked `corpus.sqlite3` cannot be read as a trusted database and dangling SQLite `-wal`/`-shm` sidecars block atomic publication;
- isolated Chromium source-bound sync regression proving deletion-only plans emit zero fetch bytes on success, have the same deletion/checkpoint result as one-command sync, and reject forged deletions before stdout;
- isolated Chromium bounded fetch regression proving a plan-bound `start-index count` slice agrees with full fetch, rejects out-of-range or zero-count requests before stdout, and allows an empty deletion-only plan to yield zero bytes;
- immutable capabilities-v1/v2 and schema-bundle-v1/v2/v3 contracts plus additive capabilities-v3 discovery and schema-bundle-v4 AJV validation, including explicit ranged-fetch feature negotiation;
- Rust plan-diff tests ensuring producer-generated plans include every changed/new upsert and every deletion relative to the prior checkpoint, while documenting that consumers must independently reject incomplete but schema-valid plans;
- Rust extension-update regressions proving redirected/dangling install roots and nested symlink destinations are rejected without modifying external files;
- corpus rebuild, transport-derived views, reconnect edges, whole-corpus `corpus verify`, failed-rebuild preservation, stale crash-staging recovery, and native-host kill/restart proof that preserves a committed SQLCipher/WAL capture while purging only an abandoned in-flight capture;
- isolated Chromium native-host SIGKILL/reconnect proof: kill only the disposable native host, then record a new POST with both response and sanitized request-body CAS hashes under the recovered host, without reusing the dead process;
- Rust staging regressions reject duplicate committed IDs before writing, refuse pre-existing symlink staging without following it, enforce the 128-concurrent-capture admission ceiling and prove abort releases capacity and staged data without manufacturing durable evidence; the native-wire test checks abort success, idempotent repeated abort, absent commit status, and automatic discard after a rejected out-of-order chunk (including a failed subsequent finish);
- Native-wire and Rust receipt regressions: intermediate capture-start/chunk messages return stage- and sequence-bound `capture_message_ack` receipts; only a successfully committed `CaptureFinish` returns `capture_committed`, a second finish fails without duplication; read-only commit probes distinguish present and absent IDs without creating a ledger, including after a successful commit; the extension tracks bounded outstanding identities, retains up to 256 UUID-only unconfirmed IDs after disconnect/timeout, checks them on its next live native connection with at most four bounded reconnect attempts, and treats missing receipts as unconfirmed rather than as successful delivery;
- the Chatarium-facing corpus interoperability contract: AJV validation against every published v1 schema plus the installed-binary schema bundle/capabilities handshake, the golden record-hash vector, deterministic full export/index/manifest/sync-checkpoint bytes, hash-guarded `export-one`, generation-pinned sync deltas with explicit deletions, source-bound bootstrap/incremental sync transactions with cross-archive, torn-checkpoint, and structurally damaged-upsert rejection without an authoritative next checkpoint, plus a real C1→late browser capture→rebuild G2→single-upsert C2 progression, cached no-change sync without raw-ledger access, and rejection of structurally damaged changed records without a successful/parseable delta;
- supported-to-foreign top-level navigation stress on disposable Chromium tabs, checking extension-owned debugger teardown rather than Playwright's own browser CDP sessions, followed by a return-to-supported-origin reattachment regression and proof that subsequent foreign-origin requests do not enter the raw ledger;
- stable installed-extension update/reload behavior in disposable Chromium, including deliberate installed-tree mutation that must flip `extension status` to invalid until the original file is restored.

Cache telemetry can continue increasing while isolated Chromium performs additional replay/revalidation. Assertions comparing counters from **separate CLI invocations** must tolerate monotonic growth; exact sum identities belong within a single CLI report/snapshot. This prevents live-capture timing from being mistaken for a corrupted aggregate.

The suite therefore proves classification, privacy-scoped deduplication, failure semantics, replay/revalidation safety, transport evidence boundaries, encrypted persistence, derived-corpus integrity, and browser deployment lifecycle without using a real ChatGPT account.

## Live-site tests

The standard suite does not require OpenAI credentials or a real ChatGPT account.

Any future live-site suite must be separately named, opt-in, and still use an isolated Playwright Chromium profile.

## Codex contract

Codex is expected to iterate against the Playwright/Chromium harness and deterministic fixtures. It should be able to break, reset, and recreate that environment freely.

If a test requires touching the personal Edge environment, the test design is wrong.

## Deployment doctor acceptance

The isolated Chromium browser-restart test exercises `mirrarium doctor chrome-for-testing`
with a missing installation, installed extension/native host, a deliberately
forged native-host `allowed_origins` entry, and an encrypted archive with
persisted private captures after a complete browser shutdown. The diagnostic
must never claim a live browser is verified; it does not inspect the user's
real browser profile, create an empty ledger, or print private conversation
content. Separate real-session acceptance remains outstanding.

## Explicit archived conversation read

The Chromium restart test reads the encrypted conversation snapshot through
`cache offline-conversation` after the browser has closed, asserts the
snapshot content, hash, and explicit unverified-freshness marker, and proves
that the origin sees no additional request. It also rejects non-conversation
API URLs. Rust unit coverage exercises latest-snapshot selection without ETags,
strict URL scope, and rejection of corrupt CAS bodies. The offline CLI writes
plaintext private response JSON **only on explicit invocation** and does not
add a background browser replay mode.

## Offline index acceptance

The Chromium browser-restart fixture now enumerates the captured
conversation metadata after shutdown, checks a normal `/c/<id>` selector
against explicit readback, verifies it performs no origin requests, confirms
that response-body text is absent, and checks the limit boundary. Rust tests
cover latest-first ordering, duplicate observations, cross-origin URL
spoofing, rejected private GET shapes, and the distinction between listing
metadata and verifying a stored body. The index never performs automatic
private response substitution.

## Offline HTML viewer acceptance

The isolated browser-restart test writes an explicit owner-only HTML export
after Chromium shutdown, checks that re-export cannot overwrite the file,
confirms the fixture origin sees no additional request, and opens the result as
a file URL in disposable Chromium. Rust unit tests cover proper escaping,
selected-branch ordering, omitted discarded branches, simple message arrays,
empty views, and exclusive private-file creation. The viewer has a strict
script/network-denying CSP and renders text only. No real account is used.

## Browsable offline library acceptance

The isolated browser-restart fixture exports a private HTML directory from
persisted synthetic responses while Chromium is closed. It verifies the
directory and index, refuses replacement of an existing directory, then opens
the local file:// index and follows an ordinary hyperlink to a captured
conversation in disposable Chromium. It checks that message text is absent
from the index and that no extra origin request occurs. Rust unit tests cover
index-title HTML escaping, relative-only local links, and owner-private
directory creation.

## Partial offline library recovery

Playwright copies the disposable archive after browser shutdown, corrupts one
private CAS object in the copy only, and exports the library from that copy.
The library must skip the unreadable conversation, preserve other verified
snapshots, report a nonzero skipped count, and avoid any ChatGPT origin fetch.
The original archive remains intact for the remaining revalidation tests.

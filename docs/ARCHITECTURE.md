# Architecture

## System boundary

Mirrarium is deliberately split into a thin Chromium extension and a durable local Rust system.

The extension owns browser observation. Rust owns persistence, indexing, integrity, corpus construction, and later replay decisions.

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

On Unix the data root, private store, incoming area, database, and private objects are permission-hardened. Encryption at rest is not implemented yet and remains required before treating the private store as cryptographically protected.

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
- sanitized transport provenance: request/response headers, frame and loader identity, initiator, wall-clock timing, response protocol, and browser-cache/service-worker/prefetch signals;
- redirect-chain preservation with stable lifecycle IDs, hop numbers, and sanitized previous-URL provenance;
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
- body-read failure recording;
- Unix permission hardening;
- CLI-readable JSON store statistics;
- local ChatGPT-shaped fixture traffic;
- Playwright/Chromium end-to-end capture test;
- rebuildable SSE event derivation into a separate corpus database;
- conversation snapshots and message observations from ChatGPT-shaped JSON;
- reconstructed stream text from conversation-tagged SSE deltas;
- read-only conversation discovery and evidence inspection through the CLI;
- branch-aware canonical conversation views computed from immutable snapshot evidence;
- per-event stream message revision history with explicit message/parent identity;
- revision-aware canonical merging: exact-ID prefix refinement plus unambiguous exact-parent tail extension;
- temporal gating that prevents stream evidence older than the canonical snapshot from rewriting it;
- explicit derived-corpus schema versioning with rebuild-required failure semantics;
- SQLCipher encryption for the rebuildable derived corpus using a domain-separated key derived from the Mirrarium master key;
- Chromium proof that the derived corpus is not a plaintext SQLite file and does not expose known fixture conversation plaintext at rest;
- SQLCipher encryption for new authoritative raw ledgers from birth with a separate domain-derived key;
- verified legacy raw-ledger migration via WAL checkpoint + `sqlcipher_export`, schema/row-count/user-version/integrity checks, atomic replacement, and interruption recovery;
- read-only ledger inspection paths for CLI/cache/corpus/daemon stats so the browser's native host remains the sole long-lived writer;
- Chromium proof that encrypted raw-ledger capture, changed private-response persistence, and subsequent revalidation remain live while read-only inspection runs concurrently;
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
- private JSON/HTML read inventory with volatility, ETag/Last-Modified, cache-control, redacted-identity detection, and conservative revalidation-candidate classification;
- aggregate private cache-coverage audit by resource/MIME family, including validator/no-store coverage, current-policy bytes, and potential expansion bytes without creating a second private URL ledger;
- verified private-CAS lookup with auth/no-store/sensitive-query safety gates, 16 MiB ceiling, path/length/SHA-256 validation, and dedicated chunked native messaging;
- response-stage conditional revalidation for exact ChatGPT `/backend-api/` JSON GET identities plus private HTML `Document` navigations, including non-sensitive query-bearing identities: validators go to the real origin, verified local bytes are substituted only on origin `304`, safe origin 304 headers are preserved on reconstruction, and changed `200` bodies pass through and replace the next revalidation basis;
- sensitive/redacted query keys remain excluded before private lookup;
- Chromium proof with browser cache disabled covering unchanged JSON v1, changed v2, capture of v2, subsequent v2 revalidation, exact query-bearing revalidation, and top-level private HTML document revalidation;
- aggregate-only private revalidation telemetry with not-modified/refresh/fulfillment-error counts and avoided private body bytes, without a second private-URL ledger;
- user-level native-host install/status/uninstall tooling for Edge, Chromium, Chrome, and Chrome for Testing.

Next:

- cautiously expand conditional revalidation to additional private MIME/route families only when `cache private-coverage` shows meaningful validator-backed bytes and stable semantics;
- cautiously evaluate additional already-classified exact public static hosts only when `cache public-coverage` shows meaningful immutable/stable expansion bytes;
- deeper redirect/body semantics where Chromium exposes safe evidence;
- extension packaging/update ergonomics for normal Edge deployment;
- explicit stale/incomplete-capture maintenance and recovery tooling for `.incoming` without ever treating incomplete ciphertext as archive evidence.

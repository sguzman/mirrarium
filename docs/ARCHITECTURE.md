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

Outbound JSON and URL-encoded request bodies are captured through CDP as private-only evidence. The extension redacts credential-like fields before transport; the Rust daemon re-parses and re-redacts the body in bounded memory before any request-body bytes are allowed onto disk. Opaque, multipart, oversized, auth/session, or otherwise unsupported request bodies are recorded as suppressed rather than archived unsafely.

For response bodies the extension calls `Network.getResponseBody`, converts the decoded body to raw bytes, and transports those bytes to `mirrariumd` as ordered base64 chunks. Chunks are deliberately far below Chromium's native-messaging message limit.

The daemon writes each capture to a private temporary incoming area, verifies chunk ordering, hashes bytes incrementally with SHA-256, and atomically moves completed bodies into a content-addressed store.

A failed body read still creates a ledger record but does not create a complete body object.

## Evidence model

Raw observations are evidence and are content-addressed.

Derived records are interpretations and must remain rebuildable from raw evidence.

The first derived adapters are protocol-agnostic SSE reconstruction plus conservative ChatGPT-shaped conversation extraction. Captured `text/event-stream` responses are reparsed from immutable CAS objects into `derived/corpus.sqlite3`, preserving capture provenance, event order, event names, and event data. JSON validity is indexed without discarding non-JSON frames such as `[DONE]`.

Conversation derivation deliberately stores observations rather than pretending there is already one canonical transcript. JSON conversation snapshots, message observations, and stream-delta reconstructions retain their source capture IDs and URLs. Canonical conversation views can be built later from this evidence without destroying revisions, branches, or protocol drift.

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
- a test-only native-host manifest;
- a deterministic local TLS fixture mapped to `chatgpt.com`;
- a disposable Mirrarium data directory.

The test never launches or reads Microsoft Edge.

## Current implementation state

Implemented:

- MV3 extension loading and stable unpacked extension identity;
- CDP attachment restricted to ChatGPT origins;
- request/response metadata observation;
- sanitized transport provenance: request/response headers, frame and loader identity, initiator, wall-clock timing, response protocol, and browser-cache/service-worker/prefetch signals;
- outbound JSON/form request-body capture with extension + daemon secret filtering;
- completed response-body extraction;
- ordered chunk transport over a persistent Native Messaging port;
- Rust native host framing and typed protocol;
- SHA-256 content-addressed storage;
- privacy-scoped deduplication;
- SQLite request ledger;
- body-read failure recording;
- Unix permission hardening;
- CLI-readable JSON store statistics;
- local ChatGPT-shaped fixture traffic;
- Playwright/Chromium end-to-end capture test;
- rebuildable SSE event derivation into a separate corpus database;
- conversation snapshots and message observations from ChatGPT-shaped JSON;
- reconstructed stream text from conversation-tagged SSE deltas.

Next:

- redirect-chain preservation and explicit request lifecycle identities;
- multipart/upload provenance without unsafe raw-body archival;
- canonical conversation views across snapshots, branches, and stream revisions;
- native-host installation tooling for normal Edge deployment;
- encryption/key management for private storage.

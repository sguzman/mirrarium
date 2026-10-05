# Mirrarium

Mirrarium is a Chromium-first local recorder, cache, and corpus builder for ChatGPT.

Its job is to observe normal ChatGPT use, preserve the site and account data the browser already receives, build a durable local corpus from that evidence, and eventually serve specifically approved reads from local storage.

## Core rule

**Capture first. Understand second. Replay third.**

Mirrarium starts as a passive recorder. It must become trustworthy before it is allowed to change ChatGPT network behavior.

## Browser and QA contract

- The extension targets Chromium Manifest V3.
- Playwright using its managed Chromium build is the canonical automated QA environment.
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
    +-- future replay engine
```

The browser extension stays thin. Durable storage, indexing, corpus construction, integrity, and future replay policy belong in Rust.

## Storage boundaries

Mirrarium keeps three distinct conceptual stores:

- `public/`: ChatGPT client resources such as JS, CSS, fonts, images, HTML, manifests, and deployment artifacts.
- `private/`: account- or conversation-derived material such as conversation responses, messages, streams, metadata, and attachments.
- `derived/`: rebuildable structured representations and indexes derived from immutable raw evidence.

Raw evidence is preserved. Derived representations may be deleted and rebuilt as parsers improve.

Reusable authentication secrets are not archival data and must not be persisted.

## MVP: Passive Recorder

The first milestone succeeds when:

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
14. Mirrarium does **not** substitute responses yet.

## Replay safety

Unknown traffic always defaults to **network + record**.

Mutations such as sending messages, editing, deleting, uploading, or changing account state are never satisfied from historical cache entries.

Replay begins only with explicitly classified safe resources, starting with immutable/versioned assets and expanding later only when we can prove the semantics.

## Relationship to Chatarium

Mirrarium observes and preserves the real ChatGPT site's ecology.

Chatarium owns the local conversation ecology.

A future bridge may let Chatarium consume Mirrarium's accumulated private corpus without independently crawling account history.

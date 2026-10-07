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
- corpus rebuild, transport-derived views, reconnect edges, whole-corpus `corpus verify`, failed-rebuild preservation, stale crash-staging recovery, and native-host kill/restart proof that preserves a committed SQLCipher/WAL capture while purging only an abandoned in-flight capture;
- the Chatarium-facing corpus interoperability contract: AJV validation against every published v1 schema plus the installed-binary schema bundle/capabilities handshake, the golden record-hash vector, deterministic full export/index/manifest/sync-checkpoint bytes, hash-guarded `export-one`, generation-pinned sync deltas with explicit deletions, source-bound bootstrap/incremental sync transactions with cross-archive, torn-checkpoint, and structurally damaged-upsert rejection without an authoritative next checkpoint, plus a real C1→late browser capture→rebuild G2→single-upsert C2 progression, cached no-change sync without raw-ledger access, and rejection of structurally damaged changed records without a successful/parseable delta;
- stable installed-extension update/reload behavior in disposable Chromium, including deliberate installed-tree mutation that must flip `extension status` to invalid until the original file is restored.

The suite therefore proves classification, privacy-scoped deduplication, failure semantics, replay/revalidation safety, transport evidence boundaries, encrypted persistence, derived-corpus integrity, and browser deployment lifecycle without using a real ChatGPT account.

## Live-site tests

The standard suite does not require OpenAI credentials or a real ChatGPT account.

Any future live-site suite must be separately named, opt-in, and still use an isolated Playwright Chromium profile.

## Codex contract

Codex is expected to iterate against the Playwright/Chromium harness and deterministic fixtures. It should be able to break, reset, and recreate that environment freely.

If a test requires touching the personal Edge environment, the test design is wrong.

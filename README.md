# Mirrarium

Mirrarium is a Chromium-first local recorder, cache, and corpus builder for ChatGPT.

Its job is to observe normal ChatGPT use, preserve the site and account data the browser already receives, build a durable local corpus from that evidence, and eventually serve specifically approved reads from local storage.

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
    +-- future replay engine
```

The browser extension stays thin. Durable storage, indexing, corpus construction, integrity, and future replay policy belong in Rust.

## Storage boundaries

Mirrarium keeps three distinct conceptual stores:

- `public/`: ChatGPT client resources such as JS, CSS, fonts, images, HTML, manifests, and deployment artifacts.
- `private/`: account- or conversation-derived material such as conversation responses, messages, streams, metadata, and attachments.
- `derived/`: rebuildable structured representations and indexes derived from immutable raw evidence.

Raw evidence is preserved. Derived representations may be deleted and rebuilt as parsers improve. The current derived corpus retains conversation snapshots, message observations, SSE events, and reconstructed stream text with source-capture provenance rather than destructively collapsing them into one transcript.

Reusable authentication secrets are not archival data and must not be persisted. Credential-bearing auth/session bodies are suppressed, credential-like query parameters are redacted before URLs are written to the ledger, request bodies are sanitized before persistence, and sensitive request/response headers are redacted before transport provenance is stored.

## Inspection CLI

The Rust CLI reads the same local store as the native host:

```bash
mirrarium stats
mirrarium captures 20
mirrarium verify
mirrarium cache stats
mirrarium cache replay-stats
mirrarium cache revalidation-stats
mirrarium cache candidates [limit]
mirrarium cache private-reads [limit]
mirrarium corpus rebuild
mirrarium corpus stats
mirrarium corpus conversations 50
mirrarium corpus conversation <conversation-id>
mirrarium corpus canonical <conversation-id>
mirrarium corpus attachments [conversation-id] [limit]
mirrarium corpus stream-revisions <conversation-id> [limit]
mirrarium native-host install
mirrarium native-host status
```

`verify` re-hashes every indexed content-addressed object and checks its class/path and byte count. It exits unsuccessfully if corruption is found.

`cache stats` and `cache candidates` audit the public replay surface. A candidate must be a successful public GET with a stored body, a static resource type, explicit `Cache-Control: immutable`, and exactly one observed body hash for its URL. Replay is stricter still: v1 fulfills exact query-free ChatGPT `/_next/static/` script, stylesheet, image, and font URLs plus exact query-free `cdn.oaistatic.com` static resources. Bodies are capped at 16 MiB, the public CAS path, byte count, and SHA-256 are re-verified before serving, and every miss, timeout, corruption, ambiguity, or native-host error fails open to the network.

Verified hits are streamed from the native host in sub-1-MiB messages and fulfilled through CDP Fetch. The Chromium integration test disables the browser's ordinary cache and verifies Mirrarium's replay marker while proving the ChatGPT and `cdn.oaistatic.com` origins receive no additional requests for replayed static resources.

`cache replay-stats` reports the durable cold/warm replay lifecycle: attempts, successful hits, misses, lookup errors, local lookup timeouts, fulfillment failures, and bytes actually replayed. Telemetry accepts only the same public static replay scope, so it cannot become a ledger for private or dynamic request URLs.

`cache private-reads` audits captured private JSON GETs and reports observation count, body-hash volatility, latest validator metadata, cache-control, and revalidation eligibility. For exact ChatGPT `/backend-api/` GET identities with a verified private CAS object and ETag or Last-Modified, Mirrarium now performs conditional network revalidation. Query strings are allowed when every query key is non-sensitive and the stored identity was not redacted, so paginated/filter reads such as `?offset=0&limit=2` can participate without permitting token/signature/auth query identities. Mirrarium injects the validator, lets the origin decide freshness, substitutes the verified local body only after a real `304 Not Modified`, and passes a changed `200` through normally so the new representation is captured. Missing validators, `no-store`, redacted/sensitive identity, auth paths, unsupported origins, and non-GET traffic remain network-only.

The Chromium test disables the browser's own HTTP cache and proves the full lifecycle: cached v1 -> origin 304 -> local v1; origin changes to v2 -> network 200 v2 wins and is captured; the next read validates v2 -> origin 304 -> local v2. A separate paginated-query fixture proves the same path for an exact non-sensitive query identity. Revalidated local responses remain observations in the raw/corpus history rather than erasing prior evidence.

`cache revalidation-stats` reports aggregate private revalidation outcomes without duplicating private URLs into the telemetry table: total candidate responses, origin `304` reuse, fresh `200` updates, fulfillment errors, and private response-body bytes avoided by successful 304 substitution.

`corpus conversations` lists observed conversation identities with snapshot/message/stream counts. `corpus conversation` returns the evidence for one identity: source-tagged message observations and stream reconstructions. `corpus canonical` computes a read-only transcript from the newest JSON snapshot, following ChatGPT's `current_node` parent chain when mapping data is present. Unselected branches remain available through the evidence command, and unlinked stream text is never silently spliced into the transcript.

`corpus attachments` lists attachment observations extracted from structured JSON and any captured download bodies correlated to them. Correlation requires the same sanitized URL identity; signed query credentials are redacted before persistence and are never treated as durable attachment identity.

`corpus stream-revisions` exposes per-event full-message stream evidence with explicit message and parent IDs. Canonicalization uses that evidence conservatively: exact message IDs may refine prefix-compatible snapshot text, and a streamed child may extend the canonical tail only when its explicit parent is the current tail and there is exactly one child candidate. Branches, conflicting revisions, older-than-snapshot streams, and otherwise ambiguous evidence are preserved but never guessed into the canonical transcript.

The derived corpus is explicitly schema-versioned. When its rebuildable schema changes, readers fail with a direct instruction to run `mirrarium corpus rebuild` rather than leaking low-level SQLite column errors.

## Linux native-host install

For normal Edge use, build or install `mirrarium` and `mirrariumd` side by side, then run:

```bash
mirrarium native-host install
```

Edge is the default browser target. The command installs only Mirrarium's user-level Native Messaging manifest and resolves `mirrariumd` to an absolute executable path. It does not inspect browser history, cookies, sessions, or other profile contents.

Use `mirrarium native-host status` to inspect whether the manifest exists and `mirrarium native-host uninstall` to remove only that manifest. Supported browser names are `edge`, `chromium`, `chrome`, and `chrome-for-testing`. An explicit daemon path may be supplied after the browser name when `mirrariumd` is not next to the CLI. `MIRRARIUM_BROWSER_USER_DATA_DIR` overrides the browser user-data root for non-default or disposable profiles.

The data root is resolved from `MIRRARIUM_DATA_DIR`, then `XDG_DATA_HOME/mirrarium`, then `~/.local/share/mirrarium`.

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

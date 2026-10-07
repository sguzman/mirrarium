# Mirrarium corpus JSONL export

`mirrarium corpus export-schema-bundle` is the frozen original v1 convenience envelope containing the original export/sync JSON Schemas in dependency order. Each embedded schema retains its normative URN `$id`; consumers can register the array with a JSON Schema implementation and then resolve the same cross-schema `$ref` graph used by Mirrarium's own Chromium QA. The bundle envelope itself is not a replacement wire contract — the embedded schemas remain authoritative.

`mirrarium corpus export-schema-bundle-v2` is the additive superset for new consumers. It preserves all v1 members and ordering, then appends consumer-requirements, compatibility, negotiated-sync-request, and sync-plan schemas. Bundle v1 is not changed retroactively; consumers can pin either bundle generation explicitly.

`mirrarium corpus export-capabilities` is the discovery handshake for an installed producer. Its v1 schema is `schemas/mirrarium-corpus-capabilities-v1.schema.json` / `mirrarium corpus export-capabilities-schema`. It reports the stable raw `archive_id`, current internal producer schema version, the v1 export/sync wire versions represented by that published contract, synchronization byte ceilings, SHA-256 hash algorithms, and support for source-bound plus `--require-fresh` sync. Capability values describe protocol support, not current corpus freshness or manifest state; use `export-status` and `export-manifest` for those.

`mirrarium corpus export-negotiate` provides producer-side compatibility preflight without exposing conversation data. The consumer writes one v1 `consumer-requirements` object to stdin and receives one v1 `compatibility` result on stdout. Requirements use accepted-version sets rather than minimum-version arithmetic, so future incompatible wire generations do not become implicitly acceptable. They can also constrain accepted record/index hash algorithms, require source-bound or fresh sync features, and demand minimum producer byte ceilings. A compatible result exits successfully. An incompatible result is still emitted in full with deterministic structured mismatches, then the command exits nonzero; consumers may inspect that result but must not proceed to private synchronization. The schemas are `schemas/mirrarium-corpus-consumer-requirements-v1.schema.json` and `schemas/mirrarium-corpus-compatibility-v1.schema.json`, also emitted by `export-consumer-requirements-schema` and `export-compatibility-schema`.

Capabilities v1 and schema-bundle v1 are intentionally not mutated to advertise this later additive negotiation surface. Their already-published member sets remain stable; negotiation is separately versioned and discoverable through its dedicated schema commands/files.

`mirrarium corpus export-negotiation-schema-bundle` is the additive one-shot schema registration surface for negotiation-aware consumers. It emits a deterministic v1 bundle containing the existing export-manifest/sync-state/sync-checkpoint/capabilities dependencies followed by consumer-requirements, compatibility, and negotiated-sync-request schemas. It is a separate bundle precisely so the original schema-bundle v1 membership and bytes remain stable.

For consumers that want the compatibility check and private synchronization bound to one producer invocation, `mirrarium corpus export-sync-negotiated` accepts a v1 `negotiated-sync-request` object containing `requirements`, an optional prior source-bound `checkpoint`, and `require_fresh`. The command validates compatibility before entering the existing sync writer. Incompatible requirements therefore produce a nonzero exit with **empty stdout**, so no private upsert prefix can escape. On success stdout is the existing `sync-transaction` v1 byte contract unchanged. This command closes the upgrade race between a standalone preflight and the actual sync without creating a second delta implementation. Its request schema is `schemas/mirrarium-corpus-negotiated-sync-request-v1.schema.json` / `mirrarium corpus export-sync-negotiated-schema`.

`mirrarium corpus export [limit]` is a read-only interoperability surface for local consumers such as Chatarium. The normative machine-readable v1 contract is checked in at `schemas/mirrarium-corpus-conversation-v1.schema.json` and is also emitted by `mirrarium corpus export-schema` for installed-binary consumers.

## v1 framing

`mirrarium corpus export-source` emits the stable identity of the authoritative raw archive. Its normative contract is `schemas/mirrarium-corpus-export-source-v1.schema.json` and `mirrarium corpus export-source-schema`. The `archive_id` is a random 256-bit lowercase-hex identifier stored inside the raw ledger, preserved by SQLCipher migration and archive copies, and unrelated to keys, filesystem paths, account identity, or conversation content. A downstream consumer should bind persisted sync state to this ID and refuse to reuse that state against a different archive ID. Sync-state v1 deliberately remains unchanged, so this source binding is a consumer-side envelope rule rather than an added field in the existing wire object.

`mirrarium corpus export-status` reports whether the currently published derived corpus was built from the current raw archive tip. Its normative contract is `schemas/mirrarium-corpus-export-status-v1.schema.json` and `mirrarium corpus export-status-schema`. A rebuild stores the raw capture count and maximum SQLite rowid from the exact pinned raw snapshot used for derivation. Status compares that watermark with the current append-only raw ledger and reports `fresh`, `pending_raw_captures`, both watermarks, and the published export manifest. Any new raw capture makes the result conservatively stale until the next successful `corpus rebuild`, even if that capture would not ultimately change a conversation export record. A raw ledger that is behind or contradicts the published watermark is treated as a lineage error rather than ordinary staleness.

`mirrarium corpus export-manifest` emits a tiny deterministic JSON object containing the full exported conversation count and SHA-256 of the exact complete `corpus export-index` JSONL bytes (including newline framing). Its normative contract is `schemas/mirrarium-corpus-export-manifest-v1.schema.json` and `mirrarium corpus export-manifest-schema`. Consumers can compare this fingerprint first and skip all further synchronization work when it is unchanged.

`mirrarium corpus export-index [limit]` writes a lightweight JSONL synchronization index in the same deterministic conversation-ID order. Each index line carries the conversation ID and the exact v1 `record_sha256`, allowing a consumer to compare local state before requesting changed records with `export-one`. `mirrarium corpus export-one <conversation-id> [expected-record-sha256]` may be given the hash from that index; when supplied, it refuses to emit if the current pinned generation produces a different record hash. Its normative contract is `schemas/mirrarium-corpus-conversation-index-v1.schema.json` and `mirrarium corpus export-index-schema`.

A full successful index export (no limit) is authoritative for the current exported conversation set, so a consumer may treat previously known IDs absent from that complete set as deletions from the current derived generation. A limited index is only a prefix and must never be used to infer deletions. The manifest makes this machine-checkable: its `index_sha256` is the hash of the complete unbounded index bytes, so a limited index cannot satisfy the manifest fingerprint and therefore cannot accidentally qualify as the authoritative deletion set.

The command writes JSON Lines to stdout: one complete JSON object per conversation, ordered by `conversation_id`. `mirrarium corpus export-one <conversation-id>` emits exactly one v1 record using the same builder and generation lock; for a given published generation its line is byte-identical to that conversation's line in the full export. With no limit it exports every derived conversation. A positive limit restricts the number of conversation records. There is no header line; every record is self-describing. The CLI streams records as they are composed while holding the generation lock; it does not materialize the full archive export in memory before writing. Peak export memory therefore scales with the largest single conversation record rather than the total number of conversations. For an unchanged published corpus generation, repeated exports with the same limit are byte-for-byte deterministic; field order, record order, and newline framing are part of that reproducibility guarantee.

An export pins one published derived-corpus generation with a shared rebuild lock for the lifetime of the command. Multiple exports may run together, but `corpus rebuild` cannot replace the published generation until active exports finish. Normal browser capture remains independent and continues through the raw-store writer.

Conversation identity discovery includes snapshots, message observations, stream reconstructions, stream-message revisions, and attachment observations, so revision-only or attachment-only derived evidence is not dropped from the export.

Each v1 record has:

- `schema: "mirrarium.corpus.conversation"`
- `schema_version: 1`
- `producer_corpus_schema_version`: the internal rebuildable corpus schema version that produced the record
- `record_type: "conversation"`
- `conversation_id`
- `source_capture_ids`: sorted/deduplicated raw capture IDs referenced anywhere in the record
- `record_sha256`: SHA-256 of the exact compact UTF-8 v1 record bytes with the single producer-emitted member `"record_sha256":"<64 lowercase hex>",` removed byte-for-byte
- `evidence`: the normal derived conversation evidence view, retaining source capture IDs
- `canonical`: the conservative canonical conversation view when safely derivable, otherwise `null` when no canonical snapshot exists
- `canonical_error`: reserved by v1 for a future explicitly recoverable canonicalization error class; current exporters emit `null`
- `stream_revisions`: ordered stream-message revision evidence with source capture IDs
- `attachments`: attachment observations plus correlated captured downloads and their raw capture IDs

## Contract

The export schema version is independent of Mirrarium's internal SQLite schema version. Internal rebuildable schema changes do not change the JSONL contract unless the export `schema_version` changes.

For a fixed published corpus generation, repeated exports are byte-for-byte deterministic. `record_sha256` therefore gives downstream consumers an idempotent content identity for each conversation record; a consumer can key updates by `conversation_id` and skip work when the hash is unchanged.

The current internal corpus schema materializes those exact v1 record hashes during `corpus rebuild`. The candidate generation computes each hash from the full record while the rebuild's coherent raw snapshot is pinned, then recomputes every record and verifies the materialized index before publication. `corpus verify` repeats that consistency check. The cache is therefore a rebuildable performance index, not a second authority.

Because `producer_corpus_schema_version` is itself part of the v1 record bytes and therefore part of the hash preimage, an internal corpus-schema bump can legitimately change every `record_sha256` even when the higher-level conversation content is otherwise identical. That does **not** change the export JSON Schema contract, but it can cause a one-time full downstream upsert after such a producer-schema migration. Consumers must key correctness to the returned hashes/schema fields rather than assuming internal-schema upgrades are hash-neutral.

### v1 record hash verification

A consumer can verify `record_sha256` without parsing and reserializing JSON:

1. Take the exact UTF-8 bytes of one Mirrarium conversation JSONL record, excluding the trailing newline.
2. Locate the single ASCII member `"record_sha256":"<64 lowercase hexadecimal characters>",`. Mirrarium v1 emits it immediately after `source_capture_ids`.
3. Remove exactly those bytes, including the trailing comma.
4. SHA-256 the remaining bytes and compare the lowercase hexadecimal digest with the removed value.

The resulting byte sequence is exactly the producer's v1 hash preimage. This byte-level rule avoids cross-language JSON object-order and escaping differences. `schemas/mirrarium-corpus-conversation-v1.hash-vector.json` contains a golden record line, its exact preimage, and the expected digest.

The export is deliberately conversation-focused. WebSocket/EventSource transport-global views, skipped transport derivations, cache telemetry, and arbitrary raw capture bodies remain available through their dedicated Mirrarium inspection commands and are not silently folded into conversation records.

The exporter does not crawl ChatGPT, mutate the archive, or read new network data. It composes already-derived local evidence. It also does not include arbitrary decrypted CAS bodies beyond text/content already represented in the derived corpus.

The encrypted-at-rest guarantee ends at this explicit interoperability boundary: JSONL records written to stdout contain the private conversation/evidence text represented by the derived corpus in plaintext. Pipes pass that plaintext to the receiving process, and shell redirection creates an ordinary plaintext file with permissions determined by the shell/filesystem environment. Consumers such as Chatarium should treat the stream as sensitive local data and establish their own at-rest protections if they persist it.

Interop stdout is pipe-friendly on Unix-style workflows: if the downstream reader closes the pipe, Mirrarium treats the resulting broken pipe as quiet successful termination rather than printing an error or panicking. This does not make a truncated export authoritative; consumers that require a complete export/delta must still require the producer process to complete normally and, where applicable, verify the manifest/hash protocol.

Canonicalization is not authoritative over evidence. Ambiguous or unsupported snapshot shapes remain explicit through the canonical view's `basis_kind` and warnings, and a conversation with no canonical snapshot simply exports `canonical: null`. Actual canonical read/decryption/JSON/database errors are structural failures and abort the export. They are never downgraded into `canonical_error` on an otherwise valid v1 record.

## One-command delta synchronization

If the consumer requires all raw evidence captured before synchronization to have been derived, first require `mirrarium corpus export-status` to report `fresh: true`. A successful delta without that check is still coherent and authoritative for the published derived generation, but that generation may intentionally lag newer raw captures. Because raw capture continues independently, freshness is a point-in-time observation rather than a global browser pause.

`mirrarium corpus export-delta` is the direct consumer path for a local importer that can hold its known conversation hash map. It reads one sync-state JSON object from stdin and emits one sync-delta JSON object to stdout. The whole calculation holds a shared corpus-generation lock, so its manifest, upserts, and deletions all describe one published generation.

For consumers that want one producer-generated persistence object, `mirrarium corpus export-sync-checkpoint` emits a source-bound checkpoint containing the stable `archive_id`, the published export manifest, and the exact sync-state. Its normative contract is `schemas/mirrarium-corpus-sync-checkpoint-v1.schema.json` and `mirrarium corpus export-sync-checkpoint-schema`. After successfully applying a delta, a consumer may fetch a checkpoint and persist it only if the checkpoint `archive_id` matches the bound source and checkpoint `manifest.index_sha256` equals the delta manifest fingerprint; a mismatch means a rebuild raced the commands and synchronization must retry. The nested sync-state is constrained to the same 64 MiB ceiling accepted by `export-delta`.

For the stronger one-command path, use `mirrarium corpus export-sync`. Empty stdin performs a bootstrap synchronization. Otherwise stdin must be a previously persisted v1 source-bound checkpoint. Mirrarium validates the checkpoint wire contract, nested sync-state, conversation count, manifest/index fingerprint, and `archive_id` before emitting anything. A checkpoint from another archive is rejected. The command emits one v1 source-bound sync transaction containing both the delta and the next checkpoint from the same pinned corpus generation. Its normative contract is `schemas/mirrarium-corpus-sync-transaction-v1.schema.json` and `mirrarium corpus export-sync-schema`. Apply the delta atomically, then persist the returned checkpoint; no second producer command or manifest recheck is required.

For large consumers that prefer not to stream private conversation bodies until they know exactly what changed, `mirrarium corpus export-sync-plan [--require-fresh]` accepts the same optional prior source-bound checkpoint and emits a metadata-only v1 plan. The plan contains the current manifest, changed conversation-index records (ID + expected v1 record hash), explicit deletions, and the next checkpoint from one pinned generation. Its schema is `schemas/mirrarium-corpus-sync-plan-v1.schema.json` / `mirrarium corpus export-sync-plan-schema`. Stage each listed upsert with `export-one <conversation-id> <record-sha256>`; if any expected-hash fetch fails, discard the staged work and request a new plan. Only after every listed record is safely staged should the consumer apply deletions and persist the plan's checkpoint. The plan path never reconstructs unchanged conversation bodies.

The sync-state v1 contract is `schemas/mirrarium-corpus-sync-state-v1.schema.json` and is emitted by `mirrarium corpus export-sync-state-schema`. `mirrarium corpus export-sync-state` emits the exact current checkpoint from the verified materialized conversation hash index under one shared generation lock; its `records` object is equivalent to converting the complete `export-index` JSONL stream into conversation-ID→record-hash pairs. For an unchanged published generation, repeated checkpoint output is byte-for-byte deterministic, including key order and trailing newline framing. The emitter first verifies manifest/index agreement, then serializes directly from that ordered index into one capped output buffer rather than materializing a second conversation map. It enforces the same 64 MiB byte ceiling as `export-delta`, so every successfully emitted checkpoint is guaranteed to be acceptable as a later delta input. If a future archive exceeds that checkpoint size, use the manifest/index/`export-one` protocol instead:

```json
{"schema":"mirrarium.corpus.sync-state","schema_version":1,"records":{"conversation-id":"<record_sha256>"}}
```

For a first import, send an empty `records` object. For subsequent imports, `records` should describe the consumer's currently committed conversation state.

The sync-delta v1 contract is `schemas/mirrarium-corpus-sync-delta-v1.schema.json` and is emitted by `mirrarium corpus export-delta-schema`. Its `manifest` is the authoritative manifest for the same pinned generation; `upserts` contains every current conversation whose hash is absent or different in the supplied state; and `deleted_conversation_ids` contains supplied IDs that no longer exist in the current export universe. Both arrays are deterministic in conversation-ID order.

Manifest and index generation read only the verified materialized ID→hash index. `export-delta` likewise compares the supplied state against that index first and constructs full conversation records only for actual upserts. An unchanged sync therefore avoids canonical snapshot decryption/parsing and attachment/revision assembly for every conversation. Operations that emit a full record still reconstruct it and require its computed hash to equal the materialized hash; structural CAS/JSON damage therefore fails `export-one`, full export, or a changed-record delta even though a no-change manifest/index/delta can still report the last verified published identity.

A consumer should apply the delta atomically: stage all `upserts`, remove the explicit deleted IDs, then persist the resulting ID→`record_sha256` map and the returned manifest fingerprint together. **The process exit status is authoritative.** Because upserts are streamed, a structural verification failure may occur before any stdout is emitted or after a private JSON prefix has already been written; on nonzero exit stdout is either empty or intentionally incomplete/non-authoritative and must be discarded in full. Never apply, cache, or attempt to salvage stdout from a failed command. On successful exit the response is one complete schema-valid JSON object generated under one shared generation lock, so no second manifest recheck is needed for this one-command path.

The sync-state request contains only conversation IDs and content hashes, but the delta response contains the same private derived conversation text as ordinary export records. Treat stdout as sensitive plaintext at the interoperability boundary.

For operational safety, `export-delta` accepts at most 64 MiB of UTF-8 sync-state JSON on stdin and rejects larger or non-UTF-8 input before JSON parsing. This byte ceiling is intentionally separate from the semantic JSON Schema contract; it does not impose a fixed conversation-count limit on normal sync states.

## Preferred source-bound synchronization

For a new consumer, run `mirrarium corpus export-sync` with empty stdin. Apply the returned `delta`, then persist the returned `checkpoint`. If the importer requires all raw captures committed before synchronization begins to already be represented in the published corpus, use `mirrarium corpus export-sync --require-fresh`. That option compares the current raw watermark with the pinned corpus watermark before writing stdout and fails with no output when the corpus is stale or the lineage is inconsistent. Browser capture may continue after the freshness observation; later captures belong to the next sync.

For every later synchronization, pipe that exact checkpoint back to `mirrarium corpus export-sync`. Mirrarium refuses checkpoints belonging to a different raw archive and refuses internally inconsistent/torn checkpoints before emitting private upsert content. On success, the response's top-level `archive_id`, nested checkpoint `archive_id`, delta manifest, and checkpoint manifest all describe one pinned generation.

The legacy `export-delta` + separate source/checkpoint protocol remains supported for consumers that already manage source binding themselves.

Like `export-delta`, `export-sync` streams changed private conversation records and can encounter structural CAS/JSON damage after stdout begins. **Exit status remains authoritative.** On nonzero exit, discard stdout in full even if it begins with a valid transaction prefix. Mirrarium writes the returned checkpoint only after every changed record and deletion list has been serialized successfully, so a failed transaction cannot contain an authoritative next checkpoint.

## Incremental synchronization

A downstream consumer can synchronize without re-ingesting unchanged conversation text while remaining safe against a rebuild between commands:

-1. Read `corpus export-source` and require its `archive_id` to match the source identity bound to the consumer's persisted sync state. For first import, bind the new state to this ID.
0. If the goal is “derive everything captured so far,” read `corpus export-status` and require `fresh: true`; otherwise the protocol deliberately synchronizes the last published derived generation.
1. Read `corpus export-manifest` as M1. If its `index_sha256` matches the consumer's previously completed synchronization, stop: the exported conversation set and every v1 record hash were unchanged at that observation point.
2. Otherwise read a complete `corpus export-index`, hash its exact stdout bytes, and require that SHA-256 to equal M1.`index_sha256`. If it differs, a rebuild raced the commands; discard the index and restart from step 1.
3. Compare each `conversation_id` / `record_sha256` pair with the consumer's local state.
4. Stage new or changed records using `corpus export-one <conversation-id> <record-sha256>`. If any command rejects the expected hash, a newer generation raced the sync; discard staged changes and restart.
5. Read `corpus export-manifest` again as M2 and require M2.`index_sha256` to equal M1.`index_sha256`. Only then commit staged records, retire local IDs absent from the authoritative index, and persist the manifest fingerprint. If the final fingerprint differs, discard the staged synchronization and restart.

The index is a bandwidth optimization, not a weaker identity. Its `record_sha256` is copied from the exact v1 conversation record produced by the same pinned generation and record builder.

Each individual command pins one corpus generation, but the lock intentionally does not span multiple CLI processes. The hash checks above are therefore part of the interoperability protocol, not optional diagnostics: they turn independent read-only commands into a coherent optimistic snapshot transaction without blocking browser capture or requiring a long-lived Mirrarium service.

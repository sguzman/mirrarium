# Mirrarium corpus JSONL export

`mirrarium corpus export [limit]` is a read-only interoperability surface for local consumers such as Chatarium. The normative machine-readable v1 contract is checked in at `schemas/mirrarium-corpus-conversation-v1.schema.json` and is also emitted by `mirrarium corpus export-schema` for installed-binary consumers.

## v1 framing

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

Internal corpus schema v5 materializes those exact v1 record hashes during `corpus rebuild`. The candidate generation computes each hash from the full record while the rebuild's coherent raw snapshot is pinned, then recomputes every record and verifies the materialized index before publication. `corpus verify` repeats that consistency check. The cache is therefore a rebuildable performance index, not a second authority.

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

Canonicalization is not authoritative over evidence. Ambiguous or unsupported snapshot shapes remain explicit through the canonical view's `basis_kind` and warnings, and a conversation with no canonical snapshot simply exports `canonical: null`. Actual canonical read/decryption/JSON/database errors are structural failures and abort the export. They are never downgraded into `canonical_error` on an otherwise valid v1 record.

## One-command delta synchronization

`mirrarium corpus export-delta` is the direct consumer path for a local importer that can hold its known conversation hash map. It reads one sync-state JSON object from stdin and emits one sync-delta JSON object to stdout. The whole calculation holds a shared corpus-generation lock, so its manifest, upserts, and deletions all describe one published generation.

The sync-state v1 contract is `schemas/mirrarium-corpus-sync-state-v1.schema.json` and is emitted by `mirrarium corpus export-sync-state-schema`:

```json
{"schema":"mirrarium.corpus.sync-state","schema_version":1,"records":{"conversation-id":"<record_sha256>"}}
```

For a first import, send an empty `records` object. For subsequent imports, `records` should describe the consumer's currently committed conversation state.

The sync-delta v1 contract is `schemas/mirrarium-corpus-sync-delta-v1.schema.json` and is emitted by `mirrarium corpus export-delta-schema`. Its `manifest` is the authoritative manifest for the same pinned generation; `upserts` contains every current conversation whose hash is absent or different in the supplied state; and `deleted_conversation_ids` contains supplied IDs that no longer exist in the current export universe. Both arrays are deterministic in conversation-ID order.

Manifest and index generation read only the verified materialized ID→hash index. `export-delta` likewise compares the supplied state against that index first and constructs full conversation records only for actual upserts. An unchanged sync therefore avoids canonical snapshot decryption/parsing and attachment/revision assembly for every conversation. Operations that emit a full record still reconstruct it and require its computed hash to equal the materialized hash; structural CAS/JSON damage therefore fails `export-one`, full export, or a changed-record delta even though a no-change manifest/index/delta can still report the last verified published identity.

A consumer should apply the delta atomically: stage all `upserts`, remove the explicit deleted IDs, then persist the resulting ID→`record_sha256` map and the returned manifest fingerprint together. If the command fails, apply nothing. Because the entire response is generated under one shared generation lock, no second manifest recheck is needed for this one-command path.

The sync-state request contains only conversation IDs and content hashes, but the delta response contains the same private derived conversation text as ordinary export records. Treat stdout as sensitive plaintext at the interoperability boundary.

## Incremental synchronization

A downstream consumer can synchronize without re-ingesting unchanged conversation text while remaining safe against a rebuild between commands:

1. Read `corpus export-manifest` as M1. If its `index_sha256` matches the consumer's previously completed synchronization, stop: the exported conversation set and every v1 record hash were unchanged at that observation point.
2. Otherwise read a complete `corpus export-index`, hash its exact stdout bytes, and require that SHA-256 to equal M1.`index_sha256`. If it differs, a rebuild raced the commands; discard the index and restart from step 1.
3. Compare each `conversation_id` / `record_sha256` pair with the consumer's local state.
4. Stage new or changed records using `corpus export-one <conversation-id> <record-sha256>`. If any command rejects the expected hash, a newer generation raced the sync; discard staged changes and restart.
5. Read `corpus export-manifest` again as M2 and require M2.`index_sha256` to equal M1.`index_sha256`. Only then commit staged records, retire local IDs absent from the authoritative index, and persist the manifest fingerprint. If the final fingerprint differs, discard the staged synchronization and restart.

The index is a bandwidth optimization, not a weaker identity. Its `record_sha256` is copied from the exact v1 conversation record produced by the same pinned generation and record builder.

Each individual command pins one corpus generation, but the lock intentionally does not span multiple CLI processes. The hash checks above are therefore part of the interoperability protocol, not optional diagnostics: they turn independent read-only commands into a coherent optimistic snapshot transaction without blocking browser capture or requiring a long-lived Mirrarium service.

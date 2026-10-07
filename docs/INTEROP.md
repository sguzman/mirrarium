# Mirrarium corpus JSONL export

`mirrarium corpus export [limit]` is a read-only interoperability surface for local consumers such as Chatarium. The normative machine-readable v1 contract is checked in at `schemas/mirrarium-corpus-conversation-v1.schema.json` and is also emitted by `mirrarium corpus export-schema` for installed-binary consumers.

## v1 framing

`mirrarium corpus export-index [limit]` writes a lightweight JSONL synchronization index in the same deterministic conversation-ID order. Each index line carries the conversation ID and the exact v1 `record_sha256`, allowing a consumer to compare local state before requesting changed records with `export-one`. Its normative contract is `schemas/mirrarium-corpus-conversation-index-v1.schema.json` and `mirrarium corpus export-index-schema`.

A full successful index export (no limit) is authoritative for the current exported conversation set, so a consumer may treat previously known IDs absent from that complete set as deletions from the current derived generation. A limited index is only a prefix and must never be used to infer deletions.

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
- `record_sha256`: SHA-256 of the full v1 record payload except the `record_sha256` field itself
- `evidence`: the normal derived conversation evidence view, retaining source capture IDs
- `canonical`: the conservative canonical conversation view when safely derivable, otherwise `null`
- `canonical_error`: `null` on normal derivation, otherwise the canonicalization failure for this conversation
- `stream_revisions`: ordered stream-message revision evidence with source capture IDs
- `attachments`: attachment observations plus correlated captured downloads and their raw capture IDs

## Contract

The export schema version is independent of Mirrarium's internal SQLite schema version. Internal rebuildable schema changes do not change the JSONL contract unless the export `schema_version` changes.

For a fixed published corpus generation, repeated exports are byte-for-byte deterministic. `record_sha256` therefore gives downstream consumers an idempotent content identity for each conversation record; a consumer can key updates by `conversation_id` and skip work when the hash is unchanged.

The export is deliberately conversation-focused. WebSocket/EventSource transport-global views, skipped transport derivations, cache telemetry, and arbitrary raw capture bodies remain available through their dedicated Mirrarium inspection commands and are not silently folded into conversation records.

The exporter does not crawl ChatGPT, mutate the archive, or read new network data. It composes already-derived local evidence. It also does not include arbitrary decrypted CAS bodies beyond text/content already represented in the derived corpus.

The encrypted-at-rest guarantee ends at this explicit interoperability boundary: JSONL records written to stdout contain the private conversation/evidence text represented by the derived corpus in plaintext. Pipes pass that plaintext to the receiving process, and shell redirection creates an ordinary plaintext file with permissions determined by the shell/filesystem environment. Consumers such as Chatarium should treat the stream as sensitive local data and establish their own at-rest protections if they persist it.

Canonicalization is not authoritative over evidence. If canonicalization of one conversation fails, the exporter keeps that conversation's evidence and provenance and emits `canonical: null` plus `canonical_error` instead of aborting the entire JSONL stream. Structural corpus/read failures still fail the command.

## Incremental synchronization

A downstream consumer can synchronize without re-ingesting unchanged conversation text:

1. Read a complete `corpus export-index`.
2. Compare each `conversation_id` / `record_sha256` pair with the consumer's local state.
3. Fetch new or changed records with `corpus export-one <conversation-id>`.
4. After the complete index command exits successfully, remove or retire local records whose IDs are absent from the authoritative index set.

The index is a bandwidth optimization, not a weaker identity. Its `record_sha256` is copied from the exact v1 conversation record produced by the same pinned generation and record builder.

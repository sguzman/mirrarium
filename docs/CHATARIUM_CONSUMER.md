# Mirrarium -> Chatarium consumer acceptance contract

This is a handoff specification, **not** a claim that Chatarium already implements Mirrarium ingestion. Mirrarium produces immutable raw evidence, rebuildable derived conversation views, and a source-bound export protocol. Chatarium owns its local conversation identity, durable import state, user edits, and any future live transport. A Mirrarium observation never by itself proves current remote account access or successful remote writes.

## Consumer-side state

For each independent Mirrarium producer, persist its stable 256-bit `archive_id`, its last **fully committed** Mirrarium `sync-checkpoint`, and a source-scoped mapping from `conversation_id` to the Chatarium-local imported identity and `record_sha256`. This state is private. It must not be mixed with account-export, Flight Recorder, or manually authored local material, even when remote-looking identifiers collide.

Do not treat the producer's `producer_corpus_schema_version` as the export protocol version. The conversation wire is currently v1, while Mirrarium's rebuildable internal schema can change independently; internal schema upgrades can legitimately force a one-time full reimport because they change hash preimages.

## Preferred pull sequence

1. Query `mirrarium corpus export-capabilities-v2` (or invoke source-bound negotiation) and check supported wire versions and algorithms. Bind the first accepted archive identity; refuse any subsequent archive mismatch.
2. Ask for `mirrarium corpus export-sync-plan --require-fresh` with empty input for bootstrap, or with **exactly** the previously committed producer checkpoint on later runs. If stale raw evidence is acceptable, omit `--require-fresh` deliberately rather than pretending the derived corpus is current.
3. Validate the plan against `schemas/mirrarium-corpus-sync-plan-v1.schema.json`, including its archive identity, checkpoint manifest, ordered upserts, and explicit deletions. The plan is **metadata only**; receiving it must not modify committed Chatarium state.
4. Pass the exact plan to `mirrarium corpus export-sync-fetch`. Stream to an isolated staging location; verify process exit status, expected IDs and order, record count, v1 schema, and each `record_sha256` by hashing the original compact JSONL line **after removing the single specified hash member**. Never hash a parsed/reformatted JSON object. If any fetch or check fails, discard the entire staged import and preserve the old checkpoint.
5. In one durable Chatarium-side commit, apply all verified upserts under the bound producer's namespace, apply the plan's explicit deletions **only** to records owned by that same producer and known in its previous checkpoint, and persist the new checkpoint. Never delete a manually authored conversation, a record from another Mirrarium archive, or evidence from a different import mechanism.
6. A successful deletion-only plan has `upserts: []` and can have a nonempty deletion list. `export-sync-fetch` correctly emits zero bytes. This is success if its process exit status is zero; the consumer must still commit the deletions and checkpoint. An unchanged plan with no upserts/deletions is a valid idempotent no-op.

For smaller consumers, `export-sync` or `export-sync-negotiated` returns a single versioned source-bound transaction. The same validate/stage/commit/rollback rules apply, except the transaction includes both changed bodies and the next checkpoint in one invocation. The exit code remains authoritative even if a partial JSON prefix has already arrived.

## Semantic boundaries

- Keep raw/source capture IDs and conversation export identity. Preserve canonical ambiguity, branches, stream revision evidence, and attachment provenance; do not invent messages or completion states to fill gaps.
- Derived conversation content is observational. A snapshot does not constitute a ChatGPT account-export file, remote authentication, transport availability, or proof that a mutation was accepted by the server.
- `deleted_conversation_ids` means absent from **this Mirrarium derived generation**, not permission to erase Chatarium-local editing history or other source evidence. Prefer a source-scoped tombstone or unlink when local history must survive.
- Mirrarium's private CAS/SQLCipher encryption stops at stdout. Staging, logs, error output, and any persisted imported text are private Chatarium responsibility. Avoid plaintext scratch dumps and never log record bodies.
- A successful sync may cover a deliberately older published derived generation. Freshness must be requested explicitly and interpreted as a point-in-time check while raw capture can continue.

## Consumer acceptance tests

- Bootstrap, no-op, one changed record, one new record, and content-hash idempotence.
- A valid **deletion-only** source-bound plan: zero fetch bytes, explicit source-scoped deletion, advanced checkpoint.
- Rebuild between plan/fetch: nonzero fetch with no authoritative imported updates; retry from old checkpoint.
- Cross-archive checkpoint or plan, malformed/torn manifest, duplicate upsert IDs, corrupted record hash, truncated JSONL, unexpected record count/order, and nonzero producer exit: no state mutation.
- Interleaved Chatarium local edits and foreign-source imports: unaffected by Mirrarium deletions.
- Producer schema migration with unchanged conversation text: legitimate hash-based reimport without duplicate Chatarium identity.
- Crash during import: either old checkpoint and state survive or new checkpoint and all upserts/deletions are fully durable; never an intermediate combination.

## Ownership and readiness

Mirrarium owns the producer wire contracts, source provenance, encrypted archive, and isolated Chromium producer tests. Chatarium owns the importer, durable consumer transaction, source-scoped conversation identities, UI exposure, and local recovery semantics. The Mirrarium fixture in `tests/playwright/smoke.spec.ts` exercises the deletion-only producer behavior; it is **not** a substitute for Chatarium's independent acceptance suite.

The bridge is complete only after Chatarium implements and independently tests this contract. Until then, describe Mirrarium as **export-ready**, not as already synchronizing into Chatarium.

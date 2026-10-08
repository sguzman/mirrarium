# Capture delivery, crash recovery and durability

This document defines the active Native Messaging capture-delivery contract. It
distinguishes current guarantees from **unimplemented** crash-resumable,
exactly-once delivery. Issue [#1](https://github.com/sguzman/mirrarium/issues/1)
tracks that separate protocol milestone.

## Current safety boundary

- All capture writes originate from a ChatGPT origin-gated extension session.
  CDP setup must verify the current top-level origin before capture or replay.
- One capture ID is bound to one `chrome.runtime.Port` and therefore one native
  host process. Incomplete chunks are **never** redirected to a new process.
- The Rust writer accepts at most 128 concurrent uncommitted captures; each
  capture has a bounded response body and any request body is bounded and
  sanitized. The host rejects IDs already committed to its SQLCipher ledger
  **before** opening a new staging file.
- Capture staging uses exclusive file creation so existing entries, including
  malicious symlinks, cannot be silently truncated. Private response staging is
  encrypted; outbound private request bodies live in the writer's bounded
  in-memory state until sanitized and persisted.
- Every accepted nonterminal message yields
  `capture_message_ack {capture_id, stage, sequence}`. These acknowledgments
  mean only that the current native process accepted the message. They are
  **not** commit records.
- Only a successful `CaptureFinish` creates
  `capture_committed {capture_id}`, after the raw-ledger transaction completes.
  The ledger is configured for WAL and `synchronous=FULL`; CAS installation is
  synced before its ledger reference is committed.
- Any rejected capture write poisons that transaction. The native writer
  removes its in-flight staged response immediately and rejects later attempts
  to finish it. The extension can also send `capture_abort` **only to the
  original port**, never to a newly connected host. Abort is idempotent and
  does not produce a capture row.
- The extension bounds outstanding fragment identities at 128 per capture.
  It does not buffer private body chunks for later retransmission.

## Observable states

| State | Meaning | Allowed next action |
| --- | --- | --- |
| Started | Native writer accepted the capture start | Append ordered messages to the same port |
| Fragment acknowledged | Stage/sequence accepted by that process | Await other acknowledgments or finish |
| Final sent, unconfirmed | `postMessage` accepted the finish, not necessarily committed | Await the terminal receipt |
| Committed | Native `capture_committed` received | Forget volatile delivery state |
| Aborted or poisoned | The host discarded a failed/incomplete transaction | Never finish or replay the same staged transaction |
| Port lost / receipt timed out | Outcome unknown to the extension | Query the ledger; do **not** claim success or automatically replay content |

The local `capture_commit_probe {capture_id}` asks whether a record with that
ID is committed, returning only
`capture_commit_status {capture_id, committed}`. A positive result
reconciles a lost receipt. A negative result describes a **point-in-time
absence**; it is not an assurance that the original payload can be recovered,
that another host is not still writing, or that an incomplete body was committed.
Probes do not initialize an otherwise absent ledger.

The extension retains at most 256 UUIDs for uncertain deliveries in
**volatile service-worker memory**. It performs a bounded, best-effort
reconnection sequence (up to four delayed attempts at 0.5, 1, 2, and 4 seconds)
to query the next live native port. Worker suspension or restart can erase this
state; no plaintext browser/local-storage spool is used.

## Failure and integrity matrix

| Failure | Current behavior |
| --- | --- |
| Duplicate committed capture ID | Reject before staging; original raw evidence stays unchanged |
| Existing staging entry or symlink | Exclusive open fails; no redirected file is overwritten |
| Out-of-order, malformed or rejected fragment | Error, immediate abort, and no later successful finish for that staging transaction |
| Native host killed mid-capture | Old staging is abandoned/purged on writer restart; new traffic can use a new process |
| Native host killed after commit but before receipt | Next live host can probe the committed ID without retrieving the body |
| Intentional abort | Drop writer and staged response/request data, no ledger row |
| Abort on missing or committed ID | Report `discarded: false`; never erase a committed record |
| Excessive concurrent captures | Reject admission before staging; active captures stay intact |
| Missing, mismatched or delayed fragment acknowledgments | Do not treat those messages as durable proof |
| Missing terminal receipt | Retain an unconfirmed ID temporarily and probe; no blind retransmission |
| Unknown raw-ledger or key error | Return an error; never treat unavailable evidence as a miss that proves data loss |

## Why exactly-once replay is not yet implemented

An extension-side `postMessage` result, a fragment ACK and a terminal receipt
have different trust levels. A process may die after writing a durable capture
but before delivering a response. Conversely, a process may accept and ACK an
incomplete fragment before dying; the browser may have already discarded the
body bytes. Querying an ID cannot regenerate missing content.

An actual durable resume protocol must first specify and test:

1. **Archive binding and immutable transaction identity.** A new host session
   must prove it is writing to the expected raw archive. Reused IDs with
   conflicting metadata, lengths or hashes must be rejected.
2. **Durable sequence/commit journal.** Persist the exact accepted prefix and
   terminal state in the protected Rust storage boundary so restart can
   distinguish accepted, missing, poisoned and fully committed messages.
3. **Private-data recovery model.** Prove where any bytes required for retry
   live and how they are encrypted, bounded, redacted, authenticated and
   expired. Never introduce plaintext conversation/credential spools in the
   MV3 extension or browser storage as an implicit workaround.
4. **Idempotent replay and explicit terminal protocol.** Repeated frames must
   match the previous hash/sequence and never create duplicate ledger rows;
   restart races and mismatched body content must fail closed.
5. **Crash fault injection.** Test native kill and browser-worker termination
   before/after start, mid-fragment, after terminal WAL commit but before the
   receipt, and during retry. Verify exactly one valid committed record or an
   explicit uncommitted outcome, without silent truncation or private leakage.

Until these conditions are satisfied, the active implementation guarantees
**fail-closed incomplete delivery plus read-only reconciliation**, not
transparent recovery of an interrupted private capture.

## Existing verification surfaces

- `crates/mirrarium-store/src/lib.rs`: stage exclusivity, duplicate-ID
  prevention, maximum in-flight admission and immediate abort tests.
- `crates/mirrariumd/tests/restart.rs`: killed native host, committed capture
  preserved, abandoned capture absent, fresh writer recovery.
- `tests/playwright/native-receipt.spec.ts`: framed native protocol, exact
  fragment acknowledgments, terminal commit receipt, poisoned sequence, abort
  and read-only status checks.
- `tests/playwright/smoke.spec.ts`: isolated Chromium crash/reconnect proof,
  origin restriction, persistent capture and encrypted corpus verification.

Never run these tests against a personal browser profile. Use only disposable
Chromium profiles and synthetic ChatGPT-origin fixtures.

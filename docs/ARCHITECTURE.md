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
  -> raw stores + ledger + derived corpus
```

## Evidence model

Raw observations are evidence and should be append-only or content-addressed.

Derived records are interpretations and must be rebuildable from raw evidence.

The project keeps public site artifacts, private account material, and derived semantic data as separate storage classes.

## Replay boundary

Recording does not imply replay permission.

Every request class begins as unknown and therefore uses the network normally. Replay is an explicit policy granted only after the endpoint/resource semantics are understood and tested.

Mutations are not historical-cache candidates.

## Current implementation state

The initial scaffold proves the two process boundaries:

- an MV3 service worker can attach CDP Network instrumentation to supported ChatGPT tabs;
- the Rust daemon speaks Chromium Native Messaging framing and shares typed request/response models through `mirrarium-protocol`.

The scaffold currently forwards response metadata only. Durable storage, response-body transport, request-ledger schema, classification, and corpus construction are the next implementation layer.

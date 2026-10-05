# QA contract

## Canonical browser

Automated QA uses **Playwright's Chromium**.

Microsoft Edge is not an automated test browser for this repository.

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

The integration fixture runs locally but is resolved inside Chromium as `https://chatgpt.com`. This exercises the production origin gate without granting Mirrarium access to arbitrary localhost pages.

The fixture deliberately includes:

- versioned static CSS and JavaScript;
- a private ChatGPT-shaped JSON read;
- two distinct private URLs returning identical bytes.

That proves both classification and privacy-scoped deduplication.

## Live-site tests

The standard suite does not require OpenAI credentials or a real ChatGPT account.

Any future live-site suite must be separately named, opt-in, and still use an isolated Chromium profile.

## Codex contract

Codex is expected to iterate against the Playwright/Chromium harness and deterministic fixtures. It should be able to break, reset, and recreate that environment freely.

If a test requires touching the personal Edge environment, the test design is wrong.

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

## Isolation

Every browser automation run gets a disposable Chromium user-data directory.

The standard Playwright suite must be able to run against deterministic local fixtures without OpenAI credentials.

Any future live-site suite must be separately named, opt-in, and still use an isolated Chromium profile.

## Codex contract

Codex is expected to iterate against the Playwright/Chromium harness and local fixture servers. It should be able to break, reset, and recreate that environment freely.

If a test requires touching the personal Edge environment, the test design is wrong.

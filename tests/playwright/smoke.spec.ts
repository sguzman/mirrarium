import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { chromium, expect, test } from "@playwright/test";

test("loads Mirrarium only in an isolated Playwright Chromium profile", async () => {
  const userDataDir = await mkdtemp(join(tmpdir(), "mirrarium-playwright-"));
  const extensionPath = resolve("extension/dist");

  try {
    const context = await chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: [
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`,
      ],
    });

    try {
      const page = await context.newPage();
      await page.goto("http://127.0.0.1:43117/");
      await expect(page).toHaveTitle("Mirrarium fixture");

      const serviceWorkers = context.serviceWorkers();
      expect(
        serviceWorkers.some((worker) => worker.url().startsWith("chrome-extension://")),
      ).toBe(true);
    } finally {
      await context.close();
    }
  } finally {
    await rm(userDataDir, { recursive: true, force: true });
  }
});

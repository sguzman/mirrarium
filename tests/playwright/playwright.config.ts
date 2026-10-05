import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  use: {
    browserName: "chromium",
  },
  webServer: {
    command: "node tests/fixtures/server.mjs",
    url: "http://127.0.0.1:43117/health",
    reuseExistingServer: false,
  },
});

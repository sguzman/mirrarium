import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "*.spec.ts",
  fullyParallel: false,
  workers: 1,
  reporter: "line",
  webServer: {
    command: "node tests/fixtures/server.mjs",
    url: "http://127.0.0.1:43118/health",
    reuseExistingServer: false,
  },
});

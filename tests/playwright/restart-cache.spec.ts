import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";

import { chromium, expect, test } from "@playwright/test";

const execFileAsync = promisify(execFile);
const fixtureOrigin = "https://chatgpt.com:43117";
const extensionId = "oodcefibmdmabgepkcpanjpjolnbignk";

type PrivateRead = {
  url: string;
  latest_etag?: string;
  revalidation_candidate: boolean;
};

type OriginCounts = { total: number; conditional_304: number };
type ReplayCounts = { js: number };

test("persists and reuses captured conversations across Chromium restarts", async () => {
  test.setTimeout(180_000);

  const root = await mkdtemp(join(tmpdir(), "mirrarium-browser-restart-"));
  const browserHome = join(root, "home");
  const profile = join(root, "chromium-profile");
  const dataDir = join(root, "data");
  const extensionDir = join(root, "installed-extension");
  const cli = resolve("target/debug/mirrarium");
  const daemon = resolve("target/debug/mirrariumd");
  const env = {
    ...process.env,
    HOME: browserHome,
    XDG_CONFIG_HOME: join(browserHome, ".config"),
    XDG_DATA_HOME: join(browserHome, ".local", "share"),
    MIRRARIUM_DATA_DIR: dataDir,
    MIRRARIUM_PRIVATE_KEY_FILE: join(root, "private.key"),
    MIRRARIUM_EXTENSION_DIR: extensionDir,
    MIRRARIUM_EXTENSION_STATE_FILE: join(root, "extension-install.json"),
    MIRRARIUM_BROWSER_USER_DATA_DIR: profile,
  };

  const runCli = async (args: string[]) => {
    const result = await execFileAsync(cli, args, { env });
    return result.stdout;
  };
  const privateReads = async (): Promise<PrivateRead[]> =>
    JSON.parse(await runCli(["cache", "private-reads", "50"])) as PrivateRead[];
  const originCounts = async (): Promise<OriginCounts> =>
    (await fetch("http://127.0.0.1:43118/private-revalidation-counts").then((r) => r.json())) as OriginCounts;
  const replayCounts = async (): Promise<ReplayCounts> =>
    (await fetch("http://127.0.0.1:43118/replay-counts").then((r) => r.json())) as ReplayCounts;
  type Doctor = {
    installation_ready: boolean;
    live_browser_verified: boolean;
    extension: { installed: boolean; valid: boolean; error: string | null };
    native_host: { installed: boolean; valid: boolean; error: string | null };
    archive: {
      present: boolean;
      readable: boolean;
      captures: number | null;
      private_captures: number | null;
    };
  };
  const doctor = async (): Promise<Doctor> =>
    JSON.parse(await runCli(["doctor", "chrome-for-testing"])) as Doctor;

  const launch = () =>
    chromium.launchPersistentContext(profile, {
      channel: "chromium",
      headless: true,
      ignoreHTTPSErrors: true,
      env,
      args: [
        "--enable-unsafe-extension-debugging",
        "--disable-extensions-except=" + extensionDir,
        "--load-extension=" + extensionDir,
        "--host-resolver-rules=MAP chatgpt.com 127.0.0.1, MAP cdn.oaistatic.com 127.0.0.1",
        "--no-proxy-server",
      ],
    });

  type ChromiumContext = Awaited<ReturnType<typeof launch>>;
  let context: ChromiumContext | null = null;

  // A response is not a valid warm-cache proof unless Mirrarium has attached
  // its own CDP debugger. Playwright's CDP session is a separate connection.
  const openReadyPage = async (browser: ChromiumContext) => {
    const worker = browser.serviceWorkers()[0] ??
      (await browser.waitForEvent("serviceworker"));
    expect(worker.url()).toBe("chrome-extension://" + extensionId + "/background.js");
    const page = await browser.newPage();
    await page.goto(fixtureOrigin + "/warmup");

    await expect.poll(async () => {
      return await worker.evaluate(async (url: string) => {
        const api = (globalThis as typeof globalThis & {
          chrome: {
            tabs: { query(query: object): Promise<Array<{ id?: number; url?: string }>> };
            debugger: {
              sendCommand(
                target: { tabId: number },
                method: string,
                params?: object,
              ): Promise<unknown>;
            };
          };
        }).chrome;
        const tabs = await api.tabs.query({});
        const current = tabs.find((tab) => tab.url === url);
        if (current?.id === undefined) return false;
        try {
          await api.debugger.sendCommand({ tabId: current.id }, "Network.enable");
          return true;
        } catch {
          return false;
        }
      }, fixtureOrigin + "/warmup");
    }, { timeout: 15_000 }).toBe(true);

    return page;
  };

  try {
    await mkdir(browserHome, { recursive: true });
    await mkdir(profile, { recursive: true });
    await mkdir(dataDir, { recursive: true });
    const beforeInstall = await doctor();
    expect(beforeInstall).toMatchObject({
      installation_ready: false,
      live_browser_verified: false,
      extension: { installed: false, valid: false },
      native_host: { installed: false, valid: false },
      archive: { present: false, readable: false, captures: null },
    });
    await runCli(["extension", "install", resolve("extension/dist")]);
    await runCli(["native-host", "install", "chrome-for-testing", daemon]);
    const ready = await doctor();
    expect(ready).toMatchObject({
      installation_ready: true,
      live_browser_verified: false,
      extension: { installed: true, valid: true },
      native_host: { installed: true, valid: true },
      archive: { present: false, readable: false },
    });

    // A native-host manifest that points at another extension must never be
    // reported as ready, even if its binary is present and executable.
    const manifestFile = join(profile, "NativeMessagingHosts", "com.sguzman.mirrarium.json");
    const originalManifest = await readFile(manifestFile, "utf8");
    const poisonedManifest = JSON.parse(originalManifest) as { allowed_origins: string[] };
    poisonedManifest.allowed_origins = ["chrome-extension://another-extension/"];
    await writeFile(manifestFile, JSON.stringify(poisonedManifest));
    const mismatched = await doctor();
    expect(mismatched.installation_ready).toBe(false);
    expect(mismatched.native_host.valid).toBe(false);
    expect(mismatched.native_host.error).toContain("not restricted to the Mirrarium extension ID");
    await writeFile(manifestFile, originalManifest);
    expect((await doctor()).installation_ready).toBe(true);

    // A pre-existing but unreadable ledger must block deployment readiness;
    // doctor never creates, repairs, or truncates this file.
    const poisonedLedger = join(dataDir, "ledger.sqlite3");
    await writeFile(poisonedLedger, "not a valid SQLCipher ledger");
    const damaged = await doctor();
    expect(damaged.installation_ready).toBe(false);
    expect(damaged.archive).toMatchObject({ present: true, readable: false });
    await rm(poisonedLedger);
    expect((await doctor()).installation_ready).toBe(true);

    context = await launch();
    const firstPage = await openReadyPage(context);
    await firstPage.goto(fixtureOrigin + "/");
    await expect(firstPage).toHaveTitle("Mirrarium fixture");
    await expect.poll(() => firstPage.locator("body").getAttribute("data-ready"), {
      timeout: 15_000,
    }).toBe("yes");

    await expect.poll(async () => {
      const reads = await privateReads();
      return reads.find((entry) => entry.url.endsWith("/backend-api/conversation/test"));
    }, { timeout: 15_000 }).toMatchObject({
      revalidation_candidate: true,
    });

    const first = await firstPage.evaluate(async () => {
      const response = await fetch("/backend-api/conversation/test", { cache: "no-store", headers: { "ChatGPT-Account-Id": "fixture-account" } });
      return response.json() as Promise<{
        id: string;
        version: number;
        messages: Array<{ role: string; content: string }>;
      }>;
    });
    expect(first.id).toBe("fixture-conversation");
    expect(first.messages).toEqual([{ role: "user", content: "private corpus material" }]);

    // A normal GET conversation response larger than the old 16 MiB limit
    // must reach the browser intact and be archived through passive CDP
    // streaming, without disabling or fulfilling the original request.
    const streamedLength = await firstPage.evaluate(async () => {
      const response = await fetch("/backend-api/conversation/stream-probe", {
        cache: "no-store",
        headers: { "ChatGPT-Account-Id": "fixture-account" },
      });
      expect(response.status).toBe(200);
      return (await response.arrayBuffer()).byteLength;
    });
    expect(streamedLength).toBeGreaterThan(16 * 1024 * 1024);
    expect(streamedLength).toBeLessThan(32 * 1024 * 1024);
    await expect.poll(async () => {
      const index = JSON.parse(await runCli(["cache", "offline-index", "100"])) as
        Array<{ api_url: string; latest_body_bytes: number }>;
      return index.find((item) => item.api_url.endsWith(
        "/backend-api/conversation/stream-probe"
      ))?.latest_body_bytes;
    }, { timeout: 30_000 }).toBe(streamedLength);

    await context.close();
    context = null;

    // Verify that the archive is readable without a running Chromium process
    // or native-host writer, rather than relying on in-memory extension state.
    await runCli(["verify"]);
    const afterShutdown = await doctor();
    expect(afterShutdown).toMatchObject({
      installation_ready: true,
      live_browser_verified: false,
      archive: { present: true, readable: true },
    });
    expect(afterShutdown.archive.captures).toBeGreaterThan(0);
    expect(afterShutdown.archive.private_captures).toBeGreaterThan(0);

    // The archived conversation is available even with Chromium fully closed,
    // and the exact-body reader never contacts the fixture origin.
    const offlineOriginBefore = await originCounts();
    const archived = JSON.parse(
      await runCli(["cache", "offline-conversation",
        fixtureOrigin + "/backend-api/conversation/test"])
    ) as {
      source_url: string;
      offline: boolean;
      freshness_verified: boolean;
      body_sha256: string;
      body_bytes: number;
      response: typeof first;
    };
    expect(archived).toMatchObject({
      source_url: fixtureOrigin + "/backend-api/conversation/test",
      offline: true,
      freshness_verified: false,
      response: first,
    });
    expect(archived.body_sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(archived.body_bytes).toBeGreaterThan(0);
    expect(await originCounts()).toEqual(offlineOriginBefore);

    // The index is metadata-only, ordered by the latest observation, and
    // immediately supplies usable browser-link selectors for the offline reader.
    const listed = JSON.parse(await runCli(["cache", "offline-index", "50"])) as
      Array<{
        conversation_id: string;
        chat_url: string;
        api_url: string;
        latest_capture_id: string;
        latest_captured_at_ms: number;
        latest_body_hash: string;
        latest_body_bytes: number;
        capture_count: number;
      }>;
    const indexed = listed.find((item) => item.api_url ===
      fixtureOrigin + "/backend-api/conversation/test");
    expect(indexed).toBeDefined();
    expect(indexed).toMatchObject({
      conversation_id: "test",
      chat_url: fixtureOrigin + "/c/test",
      latest_body_hash: archived.body_sha256,
      latest_body_bytes: archived.body_bytes,
    });
    expect(indexed!.capture_count).toBeGreaterThan(0);
    const fingerprint = (indexed as typeof indexed & { account_scope_hash: string })
      .account_scope_hash;
    expect(fingerprint).toMatch(/^[0-9a-f]{64}$/);
    const gaps = JSON.parse(await runCli(["cache", "offline-gaps"])) as {
      response_capture_ceiling_bytes: number;
      observed_conversation_scopes: number;
      with_completed_snapshots: number;
      without_completed_snapshots: number;
      max_completed_snapshot_bytes: number;
    };
    expect(gaps.response_capture_ceiling_bytes).toBe(16 * 1024 * 1024);
    expect(gaps.observed_conversation_scopes).toBeGreaterThan(0);
    expect(gaps.with_completed_snapshots).toBeGreaterThan(0);
    expect(JSON.stringify(gaps)).not.toContain("private corpus material");
    expect(await originCounts()).toEqual(offlineOriginBefore);

    const accounts = JSON.parse(await runCli(["cache", "offline-accounts"])) as
      Array<{ account_scope_hash: string | null; conversation_count: number;
        capture_count: number; unscoped: boolean }>;
    expect(accounts.some((entry) =>
      entry.account_scope_hash === fingerprint &&
      entry.conversation_count >= 1 && entry.capture_count >= 1
    )).toBe(true);
    expect(JSON.stringify(accounts)).not.toContain("private corpus material");
    expect(JSON.stringify(accounts)).not.toContain("backend-api");
    expect(await originCounts()).toEqual(offlineOriginBefore);
    const restricted = JSON.parse(await runCli([
      "cache", "offline-index", "50", "--scope", fingerprint,
    ])) as Array<{ api_url: string; account_scope_hash: string }>;
    expect(restricted.some((entry) => entry.api_url === indexed?.api_url)).toBe(true);
    expect(restricted.every((entry) => entry.account_scope_hash === fingerprint)).toBe(true);
    const oneScoped = JSON.parse(await runCli([
      "cache", "offline-conversation", fixtureOrigin + "/c/test",
      "--scope", fingerprint,
    ])) as {
      selected_account_scope: string;
      response: typeof first;
    };
    expect(oneScoped.selected_account_scope).toBe(fingerprint);
    expect(oneScoped.response).toEqual(first);
    await expect(runCli([
      "cache", "offline-conversation", fixtureOrigin + "/c/test",
      "--scope", "f".repeat(64),
    ])).rejects.toThrow("no complete, locally archived conversation JSON");

    expect(JSON.parse(await runCli([
      "cache", "offline-index", "50", "--scope", "f".repeat(64),
    ]))).toEqual([]);
    expect(await originCounts()).toEqual(offlineOriginBefore);
    // The read-only index must never contain private response message text.
    expect(JSON.stringify(listed)).not.toContain("private corpus material");
    expect(JSON.stringify(listed)).not.toContain('"messages"');
    expect(JSON.parse(await runCli(["cache", "offline-index", "1"]))).toHaveLength(1);
    await expect(runCli(["cache", "offline-index", "0"]))
      .rejects.toThrow("offline list limit must be between 1 and 1000");

    const exportedHtmlPath = join(root, "selected-conversation.html");
    const exportedSummary = JSON.parse(await runCli([
      "cache", "offline-html", fixtureOrigin + "/c/test", exportedHtmlPath,
    ])) as {
      written: boolean;
      messages: number;
      plaintext_private_file: boolean;
      offline: boolean;
      freshness_verified: boolean;
    };
    expect(exportedSummary).toMatchObject({
      written: true,
      messages: 1,
      plaintext_private_file: true,
      offline: true,
      freshness_verified: false,
    });
    const offlineHtml = await readFile(exportedHtmlPath, "utf8");
    expect(offlineHtml).toContain("private corpus material");
    expect(offlineHtml).toContain("script-src 'none'");
    expect(offlineHtml).not.toContain("<script>");
    const scopedOneHtmlPath = join(root, "scoped-selected-conversation.html");
    const scopedOneHtml = JSON.parse(await runCli([
      "cache", "offline-html", fixtureOrigin + "/c/test",
      scopedOneHtmlPath, "--scope", fingerprint,
    ])) as { written: boolean; selected_account_scope: string };
    expect(scopedOneHtml).toMatchObject({
      written: true,
      selected_account_scope: fingerprint,
    });
    expect(await readFile(scopedOneHtmlPath, "utf8"))
      .toContain("private corpus material");
    await expect(runCli([
      "cache", "offline-html", fixtureOrigin + "/c/test",
      join(root, "other-account-only.html"), "--scope", "f".repeat(64),
    ])).rejects.toThrow("no complete, locally archived conversation JSON");

    await expect(runCli([
      "cache", "offline-html", fixtureOrigin + "/c/test", exportedHtmlPath,
    ])).rejects.toThrow("creating new private offline page");
    expect(await originCounts()).toEqual(offlineOriginBefore);

    // Explicit batch export: new owner-private directory with an offline
    // index whose links resolve to file:// pages, not ChatGPT network loads.
    const libraryPath = join(root, "offline-library");
    const libraryReport = JSON.parse(await runCli([
      "cache", "offline-library", libraryPath, "20",
    ])) as {
      written: boolean;
      conversations: number;
      text_messages: number;
      plaintext_private_files: boolean;
    };
    expect(libraryReport).toMatchObject({
      written: true,
      plaintext_private_files: true,
      offline: true,
      freshness_verified: false,
    });
    expect(libraryReport.conversations).toBeGreaterThanOrEqual(1);
    expect(libraryReport.text_messages).toBeGreaterThan(0);
    expect((libraryReport as typeof libraryReport & { skipped_unreadable: number }).skipped_unreadable).toBe(0);
    const scopedPath = join(root, "single-account-offline-library");
    const scopedReport = JSON.parse(await runCli([
      "cache", "offline-library", scopedPath, "20", "--scope", fingerprint,
    ])) as { conversations: number; selected_account_scope: string };
    expect(scopedReport.selected_account_scope).toBe(fingerprint);
    expect(scopedReport.conversations).toBeGreaterThan(0);
    const scopedHtml = await readFile(join(scopedPath, "index.html"), "utf8");
    expect(scopedHtml).toContain("Private fixture");
    await expect(runCli([
      "cache", "offline-library", join(root, "missing-account-export"),
      "20", "--scope", "f".repeat(64),
    ])).rejects.toThrow("no archived conversation snapshots found");
    const libraryHtml = await readFile(join(libraryPath, "index.html"), "utf8");
    expect(libraryHtml).toContain("Mirrarium offline library");
    expect(libraryHtml).toContain("Private fixture");
    expect(libraryHtml).not.toContain("private corpus material");
    await expect(runCli([
      "cache", "offline-library", libraryPath, "20",
    ])).rejects.toThrow("creating new private offline library");
    expect(await originCounts()).toEqual(offlineOriginBefore);

    // Deliberately corrupt only a copied archive. The normal source archive
    // remains intact for subsequent revalidation and restart assertions.
    const corruptArchive = join(root, "corrupted-archive-copy");
    await cp(dataDir, corruptArchive, { recursive: true });
    const hash = archived.body_sha256;
    await writeFile(
      join(corruptArchive, "private", "objects", hash.slice(0, 2), hash),
      Buffer.from("corrupt copied private object"),
    );
    const partialDirectory = join(root, "partial-offline-library");
    const partialSummary = JSON.parse((await execFileAsync(cli, [
      "cache", "offline-library", partialDirectory, "20",
    ], { env: { ...env, MIRRARIUM_DATA_DIR: corruptArchive } })).stdout) as {
      conversations: number;
      skipped_unreadable: number;
      written: boolean;
    };
    expect(partialSummary.written).toBe(true);
    expect(partialSummary.skipped_unreadable).toBeGreaterThanOrEqual(1);
    expect(partialSummary.conversations).toBeGreaterThanOrEqual(1);
    const partialIndex = await readFile(join(partialDirectory, "index.html"), "utf8");
    expect(partialIndex).not.toContain("Private fixture");
    expect(await originCounts()).toEqual(offlineOriginBefore);

    const byChatLink = JSON.parse(
      await runCli(["cache", "offline-conversation", fixtureOrigin + "/c/test"])
    ) as typeof archived;
    expect(byChatLink).toEqual(archived);
    expect(await originCounts()).toEqual(offlineOriginBefore);

    // Restrict the explicit reader to exact ChatGPT conversation API URLs.
    await expect(
      runCli(["cache", "offline-conversation", fixtureOrigin + "/backend-api/auth/session"])
    ).rejects.toThrow("no complete, locally archived conversation JSON");
    const persisted = (await privateReads()).find((entry) =>
      entry.url.endsWith("/backend-api/conversation/test")
    );
    expect(persisted).toMatchObject({
      revalidation_candidate: true,
      latest_etag: '"fixture-conversation-v' + first.version + '"',
    });

    const requestsBeforeRestart = await originCounts();
    const staticBeforeRestart = await replayCounts();

    context = await launch();
    const reopenedPage = await openReadyPage(context);
    const offlinePage = await context.newPage();
    await offlinePage.goto(pathToFileURL(exportedHtmlPath).href);
    await expect(offlinePage.locator("h1")).toHaveText("Private fixture");
    await expect(offlinePage.locator("article")).toHaveCount(1);
    await expect(offlinePage.locator("article .content")).toContainText(
      "private corpus material"
    );
    await offlinePage.close();
    const libraryPage = await context.newPage();
    await libraryPage.goto(pathToFileURL(join(libraryPath, "index.html")).href);
    await expect(libraryPage.getByRole("heading", { name: "Mirrarium offline library" }))
      .toBeVisible();
    await libraryPage.getByRole("link", { name: "Private fixture" }).click();
    await expect(libraryPage.locator("h1")).toHaveText("Private fixture");
    await expect(libraryPage.locator("article .content"))
      .toContainText("private corpus material");
    await libraryPage.close();
    const session = await context.newCDPSession(reopenedPage);
    try {
      await session.send("Network.enable");
      await session.send("Network.setCacheDisabled", { cacheDisabled: true });

      // Normal private reads still contact the origin. Only an origin 304 may
      // authorize substituting the verified local body for a 200 response.
      const reused = await reopenedPage.evaluate(async () => {
        const response = await fetch("/backend-api/conversation/test", { cache: "no-store", headers: { "ChatGPT-Account-Id": "fixture-account" } });
        return {
          status: response.status,
          marker: response.headers.get("x-mirrarium-revalidated"),
          origin304: response.headers.get("x-mirrarium-origin-304"),
          body: await response.json(),
        };
      });
      expect(reused).toMatchObject({
        status: 200,
        marker: "hit",
        origin304: "preserved",
        body: first,
      });
      const requestsAfterRestart = await originCounts();
      expect(requestsAfterRestart.total).toBe(requestsBeforeRestart.total + 1);
      expect(requestsAfterRestart.conditional_304).toBe(
        requestsBeforeRestart.conditional_304 + 1
      );

      // Disabling Chromium's own disk cache makes this a Mirrarium replay
      // assertion, rather than accidentally accepting the browser HTTP cache.
      const scriptReplay = reopenedPage.waitForResponse(
        (response) =>
          response.url().endsWith("/_next/static/app.js") &&
          response.headers()["x-mirrarium-cache"] === "hit",
        { timeout: 15_000 },
      );
      const [, script] = await Promise.all([
        reopenedPage.goto(fixtureOrigin + "/replay-probe"),
        scriptReplay,
      ]);
      expect(script.status()).toBe(200);
      const staticAfterRestart = await replayCounts();
      expect(staticAfterRestart.js).toBe(staticBeforeRestart.js);
    } finally {
      await session.detach();
    }
  } finally {
    if (context) await context.close();
    await rm(root, { recursive: true, force: true });
  }
});

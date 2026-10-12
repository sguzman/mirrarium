import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
  test.setTimeout(120_000);

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
    await runCli(["extension", "install", resolve("extension/dist")]);
    await runCli(["native-host", "install", "chrome-for-testing", daemon]);

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
      const response = await fetch("/backend-api/conversation/test", { cache: "no-store" });
      return response.json() as Promise<{
        id: string;
        version: number;
        messages: Array<{ role: string; content: string }>;
      }>;
    });
    expect(first.id).toBe("fixture-conversation");
    expect(first.messages).toEqual([{ role: "user", content: "private corpus material" }]);

    await context.close();
    context = null;

    // Verify that the archive is readable without a running Chromium process
    // or native-host writer, rather than relying on in-memory extension state.
    await runCli(["verify"]);
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
    const session = await context.newCDPSession(reopenedPage);
    try {
      await session.send("Network.enable");
      await session.send("Network.setCacheDisabled", { cacheDisabled: true });

      // Normal private reads still contact the origin. Only an origin 304 may
      // authorize substituting the verified local body for a 200 response.
      const reused = await reopenedPage.evaluate(async () => {
        const response = await fetch("/backend-api/conversation/test", { cache: "no-store" });
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

import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import { chromium, expect, test } from "@playwright/test";

const execFileAsync = promisify(execFile);
const nativeHostName = "com.sguzman.mirrarium";
const expectedExtensionId = "oodcefibmdmabgepkcpanjpjolnbignk";

type StoreStats = {
  captures: number;
  public_captures: number;
  private_captures: number;
  unknown_captures: number;
  objects: number;
  public_objects: number;
  private_objects: number;
  unknown_objects: number;
  body_errors: number;
  request_bodies: number;
  request_body_bytes: number;
  request_body_errors: number;
  suppressed_request_bodies: number;
};

async function readFilesRecursively(directory: string): Promise<Buffer[]> {
  const files: Buffer[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await readFilesRecursively(path)));
    } else if (entry.isFile()) {
      files.push(await readFile(path));
    }
  }
  return files;
}

test("captures ChatGPT-shaped traffic into isolated durable storage", async () => {
  const root = await mkdtemp(join(tmpdir(), "mirrarium-e2e-"));
  const browserHome = join(root, "home");
  const userDataDir = join(root, "chromium-profile");
  const dataDir = join(root, "data");
  const privateKeyFile = join(root, "private.key");
  const installedExtensionPath = join(root, "installed-extension");
  const extensionStateFile = join(root, "extension-install.json");
  const extensionRuntimeStateFile = join(root, "extension-runtime.json");
  const childEnv = {
    ...process.env,
    MIRRARIUM_PRIVATE_KEY_FILE: privateKeyFile,
    MIRRARIUM_EXTENSION_DIR: installedExtensionPath,
    MIRRARIUM_EXTENSION_STATE_FILE: extensionStateFile,
  };
  const extensionSourcePath = resolve("extension/dist");
  const daemonPath = resolve("target/debug/mirrariumd");
  const cliPath = resolve("target/debug/mirrarium");

  async function readStats(): Promise<StoreStats> {
    const { stdout } = await execFileAsync(daemonPath, ["--stats"], {
      env: {
        ...childEnv,
        MIRRARIUM_DATA_DIR: dataDir,
      },
    });
    return JSON.parse(stdout) as StoreStats;
  }

  async function readRunningExtensionBuildId(): Promise<string | null> {
    try {
      const state = JSON.parse(
        await readFile(extensionRuntimeStateFile, "utf8"),
      ) as {
        schema_version?: number;
        extension_id?: string;
        build_id?: string;
      };
      if (
        state.schema_version !== 1 ||
        state.extension_id !== expectedExtensionId ||
        typeof state.build_id !== "string"
      ) {
        return null;
      }
      return state.build_id;
    } catch {
      return null;
    }
  }

  try {
    await mkdir(userDataDir, { recursive: true });
    await mkdir(dataDir, { recursive: true });

    const { stdout: extensionInstallStdout } = await execFileAsync(
      cliPath,
      ["extension", "install", extensionSourcePath],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_DATA_HOME: join(browserHome, ".local", "share"),
        },
      },
    );
    const extensionInstall = JSON.parse(extensionInstallStdout) as {
      source_path: string;
      install_path: string;
      extension_id: string;
      manifest_version: number;
      version: string;
      build_id: string;
    };
    expect(extensionInstall).toMatchObject({
      install_path: installedExtensionPath,
      extension_id: expectedExtensionId,
      manifest_version: 3,
      version: "0.1.0",
    });
    expect(extensionInstall.build_id).toMatch(/^0\.1\.0\+[0-9a-f]{16}$/);
    expect(extensionInstall.source_path).toBe(extensionSourcePath);

    const { stdout: extensionStatusStdout } = await execFileAsync(
      cliPath,
      ["extension", "status"],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_DATA_HOME: join(browserHome, ".local", "share"),
        },
      },
    );
    const extensionStatus = JSON.parse(extensionStatusStdout) as {
      install_path: string;
      installed: boolean;
      valid: boolean;
      version: string;
      build_id: string;
      extension_id: string;
    };
    expect(extensionStatus).toMatchObject({
      install_path: installedExtensionPath,
      installed: true,
      valid: true,
      version: "0.1.0",
      build_id: extensionInstall.build_id,
      extension_id: expectedExtensionId,
    });

    const { stdout: nativeInstallStdout } = await execFileAsync(
      cliPath,
      ["native-host", "install", "chrome-for-testing", daemonPath],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_CONFIG_HOME: join(browserHome, ".config"),
          MIRRARIUM_BROWSER_USER_DATA_DIR: userDataDir,
        },
      },
    );
    const nativeInstall = JSON.parse(nativeInstallStdout) as {
      browser: string;
      manifest_path: string;
      host_path: string;
      extension_id: string;
    };
    expect(nativeInstall).toMatchObject({
      browser: "chrome-for-testing",
      host_path: daemonPath,
      extension_id: expectedExtensionId,
    });
    expect(nativeInstall.manifest_path).toBe(
      join(userDataDir, "NativeMessagingHosts", `${nativeHostName}.json`),
    );

    const context = await chromium.launchPersistentContext(userDataDir, {
      channel: "chromium",
      headless: true,
      ignoreHTTPSErrors: true,
      env: {
        ...childEnv,
        HOME: browserHome,
        XDG_CONFIG_HOME: join(browserHome, ".config"),
        MIRRARIUM_DATA_DIR: dataDir,
      },
      args: [
        "--enable-unsafe-extension-debugging",
        `--disable-extensions-except=${installedExtensionPath}`,
        `--load-extension=${installedExtensionPath}`,
        "--host-resolver-rules=MAP chatgpt.com 127.0.0.1, MAP cdn.oaistatic.com 127.0.0.1",
        "--no-proxy-server",
      ],
    });

    try {
      const workers = context.serviceWorkers();
      const worker = workers[0] ?? (await context.waitForEvent("serviceworker"));
      expect(worker.url()).toBe(
        `chrome-extension://${expectedExtensionId}/background.js`,
      );

      const nativePing = await worker.evaluate(async (hostName) => {
        const chromeApi = (globalThis as typeof globalThis & {
          chrome: {
            runtime: {
              lastError?: { message?: string };
              sendNativeMessage(
                name: string,
                message: unknown,
                callback: (response: unknown) => void,
              ): void;
            };
          };
        }).chrome;

        return await new Promise<unknown>((resolve, reject) => {
          chromeApi.runtime.sendNativeMessage(hostName, { type: "ping" }, (response) => {
            const error = chromeApi.runtime.lastError;
            if (error) {
              reject(new Error(error.message ?? "native messaging failed"));
              return;
            }
            resolve(response);
          });
        });
      }, nativeHostName);
      expect(nativePing).toEqual({ type: "pong" });

      const page = await context.newPage();

      // Warm up the ChatGPT origin so the extension can attach CDP before the
      // page whose requests we actually assert.
      await page.goto("https://chatgpt.com:43117/warmup");
      await page.waitForTimeout(500);

      await page.goto("https://chatgpt.com:43117/");
      await expect(page).toHaveTitle("Mirrarium fixture");
      await expect
        .poll(() => page.locator("body").getAttribute("data-ready"))
        .toBe("yes");

      await expect
        .poll(async () => (await readStats()).captures, { timeout: 10_000 })
        .toBeGreaterThanOrEqual(5);

      await expect
        .poll(async () => (await readStats()).request_bodies, { timeout: 10_000 })
        .toBeGreaterThanOrEqual(1);

      await expect
        .poll(async () => (await readStats()).private_captures, { timeout: 10_000 })
        .toBeGreaterThanOrEqual(4);
      await expect
        .poll(async () => (await readStats()).private_objects, { timeout: 10_000 })
        .toBeGreaterThanOrEqual(3);

      const stats = await readStats();
      expect(stats.public_objects).toBeGreaterThanOrEqual(2);
      expect(stats.private_objects).toBeLessThan(stats.private_captures + stats.request_bodies);
      expect(stats.request_body_errors).toBe(0);

      await page.evaluate(async () => {
        await document.fonts.ready;
      });

      await expect
        .poll(
          async () => {
            const { stdout } = await execFileAsync(cliPath, ["cache", "stats"], {
              env: {
                ...childEnv,
                MIRRARIUM_DATA_DIR: dataDir,
              },
            });
            return (JSON.parse(stdout) as { eligible_urls: number }).eligible_urls;
          },
          { timeout: 10_000 },
        )
        .toBeGreaterThanOrEqual(4);

      const { stdout: cacheCandidatesStdout } = await execFileAsync(
        cliPath,
        ["cache", "candidates", "50"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const cacheCandidates = JSON.parse(cacheCandidatesStdout) as Array<{
        url: string;
        resource_type: string;
        body_hash: string;
        cache_control?: string;
        capture_count: number;
        distinct_body_hashes: number;
        eligible: boolean;
        reasons: string[];
      }>;
      for (const suffix of [
        "/_next/static/app.js",
        "/_next/static/app.css",
        "/_next/static/pixel.svg",
        "/_next/static/fixture.woff2",
        "/assets/cdn-app.js",
      ]) {
        const candidate = cacheCandidates.find((item) => item.url.endsWith(suffix));
        expect(candidate, `missing cache candidate for ${suffix}`).toBeTruthy();
        expect(candidate?.eligible).toBe(true);
        expect(candidate?.distinct_body_hashes).toBe(1);
        expect(candidate?.body_hash).toMatch(/^[0-9a-f]{64}$/);
        expect(candidate?.cache_control?.toLowerCase()).toContain("immutable");
        expect(candidate?.reasons).toEqual([]);
      }

      const { stdout: publicCoverageStdout } = await execFileAsync(
        cliPath,
        ["cache", "public-coverage"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const publicCoverage = JSON.parse(publicCoverageStdout) as Array<{
        host: string;
        observed_captures: number;
        unique_urls: number;
        unique_body_bytes: number;
        eligible_urls: number;
        eligible_body_bytes: number;
        replay_supported_urls: number;
        replay_supported_body_bytes: number;
        expansion_candidate_urls: number;
        expansion_candidate_body_bytes: number;
      }>;

      const chatgptPublicCoverage = publicCoverage.find(
        (item) => item.host === "chatgpt.com",
      );
      expect(chatgptPublicCoverage).toBeTruthy();
      expect(chatgptPublicCoverage?.replay_supported_urls).toBeGreaterThanOrEqual(4);
      expect(chatgptPublicCoverage?.replay_supported_body_bytes).toBeGreaterThan(0);
      expect(chatgptPublicCoverage?.expansion_candidate_urls).toBe(0);
      expect(chatgptPublicCoverage?.expansion_candidate_body_bytes).toBe(0);

      const cdnPublicCoverage = publicCoverage.find(
        (item) => item.host === "cdn.oaistatic.com",
      );
      expect(cdnPublicCoverage).toBeTruthy();
      expect(cdnPublicCoverage?.replay_supported_urls).toBeGreaterThanOrEqual(1);
      expect(cdnPublicCoverage?.replay_supported_body_bytes).toBeGreaterThan(0);
      expect(cdnPublicCoverage?.expansion_candidate_urls).toBe(0);
      expect(cdnPublicCoverage?.expansion_candidate_body_bytes).toBe(0);

      const { stdout: privateReadsStdout } = await execFileAsync(
        cliPath,
        ["cache", "private-reads", "50"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const privateReads = JSON.parse(privateReadsStdout) as Array<{
        url: string;
        mime_type: string;
        capture_count: number;
        distinct_body_hashes: number;
        latest_etag?: string;
        latest_cache_control?: string;
        has_validator: boolean;
        stable_so_far: boolean;
        revalidation_candidate: boolean;
        reasons: string[];
      }>;
      const fixtureQueryRead = privateReads.find((item) =>
        item.url.includes("/backend-api/conversations?offset=0&limit=2"),
      );
      expect(fixtureQueryRead).toBeTruthy();
      expect(fixtureQueryRead).toMatchObject({
        mime_type: "application/json",
        has_validator: true,
        stable_so_far: true,
        revalidation_candidate: true,
        latest_etag: "\"fixture-conversations-page-v1\"",
        reasons: [],
      });

      const fixtureConversationRead = privateReads.find((item) =>
        item.url.includes("/backend-api/conversation/test"),
      );
      expect(fixtureConversationRead).toBeTruthy();
      expect(fixtureConversationRead).toMatchObject({
        mime_type: "application/json",
        has_validator: true,
        stable_so_far: true,
        revalidation_candidate: true,
        latest_etag: "\"fixture-conversation-v1\"",
        reasons: [],
      });

      const fixtureDocumentRead = privateReads.find(
        (item) => item.url === "https://chatgpt.com:43117/",
      );
      expect(fixtureDocumentRead).toBeTruthy();
      expect(fixtureDocumentRead).toMatchObject({
        mime_type: "text/html",
        has_validator: true,
        stable_so_far: true,
        revalidation_candidate: true,
        latest_etag: "\"fixture-document-v1\"",
        reasons: [],
      });

      const documentCountsBefore = (await fetch(
        "http://127.0.0.1:43118/private-document-counts",
      ).then((response) => response.json())) as {
        total: number;
        conditional_304: number;
      };
      const documentSession = await context.newCDPSession(page);
      try {
        await documentSession.send("Network.enable");
        await documentSession.send("Network.setCacheDisabled", {
          cacheDisabled: true,
        });
        const reloaded = await page.reload({ waitUntil: "load" });
        expect(reloaded?.status()).toBe(200);
        expect(reloaded?.headers()["x-mirrarium-revalidated"]).toBe("hit");
        expect(reloaded?.headers()["x-mirrarium-origin-304"]).toBe("preserved");
        await expect(page).toHaveTitle("Mirrarium fixture");
        await expect
          .poll(() => page.locator("body").getAttribute("data-ready"))
          .toBe("yes");
      } finally {
        await documentSession.detach();
      }

      const documentCountsAfter = (await fetch(
        "http://127.0.0.1:43118/private-document-counts",
      ).then((response) => response.json())) as {
        total: number;
        conditional_304: number;
      };
      expect(documentCountsAfter.total).toBe(documentCountsBefore.total + 1);
      expect(documentCountsAfter.conditional_304).toBe(
        documentCountsBefore.conditional_304 + 1,
      );

      const { stdout: privateCoverageStdout } = await execFileAsync(
        cliPath,
        ["cache", "private-coverage"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const privateCoverage = JSON.parse(privateCoverageStdout) as Array<{
        resource_type: string;
        mime_type: string;
        capture_count: number;
        unique_urls: number;
        body_bytes: number;
        validator_captures: number;
        validator_body_bytes: number;
        no_store_captures: number;
        current_policy_captures: number;
        current_policy_body_bytes: number;
        expansion_candidate_captures: number;
        expansion_candidate_body_bytes: number;
      }>;

      const jsonCoverage = privateCoverage.find(
        (item) =>
          item.resource_type.toLowerCase() === "fetch" &&
          item.mime_type.toLowerCase().includes("json"),
      );
      expect(jsonCoverage).toBeTruthy();
      expect(jsonCoverage?.capture_count).toBeGreaterThanOrEqual(1);
      expect(jsonCoverage?.validator_captures).toBeGreaterThanOrEqual(1);
      expect(jsonCoverage?.current_policy_captures).toBeGreaterThanOrEqual(1);
      expect(jsonCoverage?.current_policy_body_bytes).toBeGreaterThan(0);

      const htmlCoverage = privateCoverage.find(
        (item) =>
          item.resource_type.toLowerCase() === "document" &&
          item.mime_type.toLowerCase().startsWith("text/html"),
      );
      expect(htmlCoverage).toBeTruthy();
      expect(htmlCoverage?.validator_captures).toBeGreaterThanOrEqual(1);
      expect(htmlCoverage?.current_policy_captures).toBeGreaterThanOrEqual(1);
      expect(htmlCoverage?.current_policy_body_bytes).toBeGreaterThan(0);

      const plainCoverage = privateCoverage.find(
        (item) =>
          item.resource_type.toLowerCase() === "fetch" &&
          item.mime_type.toLowerCase() === "text/plain",
      );
      expect(plainCoverage).toBeTruthy();
      expect(plainCoverage?.capture_count).toBeGreaterThanOrEqual(1);
      expect(plainCoverage?.validator_captures).toBe(0);
      expect(plainCoverage?.current_policy_captures).toBe(0);
      expect(plainCoverage?.expansion_candidate_captures).toBe(0);

      const replayCountsBefore = (await fetch(
        "http://127.0.0.1:43118/replay-counts",
      ).then((response) => response.json())) as {
        css: number;
        js: number;
        image: number;
        font: number;
        stress_js: number;
        cdn_js: number;
      };
      expect(replayCountsBefore.css).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.js).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.image).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.font).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.stress_js).toBeGreaterThanOrEqual(16);
      expect(replayCountsBefore.cdn_js).toBeGreaterThanOrEqual(1);

      const replaySession = await context.newCDPSession(page);
      try {
        await replaySession.send("Network.enable");
        await replaySession.send("Network.setCacheDisabled", {
          cacheDisabled: true,
        });

        const privateQueryCountsBefore = (await fetch(
          "http://127.0.0.1:43118/private-query-counts",
        ).then((response) => response.json())) as {
          total: number;
          conditional_304: number;
        };

        const queryRevalidation = await page.evaluate(async () => {
          const response = await fetch(
            "/backend-api/conversations?offset=0&limit=2",
            { cache: "no-store" },
          );
          return {
            status: response.status,
            marker: response.headers.get("x-mirrarium-revalidated"),
            origin304: response.headers.get("x-mirrarium-origin-304"),
            body: await response.json(),
          };
        });
        expect(queryRevalidation).toEqual({
          status: 200,
          marker: "hit",
          origin304: "preserved",
          body: {
            items: ["conversation-a", "conversation-b"],
            offset: 0,
            limit: 2,
          },
        });

        const privateQueryCountsAfter = (await fetch(
          "http://127.0.0.1:43118/private-query-counts",
        ).then((response) => response.json())) as {
          total: number;
          conditional_304: number;
        };
        expect(privateQueryCountsAfter.total).toBe(
          privateQueryCountsBefore.total + 1,
        );
        expect(privateQueryCountsAfter.conditional_304).toBe(
          privateQueryCountsBefore.conditional_304 + 1,
        );

        const privateCountsBefore = (await fetch(
          "http://127.0.0.1:43118/private-revalidation-counts",
        ).then((response) => response.json())) as {
          total: number;
          conditional_304: number;
        };

        const privateRevalidation = await page.evaluate(async () => {
          const response = await fetch("/backend-api/conversation/test", {
            cache: "no-store",
          });
          return {
            status: response.status,
            marker: response.headers.get("x-mirrarium-revalidated"),
            origin304: response.headers.get("x-mirrarium-origin-304"),
            body: await response.json(),
          };
        });
        expect(privateRevalidation).toEqual({
          status: 200,
          marker: "hit",
          origin304: "preserved",
          body: {
            id: "fixture-conversation",
            title: "Private fixture",
            version: 1,
            messages: [{ role: "user", content: "private corpus material" }],
          },
        });

        type CaptureDiagnostic = {
          captured_at_ms: number;
          method: string;
          url: string;
          status: number;
          resource_type: string;
          body_hash?: string;
          body_bytes: number;
          body_error?: string;
        };
        const readConversationCaptureDiagnostics = async (): Promise<
          CaptureDiagnostic[]
        > => {
          const { stdout } = await execFileAsync(
            cliPath,
            ["captures", "200"],
            {
              env: {
                ...childEnv,
                MIRRARIUM_DATA_DIR: dataDir,
              },
            },
          );
          return (JSON.parse(stdout) as CaptureDiagnostic[]).filter(
            (capture) =>
              capture.method === "GET" &&
              capture.url.includes("/backend-api/conversation/test"),
          );
        };
        const conversationCapturesBeforeRefresh =
          await readConversationCaptureDiagnostics();
        const successfulHashesBeforeRefresh = new Set(
          conversationCapturesBeforeRefresh
            .filter((capture) => !capture.body_error && capture.body_hash)
            .map((capture) => capture.body_hash as string),
        );

        const bumped = (await fetch(
          "http://127.0.0.1:43118/private-bump",
        ).then((response) => response.json())) as { version: number };
        expect(bumped.version).toBe(2);

        const refreshedPrivateRead = await page.evaluate(async () => {
          const response = await fetch("/backend-api/conversation/test", {
            cache: "no-store",
          });
          return {
            status: response.status,
            marker: response.headers.get("x-mirrarium-revalidated"),
            origin304: response.headers.get("x-mirrarium-origin-304"),
            body: await response.json(),
          };
        });
        expect(refreshedPrivateRead).toEqual({
          status: 200,
          marker: null,
          origin304: null,
          body: {
            id: "fixture-conversation",
            title: "Private fixture",
            version: 2,
            messages: [{ role: "user", content: "private corpus material" }],
          },
        });

        await expect
          .poll(
            async () => {
              const captures = await readConversationCaptureDiagnostics();
              return captures.length;
            },
            { timeout: 10_000 },
          )
          .toBeGreaterThan(conversationCapturesBeforeRefresh.length);

        const conversationCapturesAfterRefresh =
          await readConversationCaptureDiagnostics();
        const refreshedCapture =
          conversationCapturesAfterRefresh.find(
            (capture) =>
              !capture.body_error &&
              !!capture.body_hash &&
              !successfulHashesBeforeRefresh.has(capture.body_hash),
          ) ?? conversationCapturesAfterRefresh[0];
        expect(refreshedCapture).toMatchObject({
          status: 200,
          body_error: null,
        });
        expect(refreshedCapture.body_hash).toBeTruthy();
        expect(
          successfulHashesBeforeRefresh.has(refreshedCapture.body_hash as string),
        ).toBe(false);

        await expect
          .poll(
            async () => {
              const { stdout } = await execFileAsync(
                cliPath,
                ["cache", "private-reads", "50"],
                {
                  env: {
                    ...childEnv,
                    MIRRARIUM_DATA_DIR: dataDir,
                  },
                },
              );
              const items = JSON.parse(stdout) as Array<{
                url: string;
                latest_etag?: string;
                distinct_body_hashes: number;
              }>;
              return items.find((item) =>
                item.url.includes("/backend-api/conversation/test"),
              );
            },
            { timeout: 10_000 },
          )
          .toMatchObject({
            latest_etag: "\"fixture-conversation-v2\"",
            distinct_body_hashes: 2,
          });

        const secondRevalidation = await page.evaluate(async () => {
          const response = await fetch("/backend-api/conversation/test", {
            cache: "no-store",
          });
          return {
            status: response.status,
            marker: response.headers.get("x-mirrarium-revalidated"),
            origin304: response.headers.get("x-mirrarium-origin-304"),
            body: await response.json(),
          };
        });
        expect(secondRevalidation).toEqual({
          status: 200,
          marker: "hit",
          origin304: "preserved",
          body: {
            id: "fixture-conversation",
            title: "Private fixture",
            version: 2,
            messages: [{ role: "user", content: "private corpus material" }],
          },
        });

        const privateCountsAfter = (await fetch(
          "http://127.0.0.1:43118/private-revalidation-counts",
        ).then((response) => response.json())) as {
          total: number;
          conditional_304: number;
        };
        expect(privateCountsAfter.total).toBe(privateCountsBefore.total + 3);
        expect(privateCountsAfter.conditional_304).toBe(
          privateCountsBefore.conditional_304 + 2,
        );

        const scriptHit = page.waitForResponse(
          (response) =>
            response.url().endsWith("/_next/static/app.js") &&
            response.headers()["x-mirrarium-cache"] === "hit",
          { timeout: 10_000 },
        );
        const stylesheetHit = page.waitForResponse(
          (response) =>
            response.url().endsWith("/_next/static/app.css") &&
            response.headers()["x-mirrarium-cache"] === "hit",
          { timeout: 10_000 },
        );
        const imageHit = page.waitForResponse(
          (response) =>
            response.url().endsWith("/_next/static/pixel.svg") &&
            response.headers()["x-mirrarium-cache"] === "hit",
          { timeout: 10_000 },
        );
        const fontHit = page.waitForResponse(
          (response) =>
            response.url().endsWith("/_next/static/fixture.woff2") &&
            response.headers()["x-mirrarium-cache"] === "hit",
          { timeout: 10_000 },
        );

        const cdnScriptHit = page.waitForResponse(
          (response) =>
            response.url().includes("cdn.oaistatic.com:43117/assets/cdn-app.js") &&
            response.headers()["x-mirrarium-cache"] === "hit",
          { timeout: 10_000 },
        );

        const [
          ,
          scriptResponse,
          stylesheetResponse,
          imageResponse,
          fontResponse,
          cdnScriptResponse,
        ] = await Promise.all([
          page.goto("https://chatgpt.com:43117/replay-probe"),
          scriptHit,
          stylesheetHit,
          imageHit,
          fontHit,
          cdnScriptHit,
        ]);
        expect(scriptResponse.status()).toBe(200);
        expect(stylesheetResponse.status()).toBe(200);
        expect(imageResponse.status()).toBe(200);
        expect(fontResponse.status()).toBe(200);
        expect(cdnScriptResponse.status()).toBe(200);

        await page.goto("https://chatgpt.com:43117/replay-stress-probe");
        await page.waitForFunction(
          () =>
            document.body.dataset.ready === "yes" &&
            (globalThis as typeof globalThis & {
              __mirrariumStressLoaded?: number;
            }).__mirrariumStressLoaded === 16,
          undefined,
          { timeout: 10_000 },
        );
      } finally {
        await replaySession.detach();
      }

      const replayCountsAfter = (await fetch(
        "http://127.0.0.1:43118/replay-counts",
      ).then((response) => response.json())) as {
        css: number;
        js: number;
        image: number;
        font: number;
        stress_js: number;
        cdn_js: number;
      };
      expect(replayCountsAfter).toEqual(replayCountsBefore);

      type ReplayStats = {
        attempts: number;
        hits: number;
        misses: number;
        lookup_errors: number;
        timeouts: number;
        fulfill_errors: number;
        replayed_bytes: number;
      };
      const readReplayStats = async (): Promise<ReplayStats> => {
        const { stdout } = await execFileAsync(
          cliPath,
          ["cache", "replay-stats"],
          {
            env: {
              ...childEnv,
              MIRRARIUM_DATA_DIR: dataDir,
            },
          },
        );
        return JSON.parse(stdout) as ReplayStats;
      };

      await expect
        .poll(async () => (await readReplayStats()).hits, { timeout: 10_000 })
        .toBeGreaterThanOrEqual(5);
      const replayStats = await readReplayStats();
      expect(replayStats.hits).toBeGreaterThanOrEqual(21);
      expect(replayStats.misses).toBeGreaterThanOrEqual(1);
      expect(replayStats.replayed_bytes).toBeGreaterThan(0);
      expect(replayStats.lookup_errors).toBe(0);
      expect(replayStats.timeouts).toBe(0);
      expect(replayStats.fulfill_errors).toBe(0);
      expect(replayStats.attempts).toBe(
        replayStats.hits +
          replayStats.misses +
          replayStats.lookup_errors +
          replayStats.timeouts +
          replayStats.fulfill_errors,
      );

      type RevalidationStats = {
        attempts: number;
        not_modified: number;
        refreshed: number;
        fulfill_errors: number;
        saved_body_bytes: number;
      };
      const readRevalidationStats = async (): Promise<RevalidationStats> => {
        const { stdout } = await execFileAsync(
          cliPath,
          ["cache", "revalidation-stats"],
          {
            env: {
              ...childEnv,
              MIRRARIUM_DATA_DIR: dataDir,
            },
          },
        );
        return JSON.parse(stdout) as RevalidationStats;
      };

      await expect
        .poll(async () => (await readRevalidationStats()).attempts, {
          timeout: 10_000,
        })
        .toBeGreaterThanOrEqual(4);
      const revalidationStats = await readRevalidationStats();
      expect(revalidationStats.not_modified).toBeGreaterThanOrEqual(3);
      expect(revalidationStats.refreshed).toBeGreaterThanOrEqual(1);
      expect(revalidationStats.fulfill_errors).toBe(0);
      expect(revalidationStats.saved_body_bytes).toBeGreaterThan(0);

      const { stdout: opportunitiesStdout } = await execFileAsync(
        cliPath,
        ["cache", "opportunities"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const opportunities = JSON.parse(opportunitiesStdout) as {
        public_observed_captures: number;
        public_unique_urls: number;
        public_eligible_body_bytes: number;
        public_replay_supported_body_bytes: number;
        public_expansion_candidate_body_bytes: number;
        private_observed_captures: number;
        private_observed_body_bytes: number;
        private_validator_body_bytes: number;
        private_current_policy_body_bytes: number;
        private_expansion_candidate_body_bytes: number;
        top_public_expansion: {
          host: string;
          candidate_urls: number;
          body_bytes: number;
          blocking_gate: string;
        } | null;
        top_private_expansion: {
          resource_type: string;
          mime_type: string;
          candidate_captures: number;
          unique_urls: number;
          body_bytes: number;
          blocking_gate: string;
        } | null;
        runtime_public_replayed_bytes: number;
        runtime_private_revalidated_saved_body_bytes: number;
        runtime_total_saved_body_bytes: number;
      };
      expect(opportunities.public_replay_supported_body_bytes).toBeGreaterThan(0);
      expect(opportunities.private_current_policy_body_bytes).toBeGreaterThan(0);
      expect(opportunities.top_public_expansion).toBeNull();
      expect(opportunities.top_private_expansion).toBeNull();
      expect(opportunities.runtime_public_replayed_bytes).toBe(
        replayStats.replayed_bytes,
      );
      expect(opportunities.runtime_private_revalidated_saved_body_bytes).toBe(
        revalidationStats.saved_body_bytes,
      );
      expect(opportunities.runtime_total_saved_body_bytes).toBe(
        replayStats.replayed_bytes + revalidationStats.saved_body_bytes,
      );

      const websocketRoundTrip = await page.evaluate(
        () =>
          new Promise<{
            textMessages: number;
            binaryMessages: number;
            jsonMessage?: string;
          }>((resolve, reject) => {
            const socket = new WebSocket(
              "wss://chatgpt.com:43117/backend-api/ws-fixture?token=fixture-ws-query-secret&keep=yes",
            );
            socket.binaryType = "arraybuffer";
            let textMessages = 0;
            let binaryMessages = 0;
            let jsonMessage: string | undefined;
            const timeout = setTimeout(() => {
              socket.close();
              reject(new Error("websocket fixture timed out"));
            }, 5_000);
            socket.addEventListener("open", () => {
              socket.send(
                JSON.stringify({
                  message: "client websocket fixture",
                  access_token: "fixture-ws-client-secret",
                }),
              );
            });
            socket.addEventListener("message", (event) => {
              if (typeof event.data === "string") {
                textMessages += 1;
                if (event.data.startsWith("{")) jsonMessage = event.data;
              } else {
                binaryMessages += 1;
              }
              if (textMessages + binaryMessages === 3) {
                clearTimeout(timeout);
                socket.close();
                resolve({ textMessages, binaryMessages, jsonMessage });
              }
            });
            socket.addEventListener("error", () => {
              clearTimeout(timeout);
              reject(new Error("websocket fixture failed"));
            });
          }),
      );
      expect(websocketRoundTrip).toMatchObject({
        textMessages: 2,
        binaryMessages: 1,
      });
      expect(JSON.parse(websocketRoundTrip.jsonMessage ?? "{}")).toMatchObject({
        message: "server websocket fixture",
      });

      const { stdout: capturesStdout } = await execFileAsync(
        cliPath,
        ["captures", "100"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const captures = JSON.parse(capturesStdout) as Array<{
        method: string;
        url: string;
        status: number;
        mime_type: string;
        resource_type: string;
        request_body_kind?: string;
        request_body_content_type?: string;
        request_body_has_post_data?: boolean;
        request_body_post_data_entry_count?: number;
        request_body_declared_content_length?: number;
        request_body_hash?: string;
        request_body_bytes: number;
        request_body_error?: string;
        body_hash?: string;
        body_bytes: number;
        body_error?: string;
        provenance: {
          lifecycle_id?: string;
          redirect_hop?: number;
          redirected_from_url?: string;
          initiator_type?: string;
          request_wall_time_ms?: number;
          response_protocol?: string;
          request_headers: Record<string, string>;
          response_headers: Record<string, string>;
        };
      }>;
      const postCapture = captures.find(
        (capture) =>
          capture.method === "POST" &&
          capture.url.includes("/backend-api/conversation/post"),
      );
      expect(postCapture).toBeTruthy();
      expect(postCapture?.provenance.initiator_type).toBe("script");
      expect(postCapture?.provenance.request_wall_time_ms).toBeGreaterThan(0);
      expect(postCapture?.provenance.response_protocol).toBeTruthy();

      const requestHeaders = Object.fromEntries(
        Object.entries(postCapture?.provenance.request_headers ?? {}).map(
          ([key, value]) => [key.toLowerCase(), value],
        ),
      );
      const responseHeaders = Object.fromEntries(
        Object.entries(postCapture?.provenance.response_headers ?? {}).map(
          ([key, value]) => [key.toLowerCase(), value],
        ),
      );
      expect(requestHeaders.authorization).toBe("[REDACTED]");
      expect(requestHeaders["x-mirrarium-fixture"]).toBe("preserve-me");
      expect(responseHeaders["x-mirrarium-response"]).toBe("preserve-me-too");

      const redirectStart = captures.find(
        (capture) =>
          capture.status === 302 &&
          capture.url.includes("/backend-api/redirect-start"),
      );
      const redirectFinal = captures.find((capture) =>
        capture.url.includes("/backend-api/redirect-final"),
      );
      expect(redirectStart).toBeTruthy();
      expect(redirectFinal).toBeTruthy();
      expect(redirectStart?.provenance.lifecycle_id).toBeTruthy();
      expect(redirectFinal?.provenance.lifecycle_id).toBe(
        redirectStart?.provenance.lifecycle_id,
      );
      expect(redirectStart?.provenance.redirect_hop).toBe(0);
      expect(redirectFinal?.provenance.redirect_hop).toBe(1);
      expect(redirectFinal?.provenance.redirected_from_url).toContain(
        "/backend-api/redirect-start",
      );
      expect(redirectFinal?.url).not.toContain("fixture-redirect-secret");
      const redirectHeaders = Object.fromEntries(
        Object.entries(redirectStart?.provenance.response_headers ?? {}).map(
          ([key, value]) => [key.toLowerCase(), value],
        ),
      );
      expect(redirectHeaders.location).not.toContain("fixture-redirect-secret");

      const redirect307Start = captures.find(
        (capture) =>
          capture.status === 307 &&
          capture.method === "POST" &&
          capture.url.includes("/backend-api/redirect-post-start"),
      );
      const redirect307Final = captures.find(
        (capture) =>
          capture.status === 200 &&
          capture.method === "POST" &&
          capture.url.includes("/backend-api/redirect-post-final"),
      );
      expect(redirect307Start).toBeTruthy();
      expect(redirect307Final).toBeTruthy();
      expect(redirect307Final?.provenance.lifecycle_id).toBe(
        redirect307Start?.provenance.lifecycle_id,
      );
      expect(redirect307Start?.provenance.redirect_hop).toBe(0);
      expect(redirect307Final?.provenance.redirect_hop).toBe(1);
      expect(redirect307Final?.provenance.redirected_from_url).toContain(
        "/backend-api/redirect-post-start",
      );
      expect(redirect307Final?.url).not.toContain("fixture-redirect-307-secret");
      expect(redirect307Start?.request_body_hash).toBeTruthy();
      expect(redirect307Final?.request_body_hash).toBe(
        redirect307Start?.request_body_hash,
      );
      expect(redirect307Start?.request_body_bytes).toBeGreaterThan(0);
      expect(redirect307Final?.request_body_bytes).toBeGreaterThan(0);
      expect(redirect307Start?.request_body_error).toBeFalsy();
      expect(redirect307Final?.request_body_error).toBeFalsy();

      const redirect303Start = captures.find(
        (capture) =>
          capture.status === 303 &&
          capture.method === "POST" &&
          capture.url.includes("/backend-api/redirect-see-other-start"),
      );
      const redirect303Final = captures.find(
        (capture) =>
          capture.status === 200 &&
          capture.method === "GET" &&
          capture.url.includes("/backend-api/redirect-see-other-final"),
      );
      expect(redirect303Start).toBeTruthy();
      expect(redirect303Final).toBeTruthy();
      expect(redirect303Final?.provenance.lifecycle_id).toBe(
        redirect303Start?.provenance.lifecycle_id,
      );
      expect(redirect303Start?.provenance.redirect_hop).toBe(0);
      expect(redirect303Final?.provenance.redirect_hop).toBe(1);
      expect(redirect303Start?.request_body_hash).toBeTruthy();
      expect(redirect303Start?.request_body_bytes).toBeGreaterThan(0);
      expect(redirect303Start?.request_body_error).toBeFalsy();
      expect(redirect303Final?.request_body_hash).toBeFalsy();
      expect(redirect303Final?.request_body_bytes).toBe(0);
      expect(redirect303Final?.request_body_error).toBeFalsy();
      expect(redirect303Final?.url).not.toContain("fixture-redirect-303-secret");

      for (const [urlFragment, method, status] of [
        ["/backend-api/no-content-fixture", "GET", 204],
        ["/backend-api/head-fixture", "HEAD", 200],
      ] as const) {
        const noBodyCapture = captures.find(
          (capture) =>
            capture.url.includes(urlFragment) &&
            capture.method === method &&
            capture.status === status,
        );
        expect(noBodyCapture, `missing no-body capture for ${urlFragment}`).toBeTruthy();
        expect(noBodyCapture?.body_hash).toBeFalsy();
        expect(noBodyCapture?.body_bytes).toBe(0);
        expect(noBodyCapture?.body_error).toBe(
          "suppressed:no_response_body_expected",
        );
      }

      const websocketSent = captures.find(
        (capture) =>
          capture.method === "WS_SEND" &&
          capture.resource_type === "WebSocketFrame" &&
          capture.url.includes("/backend-api/ws-fixture"),
      );
      const websocketReceived = captures.filter(
        (capture) =>
          capture.method === "WS_RECV" &&
          capture.resource_type === "WebSocketFrame" &&
          capture.url.includes("/backend-api/ws-fixture"),
      );
      const websocketReceivedJson = websocketReceived.find(
        (capture) => capture.mime_type === "application/json",
      );
      const websocketReceivedPlain = websocketReceived.find(
        (capture) => capture.mime_type === "text/plain; charset=utf-8",
      );
      const websocketReceivedBinary = websocketReceived.find(
        (capture) => capture.mime_type === "application/octet-stream",
      );
      expect(websocketSent).toBeTruthy();
      expect(websocketReceived).toHaveLength(3);
      expect(websocketReceivedJson).toBeTruthy();
      expect(websocketReceivedPlain).toBeTruthy();
      expect(websocketReceivedBinary).toBeTruthy();
      expect(websocketSent?.status).toBe(101);
      expect(websocketReceivedJson?.status).toBe(101);
      expect(websocketSent?.mime_type).toBe("application/json");
      expect(websocketSent?.body_hash).toBeTruthy();
      expect(websocketReceivedJson?.body_hash).toBeTruthy();
      expect(websocketSent?.body_bytes).toBeGreaterThan(0);
      expect(websocketReceivedJson?.body_bytes).toBeGreaterThan(0);
      expect(websocketSent?.body_error).toBeFalsy();
      expect(websocketReceivedJson?.body_error).toBeFalsy();
      expect(websocketReceivedPlain?.body_hash).toBeFalsy();
      expect(websocketReceivedPlain?.body_bytes).toBe(0);
      expect(websocketReceivedPlain?.body_error).toBe(
        "suppressed:unparseable_websocket_text_frame",
      );
      expect(websocketReceivedBinary?.body_hash).toBeFalsy();
      expect(websocketReceivedBinary?.body_bytes).toBe(0);
      expect(websocketReceivedBinary?.body_error).toBe(
        "suppressed:websocket_binary_frame_not_archived",
      );
      expect(websocketSent?.provenance.lifecycle_id).toBeTruthy();
      for (const received of websocketReceived) {
        expect(received.provenance.lifecycle_id).toBe(
          websocketSent?.provenance.lifecycle_id,
        );
        expect(received.url).not.toContain("fixture-ws-query-secret");
      }
      expect(websocketSent?.provenance.response_protocol).toBe("websocket");
      expect(websocketSent?.url).not.toContain("fixture-ws-query-secret");
      expect(websocketSent?.url).toContain("keep=yes");

      const { stdout: websocketStatsStdout } = await execFileAsync(
        cliPath,
        ["stats"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const websocketStats = JSON.parse(websocketStatsStdout) as {
        websocket_frames: number;
        websocket_frame_body_bytes: number;
        websocket_frame_errors: number;
        suppressed_websocket_frames: number;
      };
      expect(websocketStats.websocket_frames).toBe(4);
      expect(websocketStats.websocket_frame_body_bytes).toBeGreaterThan(0);
      expect(websocketStats.websocket_frame_errors).toBe(0);
      expect(websocketStats.suppressed_websocket_frames).toBe(2);

      const abortedCapture = captures.find(
        (capture) =>
          capture.method === "POST" &&
          capture.url.includes("/backend-api/abort-fixture"),
      );
      expect(abortedCapture).toBeTruthy();
      expect(abortedCapture?.status).toBe(200);
      expect(abortedCapture?.request_body_kind).toBe("json");
      expect(abortedCapture?.request_body_hash).toBeTruthy();
      expect(abortedCapture?.request_body_bytes).toBeGreaterThan(0);
      expect(abortedCapture?.request_body_error).toBeFalsy();
      expect(abortedCapture?.body_hash).toBeFalsy();
      expect(abortedCapture?.body_bytes).toBe(0);
      expect(abortedCapture?.body_error).toBeTruthy();
      expect(abortedCapture?.body_error).not.toContain("suppressed:");
      expect(
        Object.fromEntries(
          Object.entries(abortedCapture?.provenance.response_headers ?? {}).map(
            ([key, value]) => [key.toLowerCase(), value],
          ),
        )["x-mirrarium-abort"],
      ).toBe("fixture");

      const uploadCapture = captures.find((capture) =>
        capture.url.includes("/backend-api/upload-fixture"),
      );
      expect(uploadCapture).toBeTruthy();
      expect(uploadCapture?.request_body_kind).toBe("multipart");
      expect(uploadCapture?.request_body_has_post_data).toBe(true);
      expect(uploadCapture?.request_body_content_type).toContain("multipart/form-data");
      expect(uploadCapture?.request_body_hash).toBeFalsy();
      expect(uploadCapture?.request_body_bytes).toBe(0);
      expect(uploadCapture?.request_body_error).toContain(
        "multipart_request_body_not_archived",
      );

      const privateObjects = await readFilesRecursively(join(dataDir, "private", "objects"));
      expect(privateObjects.length).toBeGreaterThanOrEqual(1);
      const privateObjectVersions = privateObjects.map((body) =>
        body.subarray(0, 8).toString("ascii"),
      );
      expect(
        privateObjectVersions.every(
          (version) => version === "MIRRPV01" || version === "MIRRPV02",
        ),
      ).toBe(true);
      expect(privateObjectVersions).toContain("MIRRPV02");
      for (const plaintext of [
        "hello from request body",
        "fixture-secret-token",
        "fixture-download-secret",
        "private corpus material",
      ]) {
        const needle = Buffer.from(plaintext, "utf8");
        expect(
          privateObjects.some((body) => body.includes(needle)),
          `private CAS leaked plaintext: ${plaintext}`,
        ).toBe(false);
      }

      const { stdout: privacyStatusStdout } = await execFileAsync(
        cliPath,
        ["privacy", "status"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const privacyStatus = JSON.parse(privacyStatusStdout) as {
        key_path: string;
        key_exists: boolean;
        ledger_exists: boolean;
        ledger_encrypted: boolean;
        ledger_plaintext_legacy: boolean;
        private_objects: number;
        encrypted_private_objects: number;
        legacy_plaintext_private_objects: number;
        missing_or_invalid_private_objects: number;
        migration_needed: boolean;
      };
      expect(privacyStatus.key_exists).toBe(true);
      expect(privacyStatus.ledger_exists).toBe(true);
      expect(privacyStatus.ledger_encrypted).toBe(true);
      expect(privacyStatus.ledger_plaintext_legacy).toBe(false);
      const ledgerBytes = await readFile(join(dataDir, "ledger.sqlite3"));
      expect(ledgerBytes.subarray(0, 16).toString("ascii")).not.toBe(
        "SQLite format 3\u0000",
      );
      expect(privacyStatus.private_objects).toBeGreaterThanOrEqual(1);
      expect(privacyStatus.encrypted_private_objects).toBe(
        privacyStatus.private_objects,
      );
      expect(privacyStatus.legacy_plaintext_private_objects).toBe(0);
      expect(privacyStatus.missing_or_invalid_private_objects).toBe(0);
      expect(privacyStatus.migration_needed).toBe(false);
      expect(privacyStatus.key_path).toBe(privateKeyFile);

      const { stdout: privacyMigrationStdout } = await execFileAsync(
        cliPath,
        ["privacy", "migrate"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const privacyMigration = JSON.parse(privacyMigrationStdout) as {
        migrated_objects: number;
        migrated_body_bytes: number;
        already_encrypted_objects: number;
        ledger_migrated: boolean;
        ledger_already_encrypted: boolean;
        ledger_plaintext_bytes: number;
      };
      expect(privacyMigration.migrated_objects).toBe(0);
      expect(privacyMigration.migrated_body_bytes).toBe(0);
      expect(privacyMigration.already_encrypted_objects).toBe(
        privacyStatus.private_objects,
      );
      expect(privacyMigration.ledger_migrated).toBe(false);
      expect(privacyMigration.ledger_already_encrypted).toBe(true);
      expect(privacyMigration.ledger_plaintext_bytes).toBe(0);

      const { stdout: corpusStdout } = await execFileAsync(
        cliPath,
        ["corpus", "rebuild"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusDatabaseBytes = await readFile(
        join(dataDir, "derived", "corpus.sqlite3"),
      );
      expect(
        corpusDatabaseBytes.subarray(0, 16).toString("ascii"),
      ).not.toBe("SQLite format 3\u0000");
      const derivedFiles = await readFilesRecursively(join(dataDir, "derived"));
      for (const plaintext of [
        "private corpus material",
        "fixture-conversation",
        "hello world",
      ]) {
        const needle = Buffer.from(plaintext, "utf8");
        expect(
          derivedFiles.some((body) => body.includes(needle)),
          `derived corpus leaked plaintext: ${plaintext}`,
        ).toBe(false);
      }

      const corpusStats = JSON.parse(corpusStdout) as {
        stream_captures: number;
        stream_events: number;
        json_stream_events: number;
        stream_message_revisions: number;
        conversation_snapshots: number;
        message_observations: number;
        stream_reconstructions: number;
        attachment_observations: number;
        attachment_downloads: number;
      };
      expect(corpusStats.stream_captures).toBeGreaterThanOrEqual(1);
      expect(corpusStats.stream_events).toBeGreaterThanOrEqual(3);
      expect(corpusStats.json_stream_events).toBeGreaterThanOrEqual(2);
      expect(corpusStats.stream_message_revisions).toBeGreaterThanOrEqual(2);
      expect(corpusStats.conversation_snapshots).toBeGreaterThanOrEqual(1);
      expect(corpusStats.message_observations).toBeGreaterThanOrEqual(1);
      expect(corpusStats.stream_reconstructions).toBeGreaterThanOrEqual(1);
      expect(corpusStats.attachment_observations).toBeGreaterThanOrEqual(1);
      expect(corpusStats.attachment_downloads).toBeGreaterThanOrEqual(1);

      const { stdout: attachmentsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "attachments", "fixture-attachment-conversation", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const attachments = JSON.parse(attachmentsStdout) as Array<{
        observation: {
          conversation_id?: string;
          message_id?: string;
          attachment_id?: string;
          file_name?: string;
          mime_type?: string;
          size_bytes?: number;
          sanitized_url?: string;
        };
        downloads: Array<{
          source_url: string;
          mime_type: string;
          body_hash: string;
          body_bytes: number;
        }>;
      }>;
      const attachment = attachments.find(
        (item) => item.observation.attachment_id === "file-123",
      );
      expect(attachment?.observation).toMatchObject({
        conversation_id: "fixture-attachment-conversation",
        message_id: "attachment-message",
        attachment_id: "file-123",
        file_name: "fixture-attachment.txt",
        mime_type: "text/plain",
        size_bytes: 24,
      });
      expect(attachment?.observation.sanitized_url).toContain(
        "/backend-api/files/file-123/download",
      );
      expect(attachment?.observation.sanitized_url).toContain("keep=yes");
      expect(attachment?.observation.sanitized_url).not.toContain(
        "fixture-download-secret",
      );
      expect(attachment?.downloads.length).toBeGreaterThanOrEqual(1);
      expect(
        attachment?.downloads.every(
          (download) =>
            download.mime_type === "text/plain" &&
            !download.source_url.includes("fixture-download-secret") &&
            download.body_hash.length === 64 &&
            download.body_bytes > 0,
        ),
      ).toBe(true);
      expect(
        new Set(attachment?.downloads.map((download) => download.body_hash)).size,
      ).toBe(1);

      const { stdout: conversationsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversations", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const conversations = JSON.parse(conversationsStdout) as Array<{
        conversation_id: string;
        title?: string;
        snapshot_count: number;
        message_observation_count: number;
        stream_reconstruction_count: number;
      }>;
      const fixtureConversation = conversations.find(
        (conversation) =>
          conversation.conversation_id === "fixture-conversation",
      );
      const fixtureStream = conversations.find(
        (conversation) => conversation.conversation_id === "fixture-stream",
      );
      expect(fixtureConversation?.title).toBe("Private fixture");
      expect(fixtureConversation?.snapshot_count).toBeGreaterThanOrEqual(4);
      expect(fixtureConversation?.message_observation_count).toBeGreaterThanOrEqual(4);
      expect(fixtureStream?.stream_reconstruction_count).toBeGreaterThanOrEqual(1);

      const { stdout: conversationStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversation", "fixture-conversation"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const conversation = JSON.parse(conversationStdout) as {
        summary: { conversation_id: string; title?: string };
        messages: Array<{
          role?: string;
          content_text?: string;
          source_kind: string;
        }>;
      };
      expect(conversation.summary).toMatchObject({
        conversation_id: "fixture-conversation",
        title: "Private fixture",
      });
      expect(conversation.messages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            role: "user",
            content_text: "private corpus material",
            source_kind: "json_snapshot",
          }),
        ]),
      );

      const { stdout: branchedEvidenceStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversation", "fixture-branched"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const branchedEvidence = JSON.parse(branchedEvidenceStdout) as {
        messages: Array<{ content_text?: string }>;
      };
      expect(branchedEvidence.messages).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ content_text: "discarded branch" }),
          expect.objectContaining({ content_text: "chosen branch" }),
        ]),
      );

      const { stdout: canonicalStdout } = await execFileAsync(
        cliPath,
        ["corpus", "canonical", "fixture-branched"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const canonical = JSON.parse(canonicalStdout) as {
        conversation_id: string;
        basis_kind: string;
        current_node?: string;
        messages: Array<{
          message_id?: string;
          parent_id?: string;
          role?: string;
          content_text?: string;
        }>;
        warnings: string[];
      };
      expect(canonical).toMatchObject({
        conversation_id: "fixture-branched",
        basis_kind: "mapping_current_node",
        current_node: "assistant-b",
      });
      expect(canonical.warnings).toEqual([]);
      expect(canonical.messages.map((message) => message.content_text)).toEqual([
        "question",
        "chosen branch",
      ]);
      expect(canonical.messages.map((message) => message.content_text)).not.toContain(
        "discarded branch",
      );

      const { stdout: streamRevisionsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "stream-revisions", "fixture-stream-tail", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streamRevisions = JSON.parse(streamRevisionsStdout) as Array<{
        capture_id: string;
        sequence: number;
        message_id: string;
        parent_id?: string;
        role?: string;
        content_text?: string;
      }>;
      expect(streamRevisions.length).toBeGreaterThanOrEqual(2);
      expect(
        streamRevisions.every(
          (revision) =>
            revision.message_id === "stream-tail-assistant" &&
            revision.parent_id === "stream-tail-user" &&
            revision.role === "assistant",
        ),
      ).toBe(true);

      const revisionsByCapture = new Map<
        string,
        Array<{ sequence: number; content_text?: string }>
      >();
      for (const revision of streamRevisions) {
        const revisions = revisionsByCapture.get(revision.capture_id) ?? [];
        revisions.push({
          sequence: revision.sequence,
          content_text: revision.content_text,
        });
        revisionsByCapture.set(revision.capture_id, revisions);
      }
      expect(revisionsByCapture.size).toBeGreaterThanOrEqual(1);
      for (const revisions of revisionsByCapture.values()) {
        revisions.sort((left, right) => left.sequence - right.sequence);
        expect(revisions.map((revision) => revision.content_text)).toEqual([
          "hello",
          "hello world",
        ]);
      }

      const { stdout: streamTailCanonicalStdout } = await execFileAsync(
        cliPath,
        ["corpus", "canonical", "fixture-stream-tail"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streamTailCanonical = JSON.parse(streamTailCanonicalStdout) as {
        conversation_id: string;
        messages: Array<{
          message_id?: string;
          parent_id?: string;
          role?: string;
          content_text?: string;
        }>;
        warnings: string[];
      };
      expect(streamTailCanonical.conversation_id).toBe("fixture-stream-tail");
      expect(
        streamTailCanonical.messages.map((message) => message.content_text),
      ).toEqual(["question", "hello world"]);
      expect(streamTailCanonical.messages[1]).toMatchObject({
        message_id: "stream-tail-assistant",
        parent_id: "stream-tail-user",
        role: "assistant",
      });
      expect(streamTailCanonical.warnings.length).toBeGreaterThanOrEqual(1);
      expect(
        streamTailCanonical.warnings.every((warning) =>
          warning.includes(
            "stream message revision(s) do not follow the canonical basis snapshot",
          ),
        ),
      ).toBe(true);

      const { stdout: streamConversationStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversation", "fixture-stream"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streamConversation = JSON.parse(streamConversationStdout) as {
        streams: Array<{ text: string; fragment_count: number }>;
      };
      expect(streamConversation.streams).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            text: "hello world",
            fragment_count: 2,
          }),
        ]),
      );

      const updateSourcePath = join(root, "extension-update-source");
      await cp(extensionSourcePath, updateSourcePath, { recursive: true });
      const updateManifestPath = join(updateSourcePath, "manifest.json");
      const updateManifest = JSON.parse(
        await readFile(updateManifestPath, "utf8"),
      ) as {
        version: string;
        version_name: string;
      };
      const updatedBuildId = `${updateManifest.version}+fixture-update`;
      expect(updatedBuildId).not.toBe(extensionInstall.build_id);
      updateManifest.version_name = updatedBuildId;
      await writeFile(
        updateManifestPath,
        JSON.stringify(updateManifest, null, 2) + "\n",
      );
      const updateBackgroundPath = join(updateSourcePath, "background.js");
      await writeFile(
        updateBackgroundPath,
        (await readFile(updateBackgroundPath, "utf8")) +
          `\n// Mirrarium hot-update fixture: ${updatedBuildId}\n`,
      );

      const { stdout: updateInstallStdout } = await execFileAsync(
        cliPath,
        ["extension", "install", updateSourcePath],
        {
          env: {
            ...childEnv,
            HOME: browserHome,
            XDG_DATA_HOME: join(browserHome, ".local", "share"),
          },
        },
      );
      const updateInstall = JSON.parse(updateInstallStdout) as {
        install_path: string;
        extension_id: string;
        build_id: string;
        running_build_id: string | null;
        reload_required: boolean;
      };
      expect(updateInstall).toMatchObject({
        install_path: installedExtensionPath,
        extension_id: expectedExtensionId,
        build_id: updatedBuildId,
        running_build_id: extensionInstall.build_id,
        reload_required: true,
      });

      const workerBeforeUpdateCheck =
        context.serviceWorkers()[0] ?? (await context.waitForEvent("serviceworker"));
      const runningBuildBeforeUpdate = await workerBeforeUpdateCheck.evaluate(() => {
        const manifest = chrome.runtime.getManifest();
        return manifest.version_name ?? manifest.version;
      });
      expect(runningBuildBeforeUpdate).toBe(extensionInstall.build_id);

      const installedState = await workerBeforeUpdateCheck.evaluate(
        async (hostName) => {
          const chromeApi = (globalThis as typeof globalThis & {
            chrome: {
              runtime: {
                lastError?: { message?: string };
                sendNativeMessage(
                  name: string,
                  message: unknown,
                  callback: (response: unknown) => void,
                ): void;
              };
            };
          }).chrome;
          return await new Promise<unknown>((resolve, reject) => {
            chromeApi.runtime.sendNativeMessage(
              hostName,
              { type: "extension_install_state" },
              (response) => {
                const error = chromeApi.runtime.lastError;
                if (error) {
                  reject(new Error(error.message ?? "native install-state query failed"));
                  return;
                }
                resolve(response);
              },
            );
          });
        },
        nativeHostName,
      );
      expect(installedState).toEqual({
        type: "extension_install_state",
        build_id: updatedBuildId,
      });

      await expect
        .poll(readRunningExtensionBuildId, { timeout: 10_000 })
        .toBe(extensionInstall.build_id);

      const { stdout: staleStatusStdout } = await execFileAsync(
        cliPath,
        ["extension", "status"],
        {
          env: {
            ...childEnv,
            HOME: browserHome,
            XDG_DATA_HOME: join(browserHome, ".local", "share"),
          },
        },
      );
      expect(JSON.parse(staleStatusStdout)).toMatchObject({
        build_id: updatedBuildId,
        running_build_id: extensionInstall.build_id,
        reload_required: true,
      });

      const browser = context.browser();
      expect(browser).not.toBeNull();
      const extensionManager = await browser!.newBrowserCDPSession();
      try {
        const reloadResult = (await extensionManager.send(
          "Extensions.loadUnpacked",
          { path: installedExtensionPath },
        )) as { id: string };
        expect(reloadResult.id).toBe(expectedExtensionId);
      } finally {
        await extensionManager.detach();
      }

      await expect
        .poll(readRunningExtensionBuildId, { timeout: 10_000 })
        .toBe(updatedBuildId);

      const { stdout: freshStatusStdout } = await execFileAsync(
        cliPath,
        ["extension", "status"],
        {
          env: {
            ...childEnv,
            HOME: browserHome,
            XDG_DATA_HOME: join(browserHome, ".local", "share"),
          },
        },
      );
      expect(JSON.parse(freshStatusStdout)).toMatchObject({
        build_id: updatedBuildId,
        running_build_id: updatedBuildId,
        reload_required: false,
      });
    } finally {
      await context.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

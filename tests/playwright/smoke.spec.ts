import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

import Ajv2020 from "ajv/dist/2020.js";
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
    const installState = JSON.parse(
      await readFile(extensionStateFile, "utf8"),
    ) as { tree_hash?: string };
    expect(installState.tree_hash).toMatch(/^[0-9a-f]{64}$/);

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

    const installedBackgroundPath = join(
      installedExtensionPath,
      "background.js",
    );
    const installedBackgroundBeforeTear = await readFile(installedBackgroundPath);
    await writeFile(
      installedBackgroundPath,
      Buffer.concat([
        installedBackgroundBeforeTear,
        Buffer.from("\n// simulated torn generation\n", "utf8"),
      ]),
    );
    const { stdout: tornStatusStdout } = await execFileAsync(
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
    const tornStatus = JSON.parse(tornStatusStdout) as {
      installed: boolean;
      valid: boolean;
      build_id: string | null;
      error: string | null;
    };
    expect(tornStatus.installed).toBe(true);
    expect(tornStatus.valid).toBe(false);
    expect(tornStatus.build_id).toBeNull();
    expect(tornStatus.error).toContain(
      "installed extension tree does not match the last fully published generation",
    );

    await writeFile(installedBackgroundPath, installedBackgroundBeforeTear);
    const { stdout: restoredStatusStdout } = await execFileAsync(
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
    expect(JSON.parse(restoredStatusStdout)).toMatchObject({
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

    const { stdout: nativeStatusStdout } = await execFileAsync(
      cliPath,
      ["native-host", "status", "chrome-for-testing"],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_CONFIG_HOME: join(browserHome, ".config"),
          MIRRARIUM_BROWSER_USER_DATA_DIR: userDataDir,
        },
      },
    );
    expect(JSON.parse(nativeStatusStdout)).toEqual({
      browser: "chrome-for-testing",
      manifest_path: nativeInstall.manifest_path,
      installed: true,
    });

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
                if (event.data.startsWith("{") && jsonMessage === undefined) {
                  jsonMessage = event.data;
                }
              } else {
                binaryMessages += 1;
              }
              if (textMessages + binaryMessages === 4) {
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
        textMessages: 3,
        binaryMessages: 1,
      });
      expect(JSON.parse(websocketRoundTrip.jsonMessage ?? "{}")).toMatchObject({
        message: "server websocket fixture",
      });

      const eventSourceRoundTrip = await page.evaluate(
        () =>
          new Promise<{ delta?: string; done?: string }>((resolve, reject) => {
            const source = new EventSource(
              "https://chatgpt.com:43117/backend-api/eventsource-fixture?token=fixture-eventsource-query-secret&keep=yes",
            );
            const root = globalThis as typeof globalThis & {
              __mirrariumEventSource?: EventSource;
            };
            root.__mirrariumEventSource = source;

            let delta: string | undefined;
            let done: string | undefined;
            const timeout = setTimeout(() => {
              source.close();
              reject(new Error("eventsource fixture timed out"));
            }, 5_000);

            const maybeResolve = () => {
              if (delta === undefined || done === undefined) return;
              clearTimeout(timeout);
              resolve({ delta, done });
            };

            source.addEventListener("delta", (event) => {
              delta = (event as MessageEvent<string>).data;
              maybeResolve();
            });
            source.addEventListener("done", (event) => {
              done = (event as MessageEvent<string>).data;
              maybeResolve();
            });
            source.addEventListener("error", () => {
              clearTimeout(timeout);
              source.close();
              reject(new Error("eventsource fixture failed"));
            });
          }),
      );
      expect(JSON.parse(eventSourceRoundTrip.delta ?? "{}")).toMatchObject({
        message: "long-lived eventsource fixture",
        access_token: "fixture-eventsource-secret",
      });
      expect(eventSourceRoundTrip.done).toBe("[DONE]");

      const eventSourceReconnect = await page.evaluate(
        () =>
          new Promise<{ messages: string[] }>((resolve, reject) => {
            const source = new EventSource(
              "https://chatgpt.com:43117/backend-api/eventsource-reconnect?token=fixture-eventsource-reconnect-secret&keep=yes",
            );
            const messages: string[] = [];
            const timeout = setTimeout(() => {
              source.close();
              reject(new Error("eventsource reconnect fixture timed out"));
            }, 5_000);

            source.addEventListener("reconnect", (event) => {
              messages.push((event as MessageEvent<string>).data);
              if (messages.length === 2) {
                clearTimeout(timeout);
                source.close();
                resolve({ messages });
              }
            });
          }),
      );
      expect(eventSourceReconnect.messages.map((message) => JSON.parse(message))).toEqual([
        { leg: 1, access_token: "fixture-reconnect-secret-1" },
        { leg: 2, access_token: "fixture-reconnect-secret-2" },
      ]);

      await expect
        .poll(
          async () => {
            const { stdout } = await execFileAsync(cliPath, ["stats"], {
              env: {
                ...childEnv,
                MIRRARIUM_DATA_DIR: dataDir,
              },
            });
            return (JSON.parse(stdout) as { websocket_frames: number })
              .websocket_frames;
          },
          { timeout: 10_000 },
        )
        .toBeGreaterThanOrEqual(5);

      await expect
        .poll(
          async () => {
            const { stdout } = await execFileAsync(cliPath, ["stats"], {
              env: {
                ...childEnv,
                MIRRARIUM_DATA_DIR: dataDir,
              },
            });
            return (JSON.parse(stdout) as { eventsource_messages: number })
              .eventsource_messages;
          },
          { timeout: 10_000 },
        )
        .toBeGreaterThanOrEqual(4);

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
        capture_id: string;
        method: string;
        url: string;
        status: number;
        mime_type: string;
        resource_type: string;
        privacy_class: string;
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
          transport_sequence?: number;
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
      expect(postCapture?.capture_id).toBeTruthy();

      const { stdout: postRequestBodyStdout } = await execFileAsync(
        cliPath,
        ["capture", postCapture?.capture_id as string, "request"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const postRequestBody = JSON.parse(postRequestBodyStdout) as {
        capture_id: string;
        body_kind: string;
        storage_class: string;
        body_hash: string;
        body_bytes: number;
        mime_type?: string;
        encoding: string;
        data: string;
      };
      expect(postRequestBody).toMatchObject({
        capture_id: postCapture?.capture_id,
        body_kind: "request",
        storage_class: "private",
        body_hash: postCapture?.request_body_hash,
        body_bytes: postCapture?.request_body_bytes,
        encoding: "utf8",
      });
      expect(postRequestBody.mime_type).toContain("application/json");
      expect(postRequestBody.data).toContain("hello from request body");
      expect(postRequestBody.data).toContain("[REDACTED]");
      expect(postRequestBody.data).not.toContain("fixture-secret-token");

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
        (capture) =>
          capture.mime_type === "application/json" &&
          !capture.body_error &&
          !!capture.body_hash,
      );
      const websocketReceivedPlain = websocketReceived.find(
        (capture) => capture.mime_type === "text/plain; charset=utf-8",
      );
      const websocketReceivedBinary = websocketReceived.find(
        (capture) => capture.mime_type === "application/octet-stream",
      );
      const websocketReceivedOversized = websocketReceived.find(
        (capture) =>
          capture.body_error ===
          "suppressed:websocket_text_frame_too_large",
      );
      expect(websocketSent).toBeTruthy();
      expect(websocketReceived).toHaveLength(4);
      expect(websocketReceivedJson).toBeTruthy();
      expect(websocketReceivedPlain).toBeTruthy();
      expect(websocketReceivedBinary).toBeTruthy();
      expect(websocketReceivedOversized).toBeTruthy();
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
      expect(websocketReceivedOversized?.body_hash).toBeFalsy();
      expect(websocketReceivedOversized?.body_bytes).toBe(0);
      expect(websocketReceivedOversized?.body_error).toBe(
        "suppressed:websocket_text_frame_too_large",
      );
      expect(websocketSent?.provenance.lifecycle_id).toBeTruthy();
      expect(websocketSent?.provenance.transport_sequence).toBe(0);
      expect(
        websocketReceived
          .map((capture) => capture.provenance.transport_sequence)
          .sort((left, right) => (left ?? 0) - (right ?? 0)),
      ).toEqual([1, 2, 3, 4]);
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
      expect(websocketStats.websocket_frames).toBe(5);
      expect(websocketStats.websocket_frame_body_bytes).toBeGreaterThan(0);
      expect(websocketStats.websocket_frame_errors).toBe(0);
      expect(websocketStats.suppressed_websocket_frames).toBe(3);

      const eventSourceMessages = captures.filter(
        (capture) =>
          capture.method === "SSE_RECV" &&
          capture.resource_type === "EventSourceMessage" &&
          capture.url.includes("/backend-api/eventsource-fixture"),
      );
      expect(eventSourceMessages).toHaveLength(2);
      expect(eventSourceMessages.every((capture) => !!capture.body_hash)).toBe(true);
      expect(eventSourceMessages.every((capture) => capture.body_bytes > 0)).toBe(true);
      expect(eventSourceMessages.every((capture) => !capture.body_error)).toBe(true);
      expect(eventSourceMessages.every((capture) => capture.status === 200)).toBe(true);
      expect(
        eventSourceMessages.every(
          (capture) => capture.mime_type === "text/event-stream; charset=utf-8",
        ),
      ).toBe(true);
      expect(
        eventSourceMessages.every(
          (capture) => !capture.url.includes("fixture-eventsource-query-secret"),
        ),
      ).toBe(true);
      expect(eventSourceMessages.every((capture) => capture.url.includes("keep=yes"))).toBe(
        true,
      );
      const eventSourceLifecycle = eventSourceMessages[0]?.provenance.lifecycle_id;
      expect(eventSourceLifecycle).toBeTruthy();
      expect(
        eventSourceMessages.every(
          (capture) => capture.provenance.lifecycle_id === eventSourceLifecycle,
        ),
      ).toBe(true);
      expect(
        eventSourceMessages
          .map((capture) => capture.provenance.transport_sequence)
          .sort((left, right) => (left ?? 0) - (right ?? 0)),
      ).toEqual([0, 1]);

      const reconnectMessages = captures.filter(
        (capture) =>
          capture.method === "SSE_RECV" &&
          capture.resource_type === "EventSourceMessage" &&
          capture.url.includes("/backend-api/eventsource-reconnect"),
      );
      expect(reconnectMessages).toHaveLength(2);
      expect(
        reconnectMessages.every(
          (capture) => capture.provenance.transport_sequence === 0,
        ),
      ).toBe(true);
      expect(
        new Set(
          reconnectMessages.map((capture) => capture.provenance.lifecycle_id),
        ).size,
      ).toBe(2);
      expect(
        reconnectMessages.every(
          (capture) => !capture.url.includes("fixture-eventsource-reconnect-secret"),
        ),
      ).toBe(true);

      const reconnectSecondLeg = reconnectMessages.find((capture) => {
        const headers = Object.fromEntries(
          Object.entries(capture.provenance.request_headers ?? {}).map(
            ([key, value]) => [key.toLowerCase(), value],
          ),
        );
        return headers["last-event-id"] === "fixture-reconnect-1";
      });
      expect(reconnectSecondLeg).toBeTruthy();

      const unfinishedEventSourceResponse = captures.find(
        (capture) =>
          capture.resource_type === "EventSource" &&
          capture.url.includes("/backend-api/eventsource-fixture"),
      );
      expect(unfinishedEventSourceResponse).toBeFalsy();

      const eventSourceStats = JSON.parse(websocketStatsStdout) as {
        eventsource_messages: number;
        eventsource_message_body_bytes: number;
        eventsource_message_errors: number;
        suppressed_eventsource_messages: number;
      };
      expect(eventSourceStats.eventsource_messages).toBe(4);
      expect(eventSourceStats.eventsource_message_body_bytes).toBeGreaterThan(0);
      expect(eventSourceStats.eventsource_message_errors).toBe(0);
      expect(eventSourceStats.suppressed_eventsource_messages).toBe(0);

      await page.evaluate(() => {
        const root = globalThis as typeof globalThis & {
          __mirrariumEventSource?: EventSource;
        };
        root.__mirrariumEventSource?.close();
        delete root.__mirrariumEventSource;
      });

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

      const { stdout: maintenanceStdout } = await execFileAsync(
        cliPath,
        ["maintenance", "incoming"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const maintenance = JSON.parse(maintenanceStdout) as {
        incoming_exists: boolean;
        writer_active: boolean;
        incomplete_capture_files: number;
        inflight_capture_files: number;
        abandoned_capture_files: number;
        ledger_recovery_files: number;
        unexpected_files: number;
        unexpected_non_file_entries: number;
        cleanup_on_next_writer_start: boolean;
      };
      expect(maintenance).toMatchObject({
        incoming_exists: true,
        writer_active: true,
        incomplete_capture_files: 0,
        inflight_capture_files: 0,
        abandoned_capture_files: 0,
        ledger_recovery_files: 0,
        unexpected_files: 0,
        unexpected_non_file_entries: 0,
        cleanup_on_next_writer_start: false,
      });

      const liveStatsForVerify = await readStats();
      const { stdout: rawVerifyStdout } = await execFileAsync(
        cliPath,
        ["verify"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const rawVerify = JSON.parse(rawVerifyStdout) as {
        sqlite_integrity_ok: boolean;
        foreign_key_violations: number;
        schema_ok: boolean;
        checked_objects: number;
        corrupt_objects: number;
        unreferenced_indexed_objects: number;
        orphan_objects: number;
        orphan_object_bytes: number;
        unexpected_object_entries: number;
        checked_capture_invariants: number;
        invalid_captures: number;
        errors: string[];
      };
      expect(rawVerify).toEqual({
        sqlite_integrity_ok: true,
        foreign_key_violations: 0,
        schema_ok: true,
        checked_objects: liveStatsForVerify.objects,
        corrupt_objects: 0,
        unreferenced_indexed_objects: 0,
        orphan_objects: 0,
        orphan_object_bytes: 0,
        unexpected_object_entries: 0,
        checked_capture_invariants: liveStatsForVerify.captures,
        invalid_captures: 0,
        errors: [],
      });

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
        websocket_streams: number;
        websocket_frames: number;
        websocket_skipped_captures: number;
        eventsource_streams: number;
        eventsource_events: number;
        eventsource_json_events: number;
        eventsource_skipped_captures: number;
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
      expect(corpusStats.websocket_streams).toBe(1);
      expect(corpusStats.websocket_frames).toBe(2);
      expect(corpusStats.websocket_skipped_captures).toBe(0);
      expect(corpusStats.eventsource_streams).toBe(3);
      expect(corpusStats.eventsource_events).toBe(4);
      expect(corpusStats.eventsource_json_events).toBe(3);
      expect(corpusStats.eventsource_skipped_captures).toBe(0);
      expect(corpusStats.stream_message_revisions).toBeGreaterThanOrEqual(2);
      expect(corpusStats.conversation_snapshots).toBeGreaterThanOrEqual(1);
      expect(corpusStats.message_observations).toBeGreaterThanOrEqual(1);
      expect(corpusStats.stream_reconstructions).toBeGreaterThanOrEqual(1);
      expect(corpusStats.attachment_observations).toBeGreaterThanOrEqual(1);
      expect(corpusStats.attachment_downloads).toBeGreaterThanOrEqual(1);

      const { stdout: corpusVerifyStdout } = await execFileAsync(
        cliPath,
        ["corpus", "verify"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusVerify = JSON.parse(corpusVerifyStdout) as {
        sqlite_integrity_ok: boolean;
        foreign_key_violations: number;
        stream_captures_checked: number;
        stream_events_checked: number;
        stream_message_revisions_checked: number;
        conversation_snapshots_checked: number;
        message_observations_checked: number;
        stream_reconstructions_checked: number;
        attachment_observations_checked: number;
        attachment_downloads_checked: number;
        websocket_streams_checked: number;
        websocket_frames_checked: number;
        eventsource_streams_checked: number;
        eventsource_events_checked: number;
        raw_source_links_checked: number;
        errors: string[];
      };
      const expectedRawSourceLinks =
        corpusStats.stream_captures +
        corpusStats.stream_message_revisions +
        corpusStats.conversation_snapshots +
        corpusStats.message_observations +
        corpusStats.stream_reconstructions +
        corpusStats.attachment_observations +
        corpusStats.attachment_downloads +
        corpusStats.websocket_frames +
        corpusStats.eventsource_events;
      expect(corpusVerify).toEqual({
        sqlite_integrity_ok: true,
        foreign_key_violations: 0,
        stream_captures_checked: corpusStats.stream_captures,
        stream_events_checked: corpusStats.stream_events,
        stream_message_revisions_checked: corpusStats.stream_message_revisions,
        conversation_snapshots_checked: corpusStats.conversation_snapshots,
        message_observations_checked: corpusStats.message_observations,
        stream_reconstructions_checked: corpusStats.stream_reconstructions,
        attachment_observations_checked: corpusStats.attachment_observations,
        attachment_downloads_checked: corpusStats.attachment_downloads,
        websocket_streams_checked: corpusStats.websocket_streams,
        websocket_frames_checked: corpusStats.websocket_frames,
        eventsource_streams_checked: corpusStats.eventsource_streams,
        eventsource_events_checked: corpusStats.eventsource_events,
        raw_source_links_checked: expectedRawSourceLinks,
        errors: [],
      });

      const rebuildFailureCapture = captures.find(
        (capture) =>
          capture.method === "GET" &&
          capture.url.includes("/backend-api/conversation/test") &&
          capture.privacy_class === "private" &&
          !!capture.body_hash &&
          !capture.body_error,
      );
      expect(rebuildFailureCapture?.body_hash).toMatch(/^[0-9a-f]{64}$/);
      const rebuildFailureHash = rebuildFailureCapture?.body_hash as string;
      const rebuildFailureObjectPath = join(
        dataDir,
        "private",
        "objects",
        rebuildFailureHash.slice(0, 2),
        rebuildFailureHash,
      );
      const publishedCorpusPath = join(dataDir, "derived", "corpus.sqlite3");
      const publishedCorpusBeforeFailedRebuild = await readFile(publishedCorpusPath);
      const rawObjectBeforeFailure = await readFile(rebuildFailureObjectPath);

      let failedRebuildObserved = false;
      try {
        await writeFile(
          rebuildFailureObjectPath,
          Buffer.from("intentional corpus rebuild failure", "utf8"),
        );
        try {
          await execFileAsync(cliPath, ["corpus", "rebuild"], {
            env: {
              ...childEnv,
              MIRRARIUM_DATA_DIR: dataDir,
            },
          });
        } catch {
          failedRebuildObserved = true;
        }
      } finally {
        await writeFile(rebuildFailureObjectPath, rawObjectBeforeFailure);
      }
      expect(failedRebuildObserved).toBe(true);
      expect(
        (await readFile(publishedCorpusPath)).equals(
          publishedCorpusBeforeFailedRebuild,
        ),
      ).toBe(true);
      expect(await readdir(join(dataDir, "derived"))).not.toContain(
        ".corpus.sqlite3.rebuild",
      );

      const { stdout: corpusStatsAfterFailedRebuildStdout } =
        await execFileAsync(cliPath, ["corpus", "stats"], {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        });
      expect(JSON.parse(corpusStatsAfterFailedRebuildStdout)).toEqual(corpusStats);

      const { stdout: corpusVerifyAfterFailedRebuildStdout } =
        await execFileAsync(cliPath, ["corpus", "verify"], {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        });
      expect(JSON.parse(corpusVerifyAfterFailedRebuildStdout)).toEqual(
        corpusVerify,
      );

      const staleCorpusStagingPath = join(
        dataDir,
        "derived",
        ".corpus.sqlite3.rebuild",
      );
      await writeFile(
        staleCorpusStagingPath,
        Buffer.from("simulated hard-crash staging artifact", "utf8"),
      );
      const { stdout: recoveredCorpusRebuildStdout } = await execFileAsync(
        cliPath,
        ["corpus", "rebuild"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(JSON.parse(recoveredCorpusRebuildStdout)).toEqual(corpusStats);
      expect(await readdir(join(dataDir, "derived"))).not.toContain(
        ".corpus.sqlite3.rebuild",
      );

      const { stdout: recoveredCorpusVerifyStdout } = await execFileAsync(
        cliPath,
        ["corpus", "verify"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(JSON.parse(recoveredCorpusVerifyStdout)).toEqual(corpusVerify);

      const { stdout: streamsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "streams", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streams = JSON.parse(streamsStdout) as Array<{
        capture_id: string;
        source_url: string;
        privacy_class: string;
        source_body_hash: string;
        event_count: number;
      }>;
      const completedStream = streams.find(
        (stream) =>
          stream.source_url.endsWith("/backend-api/conversation/stream"),
      );
      expect(completedStream).toMatchObject({
        privacy_class: "private",
        event_count: 3,
      });
      expect(completedStream?.source_body_hash).toMatch(/^[0-9a-f]{64}$/);

      const { stdout: rawStreamCaptureStdout } = await execFileAsync(
        cliPath,
        ["capture", completedStream?.capture_id as string],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const rawStreamCapture = JSON.parse(rawStreamCaptureStdout) as {
        capture_id: string;
        url: string;
        body_hash?: string;
        privacy_class: string;
      };
      expect(rawStreamCapture).toMatchObject({
        capture_id: completedStream?.capture_id,
        url: completedStream?.source_url,
        body_hash: completedStream?.source_body_hash,
        privacy_class: "private",
      });

      const { stdout: rawStreamBodyStdout } = await execFileAsync(
        cliPath,
        ["capture", completedStream?.capture_id as string, "response"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const rawStreamBody = JSON.parse(rawStreamBodyStdout) as {
        capture_id: string;
        body_kind: string;
        storage_class: string;
        body_hash: string;
        body_bytes: number;
        mime_type?: string;
        encoding: string;
        data: string;
      };
      expect(rawStreamBody).toMatchObject({
        capture_id: completedStream?.capture_id,
        body_kind: "response",
        storage_class: "private",
        body_hash: completedStream?.source_body_hash,
        encoding: "utf8",
      });
      expect(rawStreamBody.mime_type).toContain("text/event-stream");
      expect(rawStreamBody.data).toContain('"conversation_id":"fixture-stream"');
      expect(rawStreamBody.data).toContain("[DONE]");

      const { stdout: streamEventsStdout } = await execFileAsync(
        cliPath,
        [
          "corpus",
          "stream-events",
          completedStream?.capture_id as string,
          "20",
        ],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streamEvents = JSON.parse(streamEventsStdout) as Array<{
        capture_id: string;
        sequence: number;
        event_name?: string;
        data: string;
        json_valid: boolean;
      }>;
      expect(streamEvents).toHaveLength(3);
      expect(streamEvents.map((event) => event.sequence)).toEqual([0, 1, 2]);
      expect(streamEvents.map((event) => event.json_valid)).toEqual([
        true,
        true,
        false,
      ]);
      expect(streamEvents[0]).toMatchObject({
        capture_id: completedStream?.capture_id,
        event_name: "message",
        data: '{"conversation_id":"fixture-stream","delta":"hello"}',
      });
      expect(streamEvents[1]).toMatchObject({
        capture_id: completedStream?.capture_id,
        event_name: "message",
        data: '{"conversation_id":"fixture-stream","delta":" world"}',
      });
      expect(streamEvents[2]).toMatchObject({
        capture_id: completedStream?.capture_id,
        data: "[DONE]",
        json_valid: false,
      });

      const { stdout: webSocketSkippedStdout } = await execFileAsync(
        cliPath,
        ["corpus", "websocket-skipped", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(JSON.parse(webSocketSkippedStdout)).toEqual([]);

      const { stdout: eventSourceSkippedStdout } = await execFileAsync(
        cliPath,
        ["corpus", "eventsource-skipped", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(JSON.parse(eventSourceSkippedStdout)).toEqual([]);

      const { stdout: corpusExportSchemaStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-schema"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusExportSchema = JSON.parse(corpusExportSchemaStdout);
      const validateCorpusExport = new Ajv2020({
        allErrors: true,
        strict: true,
      }).compile(corpusExportSchema);

      const { stdout: corpusExportIndexSchemaStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-index-schema"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusExportIndexSchema = JSON.parse(corpusExportIndexSchemaStdout);
      const validateCorpusExportIndex = new Ajv2020({
        allErrors: true,
        strict: true,
      }).compile(corpusExportIndexSchema);

      const { stdout: corpusExportManifestSchemaStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-manifest-schema"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusExportManifestSchema = JSON.parse(
        corpusExportManifestSchemaStdout,
      );
      const validateCorpusExportManifest = new Ajv2020({
        allErrors: true,
        strict: true,
      }).compile(corpusExportManifestSchema);

      const { stdout: corpusExportStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const { stdout: corpusExportRepeatStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(corpusExportRepeatStdout).toBe(corpusExportStdout);

      const exportLines = corpusExportStdout
        .trim()
        .split("\n")
        .filter((line) => line.length > 0);
      const exportRecords = exportLines
        .map((line) => JSON.parse(line)) as Array<{
        schema: string;
        schema_version: number;
        producer_corpus_schema_version: number;
        record_type: string;
        conversation_id: string;
        source_capture_ids: string[];
        record_sha256: string;
        evidence: {
          summary: { conversation_id: string; title?: string };
          messages: Array<{ capture_id: string }>;
          streams: Array<{ capture_id: string }>;
        };
        canonical?: {
          basis_capture_id: string;
          basis_source_body_hash: string;
        };
        canonical_error?: string;
        stream_revisions: Array<{ capture_id: string }>;
        attachments: Array<{
          observation: { capture_id: string };
          downloads: Array<{ download_capture_id: string }>;
        }>;
      }>;
      expect(exportRecords.length).toBeGreaterThan(0);
      for (const record of exportRecords) {
        expect(
          validateCorpusExport(record),
          JSON.stringify(validateCorpusExport.errors),
        ).toBe(true);
      }
      expect(
        exportRecords.every(
          (record) =>
            record.schema === "mirrarium.corpus.conversation" &&
            record.schema_version === 1 &&
            record.producer_corpus_schema_version > 0 &&
            record.record_type === "conversation" &&
            record.conversation_id === record.evidence.summary.conversation_id &&
            /^[0-9a-f]{64}$/.test(record.record_sha256) &&
            record.source_capture_ids.length ===
              new Set(record.source_capture_ids).size &&
            record.source_capture_ids.join("\n") ===
              [...record.source_capture_ids].sort().join("\n"),
        ),
      ).toBe(true);

      const { stdout: corpusExportIndexStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-index"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const { stdout: corpusExportIndexRepeatStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-index"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(corpusExportIndexRepeatStdout).toBe(corpusExportIndexStdout);

      const exportIndexRecords = corpusExportIndexStdout
        .trim()
        .split("\n")
        .filter((line) => line.length > 0)
        .map((line) => JSON.parse(line)) as Array<{
        schema: string;
        schema_version: number;
        conversation_schema_version: number;
        producer_corpus_schema_version: number;
        record_type: string;
        conversation_id: string;
        record_sha256: string;
      }>;
      expect(exportIndexRecords.length).toBe(exportRecords.length);
      for (const indexRecord of exportIndexRecords) {
        expect(
          validateCorpusExportIndex(indexRecord),
          JSON.stringify(validateCorpusExportIndex.errors),
        ).toBe(true);
        const fullRecord = exportRecords.find(
          (record) => record.conversation_id === indexRecord.conversation_id,
        );
        expect(fullRecord?.record_sha256).toBe(indexRecord.record_sha256);
      }
      expect(exportIndexRecords.map((record) => record.conversation_id)).toEqual(
        exportRecords.map((record) => record.conversation_id),
      );

      const { stdout: corpusExportManifestStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-manifest"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const { stdout: corpusExportManifestRepeatStdout } = await execFileAsync(
        cliPath,
        ["corpus", "export-manifest"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(corpusExportManifestRepeatStdout).toBe(corpusExportManifestStdout);
      const exportManifest = JSON.parse(corpusExportManifestStdout) as {
        schema: string;
        schema_version: number;
        conversation_schema_version: number;
        index_schema_version: number;
        producer_corpus_schema_version: number;
        record_type: string;
        conversation_count: number;
        index_sha256: string;
      };
      expect(
        validateCorpusExportManifest(exportManifest),
        JSON.stringify(validateCorpusExportManifest.errors),
      ).toBe(true);
      expect(exportManifest.conversation_count).toBe(exportIndexRecords.length);
      expect(exportManifest.index_sha256).toBe(
        createHash("sha256").update(corpusExportIndexStdout).digest("hex"),
      );

      const exportedConversation = exportRecords.find(
        (record) => record.conversation_id === "fixture-conversation",
      );
      const exportedConversationLine = exportLines.find(
        (line) => JSON.parse(line).conversation_id === "fixture-conversation",
      );
      expect(exportedConversationLine).toBeTruthy();

      const { stdout: singleConversationExportStdout } = await execFileAsync(
        cliPath,
        [
          "corpus",
          "export-one",
          "fixture-conversation",
          exportedConversation?.record_sha256 as string,
        ],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      expect(singleConversationExportStdout.trimEnd()).toBe(
        exportedConversationLine,
      );
      expect(
        validateCorpusExport(JSON.parse(singleConversationExportStdout)),
        JSON.stringify(validateCorpusExport.errors),
      ).toBe(true);

      let staleExportRejected = false;
      try {
        await execFileAsync(
          cliPath,
          [
            "corpus",
            "export-one",
            "fixture-conversation",
            "0".repeat(64),
          ],
          {
            env: {
              ...childEnv,
              MIRRARIUM_DATA_DIR: dataDir,
            },
          },
        );
      } catch (error) {
        staleExportRejected = true;
        expect(String(error)).toContain("changed since the export index");
      }
      expect(staleExportRejected).toBe(true);
      expect(exportedConversation?.canonical_error ?? null).toBeNull();
      expect(exportedConversation?.canonical?.basis_capture_id.length).toBeGreaterThan(0);
      expect(exportedConversation?.canonical?.basis_source_body_hash).toMatch(
        /^[0-9a-f]{64}$/,
      );
      expect(exportedConversation?.source_capture_ids).toContain(
        exportedConversation?.canonical?.basis_capture_id,
      );
      expect(
        exportedConversation?.evidence.messages.every(
          (message) => message.capture_id.length > 0,
        ),
      ).toBe(true);

      const exportedStreamTail = exportRecords.find(
        (record) => record.conversation_id === "fixture-stream-tail",
      );
      expect(exportedStreamTail?.stream_revisions.length).toBeGreaterThanOrEqual(2);
      expect(
        exportedStreamTail?.stream_revisions.every(
          (revision) =>
            revision.capture_id.length > 0 &&
            exportedStreamTail.source_capture_ids.includes(revision.capture_id),
        ),
      ).toBe(true);

      const exportedAttachment = exportRecords.find(
        (record) => record.conversation_id === "fixture-attachment-conversation",
      );
      expect(exportedAttachment?.attachments.length).toBeGreaterThanOrEqual(1);
      expect(
        exportedAttachment?.attachments.some(
          (attachment) =>
            attachment.observation.capture_id.length > 0 &&
            exportedAttachment.source_capture_ids.includes(
              attachment.observation.capture_id,
            ) &&
            attachment.downloads.some(
              (download) =>
                download.download_capture_id.length > 0 &&
                exportedAttachment.source_capture_ids.includes(
                  download.download_capture_id,
                ),
            ),
        ),
      ).toBe(true);

      const { stdout: webSocketStreamsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "websocket-streams", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const webSocketStreams = JSON.parse(webSocketStreamsStdout) as Array<{
        lifecycle_id: string;
        source_url: string;
        privacy_class: string;
        frame_count: number;
      }>;
      const derivedWebSocket = webSocketStreams.find((stream) =>
        stream.source_url.includes("/backend-api/ws-fixture"),
      );
      expect(derivedWebSocket).toBeTruthy();
      expect(derivedWebSocket?.privacy_class).toBe("private");
      expect(derivedWebSocket?.frame_count).toBe(2);
      expect(derivedWebSocket?.source_url).not.toContain("fixture-ws-query-secret");
      expect(derivedWebSocket?.source_url).toContain("keep=yes");

      const { stdout: webSocketFramesStdout } = await execFileAsync(
        cliPath,
        [
          "corpus",
          "websocket-frames",
          derivedWebSocket?.lifecycle_id ?? "",
          "20",
        ],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const derivedWebSocketFrames = JSON.parse(webSocketFramesStdout) as Array<{
        lifecycle_id: string;
        transport_sequence: number;
        direction: string;
        source_capture_id: string;
        source_body_hash: string;
        data: string;
      }>;
      expect(derivedWebSocketFrames).toHaveLength(2);
      expect(
        derivedWebSocketFrames.map((frame) => frame.transport_sequence),
      ).toEqual([0, 1]);
      expect(derivedWebSocketFrames.map((frame) => frame.direction)).toEqual([
        "sent",
        "received",
      ]);
      expect(
        derivedWebSocketFrames.every(
          (frame) =>
            frame.lifecycle_id === derivedWebSocket?.lifecycle_id &&
            !!frame.source_capture_id &&
            !!frame.source_body_hash &&
            !frame.data.includes("fixture-ws-client-secret") &&
            !frame.data.includes("fixture-ws-server-secret"),
        ),
      ).toBe(true);
      expect(
        JSON.parse(
          derivedWebSocketFrames.find((frame) => frame.direction === "sent")
            ?.data ?? "{}",
        ),
      ).toMatchObject({
        message: "client websocket fixture",
        access_token: "[REDACTED]",
      });
      expect(
        JSON.parse(
          derivedWebSocketFrames.find((frame) => frame.direction === "received")
            ?.data ?? "{}",
        ),
      ).toMatchObject({
        message: "server websocket fixture",
        access_token: "[REDACTED]",
      });

      const { stdout: eventSourceStreamsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "eventsource-streams", "20"],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const eventSourceStreams = JSON.parse(eventSourceStreamsStdout) as Array<{
        lifecycle_id: string;
        source_url: string;
        privacy_class: string;
        event_count: number;
        reconnect_last_event_id: string | null;
        reconnect_from_lifecycle_id: string | null;
      }>;
      const derivedEventSource = eventSourceStreams.find((stream) =>
        stream.source_url.includes("/backend-api/eventsource-fixture"),
      );
      expect(derivedEventSource).toBeTruthy();
      expect(derivedEventSource?.privacy_class).toBe("private");
      expect(derivedEventSource?.event_count).toBe(2);
      expect(derivedEventSource?.source_url).not.toContain(
        "fixture-eventsource-query-secret",
      );
      expect(derivedEventSource?.source_url).toContain("keep=yes");

      const derivedReconnectStreams = eventSourceStreams.filter((stream) =>
        stream.source_url.includes("/backend-api/eventsource-reconnect"),
      );
      expect(derivedReconnectStreams).toHaveLength(2);
      expect(
        derivedReconnectStreams.every(
          (stream) =>
            stream.privacy_class === "private" &&
            stream.event_count === 1 &&
            !stream.source_url.includes("fixture-eventsource-reconnect-secret"),
        ),
      ).toBe(true);
      expect(
        new Set(derivedReconnectStreams.map((stream) => stream.lifecycle_id)).size,
      ).toBe(2);
      const reconnectContinuation = derivedReconnectStreams.find(
        (stream) => stream.reconnect_last_event_id === "fixture-reconnect-1",
      );
      const reconnectOrigin = derivedReconnectStreams.find(
        (stream) => stream.reconnect_last_event_id === null,
      );
      expect(reconnectContinuation).toBeTruthy();
      expect(reconnectOrigin).toBeTruthy();
      expect(reconnectContinuation?.reconnect_from_lifecycle_id).toBe(
        reconnectOrigin?.lifecycle_id,
      );
      expect(reconnectOrigin?.reconnect_from_lifecycle_id).toBeNull();

      const { stdout: eventSourceEventsStdout } = await execFileAsync(
        cliPath,
        [
          "corpus",
          "eventsource-events",
          derivedEventSource?.lifecycle_id ?? "",
          "20",
        ],
        {
          env: {
            ...childEnv,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const derivedEventSourceEvents = JSON.parse(
        eventSourceEventsStdout,
      ) as Array<{
        lifecycle_id: string;
        transport_sequence: number;
        source_capture_id: string;
        source_body_hash: string;
        event_name?: string;
        event_id?: string;
        data: string;
        json_valid: boolean;
      }>;
      expect(
        derivedEventSourceEvents.map((event) => event.transport_sequence),
      ).toEqual([0, 1]);
      expect(derivedEventSourceEvents.map((event) => event.event_name)).toEqual([
        "delta",
        "done",
      ]);
      expect(derivedEventSourceEvents.map((event) => event.event_id)).toEqual([
        "fixture-event-1",
        "fixture-event-2",
      ]);
      expect(
        derivedEventSourceEvents.every(
          (event) =>
            event.lifecycle_id === derivedEventSource?.lifecycle_id &&
            event.source_capture_id.length > 0 &&
            event.source_body_hash.length === 64,
        ),
      ).toBe(true);
      expect(derivedEventSourceEvents[0]?.json_valid).toBe(true);
      expect(JSON.parse(derivedEventSourceEvents[0]?.data ?? "{}")).toMatchObject({
        message: "long-lived eventsource fixture",
        access_token: "[REDACTED]",
      });
      expect(derivedEventSourceEvents[0]?.data).not.toContain(
        "fixture-eventsource-secret",
      );
      expect(derivedEventSourceEvents[1]?.json_valid).toBe(false);
      expect(derivedEventSourceEvents[1]?.data).toBe("[DONE]");

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
        stream_revision_count: number;
        attachment_observation_count: number;
      }>;
      const fixtureConversation = conversations.find(
        (conversation) =>
          conversation.conversation_id === "fixture-conversation",
      );
      const fixtureStream = conversations.find(
        (conversation) => conversation.conversation_id === "fixture-stream",
      );
      const fixtureStreamTail = conversations.find(
        (conversation) => conversation.conversation_id === "fixture-stream-tail",
      );
      const fixtureAttachmentConversation = conversations.find(
        (conversation) =>
          conversation.conversation_id === "fixture-attachment-conversation",
      );
      expect(fixtureConversation?.title).toBe("Private fixture");
      expect(fixtureConversation?.snapshot_count).toBeGreaterThanOrEqual(4);
      expect(fixtureConversation?.message_observation_count).toBeGreaterThanOrEqual(4);
      expect(fixtureStream?.stream_reconstruction_count).toBeGreaterThanOrEqual(1);
      expect(fixtureStreamTail?.stream_revision_count).toBeGreaterThanOrEqual(2);
      expect(
        fixtureAttachmentConversation?.attachment_observation_count,
      ).toBeGreaterThanOrEqual(1);

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

    const { stdout: extensionUninstallStdout } = await execFileAsync(
      cliPath,
      ["extension", "uninstall"],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_DATA_HOME: join(browserHome, ".local", "share"),
        },
      },
    );
    expect(JSON.parse(extensionUninstallStdout)).toEqual({
      install_path: installedExtensionPath,
      removed: true,
    });

    const { stdout: extensionStatusAfterUninstallStdout } = await execFileAsync(
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
    expect(JSON.parse(extensionStatusAfterUninstallStdout)).toMatchObject({
      install_path: installedExtensionPath,
      installed: false,
      valid: false,
      running_build_id: null,
      reload_required: false,
      extension_id: expectedExtensionId,
    });

    const { stdout: nativeUninstallStdout } = await execFileAsync(
      cliPath,
      ["native-host", "uninstall", "chrome-for-testing"],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_CONFIG_HOME: join(browserHome, ".config"),
          MIRRARIUM_BROWSER_USER_DATA_DIR: userDataDir,
        },
      },
    );
    expect(JSON.parse(nativeUninstallStdout)).toEqual({
      browser: "chrome-for-testing",
      manifest_path: nativeInstall.manifest_path,
      removed: true,
    });

    const { stdout: nativeStatusAfterUninstallStdout } = await execFileAsync(
      cliPath,
      ["native-host", "status", "chrome-for-testing"],
      {
        env: {
          ...childEnv,
          HOME: browserHome,
          XDG_CONFIG_HOME: join(browserHome, ".config"),
          MIRRARIUM_BROWSER_USER_DATA_DIR: userDataDir,
        },
      },
    );
    expect(JSON.parse(nativeStatusAfterUninstallStdout)).toEqual({
      browser: "chrome-for-testing",
      manifest_path: nativeInstall.manifest_path,
      installed: false,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
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
  const extensionPath = resolve("extension/dist");
  const daemonPath = resolve("target/debug/mirrariumd");
  const cliPath = resolve("target/debug/mirrarium");

  async function readStats(): Promise<StoreStats> {
    const { stdout } = await execFileAsync(daemonPath, ["--stats"], {
      env: {
        ...process.env,
        MIRRARIUM_DATA_DIR: dataDir,
      },
    });
    return JSON.parse(stdout) as StoreStats;
  }

  try {
    const nativeManifest = JSON.stringify({
      name: nativeHostName,
      description: "Mirrarium test native host",
      path: daemonPath,
      type: "stdio",
      allowed_origins: [`chrome-extension://${expectedExtensionId}/`],
    });
    const nativeManifestDirectories = [
      join(userDataDir, "NativeMessagingHosts"),
      join(browserHome, ".config/google-chrome-for-testing/NativeMessagingHosts"),
      join(browserHome, ".config/chromium/NativeMessagingHosts"),
      join(browserHome, ".config/google-chrome/NativeMessagingHosts"),
    ];

    for (const directory of nativeManifestDirectories) {
      const nativeManifestPath = join(directory, `${nativeHostName}.json`);
      await mkdir(dirname(nativeManifestPath), { recursive: true });
      await writeFile(nativeManifestPath, nativeManifest);
    }

    await mkdir(userDataDir, { recursive: true });
    await mkdir(dataDir, { recursive: true });

    const context = await chromium.launchPersistentContext(userDataDir, {
      channel: "chromium",
      headless: true,
      ignoreHTTPSErrors: true,
      env: {
        ...process.env,
        HOME: browserHome,
        XDG_CONFIG_HOME: join(browserHome, ".config"),
        MIRRARIUM_DATA_DIR: dataDir,
      },
      args: [
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`,
        "--host-resolver-rules=MAP chatgpt.com 127.0.0.1",
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

      const stats = await readStats();
      expect(stats.public_objects).toBeGreaterThanOrEqual(2);
      expect(stats.private_captures).toBeGreaterThanOrEqual(4);
      expect(stats.private_objects).toBeGreaterThanOrEqual(3);
      expect(stats.private_objects).toBeLessThan(stats.private_captures + stats.request_bodies);
      expect(stats.request_body_errors).toBe(0);

      const { stdout: capturesStdout } = await execFileAsync(
        cliPath,
        ["captures", "20"],
        {
          env: {
            ...process.env,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const captures = JSON.parse(capturesStdout) as Array<{
        method: string;
        url: string;
        status: number;
        request_body_kind?: string;
        request_body_content_type?: string;
        request_body_has_post_data?: boolean;
        request_body_post_data_entry_count?: number;
        request_body_declared_content_length?: number;
        request_body_hash?: string;
        request_body_bytes: number;
        request_body_error?: string;
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
      const requestBodyObjects = privateObjects
        .map((body) => body.toString("utf8"))
        .filter((body) => body.includes("hello from request body"));
      expect(requestBodyObjects.length).toBeGreaterThanOrEqual(1);
      expect(requestBodyObjects.some((body) => body.includes("[REDACTED]"))).toBe(true);
      expect(requestBodyObjects.some((body) => body.includes("fixture-secret-token"))).toBe(false);

      const { stdout: corpusStdout } = await execFileAsync(
        cliPath,
        ["corpus", "rebuild"],
        {
          env: {
            ...process.env,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const corpusStats = JSON.parse(corpusStdout) as {
        stream_captures: number;
        stream_events: number;
        json_stream_events: number;
        conversation_snapshots: number;
        message_observations: number;
        stream_reconstructions: number;
      };
      expect(corpusStats.stream_captures).toBeGreaterThanOrEqual(1);
      expect(corpusStats.stream_events).toBeGreaterThanOrEqual(3);
      expect(corpusStats.json_stream_events).toBeGreaterThanOrEqual(2);
      expect(corpusStats.conversation_snapshots).toBeGreaterThanOrEqual(1);
      expect(corpusStats.message_observations).toBeGreaterThanOrEqual(1);
      expect(corpusStats.stream_reconstructions).toBeGreaterThanOrEqual(1);
    } finally {
      await context.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

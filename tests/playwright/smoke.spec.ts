import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readdir, readFile, rm } from "node:fs/promises";
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
    await mkdir(userDataDir, { recursive: true });
    await mkdir(dataDir, { recursive: true });

    const { stdout: nativeInstallStdout } = await execFileAsync(
      cliPath,
      ["native-host", "install", "chrome-for-testing", daemonPath],
      {
        env: {
          ...process.env,
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
        ...process.env,
        HOME: browserHome,
        XDG_CONFIG_HOME: join(browserHome, ".config"),
        MIRRARIUM_DATA_DIR: dataDir,
      },
      args: [
        `--disable-extensions-except=${extensionPath}`,
        `--load-extension=${extensionPath}`,
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
                ...process.env,
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
            ...process.env,
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

      const { stdout: privateReadsStdout } = await execFileAsync(
        cliPath,
        ["cache", "private-reads", "50"],
        {
          env: {
            ...process.env,
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

      const replayCountsBefore = (await fetch(
        "http://127.0.0.1:43118/replay-counts",
      ).then((response) => response.json())) as {
        css: number;
        js: number;
        image: number;
        font: number;
        cdn_js: number;
      };
      expect(replayCountsBefore.css).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.js).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.image).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.font).toBeGreaterThanOrEqual(1);
      expect(replayCountsBefore.cdn_js).toBeGreaterThanOrEqual(1);

      const replaySession = await context.newCDPSession(page);
      try {
        await replaySession.send("Network.enable");
        await replaySession.send("Network.setCacheDisabled", {
          cacheDisabled: true,
        });

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
              ...process.env,
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
      expect(replayStats.attempts).toBeGreaterThanOrEqual(10);
      expect(replayStats.hits).toBeGreaterThanOrEqual(5);
      expect(replayStats.misses).toBeGreaterThanOrEqual(4);
      expect(replayStats.replayed_bytes).toBeGreaterThan(0);
      expect(replayStats.lookup_errors).toBe(0);
      expect(replayStats.timeouts).toBe(0);
      expect(replayStats.fulfill_errors).toBe(0);

      const { stdout: capturesStdout } = await execFileAsync(
        cliPath,
        ["captures", "100"],
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
      expect(
        privateObjects.some((body) =>
          body.toString("utf8").includes("fixture-download-secret"),
        ),
      ).toBe(false);

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
            ...process.env,
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
      expect(attachment?.downloads).toHaveLength(1);
      expect(attachment?.downloads[0]).toMatchObject({
        mime_type: "text/plain",
      });
      expect(attachment?.downloads[0].source_url).not.toContain(
        "fixture-download-secret",
      );
      expect(attachment?.downloads[0].body_hash).toHaveLength(64);
      expect(attachment?.downloads[0].body_bytes).toBeGreaterThan(0);

      const { stdout: conversationsStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversations", "20"],
        {
          env: {
            ...process.env,
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
      expect(fixtureConversation).toMatchObject({
        title: "Private fixture",
        snapshot_count: 1,
        message_observation_count: 1,
      });
      expect(fixtureStream?.stream_reconstruction_count).toBeGreaterThanOrEqual(1);

      const { stdout: conversationStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversation", "fixture-conversation"],
        {
          env: {
            ...process.env,
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
            ...process.env,
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
            ...process.env,
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
            ...process.env,
            MIRRARIUM_DATA_DIR: dataDir,
          },
        },
      );
      const streamRevisions = JSON.parse(streamRevisionsStdout) as Array<{
        message_id: string;
        parent_id?: string;
        role?: string;
        content_text?: string;
      }>;
      expect(streamRevisions).toHaveLength(2);
      expect(streamRevisions.map((revision) => revision.content_text)).toEqual([
        "hello",
        "hello world",
      ]);
      expect(
        streamRevisions.every(
          (revision) =>
            revision.message_id === "stream-tail-assistant" &&
            revision.parent_id === "stream-tail-user" &&
            revision.role === "assistant",
        ),
      ).toBe(true);

      const { stdout: streamTailCanonicalStdout } = await execFileAsync(
        cliPath,
        ["corpus", "canonical", "fixture-stream-tail"],
        {
          env: {
            ...process.env,
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
      expect(streamTailCanonical.warnings).toEqual([]);

      const { stdout: streamConversationStdout } = await execFileAsync(
        cliPath,
        ["corpus", "conversation", "fixture-stream"],
        {
          env: {
            ...process.env,
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
    } finally {
      await context.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

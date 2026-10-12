import { createHash } from "node:crypto";

import { chromium, expect, test } from "@playwright/test";

// Feasibility probe only: no Mirrarium extension or real ChatGPT account.
// The current production recorder still uses Network.getResponseBody.
test("CDP passively streams a 17 MiB response while the webpage reads it", async () => {
  test.setTimeout(90_000);
  const origin = "https://chatgpt.com:43117";
  const route = "/backend-api/passive-stream-probe";
  const browser = await chromium.launch({
    channel: "chromium",
    headless: true,
    args: [
      "--host-resolver-rules=MAP chatgpt.com 127.0.0.1",
      "--no-proxy-server",
    ],
  });
  try {
    const context = await browser.newContext({ ignoreHTTPSErrors: true });
    const page = await context.newPage();
    await page.goto(origin + "/warmup");

    const session = await context.newCDPSession(page);
    await session.send("Network.enable", {
      maxResourceBufferSize: 512 * 1024,
      maxTotalBufferSize: 1024 * 1024,
    });

    let requestId: string | undefined;
    let enableStream: Promise<{ bufferedData: string }> | undefined;
    const streamed: Buffer[] = [];
    let finished: () => void;
    const completed = new Promise<void>((resolve) => {
      finished = resolve;
    });

    session.on("Network.responseReceived", (event) => {
      if (event.response.url.endsWith(route)) {
        requestId = event.requestId;
        // Unlike Fetch.takeResponseBodyAsStream, Network streaming is
        // passive: it should not require replacing the browser response.
        enableStream = session.send("Network.streamResourceContent", {
          requestId: event.requestId,
        }) as Promise<{ bufferedData: string }>;
      }
    });
    session.on("Network.dataReceived", (event) => {
      if (event.requestId === requestId && event.data) {
        streamed.push(Buffer.from(event.data, "base64"));
      }
    });
    session.on("Network.loadingFinished", (event) => {
      if (event.requestId === requestId) {
        finished();
      }
    });

    const pageResult = await page.evaluate(async (url: string) => {
      const response = await fetch(url, { cache: "no-store" });
      const body = await response.text();
      return {
        status: response.status,
        length: new TextEncoder().encode(body).length,
        beginning: body.slice(0, 42),
        end: body.slice(-2),
      };
    }, route);
    expect(pageResult.status).toBe(200);
    expect(pageResult.beginning).toContain('"passive-stream-probe"');
    expect(pageResult.end).toBe('"}');

    await completed;
    expect(enableStream).toBeDefined();
    const { bufferedData } = await enableStream!;
    expect(streamed.length).toBeGreaterThan(0);
    const actual = Buffer.concat([
      Buffer.from(bufferedData, "base64"),
      ...streamed,
    ]);
    const expected = Buffer.from(
      '{"id":"passive-stream-probe","content":"' +
        "x".repeat(17 * 1024 * 1024) + '"}',
      "utf8",
    );
    expect(actual.byteLength).toBe(expected.byteLength);
    expect(pageResult.length).toBe(expected.byteLength);
    expect(createHash("sha256").update(actual).digest("hex")).toBe(
      createHash("sha256").update(expected).digest("hex"),
    );

    await session.detach();
  } finally {
    await browser.close();
  }
});

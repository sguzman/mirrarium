import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, test } from "@playwright/test";

function nativeFrame(message: unknown): Buffer {
  const payload = Buffer.from(JSON.stringify(message), "utf8");
  const frame = Buffer.allocUnsafe(4 + payload.length);
  frame.writeUInt32LE(payload.length, 0);
  payload.copy(frame, 4);
  return frame;
}

function readFrames(raw: Buffer): Array<Record<string, unknown>> {
  const messages: Array<Record<string, unknown>> = [];
  let offset = 0;
  while (offset < raw.length) {
    expect(raw.length - offset).toBeGreaterThanOrEqual(4);
    const length = raw.readUInt32LE(offset);
    offset += 4;
    expect(length).toBeLessThanOrEqual(1024 * 1024);
    expect(raw.length - offset).toBeGreaterThanOrEqual(length);
    messages.push(
      JSON.parse(raw.subarray(offset, offset + length).toString("utf8")) as
        Record<string, unknown>,
    );
    offset += length;
  }
  return messages;
}

test("native transport acknowledges only a durably finished capture", async () => {
  const root = await mkdtemp(join(tmpdir(), "mirrarium-receipt-"));
  const daemonPath = resolve("target/debug/mirrariumd");
  const cliPath = resolve("target/debug/mirrarium");
  const env = {
    ...process.env,
    MIRRARIUM_DATA_DIR: join(root, "data"),
    MIRRARIUM_PRIVATE_KEY_FILE: join(root, "private.key"),
  };
  const captureId = "wire-receipt-fixture";
  const body = Buffer.from('{"id":"wire-receipt-fixture","title":"Committed"}', "utf8");
  const requests = [
    {
      type: "capture_start",
      metadata: {
        capture_id: captureId,
        tab_id: 1,
        request_id: "wire-receipt-request",
        method: "GET",
        url: "https://chatgpt.com/backend-api/conversation/wire-receipt-fixture",
        status: 200,
        mime_type: "application/json",
        resource_type: "Fetch",
        provenance: {},
      },
    },
    {
      type: "capture_chunk",
      capture_id: captureId,
      sequence: 0,
      data_base64: body.toString("base64"),
    },
    {
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: body.length,
      body_error: null,
    },
    {
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: body.length,
      body_error: null,
    },
    { type: "ping" },
  ];

  try {
    const child = spawn(daemonPath, [], {
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const outputChunks: Buffer[] = [];
    const errorChunks: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => outputChunks.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => errorChunks.push(chunk));
    const exited = new Promise<number | null>((resolveExit, rejectExit) => {
      child.on("error", rejectExit);
      child.on("close", (code) => resolveExit(code));
    });
    child.stdin.end(Buffer.concat(requests.map(nativeFrame)));
    const code = await exited;
    expect(
      code,
      Buffer.concat(errorChunks).toString("utf8"),
    ).toBe(0);
    const responses = readFrames(Buffer.concat(outputChunks));
    expect(responses).toHaveLength(5);
    expect(responses[0]).toEqual({
      type: "capture_message_ack",
      capture_id: captureId,
      stage: "capture_start",
      sequence: null,
    });
    expect(responses[1]).toEqual({
      type: "capture_message_ack",
      capture_id: captureId,
      stage: "capture_chunk",
      sequence: 0,
    });
    expect(responses[2]).toEqual({
      type: "capture_committed",
      capture_id: captureId,
    });
    expect(responses[3]).toMatchObject({
      type: "error",
      capture_id: captureId,
    });
    expect(responses[4]).toEqual({ type: "pong" });

    const archive = spawn(cliPath, ["captures", "20"], {
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const archiveChunks: Buffer[] = [];
    const archiveErrors: Buffer[] = [];
    archive.stdout.on("data", (chunk: Buffer) => archiveChunks.push(chunk));
    archive.stderr.on("data", (chunk: Buffer) => archiveErrors.push(chunk));
    const archiveCode = await new Promise<number | null>((resolveExit, rejectExit) => {
      archive.on("error", rejectExit);
      archive.on("close", resolveExit);
    });
    expect(
      archiveCode,
      Buffer.concat(archiveErrors).toString("utf8"),
    ).toBe(0);
    const captures = JSON.parse(
      Buffer.concat(archiveChunks).toString("utf8"),
    ) as Array<{
      capture_id: string;
      body_hash: string | null;
      body_bytes: number;
    }>;
    expect(captures).toHaveLength(1);
    expect(captures[0]).toMatchObject({
      capture_id: captureId,
      body_bytes: body.length,
    });
    expect(captures[0]?.body_hash).toMatch(/^[a-f0-9]{64}$/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

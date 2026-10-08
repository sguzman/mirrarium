import { spawn } from "node:child_process";
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
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
  const requestBody = Buffer.from('{"prompt":"synthetic receipt check"}', "utf8");
  const requests = [
    {
      type: "capture_start",
      metadata: {
        capture_id: captureId,
        tab_id: 1,
        request_id: "wire-receipt-request",
        method: "POST",
        url: "https://chatgpt.com/backend-api/conversation/wire-receipt-fixture",
        status: 200,
        mime_type: "application/json",
        resource_type: "Fetch",
        provenance: {},
      },
    },
    {
      type: "request_body_start",
      capture_id: captureId,
      metadata: {
        content_type: "application/json",
        has_post_data: true,
        declared_content_length: requestBody.length,
      },
    },
    {
      type: "request_body_chunk",
      capture_id: captureId,
      sequence: 0,
      data_base64: requestBody.toString("base64"),
    },
    {
      type: "request_body_finish",
      capture_id: captureId,
      body_error: null,
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
    { type: "capture_commit_probe", capture_id: captureId },
    { type: "capture_commit_probe", capture_id: "absent-capture" },
    {
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: body.length,
      body_error: null,
    },
    {
      type: "capture_start",
      metadata: {
        capture_id: "poisoned-sequence-fixture",
        tab_id: 1,
        request_id: "poisoned-request",
        method: "GET",
        url: "https://chatgpt.com/backend-api/poisoned-sequence",
        status: 200,
        mime_type: "application/json",
        resource_type: "Fetch",
        provenance: {},
      },
    },
    {
      type: "capture_chunk",
      capture_id: "poisoned-sequence-fixture",
      sequence: 1,
      data_base64: body.toString("base64"),
    },
    {
      type: "capture_finish",
      capture_id: "poisoned-sequence-fixture",
      encoded_data_length: body.length,
      body_error: null,
    },
    { type: "capture_commit_probe", capture_id: "poisoned-sequence-fixture" },
    {
      type: "capture_start",
      metadata: {
        capture_id: "abort-wire-fixture",
        tab_id: 1,
        request_id: "abort-wire-request",
        method: "GET",
        url: "https://chatgpt.com/backend-api/abort-fixture",
        status: 200,
        mime_type: "application/json",
        resource_type: "Fetch",
        provenance: {},
      },
    },
    {
      type: "capture_chunk",
      capture_id: "abort-wire-fixture",
      sequence: 0,
      data_base64: body.toString("base64"),
    },
    { type: "capture_abort", capture_id: "abort-wire-fixture" },
    { type: "capture_commit_probe", capture_id: "abort-wire-fixture" },
    { type: "capture_abort", capture_id: "abort-wire-fixture" },
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
    expect(responses).toHaveLength(19);
    const expectProgress = (index: number, stage: string, sequence: number | null) =>
      expect(responses[index]).toEqual({
        type: "capture_message_ack",
        capture_id: captureId,
        stage,
        sequence,
      });
    expectProgress(0, "capture_start", null);
    expectProgress(1, "request_body_start", null);
    expectProgress(2, "request_body_chunk", 0);
    expectProgress(3, "request_body_finish", null);
    expectProgress(4, "capture_chunk", 0);
    expect(responses[5]).toEqual({
      type: "capture_committed",
      capture_id: captureId,
    });
    expect(responses[6]).toEqual({
      type: "capture_commit_status",
      capture_id: captureId,
      committed: true,
    });
    expect(responses[7]).toEqual({
      type: "capture_commit_status",
      capture_id: "absent-capture",
      committed: false,
    });
    expect(responses[8]).toMatchObject({
      type: "error",
      capture_id: captureId,
    });
    expect(responses[9]).toEqual({
      type: "capture_message_ack",
      capture_id: "poisoned-sequence-fixture",
      stage: "capture_start",
      sequence: null,
    });
    expect(responses[10]).toMatchObject({
      type: "error",
      capture_id: "poisoned-sequence-fixture",
    });
    expect(responses[11]).toMatchObject({
      type: "error",
      capture_id: "poisoned-sequence-fixture",
    });
    expect(responses[12]).toEqual({
      type: "capture_commit_status",
      capture_id: "poisoned-sequence-fixture",
      committed: false,
    });
    expect(responses[13]).toEqual({
      type: "capture_message_ack",
      capture_id: "abort-wire-fixture",
      stage: "capture_start",
      sequence: null,
    });
    expect(responses[14]).toEqual({
      type: "capture_message_ack",
      capture_id: "abort-wire-fixture",
      stage: "capture_chunk",
      sequence: 0,
    });
    expect(responses[15]).toEqual({
      type: "capture_aborted",
      capture_id: "abort-wire-fixture",
      discarded: true,
    });
    expect(responses[16]).toEqual({
      type: "capture_commit_status",
      capture_id: "abort-wire-fixture",
      committed: false,
    });
    expect(responses[17]).toEqual({
      type: "capture_aborted",
      capture_id: "abort-wire-fixture",
      discarded: false,
    });
    expect(responses[18]).toEqual({ type: "pong" });

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
      request_body_hash: string | null;
      request_body_bytes: number;
    }>;
    expect(captures).toHaveLength(1);
    expect(captures[0]).toMatchObject({
      capture_id: captureId,
      body_bytes: body.length,
      request_body_bytes: requestBody.length,
    });
    expect(captures[0]?.body_hash).toMatch(/^[a-f0-9]{64}$/);
    expect(captures[0]?.request_body_hash).toMatch(/^[a-f0-9]{64}$/);
    // Aborted fragments must be discarded on the same live host rather than
    // stranded until its next startup cleanup.
    const staging = await readdir(join(root, "data", ".incoming"));
    expect(staging.filter((name) => name.endsWith(".part"))).toEqual([]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("aborting on an untouched native host does not initialize a writable ledger", async () => {
  const root = await mkdtemp(join(tmpdir(), "mirrarium-cold-abort-"));
  const daemonPath = resolve("target/debug/mirrariumd");
  const dataDir = join(root, "data");
  const child = spawn(daemonPath, [], {
    env: {
      ...process.env,
      MIRRARIUM_DATA_DIR: dataDir,
      MIRRARIUM_PRIVATE_KEY_FILE: join(root, "private.key"),
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const output: Buffer[] = [];
  const errors: Buffer[] = [];
  child.stdout.on("data", (chunk: Buffer) => output.push(chunk));
  child.stderr.on("data", (chunk: Buffer) => errors.push(chunk));
  const exited = new Promise<number | null>((resolveExit, rejectExit) => {
    child.on("error", rejectExit);
    child.on("close", resolveExit);
  });
  try {
    child.stdin.end(Buffer.concat([
      nativeFrame({ type: "capture_abort", capture_id: "never-started" }),
      nativeFrame({ type: "ping" }),
    ]));
    expect(
      await exited,
      Buffer.concat(errors).toString("utf8"),
    ).toBe(0);
    expect(readFrames(Buffer.concat(output))).toEqual([
      { type: "capture_aborted", capture_id: "never-started", discarded: false },
      { type: "pong" },
    ]);
    await expect(stat(join(dataDir, "ledger.sqlite3"))).rejects.toMatchObject({
      code: "ENOENT",
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

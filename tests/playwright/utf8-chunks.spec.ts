import { expect, test } from "@playwright/test";

import { encodeUtf8Chunks } from "../../extension/src/utf8Chunks";

function assertEquivalent(input: string, capacity: number): number {
  const chunks = Array.from(
    encodeUtf8Chunks(input, capacity),
    (chunk) => Uint8Array.from(chunk), // yielded views reuse one scratch buffer
  );
  for (const chunk of chunks) {
    expect(chunk.byteLength).toBeGreaterThan(0);
    expect(chunk.byteLength).toBeLessThanOrEqual(capacity);
  }
  const reconstructed = Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
  const expected = Buffer.from(new TextEncoder().encode(input));
  expect(reconstructed.equals(expected)).toBe(true);
  return chunks.length;
}

test("bounded UTF-8 encoding preserves large ASCII, surrogate-pair and replacement sequences", () => {
  // A surrogate pair straddles the 16 KiB input-window boundary.
  const pairAtWindowBoundary = "a".repeat(16 * 1024 - 1) + "🛰️" + "b".repeat(35_000);
  const mixed = "汉字é🙂👩‍🚀\uD800 \uDC00\uD83D\uDE80";
  for (const capacity of [4, 5, 7, 257, 384 * 1024]) {
    assertEquivalent("", capacity);
    assertEquivalent("Hello", capacity);
    assertEquivalent(mixed, capacity);
    assertEquivalent(pairAtWindowBoundary, capacity);
  }
  const chunks = assertEquivalent("x".repeat(2 * 1024 * 1024) + mixed, 384 * 1024);
  expect(chunks).toBeGreaterThan(1);
});

test("bounded UTF-8 encoding rejects invalid capacities", () => {
  for (const invalid of [0, -1, 3, 0.5, Number.POSITIVE_INFINITY]) {
    expect(() => Array.from(encodeUtf8Chunks("hello", invalid))).toThrow(RangeError);
  }
});

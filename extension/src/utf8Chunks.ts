/**
 * Encodes a UTF-16 JS string using one bounded byte buffer instead of
 * allocating a Uint8Array as large as the entire response.
 *
 * IMPORTANT: yielded views share the backing buffer; consume each view
 * synchronously (or copy it) before advancing the iterator.
 */
export function* encodeUtf8Chunks(
  value: string,
  maxChunkBytes: number,
): Generator<Uint8Array, void, undefined> {
  if (!Number.isSafeInteger(maxChunkBytes) || maxChunkBytes < 4) {
    throw new RangeError("UTF-8 chunk capacity must be a safe integer >= 4");
  }
  const encoder = new TextEncoder();
  const buffer = new Uint8Array(maxChunkBytes);
  const maxSourceWindowChars = 16 * 1024;
  let consumed = 0;
  let used = 0;

  while (consumed < value.length) {
    let end = Math.min(value.length, consumed + maxSourceWindowChars);
    // Do not break a UTF-16 surrogate pair between bounded input windows.
    if (
      end < value.length &&
      end > consumed &&
      value.charCodeAt(end - 1) >= 0xd800 &&
      value.charCodeAt(end - 1) <= 0xdbff &&
      value.charCodeAt(end) >= 0xdc00 &&
      value.charCodeAt(end) <= 0xdfff
    ) {
      end -= 1;
    }

    const progress = encoder.encodeInto(
      value.slice(consumed, end),
      buffer.subarray(used),
    );
    if (progress.read === 0 && progress.written === 0) {
      if (used === 0) {
        throw new Error("UTF-8 chunk encoder could not advance");
      }
      yield buffer.subarray(0, used);
      used = 0;
      continue;
    }
    consumed += progress.read;
    used += progress.written;

    // Keep enough free space for any one UTF-8 code point on the next pass.
    if (used > 0 && maxChunkBytes - used < 4) {
      yield buffer.subarray(0, used);
      used = 0;
    }
  }
  if (used > 0) {
    yield buffer.subarray(0, used);
  }
}

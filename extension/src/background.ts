const NATIVE_HOST = "com.sguzman.mirrarium";
const CDP_VERSION = "1.3";
const BASE64_CHUNK_CHARS = 512 * 1024;
const RAW_CHUNK_BYTES = 384 * 1024;
const CACHE_LOOKUP_TIMEOUT_MS = 750;
const MAX_REQUEST_BODY_BYTES = 16 * 1024 * 1024;
const MAX_RESPONSE_BODY_BYTES = 16 * 1024 * 1024;
const CDP_MAX_RESOURCE_BUFFER_BYTES = MAX_RESPONSE_BODY_BYTES + 4 * 1024 * 1024;
const CDP_MAX_TOTAL_BUFFER_BYTES = 64 * 1024 * 1024;
const MAX_WEBSOCKET_JSON_FRAME_BYTES = 1024 * 1024;
const MAX_EVENTSOURCE_MESSAGE_BYTES = 1024 * 1024;
const RUNNING_BUILD_ID =
  chrome.runtime.getManifest().version_name ?? chrome.runtime.getManifest().version;

type RequestMetadata = {
  method: string;
  url: string;
  postData?: string;
  hasPostData?: boolean;
  contentType?: string;
  postDataEntryCount?: number;
  declaredContentLength?: number;
  headers: Record<string, string>;
  frameId?: string;
  loaderId?: string;
  documentUrl?: string;
  initiatorType?: string;
  requestWallTimeMs?: number;
  servedFromCache: boolean;
  lifecycleId: string;
  redirectHop: number;
  redirectedFromUrl?: string;
  resourceType?: string;
};

type ResponseMetadata = {
  url: string;
  status: number;
  mimeType: string;
  resourceType: string;
  etag?: string;
  lastModified?: string;
  cacheControl?: string;
  headers: Record<string, string>;
  responseProtocol?: string;
  responseTimeMs?: number;
  fromDiskCache: boolean;
  fromServiceWorker: boolean;
  fromPrefetchCache: boolean;
  encodedDataLength?: number;
};

type CacheReplayHit = {
  mimeType: string;
  bodyHash: string;
  bodyBytes: number;
  cacheControl?: string;
  etag?: string;
  lastModified?: string;
  bodyBase64: string;
};

type PendingCacheLookup = {
  port: chrome.runtime.Port;
  resolve: (hit: CacheReplayHit | null) => void;
  timeoutId: number;
  url: string;
  resourceType: string;
  metadata?: Omit<CacheReplayHit, "bodyBase64">;
  chunks: string[];
  nextSequence: number;
};

type PrivateReadHit = {
  mimeType: string;
  bodyHash: string;
  bodyBytes: number;
  capturedAtMs: number;
  cacheControl?: string;
  etag?: string;
  lastModified?: string;
  bodyBase64: string;
};

type PendingPrivateReadLookup = {
  port: chrome.runtime.Port;
  resolve: (hit: PrivateReadHit | null) => void;
  timeoutId: number;
  url: string;
  metadata?: Omit<PrivateReadHit, "bodyBase64">;
  chunks: string[];
  nextSequence: number;
};

type WebSocketMetadata = {
  url: string;
  lifecycleId: string;
  initiatorType?: string;
  requestWallTimeMs?: number;
  requestHeaders: Record<string, string>;
  responseHeaders: Record<string, string>;
  status?: number;
  nextFrameSequence: number;
};

type CdpWebSocketFrame = {
  opcode: number;
  mask: boolean;
  payloadData: string;
};

type CdpResponse = {
  url: string;
  status: number;
  mimeType: string;
  protocol?: string;
  responseTime?: number;
  fromDiskCache?: boolean;
  fromServiceWorker?: boolean;
  fromPrefetchCache?: boolean;
  encodedDataLength?: number;
  headers?: Record<string, string | number>;
};

const attachedTabs = new Set<number>();
// A tab can navigate away while debugger.attach/Network.enable/Fetch.enable
// are still pending. Cancellation must work before attachedTabs is populated.
type PendingAttach = {
  cancelled: boolean;
  retryIfSupported: boolean;
  selfDetaching: boolean;
};
const pendingAttachTabs = new Map<number, PendingAttach>();
const detachingTabs = new Map<number, Promise<void>>();
const fetchSetupTabs = new Set<number>();
const requests = new Map<string, RequestMetadata>();
const responses = new Map<string, ResponseMetadata>();
const webSockets = new Map<string, WebSocketMetadata>();
const eventSourceSequences = new Map<string, number>();
const pendingEventSourceLastEventIds = new Map<string, string>();
const pendingCacheLookups = new Map<string, PendingCacheLookup>();
const pendingPrivateReadLookups = new Map<string, PendingPrivateReadLookup>();
const privateRevalidations = new Map<string, PrivateReadHit>();
let nativePort: chrome.runtime.Port | undefined;
// A capture is one native-host transaction. Its start, request body, response
// chunks and finish may never be spliced across host process lifetimes.
const capturePorts = new Map<string, chrome.runtime.Port>();
let staleInstalledBuildNotice: string | undefined;

function requestKey(tabId: number, requestId: string): string {
  return `${tabId}:${requestId}`;
}

function isSupportedChatGptSocketUrl(rawUrl: string | undefined): boolean {
  if (!rawUrl) return false;

  try {
    const url = new URL(rawUrl);
    return (
      url.protocol === "wss:" &&
      (url.hostname === "chatgpt.com" || url.hostname === "chat.openai.com")
    );
  } catch {
    return false;
  }
}

function isSupportedChatGptUrl(rawUrl: string | undefined): boolean {
  if (!rawUrl) return false;

  try {
    const url = new URL(rawUrl);
    return (
      url.protocol === "https:" &&
      (url.hostname === "chatgpt.com" || url.hostname === "chat.openai.com")
    );
  } catch {
    return false;
  }
}

function clearTabState(tabId: number): void {
  const prefix = `${tabId}:`;

  for (const key of requests.keys()) {
    if (key.startsWith(prefix)) requests.delete(key);
  }

  for (const key of responses.keys()) {
    if (key.startsWith(prefix)) responses.delete(key);
  }

  for (const key of webSockets.keys()) {
    if (key.startsWith(prefix)) webSockets.delete(key);
  }

  for (const key of eventSourceSequences.keys()) {
    if (key.startsWith(prefix)) eventSourceSequences.delete(key);
  }

  for (const key of pendingEventSourceLastEventIds.keys()) {
    if (key.startsWith(prefix)) pendingEventSourceLastEventIds.delete(key);
  }

  for (const key of privateRevalidations.keys()) {
    if (key.startsWith(prefix)) privateRevalidations.delete(key);
  }
}

function finishCacheLookup(
  lookupId: string,
  hit: CacheReplayHit | null,
): void {
  const pending = pendingCacheLookups.get(lookupId);
  if (!pending) return;
  clearTimeout(pending.timeoutId);
  pendingCacheLookups.delete(lookupId);
  pending.resolve(hit);
}

function finishPrivateReadLookup(
  lookupId: string,
  hit: PrivateReadHit | null,
): void {
  const pending = pendingPrivateReadLookups.get(lookupId);
  if (!pending) return;
  clearTimeout(pending.timeoutId);
  pendingPrivateReadLookups.delete(lookupId);
  pending.resolve(hit);
}

function failLookupsForPort(port: chrome.runtime.Port): void {
  for (const [lookupId, pending] of pendingCacheLookups) {
    if (pending.port === port) finishCacheLookup(lookupId, null);
  }
  for (const [lookupId, pending] of pendingPrivateReadLookups) {
    if (pending.port === port) finishPrivateReadLookup(lookupId, null);
  }
}

function retireNativePort(port: chrome.runtime.Port): void {
  if (nativePort === port) nativePort = undefined;
  failLookupsForPort(port);
  for (const [captureId, owner] of capturePorts) {
    if (owner === port) capturePorts.delete(captureId);
  }
}

function handleNativeMessage(message: unknown): void {
  if (message === null || typeof message !== "object") return;
  const record = message as Record<string, unknown>;
  const type = typeof record.type === "string" ? record.type : undefined;

  if (type === "error") {
    // A writable-store error invalidates the capture transaction even if
    // the native host process itself remains connected. Do not send more
    // chunks or a successful finish for a rejected capture start/chunk.
    if (typeof record.capture_id === "string") {
      capturePorts.delete(record.capture_id);
    }
    console.error("Mirrarium native host error", message);
    return;
  }

  if (type === "extension_install_state") {
    const installedBuildId =
      typeof record.build_id === "string" ? record.build_id : undefined;
    if (installedBuildId && installedBuildId !== RUNNING_BUILD_ID) {
      if (staleInstalledBuildNotice !== installedBuildId) {
        staleInstalledBuildNotice = installedBuildId;
        console.info(
          "Mirrarium extension update installed; reload the unpacked extension in the browser",
          RUNNING_BUILD_ID,
          "->",
          installedBuildId,
        );
      }
    } else {
      staleInstalledBuildNotice = undefined;
    }
    return;
  }

  const lookupId =
    typeof record.lookup_id === "string" ? record.lookup_id : undefined;
  if (!lookupId) return;
  const privatePending = pendingPrivateReadLookups.get(lookupId);
  if (privatePending) {
    if (type === "private_read_miss") {
      finishPrivateReadLookup(lookupId, null);
      return;
    }

    if (type === "private_read_lookup_error") {
      console.warn("Mirrarium private-read lookup failed", record.message);
      finishPrivateReadLookup(lookupId, null);
      return;
    }

    if (type === "private_read_hit_start") {
      const mimeType =
        typeof record.mime_type === "string" ? record.mime_type : undefined;
      const bodyHash =
        typeof record.body_hash === "string" ? record.body_hash : undefined;
      const bodyBytes =
        typeof record.body_bytes === "number" &&
        Number.isSafeInteger(record.body_bytes) &&
        record.body_bytes >= 0
          ? record.body_bytes
          : undefined;
      const capturedAtMs =
        typeof record.captured_at_ms === "number" &&
        Number.isSafeInteger(record.captured_at_ms) &&
        record.captured_at_ms >= 0
          ? record.captured_at_ms
          : undefined;
      if (
        !mimeType ||
        !bodyHash ||
        bodyBytes === undefined ||
        capturedAtMs === undefined
      ) {
        finishPrivateReadLookup(lookupId, null);
        return;
      }
      privatePending.metadata = {
        mimeType,
        bodyHash,
        bodyBytes,
        capturedAtMs,
        cacheControl:
          typeof record.cache_control === "string"
            ? record.cache_control
            : undefined,
        etag: typeof record.etag === "string" ? record.etag : undefined,
        lastModified:
          typeof record.last_modified === "string"
            ? record.last_modified
            : undefined,
      };
      privatePending.chunks = [];
      privatePending.nextSequence = 0;
      return;
    }

    if (type === "private_read_hit_chunk") {
      const sequence =
        typeof record.sequence === "number" &&
        Number.isSafeInteger(record.sequence) &&
        record.sequence >= 0
          ? record.sequence
          : undefined;
      const dataBase64 =
        typeof record.data_base64 === "string" ? record.data_base64 : undefined;
      if (
        !privatePending.metadata ||
        sequence === undefined ||
        sequence !== privatePending.nextSequence ||
        dataBase64 === undefined
      ) {
        finishPrivateReadLookup(lookupId, null);
        return;
      }
      privatePending.chunks.push(dataBase64);
      privatePending.nextSequence += 1;
      return;
    }

    if (type === "private_read_hit_finish") {
      if (!privatePending.metadata) {
        finishPrivateReadLookup(lookupId, null);
        return;
      }
      const bodyBase64 = privatePending.chunks.join("");
      const expectedBase64Length =
        Math.ceil(privatePending.metadata.bodyBytes / 3) * 4;
      if (bodyBase64.length !== expectedBase64Length) {
        console.warn("Mirrarium private-read chunk length mismatch", lookupId);
        finishPrivateReadLookup(lookupId, null);
        return;
      }
      finishPrivateReadLookup(lookupId, {
        ...privatePending.metadata,
        bodyBase64,
      });
      return;
    }

    return;
  }

  const pending = pendingCacheLookups.get(lookupId);
  if (!pending) return;

  if (type === "cache_miss") {
    recordReplayOutcome(pending.url, pending.resourceType, "miss");
    finishCacheLookup(lookupId, null);
    return;
  }

  if (type === "cache_lookup_error") {
    console.warn("Mirrarium cache lookup failed", record.message);
    recordReplayOutcome(pending.url, pending.resourceType, "lookup_error");
    finishCacheLookup(lookupId, null);
    return;
  }

  if (type === "cache_hit_start") {
    const mimeType =
      typeof record.mime_type === "string" ? record.mime_type : undefined;
    const bodyHash =
      typeof record.body_hash === "string" ? record.body_hash : undefined;
    const bodyBytes =
      typeof record.body_bytes === "number" &&
      Number.isSafeInteger(record.body_bytes) &&
      record.body_bytes >= 0
        ? record.body_bytes
        : undefined;
    if (!mimeType || !bodyHash || bodyBytes === undefined) {
      recordReplayOutcome(pending.url, pending.resourceType, "lookup_error");
      finishCacheLookup(lookupId, null);
      return;
    }
    pending.metadata = {
      mimeType,
      bodyHash,
      bodyBytes,
      cacheControl:
        typeof record.cache_control === "string" ? record.cache_control : undefined,
      etag: typeof record.etag === "string" ? record.etag : undefined,
      lastModified:
        typeof record.last_modified === "string" ? record.last_modified : undefined,
    };
    pending.chunks = [];
    pending.nextSequence = 0;
    return;
  }

  if (type === "cache_hit_chunk") {
    const sequence =
      typeof record.sequence === "number" &&
      Number.isSafeInteger(record.sequence) &&
      record.sequence >= 0
        ? record.sequence
        : undefined;
    const dataBase64 =
      typeof record.data_base64 === "string" ? record.data_base64 : undefined;
    if (
      !pending.metadata ||
      sequence === undefined ||
      sequence !== pending.nextSequence ||
      dataBase64 === undefined
    ) {
      recordReplayOutcome(pending.url, pending.resourceType, "lookup_error");
      finishCacheLookup(lookupId, null);
      return;
    }
    pending.chunks.push(dataBase64);
    pending.nextSequence += 1;
    return;
  }

  if (type === "cache_hit_finish") {
    if (!pending.metadata) {
      recordReplayOutcome(pending.url, pending.resourceType, "lookup_error");
      finishCacheLookup(lookupId, null);
      return;
    }
    const bodyBase64 = pending.chunks.join("");
    const expectedBase64Length = Math.ceil(pending.metadata.bodyBytes / 3) * 4;
    if (bodyBase64.length !== expectedBase64Length) {
      console.warn("Mirrarium replay chunk length mismatch", lookupId);
      recordReplayOutcome(pending.url, pending.resourceType, "lookup_error");
      finishCacheLookup(lookupId, null);
      return;
    }
    finishCacheLookup(lookupId, {
      ...pending.metadata,
      bodyBase64,
    });
  }
}

function getNativePort(): chrome.runtime.Port | undefined {
  if (nativePort) return nativePort;

  try {
    const port = chrome.runtime.connectNative(NATIVE_HOST);
    port.onDisconnect.addListener(() => {
      retireNativePort(port);
      void chrome.runtime.lastError;
    });
    port.onMessage.addListener((message: unknown) => {
      if (nativePort === port) handleNativeMessage(message);
    });
    nativePort = port;
    port.postMessage({
      type: "extension_runtime_state",
      build_id: RUNNING_BUILD_ID,
    });
    return port;
  } catch (error) {
    if (nativePort) retireNativePort(nativePort);
    console.warn("Mirrarium native host unavailable", error);
    return undefined;
  }
}

function postNative(message: unknown): void {
  const record =
    message !== null && typeof message === "object"
      ? (message as Record<string, unknown>)
      : undefined;
  const type = record?.type;
  const captureId =
    typeof record?.capture_id === "string"
      ? record.capture_id
      : type === "capture_start" &&
          record?.metadata !== null &&
          typeof record?.metadata === "object"
        ? (record.metadata as Record<string, unknown>).capture_id
        : undefined;
  const captureMessage =
    type === "capture_start" ||
    type === "capture_chunk" ||
    type === "request_body_start" ||
    type === "request_body_chunk" ||
    type === "request_body_finish" ||
    type === "capture_finish";

  let port: chrome.runtime.Port | undefined;
  if (type === "capture_start") {
    port = getNativePort();
    if (port && typeof captureId === "string") {
      capturePorts.set(captureId, port);
    }
  } else if (captureMessage) {
    // Missing or retired owner means the old host lost this in-flight
    // transaction. Never connect a new host just to send an orphaned chunk.
    if (typeof captureId !== "string") return;
    port = capturePorts.get(captureId);
    if (!port || port !== nativePort) {
      capturePorts.delete(captureId);
      return;
    }
  } else {
    port = getNativePort();
  }
  if (!port) return;

  try {
    port.postMessage(message);
  } catch (error) {
    retireNativePort(port);
    console.warn("Mirrarium could not send to native host", error);
  } finally {
    if (type === "capture_finish" && typeof captureId === "string") {
      capturePorts.delete(captureId);
    }
  }
}

function checkInstalledExtensionVersion(): void {
  postNative({ type: "extension_install_state" });
}

function recordReplayOutcome(
  url: string,
  resourceType: string,
  outcome: "hit" | "miss" | "lookup_error" | "timeout" | "fulfill_error",
  bodyBytes = 0,
): void {
  postNative({
    type: "cache_replay_outcome",
    url,
    resource_type: resourceType,
    outcome,
    body_bytes: outcome === "hit" ? bodyBytes : 0,
  });
}

function recordPrivateRevalidationOutcome(
  outcome: "not_modified" | "refreshed" | "fulfill_error",
  bodyBytes = 0,
): void {
  postNative({
    type: "private_revalidation_outcome",
    outcome,
    body_bytes: outcome === "not_modified" ? bodyBytes : 0,
  });
}

function lookupCachedResponse(
  url: string,
  resourceType: string,
): Promise<CacheReplayHit | null> {
  const port = getNativePort();
  if (!port) return Promise.resolve(null);

  const lookupId = crypto.randomUUID();
  return new Promise((resolve) => {
    const timeoutId = setTimeout(() => {
      recordReplayOutcome(url, resourceType, "timeout");
      finishCacheLookup(lookupId, null);
    }, CACHE_LOOKUP_TIMEOUT_MS);
    pendingCacheLookups.set(lookupId, {
      port,
      resolve,
      timeoutId,
      url,
      resourceType,
      chunks: [],
      nextSequence: 0,
    });

    try {
      port.postMessage({
        type: "cache_lookup",
        lookup_id: lookupId,
        url,
        resource_type: resourceType,
      });
    } catch (error) {
      retireNativePort(port);
      console.warn("Mirrarium could not query local cache", error);
      finishCacheLookup(lookupId, null);
    }
  });
}

function lookupPrivateRead(url: string): Promise<PrivateReadHit | null> {
  const port = getNativePort();
  if (!port) return Promise.resolve(null);

  const lookupId = crypto.randomUUID();
  return new Promise((resolve) => {
    const timeoutId = setTimeout(() => {
      finishPrivateReadLookup(lookupId, null);
    }, CACHE_LOOKUP_TIMEOUT_MS);
    pendingPrivateReadLookups.set(lookupId, {
      port,
      resolve,
      timeoutId,
      url,
      chunks: [],
      nextSequence: 0,
    });

    try {
      port.postMessage({
        type: "private_read_lookup",
        lookup_id: lookupId,
        url,
      });
    } catch (error) {
      retireNativePort(port);
      console.warn("Mirrarium could not query private read cache", error);
      finishPrivateReadLookup(lookupId, null);
    }
  });
}

function isPrivateRevalidationCandidate(
  rawUrl: string,
  method: string,
  resourceType: string | undefined,
): boolean {
  if (method.toUpperCase() !== "GET" || !resourceType) {
    return false;
  }

  try {
    const url = new URL(rawUrl);
    const path = url.pathname.toLowerCase();
    const type = resourceType.toLowerCase();
    const safeIdentity =
      url.protocol === "https:" &&
      (url.hostname === "chatgpt.com" || url.hostname === "chat.openai.com") &&
      Array.from(url.searchParams.entries()).every(
        ([key, value]) =>
          !isSensitiveQueryKey(key) && value !== "[REDACTED]",
      ) &&
      url.hash === "";

    if (!safeIdentity) return false;

    if (type === "document") {
      return (
        path !== "/api/auth" &&
        !path.startsWith("/api/auth/") &&
        path !== "/auth" &&
        !path.startsWith("/auth/") &&
        !path.startsWith("/backend-api/auth/") &&
        !path.includes("/oauth/") &&
        !path.endsWith("/oauth") &&
        !path.includes("/login")
      );
    }

    return (
      ["fetch", "xhr"].includes(type) &&
      path.startsWith("/backend-api/") &&
      !path.startsWith("/backend-api/auth/")
    );
  } catch {
    return false;
  }
}

function requestHeadersWithValidators(
  headers: Record<string, string> | undefined,
  hit: PrivateReadHit,
): Array<{ name: string; value: string }> | null {
  const etag = safeReplayHeaderValue(hit.etag);
  const lastModified = safeReplayHeaderValue(hit.lastModified);
  if (!etag && !lastModified) return null;

  const result: Array<{ name: string; value: string }> = [];
  for (const [name, value] of Object.entries(headers ?? {})) {
    const normalized = name.toLowerCase();
    if (normalized === "if-none-match" || normalized === "if-modified-since") {
      continue;
    }
    result.push({ name, value: String(value) });
  }
  if (etag) {
    result.push({ name: "If-None-Match", value: etag });
  } else if (lastModified) {
    result.push({ name: "If-Modified-Since", value: lastModified });
  }
  return result;
}

function isReplayInterceptCandidate(
  rawUrl: string,
  method: string,
  resourceType: string | undefined,
): boolean {
  if (
    method.toUpperCase() !== "GET" ||
    !resourceType ||
    !["script", "stylesheet", "image", "font"].includes(resourceType.toLowerCase())
  ) {
    return false;
  }

  try {
    const url = new URL(rawUrl);
    const allowedPath =
      url.hostname === "cdn.oaistatic.com"
        ? true
        : (url.hostname === "chatgpt.com" || url.hostname === "chat.openai.com") &&
          url.pathname.startsWith("/_next/static/");
    return (
      url.protocol === "https:" &&
      allowedPath &&
      url.search === "" &&
      url.hash === ""
    );
  } catch {
    return false;
  }
}

function safeReplayHeaderValue(value: string | undefined): string | undefined {
  if (!value || value.includes("\r") || value.includes("\n")) return undefined;
  return value;
}

async function continuePausedRequest(tabId: number, requestId: string): Promise<void> {
  try {
    await chrome.debugger.sendCommand({ tabId }, "Fetch.continueRequest", {
      requestId,
    });
  } catch {
    // The tab/request may have disappeared while a local cache lookup was pending.
  }
}

async function continuePausedResponse(tabId: number, requestId: string): Promise<void> {
  try {
    await chrome.debugger.sendCommand({ tabId }, "Fetch.continueResponse", {
      requestId,
    });
  } catch {
    // The tab/request may have disappeared while a response was paused.
  }
}

async function beginPrivateRevalidation(
  tabId: number,
  event: {
    requestId: string;
    resourceType?: string;
    request: {
      method: string;
      url: string;
      headers?: Record<string, string>;
    };
  },
): Promise<void> {
  const hit = await lookupPrivateRead(event.request.url);
  if (!hit) {
    await continuePausedRequest(tabId, event.requestId);
    return;
  }

  const headers = requestHeadersWithValidators(event.request.headers, hit);
  if (!headers) {
    await continuePausedRequest(tabId, event.requestId);
    return;
  }

  const key = requestKey(tabId, event.requestId);
  privateRevalidations.set(key, hit);
  try {
    await chrome.debugger.sendCommand({ tabId }, "Fetch.continueRequest", {
      requestId: event.requestId,
      headers,
    });
  } catch (error) {
    privateRevalidations.delete(key);
    console.warn("Mirrarium could not start private revalidation", error);
    await continuePausedRequest(tabId, event.requestId);
  }
}

function privateRevalidationResponseHeaders(
  originHeaders: Array<{ name: string; value: string }> | undefined,
  hit: PrivateReadHit,
): Array<{ name: string; value: string }> {
  const replaced = new Set([
    "content-type",
    "cache-control",
    "etag",
    "last-modified",
    "x-mirrarium-revalidated",
  ]);
  const invalidForRebuiltBody = new Set([
    "content-length",
    "content-encoding",
    "transfer-encoding",
  ]);
  const responseHeaders: Array<{ name: string; value: string }> = [];

  for (const item of originHeaders ?? []) {
    const normalized = item.name.toLowerCase();
    const value = safeReplayHeaderValue(item.value);
    if (
      !value ||
      replaced.has(normalized) ||
      invalidForRebuiltBody.has(normalized)
    ) {
      continue;
    }
    responseHeaders.push({ name: item.name, value });
  }

  responseHeaders.push(
    { name: "Content-Type", value: hit.mimeType },
    { name: "X-Mirrarium-Revalidated", value: "hit" },
  );
  for (const [name, rawValue] of [
    ["Cache-Control", hit.cacheControl],
    ["ETag", hit.etag],
    ["Last-Modified", hit.lastModified],
  ] as const) {
    const value = safeReplayHeaderValue(rawValue);
    if (value) responseHeaders.push({ name, value });
  }

  return responseHeaders;
}

async function finishPrivateRevalidation(
  tabId: number,
  event: {
    requestId: string;
    responseStatusCode?: number;
    responseHeaders?: Array<{ name: string; value: string }>;
  },
): Promise<void> {
  const key = requestKey(tabId, event.requestId);
  const hit = privateRevalidations.get(key);
  privateRevalidations.delete(key);

  if (!hit) {
    await continuePausedResponse(tabId, event.requestId);
    return;
  }

  if (event.responseStatusCode !== 304) {
    if (event.responseStatusCode === 200) {
      recordPrivateRevalidationOutcome("refreshed");
    }
    await continuePausedResponse(tabId, event.requestId);
    return;
  }

  const responseHeaders = privateRevalidationResponseHeaders(
    event.responseHeaders,
    hit,
  );

  try {
    await chrome.debugger.sendCommand({ tabId }, "Fetch.fulfillRequest", {
      requestId: event.requestId,
      responseCode: 200,
      responsePhrase: "OK",
      responseHeaders,
      body: hit.bodyBase64,
    });
    recordPrivateRevalidationOutcome("not_modified", hit.bodyBytes);
  } catch (error) {
    console.warn("Mirrarium could not fulfill private 304 revalidation", error);
    recordPrivateRevalidationOutcome("fulfill_error");
    await continuePausedResponse(tabId, event.requestId);
  }
}

async function handlePausedRequest(
  tabId: number,
  event: {
    requestId: string;
    resourceType?: string;
    responseStatusCode?: number;
    responseErrorReason?: string;
    responseHeaders?: Array<{ name: string; value: string }>;
    request: {
      method: string;
      url: string;
      headers?: Record<string, string>;
    };
  },
): Promise<void> {
  if (
    event.responseStatusCode !== undefined ||
    event.responseErrorReason !== undefined
  ) {
    await finishPrivateRevalidation(tabId, event);
    return;
  }

  const resourceType = event.resourceType ?? "";
  if (
    isPrivateRevalidationCandidate(
      event.request.url,
      event.request.method,
      resourceType,
    )
  ) {
    await beginPrivateRevalidation(tabId, event);
    return;
  }

  if (
    !isReplayInterceptCandidate(
      event.request.url,
      event.request.method,
      resourceType,
    )
  ) {
    await continuePausedRequest(tabId, event.requestId);
    return;
  }

  const hit = await lookupCachedResponse(event.request.url, resourceType);
  if (!hit) {
    await continuePausedRequest(tabId, event.requestId);
    return;
  }

  const responseHeaders: Array<{ name: string; value: string }> = [
    { name: "Content-Type", value: hit.mimeType },
    { name: "X-Mirrarium-Cache", value: "hit" },
  ];
  for (const [name, rawValue] of [
    ["Cache-Control", hit.cacheControl],
    ["ETag", hit.etag],
    ["Last-Modified", hit.lastModified],
  ] as const) {
    const value = safeReplayHeaderValue(rawValue);
    if (value) responseHeaders.push({ name, value });
  }

  try {
    await chrome.debugger.sendCommand({ tabId }, "Fetch.fulfillRequest", {
      requestId: event.requestId,
      responseCode: 200,
      responsePhrase: "OK",
      responseHeaders,
      body: hit.bodyBase64,
    });
    recordReplayOutcome(
      event.request.url,
      resourceType,
      "hit",
      hit.bodyBytes,
    );
  } catch (error) {
    console.warn("Mirrarium could not fulfill cached response", error);
    recordReplayOutcome(event.request.url, resourceType, "fulfill_error");
    await continuePausedRequest(tabId, event.requestId);
  }
}

async function attach(tabId: number, url: string | undefined): Promise<void> {
  if (!isSupportedChatGptUrl(url)) return;
  const detaching = detachingTabs.get(tabId);
  if (detaching) {
    // A supported return navigation can arrive before the old debugger
    // has completed detach. Wait rather than losing this attach request.
    await detaching;
    const currentTab = await chrome.tabs.get(tabId).catch(() => undefined);
    if (isSupportedChatGptUrl(currentTab?.url)) {
      await attach(tabId, currentTab?.url);
    }
    return;
  }
  checkInstalledExtensionVersion();
  // Avoid overlapping asynchronous debugger.attach attempts for one tab.
  if (attachedTabs.has(tabId) || pendingAttachTabs.has(tabId)) return;
  const pending: PendingAttach = {
    cancelled: false,
    retryIfSupported: false,
    selfDetaching: false,
  };
  pendingAttachTabs.set(tabId, pending);

  let debuggerAttached = false;
  let ready = false;
  try {
    await chrome.debugger.attach({ tabId }, CDP_VERSION);
    debuggerAttached = true;
    if (pending.cancelled) return;
    await chrome.debugger.sendCommand({ tabId }, "Network.enable", {
      maxResourceBufferSize: CDP_MAX_RESOURCE_BUFFER_BYTES,
      maxTotalBufferSize: CDP_MAX_TOTAL_BUFFER_BYTES,
      enableDurableMessages: true,
    });
    if (pending.cancelled) return;
    fetchSetupTabs.add(tabId);
    await chrome.debugger.sendCommand({ tabId }, "Fetch.enable", {
      patterns: [
        {
          urlPattern: "https://chatgpt.com/_next/static/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com:*/_next/static/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com/_next/static/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com:*/_next/static/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://cdn.oaistatic.com/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://cdn.oaistatic.com:*/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com/backend-api/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com:*/backend-api/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com/backend-api/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com:*/backend-api/*",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com/backend-api/*",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chatgpt.com:*/backend-api/*",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chat.openai.com/backend-api/*",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chat.openai.com:*/backend-api/*",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chatgpt.com/*",
          resourceType: "Document",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com:*/*",
          resourceType: "Document",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com/*",
          resourceType: "Document",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chat.openai.com:*/*",
          resourceType: "Document",
          requestStage: "Request",
        },
        {
          urlPattern: "https://chatgpt.com/*",
          resourceType: "Document",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chatgpt.com:*/*",
          resourceType: "Document",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chat.openai.com/*",
          resourceType: "Document",
          requestStage: "Response",
        },
        {
          urlPattern: "https://chat.openai.com:*/*",
          resourceType: "Document",
          requestStage: "Response",
        },
      ],
    });
    if (pending.cancelled) return;
    // The URL supplied by onUpdated/onActivated may already be obsolete.
    // Rechecking the current top-level tab is mandatory before publication.
    const currentTab = await chrome.tabs.get(tabId);
    if (pending.cancelled || !isSupportedChatGptUrl(currentTab.url)) return;
    attachedTabs.add(tabId);
    ready = true;
  } catch (error) {
    if (!pending.cancelled) {
      console.warn("Mirrarium could not attach to ChatGPT tab", tabId, error);
    }
  } finally {
    fetchSetupTabs.delete(tabId);
    if (!ready && debuggerAttached) {
      pending.selfDetaching = true;
      try {
        await chrome.debugger.detach({ tabId });
      } catch {
        // Ignore cleanup failure after partial setup, tab closure or navigation.
      }
    }
    if (pendingAttachTabs.get(tabId) === pending) {
      pendingAttachTabs.delete(tabId);
    }
    if (pending.cancelled && pending.retryIfSupported) {
      // An out-and-back navigation can finish while the prior setup or
      // a completed attachment's detach is still unwinding.
      void (async () => {
        const detaching = detachingTabs.get(tabId);
        if (detaching) await detaching;
        const tab = await chrome.tabs.get(tabId);
        if (isSupportedChatGptUrl(tab.url)) await attach(tabId, tab.url);
      })().catch(() => {
        // Closed tabs have no debugger session to recover.
      });
    }
  }
}

async function detach(tabId: number): Promise<void> {
  const pending = pendingAttachTabs.get(tabId);
  if (pending) {
    pending.cancelled = true;
    pending.retryIfSupported = true;
    pending.selfDetaching = true;
  }
  clearTabState(tabId);

  const alreadyDetaching = detachingTabs.get(tabId);
  if (alreadyDetaching) {
    await alreadyDetaching;
    return;
  }
  if (!attachedTabs.has(tabId)) return;

  const operation = (async () => {
    try {
      await chrome.debugger.detach({ tabId });
    } catch {
      // The tab may already be gone or Chromium may already have detached us.
    } finally {
      attachedTabs.delete(tabId);
    }
  })();
  detachingTabs.set(tabId, operation);
  try {
    await operation;
  } finally {
    if (detachingTabs.get(tabId) === operation) {
      detachingTabs.delete(tabId);
    }
  }
  // If a tab navigated away and back before detach finished, onUpdated's
  // attach request might have arrived while attachedTabs still contained it.
  const currentTab = await chrome.tabs.get(tabId).catch(() => undefined);
  if (isSupportedChatGptUrl(currentTab?.url)) {
    void attach(tabId, currentTab?.url);
  }
}

function header(
  headers: Record<string, string | number> | undefined,
  name: string,
): string | undefined {
  if (!headers) return undefined;
  const target = name.toLowerCase();

  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === target) return String(value);
  }

  return undefined;
}

function isSensitiveHeaderName(name: string): boolean {
  const normalized = name.trim().toLowerCase().replaceAll("-", "_");
  return (
    [
      "authorization",
      "proxy_authorization",
      "cookie",
      "set_cookie",
      "authentication_info",
      "proxy_authenticate",
      "www_authenticate",
      "x_csrf_token",
      "x_xsrf_token",
      "x_auth_token",
      "x_api_key",
    ].includes(normalized) ||
    normalized.endsWith("_token") ||
    normalized.includes("credential")
  );
}

function isSensitiveQueryKey(key: string): boolean {
  const normalized = key.trim().toLowerCase().replaceAll("-", "_");
  return (
    [
      "token",
      "access_token",
      "id_token",
      "refresh_token",
      "session",
      "session_token",
      "auth",
      "authorization",
      "signature",
      "x_amz_signature",
      "x_goog_signature",
      "key",
      "api_key",
      "apikey",
      "code",
    ].includes(normalized) ||
    normalized.endsWith("_token") ||
    normalized.endsWith("_signature") ||
    normalized.includes("credential")
  );
}

function sanitizeUrlForStorage(rawUrl: string): string {
  try {
    const url = new URL(rawUrl);
    url.username = "";
    url.password = "";
    url.hash = "";
    for (const key of Array.from(url.searchParams.keys())) {
      if (isSensitiveQueryKey(key)) url.searchParams.set(key, "[REDACTED]");
    }
    return url.toString();
  } catch {
    const fragmentIndex = rawUrl.indexOf("#");
    const fragmentless =
      fragmentIndex === -1 ? rawUrl : rawUrl.slice(0, fragmentIndex);
    const queryIndex = fragmentless.indexOf("?");

    if (queryIndex === -1) return fragmentless;

    const prefix = fragmentless.slice(0, queryIndex);
    const query = new URLSearchParams(fragmentless.slice(queryIndex + 1));
    for (const key of Array.from(query.keys())) {
      if (isSensitiveQueryKey(key)) query.set(key, "[REDACTED]");
    }

    const serialized = query.toString();
    return serialized.length === 0 ? prefix : `${prefix}?${serialized}`;
  }
}

function sanitizeHeaders(
  headers: Record<string, string | number> | undefined,
): Record<string, string> {
  const sanitized: Record<string, string> = {};
  if (!headers) return sanitized;

  for (const [name, rawValue] of Object.entries(headers)) {
    const normalized = name.trim().toLowerCase().replaceAll("-", "_");
    let value = String(rawValue);
    if (isSensitiveHeaderName(name)) {
      value = "[REDACTED]";
    } else if (
      ["referer", "referrer", "location", "content_location"].includes(normalized)
    ) {
      value = sanitizeUrlForStorage(value);
    }
    sanitized[name] = value;
  }

  return sanitized;
}

function parseNonNegativeInteger(value: string | undefined): number | undefined {
  if (value === undefined || !/^\d+$/.test(value.trim())) return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) return undefined;
  return parsed;
}

function secondsToMilliseconds(value: number | undefined): number | undefined {
  if (value === undefined || !Number.isFinite(value) || value < 0) return undefined;
  return Math.trunc(value * 1000);
}

function responseMustNotHaveBody(
  method: string | undefined,
  status: number | undefined,
): boolean {
  if (method?.toUpperCase() === "HEAD") return true;
  if (status === undefined) return false;
  const code = Math.trunc(status);
  return (code >= 100 && code < 200) || code === 204 || code === 205 || code === 304;
}

function shouldSuppressResponseBody(rawUrl: string): boolean {
  try {
    const url = new URL(rawUrl);
    if (
      url.hostname !== "chatgpt.com" &&
      url.hostname !== "chat.openai.com"
    ) {
      return false;
    }

    const path = url.pathname.toLowerCase();
    return (
      path === "/api/auth" ||
      path.startsWith("/api/auth/") ||
      path === "/auth" ||
      path.startsWith("/auth/") ||
      path.startsWith("/backend-api/auth/") ||
      path.includes("/oauth/") ||
      path.endsWith("/oauth") ||
      path.includes("/login")
    );
  } catch {
    return false;
  }
}

function isSensitiveBodyKey(key: string): boolean {
  const normalized = key.trim().toLowerCase().replaceAll("-", "_");
  return (
    [
      "authorization",
      "password",
      "passwd",
      "secret",
      "client_secret",
      "access_token",
      "refresh_token",
      "id_token",
      "session_token",
      "auth_token",
      "api_key",
      "apikey",
      "cookie",
      "csrf_token",
    ].includes(normalized) ||
    normalized.endsWith("_token") ||
    normalized.includes("credential")
  );
}

function redactJsonSecrets(value: unknown): boolean {
  if (Array.isArray(value)) {
    let changed = false;
    for (const child of value) changed = redactJsonSecrets(child) || changed;
    return changed;
  }

  if (value === null || typeof value !== "object") return false;

  let changed = false;
  const record = value as Record<string, unknown>;
  for (const [key, child] of Object.entries(record)) {
    if (isSensitiveBodyKey(key)) {
      record[key] = "[REDACTED]";
      changed = true;
    } else {
      changed = redactJsonSecrets(child) || changed;
    }
  }
  return changed;
}

function sanitizeRequestBody(
  body: string,
  contentType: string | undefined,
): { body?: string; error?: string } {
  const type = (contentType ?? "").toLowerCase();

  if (type.includes("multipart/form-data")) {
    return { error: "suppressed:multipart_request_body_not_archived" };
  }

  const trimmed = body.trimStart();
  const looksJson =
    type.includes("json") || trimmed.startsWith("{") || trimmed.startsWith("[");

  if (looksJson) {
    try {
      const value: unknown = JSON.parse(body);
      return {
        body: redactJsonSecrets(value) ? JSON.stringify(value) : body,
      };
    } catch {
      return { error: "suppressed:unparseable_json_request_body" };
    }
  }

  if (type.includes("application/x-www-form-urlencoded")) {
    const params = new URLSearchParams(body);
    let changed = false;
    for (const key of Array.from(params.keys())) {
      if (isSensitiveBodyKey(key)) {
        params.set(key, "[REDACTED]");
        changed = true;
      }
    }
    return { body: changed ? params.toString() : body };
  }

  return { error: "suppressed:unsupported_request_body_content_type" };
}

function postRequestUtf8Body(captureId: string, body: string): void {
  const bytes = new TextEncoder().encode(body);
  let sequence = 0;

  for (let offset = 0; offset < bytes.length; offset += RAW_CHUNK_BYTES) {
    postNative({
      type: "request_body_chunk",
      capture_id: captureId,
      sequence,
      data_base64: bytesToBase64(bytes.subarray(offset, offset + RAW_CHUNK_BYTES)),
    });
    sequence += 1;
  }
}

async function captureRequestBody(
  tabId: number,
  requestId: string,
  captureId: string,
  request: RequestMetadata | undefined,
  allowPostDataFetch = true,
): Promise<void> {
  if (!request || (request.postData === undefined && !request.hasPostData)) return;

  postNative({
    type: "request_body_start",
    capture_id: captureId,
    metadata: {
      content_type: request.contentType,
      has_post_data: request.hasPostData ?? request.postData !== undefined,
      post_data_entry_count: request.postDataEntryCount,
      declared_content_length: request.declaredContentLength,
    },
  });

  if (shouldSuppressResponseBody(request.url)) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "suppressed:credential_endpoint",
    });
    return;
  }

  if (request.contentType?.toLowerCase().includes("multipart/form-data")) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "suppressed:multipart_request_body_not_archived",
    });
    return;
  }

  if (
    request.declaredContentLength !== undefined &&
    request.declaredContentLength > MAX_REQUEST_BODY_BYTES
  ) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "suppressed:request_body_too_large",
    });
    return;
  }

  let body = request.postData;
  if (body === undefined && request.hasPostData && !allowPostDataFetch) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "request body unavailable after redirect",
    });
    return;
  }

  if (body === undefined && request.hasPostData) {
    try {
      const result = (await chrome.debugger.sendCommand(
        { tabId },
        "Network.getRequestPostData",
        { requestId },
      )) as { postData: string };
      body = result.postData;
    } catch (error) {
      postNative({
        type: "request_body_finish",
        capture_id: captureId,
        body_error: String(error),
      });
      return;
    }
  }

  if (body === undefined) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "request body unavailable",
    });
    return;
  }

  if (utf8ByteLength(body) > MAX_REQUEST_BODY_BYTES) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "suppressed:request_body_too_large",
    });
    return;
  }

  const sanitized = sanitizeRequestBody(body, request.contentType);
  if (sanitized.error || sanitized.body === undefined) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: sanitized.error ?? "request body sanitizer failed",
    });
    return;
  }

  if (utf8ByteLength(sanitized.body) > MAX_REQUEST_BODY_BYTES) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: "suppressed:request_body_too_large",
    });
    return;
  }

  postRequestUtf8Body(captureId, sanitized.body);
  postNative({
    type: "request_body_finish",
    capture_id: captureId,
    body_error: null,
  });
}

function postBase64Body(captureId: string, body: string): void {
  const chunkSize = BASE64_CHUNK_CHARS - (BASE64_CHUNK_CHARS % 4);
  let sequence = 0;

  for (let offset = 0; offset < body.length; offset += chunkSize) {
    postNative({
      type: "capture_chunk",
      capture_id: captureId,
      sequence,
      data_base64: body.slice(offset, offset + chunkSize),
    });
    sequence += 1;
  }
}

function utf8ByteLength(value: string): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

function base64DecodedByteLength(value: string): number | undefined {
  if (value.length === 0) return 0;
  if (value.length % 4 !== 0) return undefined;

  let padding = 0;
  if (value.endsWith("==")) {
    padding = 2;
  } else if (value.endsWith("=")) {
    padding = 1;
  }
  return (value.length / 4) * 3 - padding;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const block = 32 * 1024;

  for (let offset = 0; offset < bytes.length; offset += block) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + block));
  }

  return btoa(binary);
}

function postUtf8Body(captureId: string, body: string): void {
  const bytes = new TextEncoder().encode(body);
  let sequence = 0;

  for (let offset = 0; offset < bytes.length; offset += RAW_CHUNK_BYTES) {
    postNative({
      type: "capture_chunk",
      capture_id: captureId,
      sequence,
      data_base64: bytesToBase64(bytes.subarray(offset, offset + RAW_CHUNK_BYTES)),
    });
    sequence += 1;
  }
}

function responseMetadataFromCdp(
  response: CdpResponse,
  resourceType: string,
): ResponseMetadata {
  return {
    url: response.url,
    status: response.status,
    mimeType: response.mimeType,
    resourceType,
    etag: header(response.headers, "etag"),
    lastModified: header(response.headers, "last-modified"),
    cacheControl: header(response.headers, "cache-control"),
    headers: sanitizeHeaders(response.headers),
    responseProtocol: response.protocol,
    responseTimeMs: secondsToMilliseconds(response.responseTime),
    fromDiskCache: response.fromDiskCache ?? false,
    fromServiceWorker: response.fromServiceWorker ?? false,
    fromPrefetchCache: response.fromPrefetchCache ?? false,
    encodedDataLength: response.encodedDataLength,
  };
}

function postCaptureStart(
  tabId: number,
  requestId: string,
  captureId: string,
  request: RequestMetadata | undefined,
  response: ResponseMetadata | undefined,
): void {
  postNative({
    type: "capture_start",
    metadata: {
      capture_id: captureId,
      tab_id: tabId,
      request_id: requestId,
      method: request?.method ?? "GET",
      url: request?.url ?? response?.url ?? "",
      status: Math.trunc(response?.status ?? 0),
      mime_type: response?.mimeType ?? "",
      resource_type: response?.resourceType ?? request?.resourceType ?? "Unknown",
      etag: response?.etag,
      last_modified: response?.lastModified,
      cache_control: response?.cacheControl,
      provenance: {
        frame_id: request?.frameId,
        loader_id: request?.loaderId,
        lifecycle_id: request?.lifecycleId,
        redirect_hop: request?.redirectHop,
        redirected_from_url: request?.redirectedFromUrl,
        document_url: request?.documentUrl,
        initiator_type: request?.initiatorType,
        request_wall_time_ms: request?.requestWallTimeMs,
        response_time_ms: response?.responseTimeMs,
        response_protocol: response?.responseProtocol,
        served_from_cache: request?.servedFromCache ?? false,
        from_disk_cache: response?.fromDiskCache ?? false,
        from_service_worker: response?.fromServiceWorker ?? false,
        from_prefetch_cache: response?.fromPrefetchCache ?? false,
        request_headers: request?.headers ?? {},
        response_headers: response?.headers ?? {},
      },
    },
  });
}

function captureWebSocketFrame(
  tabId: number,
  requestId: string,
  direction: "sent" | "received",
  frame: CdpWebSocketFrame,
): void {
  if (!attachedTabs.has(tabId)) return;

  const socket = webSockets.get(requestKey(tabId, requestId));
  if (!socket) return;

  const frameSequence = socket.nextFrameSequence;
  socket.nextFrameSequence += 1;

  const captureId = crypto.randomUUID();
  const method = direction === "sent" ? "WS_SEND" : "WS_RECV";
  let body: string | undefined;
  let mimeType = "application/octet-stream";
  let bodyError: string | null = null;

  if (shouldSuppressResponseBody(socket.url)) {
    bodyError = "suppressed:credential_endpoint";
  } else if (frame.opcode === 1) {
    mimeType = "application/json";
    const payloadBytes = utf8ByteLength(frame.payloadData);
    if (payloadBytes > MAX_WEBSOCKET_JSON_FRAME_BYTES) {
      bodyError = "suppressed:websocket_text_frame_too_large";
    } else {
      try {
        const value: unknown = JSON.parse(frame.payloadData);
        body = redactJsonSecrets(value) ? JSON.stringify(value) : frame.payloadData;
        if (utf8ByteLength(body) > MAX_WEBSOCKET_JSON_FRAME_BYTES) {
          body = undefined;
          bodyError = "suppressed:websocket_text_frame_too_large";
        }
      } catch {
        bodyError = "suppressed:unparseable_websocket_text_frame";
        mimeType = "text/plain; charset=utf-8";
      }
    }
  } else if (frame.opcode === 2) {
    bodyError = "suppressed:websocket_binary_frame_not_archived";
  } else {
    return;
  }

  postNative({
    type: "capture_start",
    metadata: {
      capture_id: captureId,
      tab_id: tabId,
      request_id: `${requestId}:ws:${frameSequence}`,
      method,
      url: socket.url,
      status: Math.trunc(socket.status ?? 0),
      mime_type: mimeType,
      resource_type: "WebSocketFrame",
      provenance: {
        lifecycle_id: socket.lifecycleId,
        transport_sequence: frameSequence,
        initiator_type: socket.initiatorType,
        request_wall_time_ms: socket.requestWallTimeMs,
        response_protocol: "websocket",
        request_headers: socket.requestHeaders,
        response_headers: socket.responseHeaders,
      },
    },
  });

  if (body === undefined || bodyError !== null) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: undefined,
      body_error: bodyError ?? "suppressed:websocket_frame_body_unavailable",
    });
    return;
  }

  const bodyBytes = utf8ByteLength(body);
  postUtf8Body(captureId, body);
  postNative({
    type: "capture_finish",
    capture_id: captureId,
    encoded_data_length: bodyBytes,
    body_error: null,
  });
}

async function captureRedirectHop(
  tabId: number,
  requestId: string,
  request: RequestMetadata,
  response: ResponseMetadata,
): Promise<void> {
  const captureId = crypto.randomUUID();
  postCaptureStart(tabId, requestId, captureId, request, response);
  await captureRequestBody(tabId, requestId, captureId, request, false);
  postNative({
    type: "capture_finish",
    capture_id: captureId,
    encoded_data_length:
      response.encodedDataLength === undefined
        ? undefined
        : Math.max(0, Math.trunc(response.encodedDataLength)),
    body_error: "suppressed:redirect_body_not_available",
  });
}

function sanitizeEventSourceData(data: string): string {
  try {
    const value: unknown = JSON.parse(data);
    return redactJsonSecrets(value) ? JSON.stringify(value) : data;
  } catch {
    return data;
  }
}

function sanitizeEventSourceField(value: string): string {
  return value.replace(/[\r\n]/g, " ");
}

function canonicalEventSourceMessage(
  eventName: string,
  eventId: string,
  data: string,
): string {
  const lines: string[] = [];
  if (eventName) lines.push(`event: ${sanitizeEventSourceField(eventName)}`);
  if (eventId) lines.push(`id: ${sanitizeEventSourceField(eventId)}`);

  const normalizedData = data.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  for (const line of normalizedData.split("\n")) {
    lines.push(`data: ${line}`);
  }

  return `${lines.join("\n")}\n\n`;
}

function captureEventSourceMessage(
  tabId: number,
  requestId: string,
  eventName: string,
  eventId: string,
  data: string,
): void {
  if (!attachedTabs.has(tabId)) return;

  const key = requestKey(tabId, requestId);
  const request = requests.get(key);
  const response = responses.get(key);
  const rawUrl = request?.url ?? response?.url;
  if (!rawUrl || !isSupportedChatGptUrl(rawUrl)) return;

  const sequence = eventSourceSequences.get(key) ?? 0;
  eventSourceSequences.set(key, sequence + 1);

  const captureId = crypto.randomUUID();
  let body: string | undefined;
  let bodyError: string | null = null;

  if (shouldSuppressResponseBody(rawUrl)) {
    bodyError = "suppressed:credential_endpoint";
  } else if (utf8ByteLength(data) > MAX_EVENTSOURCE_MESSAGE_BYTES) {
    bodyError = "suppressed:eventsource_message_too_large";
  } else {
    const sanitizedData = sanitizeEventSourceData(data);
    const canonical = canonicalEventSourceMessage(
      eventName,
      eventId,
      sanitizedData,
    );
    if (utf8ByteLength(canonical) > MAX_EVENTSOURCE_MESSAGE_BYTES) {
      bodyError = "suppressed:eventsource_message_too_large";
    } else {
      body = canonical;
    }
  }

  postNative({
    type: "capture_start",
    metadata: {
      capture_id: captureId,
      tab_id: tabId,
      request_id: `${requestId}:sse:${sequence}`,
      method: "SSE_RECV",
      url: sanitizeUrlForStorage(rawUrl ?? ""),
      status: Math.trunc(response?.status ?? 0),
      mime_type: "text/event-stream; charset=utf-8",
      resource_type: "EventSourceMessage",
      provenance: {
        frame_id: request?.frameId,
        loader_id: request?.loaderId,
        lifecycle_id: request?.lifecycleId,
        transport_sequence: sequence,
        redirect_hop: request?.redirectHop,
        redirected_from_url: request?.redirectedFromUrl,
        document_url: request?.documentUrl,
        initiator_type: request?.initiatorType,
        request_wall_time_ms: request?.requestWallTimeMs,
        response_time_ms: response?.responseTimeMs,
        response_protocol: response?.responseProtocol,
        served_from_cache: request?.servedFromCache ?? false,
        from_disk_cache: response?.fromDiskCache ?? false,
        from_service_worker: response?.fromServiceWorker ?? false,
        from_prefetch_cache: response?.fromPrefetchCache ?? false,
        request_headers: request?.headers ?? {},
        response_headers: response?.headers ?? {},
      },
    },
  });

  if (body === undefined || bodyError !== null) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: undefined,
      body_error: bodyError ?? "suppressed:eventsource_message_body_unavailable",
    });
    return;
  }

  const bodyBytes = utf8ByteLength(body);
  postUtf8Body(captureId, body);
  postNative({
    type: "capture_finish",
    capture_id: captureId,
    encoded_data_length: bodyBytes,
    body_error: null,
  });
}

async function captureBody(
  tabId: number,
  requestId: string,
  encodedDataLength: number | undefined,
  failure?: string,
): Promise<void> {
  if (!attachedTabs.has(tabId)) return;

  const key = requestKey(tabId, requestId);
  const request = requests.get(key);
  const response = responses.get(key);
  requests.delete(key);
  responses.delete(key);
  eventSourceSequences.delete(key);
  pendingEventSourceLastEventIds.delete(key);

  if (!request && !response) return;

  const captureId = crypto.randomUUID();

  postCaptureStart(tabId, requestId, captureId, request, response);

  await captureRequestBody(tabId, requestId, captureId, request);

  if (response && shouldSuppressResponseBody(response.url)) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length:
        encodedDataLength === undefined
          ? undefined
          : Math.max(0, Math.trunc(encodedDataLength)),
      body_error: "suppressed:credential_endpoint",
    });
    return;
  }

  if (failure) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: encodedDataLength,
      body_error: failure,
    });
    return;
  }

  if (responseMustNotHaveBody(request?.method, response?.status)) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length:
        encodedDataLength === undefined
          ? undefined
          : Math.max(0, Math.trunc(encodedDataLength)),
      body_error: "suppressed:no_response_body_expected",
    });
    return;
  }

  const normalizedEncodedDataLength =
    encodedDataLength === undefined
      ? undefined
      : Math.max(0, Math.trunc(encodedDataLength));
  if (
    normalizedEncodedDataLength !== undefined &&
    normalizedEncodedDataLength > MAX_RESPONSE_BODY_BYTES
  ) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: normalizedEncodedDataLength,
      body_error: "suppressed:response_body_too_large",
    });
    return;
  }

  try {
    const result = (await chrome.debugger.sendCommand(
      { tabId },
      "Network.getResponseBody",
      { requestId },
    )) as { body: string; base64Encoded: boolean };

    const actualBodyBytes = result.base64Encoded
      ? base64DecodedByteLength(result.body)
      : utf8ByteLength(result.body);
    if (
      actualBodyBytes !== undefined &&
      actualBodyBytes > MAX_RESPONSE_BODY_BYTES
    ) {
      postNative({
        type: "capture_finish",
        capture_id: captureId,
        encoded_data_length: normalizedEncodedDataLength,
        body_error: "suppressed:response_body_too_large",
      });
      return;
    }

    if (result.base64Encoded) {
      postBase64Body(captureId, result.body);
    } else {
      postUtf8Body(captureId, result.body);
    }

    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: normalizedEncodedDataLength,
      body_error: null,
    });
  } catch (error) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length: normalizedEncodedDataLength,
      body_error: String(error),
    });
  }
}

chrome.runtime.onInstalled.addListener(() => {
  checkInstalledExtensionVersion();
});

chrome.debugger.onDetach.addListener((source) => {
  if (source.tabId !== undefined) {
    const pending = pendingAttachTabs.get(source.tabId);
    if (pending && !pending.selfDetaching) {
      // An external debugger takeover/detach is not an origin-navigation
      // retry. Never resurrect our attachment against the other debugger.
      pending.cancelled = true;
      pending.retryIfSupported = false;
    }
    fetchSetupTabs.delete(source.tabId);
    attachedTabs.delete(source.tabId);
    clearTabState(source.tabId);
  }
});

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (source.tabId === undefined) return;

  const tabId = source.tabId;

  if (method === "Fetch.requestPaused") {
    if (!attachedTabs.has(tabId) && !fetchSetupTabs.has(tabId)) return;
    const event = params as {
      requestId: string;
      resourceType?: string;
      responseStatusCode?: number;
      responseErrorReason?: string;
      request: {
        method: string;
        url: string;
      };
    };
    if (!attachedTabs.has(tabId)) {
      // Fetch.enable may pause requests while the debugger is being set up.
      // Never apply replay/revalidation policy before the top-level origin
      // has been rechecked and the attachment is fully committed.
      if (
        event.responseStatusCode !== undefined ||
        event.responseErrorReason !== undefined
      ) {
        void continuePausedResponse(tabId, event.requestId);
      } else {
        void continuePausedRequest(tabId, event.requestId);
      }
      return;
    }
    void handlePausedRequest(tabId, event);
    return;
  }

  if (!attachedTabs.has(tabId)) return;

  if (method === "Network.webSocketCreated") {
    const event = params as {
      requestId: string;
      url: string;
      initiator?: { type?: string };
    };
    if (!isSupportedChatGptSocketUrl(event.url)) return;
    webSockets.set(requestKey(tabId, event.requestId), {
      url: sanitizeUrlForStorage(event.url),
      lifecycleId: crypto.randomUUID(),
      initiatorType: event.initiator?.type,
      requestHeaders: {},
      responseHeaders: {},
      nextFrameSequence: 0,
    });
    return;
  }

  if (method === "Network.webSocketWillSendHandshakeRequest") {
    const event = params as {
      requestId: string;
      wallTime?: number;
      request: { headers?: Record<string, string | number> };
    };
    const socket = webSockets.get(requestKey(tabId, event.requestId));
    if (socket) {
      socket.requestWallTimeMs = secondsToMilliseconds(event.wallTime);
      socket.requestHeaders = sanitizeHeaders(event.request.headers);
    }
    return;
  }

  if (method === "Network.webSocketHandshakeResponseReceived") {
    const event = params as {
      requestId: string;
      response: {
        status?: number;
        headers?: Record<string, string | number>;
      };
    };
    const socket = webSockets.get(requestKey(tabId, event.requestId));
    if (socket) {
      socket.status = event.response.status;
      socket.responseHeaders = sanitizeHeaders(event.response.headers);
    }
    return;
  }

  if (
    method === "Network.webSocketFrameSent" ||
    method === "Network.webSocketFrameReceived"
  ) {
    const event = params as {
      requestId: string;
      response: CdpWebSocketFrame;
    };
    captureWebSocketFrame(
      tabId,
      event.requestId,
      method === "Network.webSocketFrameSent" ? "sent" : "received",
      event.response,
    );
    return;
  }

  if (method === "Network.webSocketClosed") {
    const event = params as { requestId: string };
    webSockets.delete(requestKey(tabId, event.requestId));
    return;
  }

  if (method === "Network.eventSourceMessageReceived") {
    const event = params as {
      requestId: string;
      eventName: string;
      eventId: string;
      data: string;
    };
    captureEventSourceMessage(
      tabId,
      event.requestId,
      event.eventName,
      event.eventId,
      event.data,
    );
    return;
  }

  if (method === "Network.requestWillBeSentExtraInfo") {
    const event = params as {
      requestId: string;
      headers?: Record<string, string | number>;
    };
    const lastEventId = header(event.headers, "last-event-id");
    if (lastEventId === undefined) return;

    const key = requestKey(tabId, event.requestId);
    const request = requests.get(key);
    const sanitized = sanitizeEventSourceField(lastEventId);
    if (request) {
      if (request.resourceType === "EventSource") {
        request.headers["Last-Event-ID"] = sanitized;
      }
    } else {
      pendingEventSourceLastEventIds.set(key, sanitized);
    }
    return;
  }

  if (method === "Network.requestWillBeSent") {
    const event = params as {
      requestId: string;
      loaderId?: string;
      frameId?: string;
      documentURL?: string;
      wallTime?: number;
      type?: string;
      initiator?: { type?: string };
      redirectResponse?: CdpResponse;
      request: {
        method: string;
        url: string;
        postData?: string;
        hasPostData?: boolean;
        postDataEntries?: Array<{ bytes?: string }>;
        headers?: Record<string, string | number>;
      };
    };
    const key = requestKey(tabId, event.requestId);
    const previous = requests.get(key);

    let lifecycleId = previous?.lifecycleId ?? crypto.randomUUID();
    let redirectHop = previous?.redirectHop ?? 0;
    let redirectedFromUrl: string | undefined;

    if (event.redirectResponse && previous) {
      const redirectResponse = responseMetadataFromCdp(
        event.redirectResponse,
        previous.resourceType ?? event.type ?? "Other",
      );
      responses.delete(key);
      void captureRedirectHop(tabId, event.requestId, previous, redirectResponse);
      lifecycleId = previous.lifecycleId;
      redirectHop = previous.redirectHop + 1;
      redirectedFromUrl = previous.url;
    }

    const requestHeaders = sanitizeHeaders(event.request.headers);
    const pendingLastEventId = pendingEventSourceLastEventIds.get(key);
    pendingEventSourceLastEventIds.delete(key);
    if (event.type === "EventSource" && pendingLastEventId !== undefined) {
      requestHeaders["Last-Event-ID"] = pendingLastEventId;
    }

    requests.set(key, {
      method: event.request.method,
      url: event.request.url,
      postData: event.request.postData,
      hasPostData: event.request.hasPostData,
      contentType: header(event.request.headers, "content-type"),
      postDataEntryCount: event.request.postDataEntries?.length,
      declaredContentLength: parseNonNegativeInteger(
        header(event.request.headers, "content-length"),
      ),
      headers: requestHeaders,
      frameId: event.frameId,
      loaderId: event.loaderId,
      documentUrl: event.documentURL,
      initiatorType: event.initiator?.type,
      requestWallTimeMs: secondsToMilliseconds(event.wallTime),
      servedFromCache: false,
      lifecycleId,
      redirectHop,
      redirectedFromUrl,
      resourceType: event.type,
    });
    return;
  }

  if (method === "Network.requestServedFromCache") {
    const event = params as { requestId: string };
    const request = requests.get(requestKey(tabId, event.requestId));
    if (request) request.servedFromCache = true;
    return;
  }

  if (method === "Network.responseReceived") {
    const event = params as {
      requestId: string;
      type: string;
      response: CdpResponse;
    };
    responses.set(
      requestKey(tabId, event.requestId),
      responseMetadataFromCdp(event.response, event.type),
    );
    return;
  }

  if (method === "Network.loadingFinished") {
    const event = params as {
      requestId: string;
      encodedDataLength?: number;
    };
    void captureBody(tabId, event.requestId, event.encodedDataLength);
    return;
  }

  if (method === "Network.loadingFailed") {
    const event = params as {
      requestId: string;
      errorText?: string;
    };
    void captureBody(
      tabId,
      event.requestId,
      undefined,
      event.errorText ?? "network load failed",
    );
  }
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  const url = changeInfo.url ?? tab.url;

  if (changeInfo.url && !isSupportedChatGptUrl(changeInfo.url)) {
    void detach(tabId);
    return;
  }

  if (changeInfo.url || changeInfo.status === "complete") {
    void attach(tabId, url);
  }
});

chrome.tabs.onActivated.addListener(({ tabId }) => {
  void chrome.tabs.get(tabId).then((tab) => {
    if (isSupportedChatGptUrl(tab.url)) {
      void attach(tabId, tab.url);
    } else {
      void detach(tabId);
    }
  });
});

chrome.tabs.onRemoved.addListener((tabId) => {
  const pending = pendingAttachTabs.get(tabId);
  if (pending) {
    pending.cancelled = true;
    pending.retryIfSupported = false;
  }
  fetchSetupTabs.delete(tabId);
  attachedTabs.delete(tabId);
  clearTabState(tabId);
});

void chrome.tabs.query({}).then((tabs) => {
  for (const tab of tabs) {
    if (tab.id !== undefined && isSupportedChatGptUrl(tab.url)) {
      void attach(tab.id, tab.url);
    }
  }
});

const NATIVE_HOST = "com.sguzman.mirrarium";
const CDP_VERSION = "1.3";
const BASE64_CHUNK_CHARS = 512 * 1024;
const RAW_CHUNK_BYTES = 384 * 1024;
const CACHE_LOOKUP_TIMEOUT_MS = 750;

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
  resolve: (hit: CacheReplayHit | null) => void;
  timeoutId: number;
  url: string;
  resourceType: string;
  metadata?: Omit<CacheReplayHit, "bodyBase64">;
  chunks: string[];
  nextSequence: number;
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
const fetchSetupTabs = new Set<number>();
const requests = new Map<string, RequestMetadata>();
const responses = new Map<string, ResponseMetadata>();
const pendingCacheLookups = new Map<string, PendingCacheLookup>();
let nativePort: chrome.runtime.Port | undefined;

function requestKey(tabId: number, requestId: string): string {
  return `${tabId}:${requestId}`;
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

function failAllCacheLookups(): void {
  for (const lookupId of Array.from(pendingCacheLookups.keys())) {
    finishCacheLookup(lookupId, null);
  }
}

function handleNativeMessage(message: unknown): void {
  if (message === null || typeof message !== "object") return;
  const record = message as Record<string, unknown>;
  const type = typeof record.type === "string" ? record.type : undefined;

  if (type === "error") {
    console.error("Mirrarium native host error", message);
    return;
  }

  const lookupId =
    typeof record.lookup_id === "string" ? record.lookup_id : undefined;
  if (!lookupId) return;
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
      if (nativePort === port) nativePort = undefined;
      failAllCacheLookups();
      void chrome.runtime.lastError;
    });
    port.onMessage.addListener(handleNativeMessage);
    nativePort = port;
    return port;
  } catch (error) {
    failAllCacheLookups();
    console.warn("Mirrarium native host unavailable", error);
    return undefined;
  }
}

function postNative(message: unknown): void {
  const port = getNativePort();
  if (!port) return;

  try {
    port.postMessage(message);
  } catch (error) {
    if (nativePort === port) nativePort = undefined;
    console.warn("Mirrarium could not send to native host", error);
  }
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
      if (nativePort === port) nativePort = undefined;
      console.warn("Mirrarium could not query local cache", error);
      finishCacheLookup(lookupId, null);
    }
  });
}

function isReplayInterceptCandidate(
  rawUrl: string,
  method: string,
  resourceType: string | undefined,
): boolean {
  if (
    method.toUpperCase() !== "GET" ||
    !resourceType ||
    !["script", "stylesheet"].includes(resourceType.toLowerCase())
  ) {
    return false;
  }

  try {
    const url = new URL(rawUrl);
    return (
      url.protocol === "https:" &&
      (url.hostname === "chatgpt.com" || url.hostname === "chat.openai.com") &&
      url.pathname.startsWith("/_next/static/") &&
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

async function handlePausedRequest(
  tabId: number,
  event: {
    requestId: string;
    resourceType?: string;
    request: { method: string; url: string };
  },
): Promise<void> {
  const resourceType = event.resourceType ?? "";
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
  if (!isSupportedChatGptUrl(url) || attachedTabs.has(tabId)) return;

  let debuggerAttached = false;
  try {
    await chrome.debugger.attach({ tabId }, CDP_VERSION);
    debuggerAttached = true;
    await chrome.debugger.sendCommand({ tabId }, "Network.enable");
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
      ],
    });
    fetchSetupTabs.delete(tabId);
    attachedTabs.add(tabId);
  } catch (error) {
    fetchSetupTabs.delete(tabId);
    attachedTabs.delete(tabId);
    if (debuggerAttached) {
      try {
        await chrome.debugger.detach({ tabId });
      } catch {
        // Ignore cleanup failure after a partial debugger setup.
      }
    }
    console.warn("Mirrarium could not attach to ChatGPT tab", tabId, error);
  }
}

async function detach(tabId: number): Promise<void> {
  clearTabState(tabId);

  if (!attachedTabs.has(tabId)) return;

  try {
    await chrome.debugger.detach({ tabId });
  } catch {
    // The tab may already be gone or Chromium may already have detached us.
  } finally {
    attachedTabs.delete(tabId);
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

  const sanitized = sanitizeRequestBody(body, request.contentType);
  if (sanitized.error || sanitized.body === undefined) {
    postNative({
      type: "request_body_finish",
      capture_id: captureId,
      body_error: sanitized.error ?? "request body sanitizer failed",
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

  try {
    const result = (await chrome.debugger.sendCommand(
      { tabId },
      "Network.getResponseBody",
      { requestId },
    )) as { body: string; base64Encoded: boolean };

    if (result.base64Encoded) {
      postBase64Body(captureId, result.body);
    } else {
      postUtf8Body(captureId, result.body);
    }

    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length:
        encodedDataLength === undefined
          ? undefined
          : Math.max(0, Math.trunc(encodedDataLength)),
      body_error: null,
    });
  } catch (error) {
    postNative({
      type: "capture_finish",
      capture_id: captureId,
      encoded_data_length:
        encodedDataLength === undefined
          ? undefined
          : Math.max(0, Math.trunc(encodedDataLength)),
      body_error: String(error),
    });
  }
}

chrome.debugger.onDetach.addListener((source) => {
  if (source.tabId !== undefined) {
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
      request: {
        method: string;
        url: string;
      };
    };
    void handlePausedRequest(tabId, event);
    return;
  }

  if (!attachedTabs.has(tabId)) return;

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
      headers: sanitizeHeaders(event.request.headers),
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

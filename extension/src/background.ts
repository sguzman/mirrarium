const NATIVE_HOST = "com.sguzman.mirrarium";
const CDP_VERSION = "1.3";
const BASE64_CHUNK_CHARS = 512 * 1024;
const RAW_CHUNK_BYTES = 384 * 1024;

type RequestMetadata = {
  method: string;
  url: string;
};

type ResponseMetadata = {
  status: number;
  mimeType: string;
  resourceType: string;
  etag?: string;
  lastModified?: string;
  cacheControl?: string;
};

const attachedTabs = new Set<number>();
const requests = new Map<string, RequestMetadata>();
const responses = new Map<string, ResponseMetadata>();
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

function getNativePort(): chrome.runtime.Port | undefined {
  if (nativePort) return nativePort;

  try {
    const port = chrome.runtime.connectNative(NATIVE_HOST);
    port.onDisconnect.addListener(() => {
      if (nativePort === port) nativePort = undefined;
      void chrome.runtime.lastError;
    });
    port.onMessage.addListener((message) => {
      if (message?.type === "error") {
        console.error("Mirrarium native host error", message);
      }
    });
    nativePort = port;
    return port;
  } catch (error) {
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

async function attach(tabId: number, url: string | undefined): Promise<void> {
  if (!isSupportedChatGptUrl(url) || attachedTabs.has(tabId)) return;

  try {
    await chrome.debugger.attach({ tabId }, CDP_VERSION);
    await chrome.debugger.sendCommand({ tabId }, "Network.enable");
    attachedTabs.add(tabId);
  } catch (error) {
    console.warn("Mirrarium could not attach to ChatGPT tab", tabId, error);
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

async function captureBody(
  tabId: number,
  requestId: string,
  encodedDataLength: number | undefined,
  failure?: string,
): Promise<void> {
  const key = requestKey(tabId, requestId);
  const request = requests.get(key);
  const response = responses.get(key);
  requests.delete(key);
  responses.delete(key);

  if (!request && !response) return;

  const captureId = crypto.randomUUID();

  postNative({
    type: "capture_start",
    metadata: {
      capture_id: captureId,
      tab_id: tabId,
      request_id: requestId,
      method: request?.method ?? "GET",
      url: response ? request?.url ?? "" : request?.url ?? "",
      status: Math.trunc(response?.status ?? 0),
      mime_type: response?.mimeType ?? "",
      resource_type: response?.resourceType ?? "Unknown",
      etag: response?.etag,
      last_modified: response?.lastModified,
      cache_control: response?.cacheControl,
    },
  });

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
  if (source.tabId !== undefined) attachedTabs.delete(source.tabId);
});

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (source.tabId === undefined) return;

  const tabId = source.tabId;

  if (method === "Network.requestWillBeSent") {
    const event = params as {
      requestId: string;
      request: { method: string; url: string };
    };
    requests.set(requestKey(tabId, event.requestId), {
      method: event.request.method,
      url: event.request.url,
    });
    return;
  }

  if (method === "Network.responseReceived") {
    const event = params as {
      requestId: string;
      type: string;
      response: {
        status: number;
        mimeType: string;
        headers?: Record<string, string | number>;
      };
    };
    responses.set(requestKey(tabId, event.requestId), {
      status: event.response.status,
      mimeType: event.response.mimeType,
      resourceType: event.type,
      etag: header(event.response.headers, "etag"),
      lastModified: header(event.response.headers, "last-modified"),
      cacheControl: header(event.response.headers, "cache-control"),
    });
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
  if (changeInfo.url || changeInfo.status === "complete") {
    void attach(tabId, changeInfo.url ?? tab.url);
  }
});

chrome.tabs.onActivated.addListener(({ tabId }) => {
  void chrome.tabs.get(tabId).then((tab) => attach(tabId, tab.url));
});

const NATIVE_HOST = "com.sguzman.mirrarium";
const CDP_VERSION = "1.3";
const attachedTabs = new Set<number>();

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

async function sendNative(message: unknown): Promise<void> {
  await new Promise<void>((resolve) => {
    chrome.runtime.sendNativeMessage(NATIVE_HOST, message, () => {
      void chrome.runtime.lastError;
      resolve();
    });
  });
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

chrome.debugger.onDetach.addListener((source) => {
  if (source.tabId !== undefined) attachedTabs.delete(source.tabId);
});

chrome.debugger.onEvent.addListener((source, method, params) => {
  if (source.tabId === undefined || method !== "Network.responseReceived") return;

  const event = params as {
    requestId: string;
    response: { url: string; status: number; mimeType: string };
  };

  void sendNative({
    type: "observe_response",
    tab_id: source.tabId,
    request_id: event.requestId,
    url: event.response.url,
    status: Math.trunc(event.response.status),
    mime_type: event.response.mimeType,
  });
});

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.url || changeInfo.status === "complete") {
    void attach(tabId, changeInfo.url ?? tab.url);
  }
});

chrome.tabs.onActivated.addListener(({ tabId }) => {
  void chrome.tabs.get(tabId).then((tab) => attach(tabId, tab.url));
});

export {};

type ModeState = {
  supported: boolean;
  enabled: boolean;
  locallyServed: number;
  networkFallbackAttempts: number;
};

function supported(url: string | undefined): boolean {
  if (!url) return false;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "https:" &&
      ["chatgpt.com", "chat.openai.com"].includes(parsed.hostname);
  } catch { return false; }
}

async function targetTab(): Promise<number | undefined> {
  const active = await chrome.tabs.query({ active: true, currentWindow: true });
  // Never enable replay for a different tab merely because it is open.
  return active.find((tab) => supported(tab.url))?.id;
}

const statusElement = document.querySelector<HTMLElement>("#status")!;
const activityElement = document.querySelector<HTMLElement>("#activity")!;
const button = document.querySelector<HTMLButtonElement>("#toggle")!;

function show(state: ModeState): void {
  button.disabled = !state.supported;
  statusElement.textContent = state.supported
    ? state.enabled ? "Historical reuse: ON for this tab" : "Historical reuse: OFF"
    : "Open a ChatGPT tab to use this option.";
  button.textContent = state.enabled ? "Disable historical reuse" : "Enable historical reuse";
  activityElement.textContent = state.supported
    ? `Locally served: ${state.locallyServed ?? 0} · Network fallback attempts: ${state.networkFallbackAttempts ?? 0}`
    : "";
}

void (async () => {
  const tabId = await targetTab();
  if (tabId === undefined) {
    show({ supported: false, enabled: false, locallyServed: 0, networkFallbackAttempts: 0 });
    return;
  }
  const query = async (type: string): Promise<ModeState> =>
    await chrome.runtime.sendMessage({ type, tab_id: tabId }) as ModeState;
  show(await query("mirrarium_local_mode_status"));
  button.addEventListener("click", () => {
    button.disabled = true;
    void query("mirrarium_local_mode_toggle")
      .then(show)
      .catch(() => show({ supported: false, enabled: false, locallyServed: 0, networkFallbackAttempts: 0 }));
  });
})().catch(() => show({ supported: false, enabled: false, locallyServed: 0, networkFallbackAttempts: 0 }));

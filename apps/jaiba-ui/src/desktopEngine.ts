import { isTauriRuntime, setApiRoot } from "./api";

export type EngineMode = "local" | "remote";

export interface EngineStatus {
  mode: EngineMode;
  running: boolean;
  pid: number | null;
  api_base: string;
  binary: string | null;
  flow: string | null;
  last_error: string | null;
}

async function invoke<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  const { invoke: tauriInvoke } = await import("@tauri-apps/api/core");
  return tauriInvoke<T>(command, args);
}

/** Aplica la base del API del motor y la recuerda en localStorage. */
export function applyEngineApiBase(base: string): void {
  setApiRoot(base);
  try {
    window.localStorage.setItem("jaiba.api.base", base);
  } catch {
    // storage blocked
  }
}

export async function fetchEngineStatus(): Promise<EngineStatus | null> {
  if (!isTauriRuntime()) return null;
  try {
    return await invoke<EngineStatus>("engine_status");
  } catch {
    return null;
  }
}

export async function setEngineMode(mode: EngineMode): Promise<EngineStatus> {
  const status = await invoke<EngineStatus>("set_engine_mode", { mode });
  applyEngineApiBase(status.api_base);
  return status;
}

export async function startLocalEngine(): Promise<EngineStatus> {
  const status = await invoke<EngineStatus>("start_local_engine");
  applyEngineApiBase(status.api_base);
  return status;
}

export async function stopLocalEngine(): Promise<EngineStatus> {
  return invoke<EngineStatus>("stop_local_engine");
}

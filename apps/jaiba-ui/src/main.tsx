import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { isTauriRuntime, setApiRoot } from "./api";
import { applyEngineApiBase, fetchEngineStatus } from "./desktopEngine";
import "./styles.css";

async function bootstrap(): Promise<void> {
  if (isTauriRuntime()) {
    try {
      const { invoke } = await import("@tauri-apps/api/core");
      const status = await fetchEngineStatus();
      if (status?.api_base) {
        applyEngineApiBase(status.api_base);
      } else {
        const base = await invoke<string>("api_base");
        setApiRoot(base);
      }
    } catch {
      // Fallback: resolveApiRoot() ya eligió http://127.0.0.1:9090
    }
  }

  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
}

void bootstrap();

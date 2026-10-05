import { useEffect, useState } from "react";
import { jaivaApi } from "./api";
import type { EngineCapabilities } from "./types";

export interface EngineState {
  online: boolean;
  /** Mensaje accionable cuando el motor no responde (null si está en línea). */
  problem: string | null;
  /** `null` = desconocidas (motor caído o anterior a `/api/v1/capabilities`). */
  capabilities: EngineCapabilities | null;
}

/** Consulta salud y capacidades del motor cada `intervalMs`. */
export function useEngineState(intervalMs = 10000): EngineState {
  const [state, setState] = useState<EngineState>({
    online: false,
    problem: null,
    capabilities: null,
  });

  useEffect(() => {
    let active = true;
    const check = async () => {
      try {
        await jaivaApi.health();
        const capabilities = await jaivaApi.capabilities().catch(() => null);
        if (active) setState({ online: true, problem: null, capabilities });
      } catch (error) {
        if (!active) return;
        setState((current) => ({
          online: false,
          problem: error instanceof Error ? error.message : String(error),
          capabilities: current.capabilities,
        }));
      }
    };
    void check();
    const timer = window.setInterval(() => void check(), intervalMs);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [intervalMs]);

  return state;
}

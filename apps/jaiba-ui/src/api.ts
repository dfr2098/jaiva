import type {
  DeadLetterEntry,
  DatabaseConnection,
  DatabaseConnectionInput,
  ConnectionDriver,
  CompiledQuery,
  DraftCreated,
  FlowAction,
  FlowRecord,
  FlowSnapshot,
  FlowValidationResult,
  FlowVersion,
  FlowView,
  ProvenanceRecord,
  QuerySpec,
  RuntimeEvent,
  DatabaseObject,
  ObjectDescription,
  DiagnosticCheck,
} from "./types";

declare global {
  interface Window {
    __JAIBA_API_BASE__?: string;
    __JAIVA_API_BASE__?: string;
  }
}

const API_ROOT =
  window.__JAIBA_API_BASE__ ??
  window.__JAIVA_API_BASE__ ??
  import.meta.env.VITE_JAIBA_API_BASE ??
  import.meta.env.VITE_JAIVA_API_BASE ??
  "/jaiba-api";

const FLOW_STATES = new Set([
  "STOPPED",
  "STARTING",
  "RUNNING",
  "PAUSED",
  "DRAINING",
  "FAILED",
]);

function apiUrl(path: string): string {
  return `${API_ROOT}${path}`;
}

function websocketUrl(path: string): string {
  const absolute = new URL(apiUrl(path), window.location.href);
  absolute.protocol = absolute.protocol === "https:" ? "wss:" : "ws:";
  return absolute.toString();
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isFlowSnapshot(value: unknown): value is FlowSnapshot {
  if (!isObject(value) || typeof value.flow_id !== "string") return false;
  if (!isObject(value.control) || !FLOW_STATES.has(String(value.control.state))) {
    return false;
  }
  return isObject(value.metrics);
}

function adminHeaders(extra?: HeadersInit): HeadersInit {
  const token =
    window.sessionStorage.getItem("jaiba.admin.token") ??
    window.sessionStorage.getItem("jaiva.admin.token");
  return {
    Accept: "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...extra,
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: adminHeaders(init?.headers),
  });
  const body = (await response.json().catch(() => ({}))) as {
    message?: string;
  };
  if (!response.ok) {
    throw new Error(body.message ?? `El motor respondió ${response.status}`);
  }
  return body as T;
}

async function requestText(path: string, init?: RequestInit): Promise<string> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: adminHeaders({
      Accept: "application/yaml, text/yaml, text/plain, */*",
      ...init?.headers,
    }),
  });
  const text = await response.text();
  if (!response.ok) {
    let message = `El motor respondió ${response.status}`;
    try {
      const body = JSON.parse(text) as { message?: string };
      if (body.message) message = body.message;
    } catch {
      if (text.trim()) message = text.trim();
    }
    throw new Error(message);
  }
  return text;
}

export const jaivaApi = {
  health: () => request<{ status: string; service: string }>("/health"),
  runtime: async (): Promise<FlowSnapshot | null> => {
    const response = await fetch(apiUrl("/runtime"), {
      headers: { Accept: "application/json" },
    });
    const body: unknown = await response.json().catch(() => null);

    if (isFlowSnapshot(body)) return body;
    if (!response.ok) {
      const message =
        isObject(body) && typeof body.message === "string"
          ? body.message
          : `El motor respondiÃ³ ${response.status}`;
      throw new Error(message);
    }
    return null;
  },
  mutate: (flowId: string, action: FlowAction) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/${action}`,
      { method: "POST" },
    ),
  triggerFlow: (flowId: string) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/trigger`,
      { method: "POST" },
    ),
  validateFlow: (yaml: string) =>
    request<FlowValidationResult>("/api/v1/flows/validate", {
      method: "POST",
      headers: { "Content-Type": "application/yaml" },
      body: yaml,
    }),
  deployFlow: (flowId: string, yaml: string, start = false) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}?start=${start}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/yaml" },
        body: yaml,
      },
    ),
  listFlows: () => request<FlowRecord[]>("/api/v1/flows"),
  createFlow: (yaml: string) =>
    request<DraftCreated>("/api/v1/flows", {
      method: "POST",
      headers: { "Content-Type": "application/yaml" },
      body: yaml,
    }),
  getFlow: (flowId: string) =>
    request<FlowView>(`/api/v1/flows/${encodeURIComponent(flowId)}`),
  listVersions: (flowId: string) =>
    request<FlowVersion[]>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/versions`,
    ),
  exportVersion: (flowId: string, version: number) =>
    requestText(
      `/api/v1/flows/${encodeURIComponent(flowId)}/versions/${version}`,
    ),
  validateVersion: (flowId: string, version: number) =>
    request<FlowRecord>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/versions/${version}/validate`,
      { method: "POST" },
    ),
  deployVersion: (flowId: string, version: number, start = false) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/versions/${version}/deploy?start=${start}`,
      { method: "POST" },
    ),
  archiveVersion: (flowId: string, version: number) =>
    request<FlowRecord>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/versions/${version}/archive`,
      { method: "POST" },
    ),
  rollbackFlow: (flowId: string, start = false) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/rollback?start=${start}`,
      { method: "POST" },
    ),
  provenance: (flowId: string, limit = 100, packetId?: string) =>
    request<ProvenanceRecord[]>(
      `/api/v1/provenance?flow=${encodeURIComponent(flowId)}&limit=${limit}${
        packetId ? `&packet_id=${encodeURIComponent(packetId)}` : ""
      }`,
    ),
  deadLetters: (flowId: string, limit = 100) =>
    request<DeadLetterEntry[]>(
      `/api/v1/dead-letter?flow=${encodeURIComponent(flowId)}&limit=${limit}`,
    ),
  replayDeadLetter: (flowId: string, queueId: string) =>
    request<{ message: string }>(
      `/api/v1/dead-letter/${encodeURIComponent(queueId)}/replay?flow=${encodeURIComponent(flowId)}`,
      { method: "POST" },
    ),
  connectionTypes: () =>
    request<ConnectionDriver[]>("/api/v1/connection-types"),
  connections: () =>
    request<DatabaseConnection[]>("/api/v1/connections"),
  createConnection: (input: DatabaseConnectionInput) =>
    request<DatabaseConnection>("/api/v1/connections", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input),
    }),
  updateConnection: (id: string, input: DatabaseConnectionInput) =>
    request<DatabaseConnection>(
      `/api/v1/connections/${encodeURIComponent(id)}`,
      {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(input),
      },
    ),
  deleteConnection: (id: string) =>
    request<void>(`/api/v1/connections/${encodeURIComponent(id)}`, {
      method: "DELETE",
    }),
  duplicateConnection: (id: string, name: string) =>
    request<DatabaseConnection>(
      `/api/v1/connections/${encodeURIComponent(id)}/duplicate`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      },
    ),
  testConnection: (id: string) =>
    request<DatabaseConnection>(
      `/api/v1/connections/${encodeURIComponent(id)}/test`,
      { method: "POST" },
    ),
  diagnoseConnection: (id: string) =>
    request<DiagnosticCheck[]>(
      `/api/v1/connections/${encodeURIComponent(id)}/diagnostics`,
    ),
  connectionMetadata: (id: string, schema?: string) =>
    request<DatabaseObject[]>(
      `/api/v1/connections/${encodeURIComponent(id)}/metadata${
        schema ? `?schema=${encodeURIComponent(schema)}` : ""
      }`,
    ),
  describeConnectionObject: (id: string, schema: string, name: string) =>
    request<ObjectDescription>(
      `/api/v1/connections/${encodeURIComponent(id)}/metadata/${encodeURIComponent(schema)}/${encodeURIComponent(name)}`,
    ),
  compileQuery: (id: string, spec: QuerySpec) =>
    request<CompiledQuery>(
      `/api/v1/connections/${encodeURIComponent(id)}/query/compile`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(spec),
      },
    ),
  runtimeSocket: (onEvent: (event: RuntimeEvent) => void) => {
    const socket = new WebSocket(websocketUrl("/ws/v1"));
    socket.addEventListener("message", (message) => {
      try {
        const event: unknown = JSON.parse(String(message.data));
        if (
          !isObject(event) ||
          event.kind !== "runtime_snapshot" ||
          (event.flow !== null &&
            event.flow !== undefined &&
            !isFlowSnapshot(event.flow))
        ) {
          return;
        }
        const flows = Array.isArray(event.flows)
          ? event.flows.filter(isFlowSnapshot)
          : undefined;
        onEvent({
          kind: "runtime_snapshot",
          flow: isFlowSnapshot(event.flow) ? event.flow : null,
          flows,
        });
      } catch {
        // Ignore malformed events; the next snapshot replaces them.
      }
    });
    return socket;
  },
};

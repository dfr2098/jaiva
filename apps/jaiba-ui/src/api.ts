import type {
  DeadLetterEntry,
  DatabaseConnection,
  DatabaseConnectionInput,
  ConnectionDriver,
  CompiledQuery,
  FlowAction,
  FlowSnapshot,
  FlowValidationResult,
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

function apiUrl(path: string): string {
  return `${API_ROOT}${path}`;
}

function websocketUrl(path: string): string {
  const absolute = new URL(apiUrl(path), window.location.href);
  absolute.protocol = absolute.protocol === "https:" ? "wss:" : "ws:";
  return absolute.toString();
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token =
    window.sessionStorage.getItem("jaiba.admin.token") ??
    window.sessionStorage.getItem("jaiva.admin.token");
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...init?.headers,
    },
  });
  const body = (await response.json().catch(() => ({}))) as {
    message?: string;
  };
  if (!response.ok) {
    throw new Error(body.message ?? `El motor respondió ${response.status}`);
  }
  return body as T;
}

export const jaivaApi = {
  health: () => request<{ status: string; service: string }>("/health"),
  flows: () => request<FlowSnapshot[]>("/api/v1/flows"),
  mutate: (flowId: string, action: FlowAction) =>
    request<FlowSnapshot>(
      `/api/v1/flows/${encodeURIComponent(flowId)}/${action}`,
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
  provenance: (limit = 100, packetId?: string) =>
    request<ProvenanceRecord[]>(
      `/api/v1/provenance?limit=${limit}${
        packetId ? `&packet_id=${encodeURIComponent(packetId)}` : ""
      }`,
    ),
  deadLetters: (limit = 100) =>
    request<DeadLetterEntry[]>(`/api/v1/dead-letter?limit=${limit}`),
  replayDeadLetter: (queueId: string) =>
    request<{ message: string }>(
      `/api/v1/dead-letter/${encodeURIComponent(queueId)}/replay`,
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
        onEvent(JSON.parse(String(message.data)) as RuntimeEvent);
      } catch {
        // Ignore malformed events; the next snapshot replaces them.
      }
    });
    return socket;
  },
};

import type { Edge, Node } from "@xyflow/react";
import { CATALOG_BY_TYPE } from "./catalog";

export type Relationship = "success" | "failure";

export interface RetrySettings {
  maximum_attempts: number;
  initial_delay_ms: number;
  maximum_delay_ms: number;
}

export type ExecutionMode = "auto" | "async_io" | "blocking_io" | "cpu";
export type OrderingMode = "unordered" | "preserve" | "partitioned";
export type SimulationMode = "real" | "mock" | "replay";

export interface SimulationSettings {
  mode: SimulationMode;
  options: Record<string, unknown>;
}

export interface SchedulingSettings {
  concurrent_tasks: number;
  maximum_in_flight: number | null;
  timeout_ms: number | null;
  execution_mode: ExecutionMode;
  ordering: OrderingMode;
  partition_by: string | null;
}

export interface ProcessorNodeData {
  processorId: string;
  type: string;
  config: Record<string, unknown>;
  retry: RetrySettings;
  scheduling: SchedulingSettings;
  simulation: SimulationSettings;
  [key: string]: unknown;
}

export interface ConnectionEdgeData {
  relationship: Relationship;
  queueCapacity: number;
  [key: string]: unknown;
}

export type ProcessorNode = Node<ProcessorNodeData>;
export type ConnectionEdge = Edge<ConnectionEdgeData>;

export interface DatabaseConnection {
  name: string;
  type: string;
  url_env: string;
  max_connections: number;
}

export interface KafkaConnection {
  name: string;
  brokers_env: string;
  client_id: string;
}

export interface ParameterEntry {
  name: string;
  value: string;
}

export interface EngineSettings {
  queue_capacity: number;
  max_concurrency: number;
  memory_maximum_percent: number;
  repository_enabled: boolean;
  circuit_breaker_enabled: boolean;
  admin_enabled: boolean;
  admin_authentication: "bearer" | "none";
  admin_token_env: string;
}

export interface FlowMeta {
  id: string;
  parameters: ParameterEntry[];
  databaseConnections: DatabaseConnection[];
  kafkaConnections: KafkaConnection[];
  engine: EngineSettings;
}

export const RETRY_DEFAULTS: RetrySettings = {
  maximum_attempts: 0,
  initial_delay_ms: 250,
  maximum_delay_ms: 30000,
};

export const SCHEDULING_DEFAULTS: SchedulingSettings = {
  concurrent_tasks: 1,
  maximum_in_flight: null,
  timeout_ms: null,
  execution_mode: "auto",
  ordering: "unordered",
  partition_by: null,
};

export const SIMULATION_DEFAULTS: SimulationSettings = {
  mode: "real",
  options: {},
};

export const ENGINE_DEFAULTS: EngineSettings = {
  queue_capacity: 100,
  max_concurrency: 4,
  memory_maximum_percent: 70,
  repository_enabled: false,
  circuit_breaker_enabled: true,
  admin_enabled: true,
  admin_authentication: "bearer",
  admin_token_env: "JAIBA_ADMIN_TOKEN",
};

export const EXECUTION_MODES: ExecutionMode[] = [
  "auto",
  "async_io",
  "blocking_io",
  "cpu",
];

export const ORDERING_MODES: OrderingMode[] = [
  "unordered",
  "preserve",
  "partitioned",
];

export function defaultFlowMeta(): FlowMeta {
  return {
    id: "mi_flujo",
    parameters: [],
    databaseConnections: [],
    kafkaConnections: [],
    engine: { ...ENGINE_DEFAULTS },
  };
}

function cloneConfig(value: Record<string, unknown>): Record<string, unknown> {
  return structuredClone(value);
}

let nodeCounter = 0;

export function createProcessorNode(
  type: string,
  position: { x: number; y: number },
  existingIds: Set<string>,
): ProcessorNode {
  const def = CATALOG_BY_TYPE[type];
  const processorId = uniqueProcessorId(type, existingIds);
  nodeCounter += 1;
  return {
    id: `node_${Date.now()}_${nodeCounter}`,
    type: "processor",
    position,
    data: {
      processorId,
      type,
      config: def ? cloneConfig(def.defaultConfig) : {},
      retry: { ...RETRY_DEFAULTS },
      scheduling: { ...SCHEDULING_DEFAULTS },
      simulation: { ...SIMULATION_DEFAULTS, options: {} },
    },
  };
}

export function uniqueProcessorId(base: string, existingIds: Set<string>): string {
  if (!existingIds.has(base)) return base;
  let index = 2;
  while (existingIds.has(`${base}_${index}`)) index += 1;
  return `${base}_${index}`;
}

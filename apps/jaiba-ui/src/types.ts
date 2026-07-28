export type FlowLifecycle =
  | "STOPPED"
  | "STARTING"
  | "RUNNING"
  | "PAUSED"
  | "DRAINING"
  | "FAILED";

export interface ProcessorMetrics {
  active_tasks: number;
  queue_depth: number;
  concurrency_limit: number;
  completed: number;
  failed: number;
  execution_duration_ms: number;
  execution_duration_seconds: number;
  saturation_ratio: number;
}

export interface ConnectionQueueMetrics {
  packets: number;
  bytes: number;
}

export interface FlowMetrics {
  flow_id: string;
  flow_status: number;
  flow_last_success_timestamp: number;
  processed: number;
  failed: number;
  retried: number;
  emitted: number;
  queue_depth: number;
  active_tasks: number;
  memory_used_bytes: number;
  memory_budget_bytes: number;
  repository_pending: number;
  repository_dead_letter: number;
  recovered_packets: number;
  circuits_open: number;
  available_parallelism: number;
  cpu_worker_limit: number;
  blocking_worker_limit: number;
  processors: Record<string, ProcessorMetrics>;
  connection_queues: Record<string, ConnectionQueueMetrics>;
}

export interface FlowSnapshot {
  flow_id: string;
  control: {
    state: FlowLifecycle;
    last_error: string | null;
    changed_at: number;
  };
  metrics: FlowMetrics;
  ready: boolean;
}

export type FlowAction = "start" | "pause" | "resume" | "drain" | "stop";

export interface RuntimeEvent {
  kind: "runtime_snapshot";
  flow: FlowSnapshot | null;
}

export interface ProvenanceRecord {
  id: number;
  queue_id: string;
  packet_id: string;
  flow_id: string;
  processor_id: string;
  event_type: string;
  details: Record<string, unknown>;
  created_at: number;
}

export interface DeadLetterEntry {
  queue_id: string;
  packet_id: string;
  flow_id: string;
  processor_id: string;
  relationship: string;
  attempt: number;
  error: string | null;
  content_size: number;
  created_at: number;
  failed_at: number;
}

export interface FlowValidationResult {
  valid: boolean;
  flow_id: string;
  processors: number;
  connections: number;
}

export type ConnectionType =
  | "postgres"
  | "mysql"
  | "maria_db"
  | "oracle"
  | "sql_server"
  | "kafka"
  | "opc_ua"
  | "rest";

export type ConnectionAvailability =
  | "unknown"
  | "testing"
  | "available"
  | "degraded"
  | "unavailable";

export interface ConnectionDriver {
  id: ConnectionType;
  name: string;
  category: string;
  default_port: number;
  enabled: boolean;
  test_supported: boolean;
  note: string;
}

export interface ConnectionStatus {
  profile_id: string;
  availability: ConnectionAvailability;
  latency_ms: number | null;
  version: string | null;
  pool_active: number | null;
  pool_maximum: number | null;
  tested_at: number | null;
  message: string | null;
}

export interface DatabaseConnection {
  id: string;
  name: string;
  connection_type: ConnectionType;
  host: string;
  port: number;
  database: string | null;
  username: string;
  ssl: boolean;
  pool_min: number;
  pool_max: number;
  timeout_ms: number;
  status: ConnectionStatus;
}

export interface DatabaseConnectionInput {
  name: string;
  connection_type: ConnectionType;
  host: string;
  port: number;
  database: string;
  username: string;
  password?: string;
  ssl: boolean;
  pool_min: number;
  pool_max: number;
  timeout_ms: number;
}

export type DatabaseObjectKind =
  | "schema"
  | "table"
  | "view"
  | "procedure"
  | "function"
  | "sequence";

export interface DatabaseObject {
  schema: string | null;
  name: string;
  kind: DatabaseObjectKind;
}

export interface ColumnMetadata {
  name: string;
  data_type: string;
  nullable: boolean;
  ordinal: number;
  default_value: string | null;
}

export interface KeyMetadata {
  name: string;
  kind: string;
  columns: string[];
}

export interface IndexMetadata {
  name: string;
  columns: string[];
  unique: boolean;
}

export interface ObjectDescription {
  object: DatabaseObject;
  columns: ColumnMetadata[];
  keys: KeyMetadata[];
  indexes: IndexMetadata[];
}

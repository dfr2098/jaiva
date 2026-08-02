import { parse, stringify } from "yaml";
import { CATALOG_BY_TYPE } from "./catalog";
import {
  ENGINE_DEFAULTS,
  RETRY_DEFAULTS,
  SCHEDULE_DEFAULTS,
  SCHEDULING_DEFAULTS,
  SIMULATION_DEFAULTS,
  type CatchUpPolicy,
  type DatabaseConnection,
  type ConnectionEdge,
  type FlowMeta,
  type KafkaConnection,
  type OverlapPolicy,
  type ProcessorNode,
  type ScheduleTriggerType,
} from "./model";

type Yaml = Record<string, unknown>;

function isBlank(value: unknown): boolean {
  return value === undefined || value === null || value === "";
}

function cleanConfig(config: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(config)) {
    if (isBlank(value)) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (
      typeof value === "object" &&
      !Array.isArray(value) &&
      value !== null &&
      Object.keys(value as object).length === 0
    ) {
      continue;
    }
    output[key] = value;
  }
  return output;
}

function retryBlock(retry: ProcessorNode["data"]["retry"]): Yaml | null {
  if (
    retry.maximum_attempts === RETRY_DEFAULTS.maximum_attempts &&
    retry.initial_delay_ms === RETRY_DEFAULTS.initial_delay_ms &&
    retry.maximum_delay_ms === RETRY_DEFAULTS.maximum_delay_ms
  ) {
    return null;
  }
  return {
    maximum_attempts: retry.maximum_attempts,
    initial_delay_ms: retry.initial_delay_ms,
    maximum_delay_ms: retry.maximum_delay_ms,
  };
}

function schedulingBlock(
  scheduling: ProcessorNode["data"]["scheduling"],
): Yaml | null {
  const block: Yaml = {};
  if (scheduling.concurrent_tasks !== SCHEDULING_DEFAULTS.concurrent_tasks) {
    block.concurrent_tasks = scheduling.concurrent_tasks;
  }
  if (scheduling.maximum_in_flight !== null) {
    block.maximum_in_flight = scheduling.maximum_in_flight;
  }
  if (scheduling.timeout_ms !== null) {
    block.timeout_ms = scheduling.timeout_ms;
  }
  if (scheduling.execution_mode !== SCHEDULING_DEFAULTS.execution_mode) {
    block.execution_mode = scheduling.execution_mode;
  }
  if (scheduling.ordering !== SCHEDULING_DEFAULTS.ordering) {
    block.ordering = scheduling.ordering;
  }
  if (scheduling.ordering === "partitioned" && scheduling.partition_by) {
    block.partition_by = scheduling.partition_by;
  }
  return Object.keys(block).length > 0 ? block : null;
}

function engineBlock(meta: FlowMeta): Yaml | null {
  const engine: Yaml = {};
  const e = meta.engine;
  if (e.queue_capacity !== ENGINE_DEFAULTS.queue_capacity) {
    engine.queue_capacity = e.queue_capacity;
  }
  if (e.max_concurrency !== ENGINE_DEFAULTS.max_concurrency) {
    engine.max_concurrency = e.max_concurrency;
  }
  if (e.memory_maximum_percent !== ENGINE_DEFAULTS.memory_maximum_percent) {
    engine.memory = { maximum_percent: e.memory_maximum_percent };
  }
  if (e.repository_enabled !== ENGINE_DEFAULTS.repository_enabled) {
    engine.repository = { enabled: e.repository_enabled };
  }
  if (e.circuit_breaker_enabled !== ENGINE_DEFAULTS.circuit_breaker_enabled) {
    engine.circuit_breaker = { enabled: e.circuit_breaker_enabled };
  }
  const admin: Yaml = {};
  if (e.admin_enabled !== ENGINE_DEFAULTS.admin_enabled) {
    admin.enabled = e.admin_enabled;
  }
  if (e.admin_authentication !== ENGINE_DEFAULTS.admin_authentication) {
    admin.authentication = e.admin_authentication;
  }
  if (e.admin_token_env !== ENGINE_DEFAULTS.admin_token_env) {
    admin.token_env = e.admin_token_env;
  }
  if (Object.keys(admin).length > 0) {
    engine.admin = admin;
  }
  return Object.keys(engine).length > 0 ? engine : null;
}

export function buildFlowObject(
  meta: FlowMeta,
  nodes: ProcessorNode[],
  edges: ConnectionEdge[],
): Yaml {
  const flow: Yaml = { id: meta.id || "flujo" };

  if (meta.parameters.length > 0) {
    flow.parameters = Object.fromEntries(
      meta.parameters
        .filter((p) => p.name.trim() !== "")
        .map((p) => [p.name, p.value]),
    );
  }

  if (meta.databaseConnections.length > 0) {
    flow.database_connections = Object.fromEntries(
      meta.databaseConnections
        .filter((c) => c.name.trim() !== "")
        .map((c) => [
          c.name,
          {
            type: c.type,
            url_env: c.url_env,
            max_connections: c.max_connections,
          },
        ]),
    );
  }

  if (meta.kafkaConnections.length > 0) {
    flow.kafka_connections = Object.fromEntries(
      meta.kafkaConnections
        .filter((c) => c.name.trim() !== "")
        .map((c) => [
          c.name,
          { brokers_env: c.brokers_env, client_id: c.client_id },
        ]),
    );
  }

  if (meta.schedule.enabled) {
    const schedule: Yaml = {
      enabled: true,
      overlap: meta.schedule.overlap,
      catch_up: meta.schedule.catchUp,
    };
    if (meta.schedule.triggerType === "interval") {
      schedule.trigger = {
        type: "interval",
        every_seconds: meta.schedule.everySeconds,
      };
    } else if (meta.schedule.triggerType === "cron") {
      schedule.trigger = {
        type: "cron",
        expression: meta.schedule.cronExpression,
      };
      if (meta.schedule.timezone.trim()) {
        schedule.timezone = meta.schedule.timezone.trim();
      }
    } else {
      schedule.trigger = { type: "webhook" };
    }
    flow.schedule = schedule;
  }

  const engine = engineBlock(meta);
  if (engine) flow.engine = engine;

  flow.processors = nodes.map((node) => {
    const processor: Yaml = {
      id: node.data.processorId,
      type: node.data.type,
    };
    const config = cleanConfig(node.data.config);
    if (Object.keys(config).length > 0) processor.config = config;
    const retry = retryBlock(node.data.retry);
    if (retry) processor.retry = retry;
    const scheduling = schedulingBlock(node.data.scheduling);
    if (scheduling) processor.scheduling = scheduling;
    if (
      node.data.simulation.mode !== SIMULATION_DEFAULTS.mode ||
      Object.keys(node.data.simulation.options).length > 0
    ) {
      processor.simulation = {
        mode: node.data.simulation.mode,
        ...(Object.keys(node.data.simulation.options).length > 0
          ? { options: node.data.simulation.options }
          : {}),
      };
    }
    return processor;
  });

  const idByNode = new Map(nodes.map((n) => [n.id, n.data.processorId]));
  const connections = edges
    .map((edge) => {
      const from = idByNode.get(edge.source);
      const to = idByNode.get(edge.target);
      if (!from || !to) return null;
      const relationship = edge.data?.relationship ?? "success";
      const capacity = edge.data?.queueCapacity ?? 100;
      const connection: Yaml = { from, relationship, to };
      if (capacity !== 100) connection.queue = { capacity };
      return connection;
    })
    .filter((value): value is Yaml => value !== null);

  if (connections.length > 0) flow.connections = connections;

  return flow;
}

export function toYaml(
  meta: FlowMeta,
  nodes: ProcessorNode[],
  edges: ConnectionEdge[],
): string {
  return stringify(buildFlowObject(meta, nodes, edges), { lineWidth: 0 });
}

export interface ValidationIssue {
  level: "error" | "warning";
  message: string;
}

function duplicateNames(names: string[]): string[] {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  for (const name of names) {
    const normalized = name.trim();
    if (normalized && seen.has(normalized)) duplicates.add(normalized);
    seen.add(normalized);
  }
  return [...duplicates];
}

function longestPath(nodes: ProcessorNode[], edges: ConnectionEdge[]): number {
  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    outgoing.set(edge.source, [...(outgoing.get(edge.source) ?? []), edge.target]);
  }
  const visiting = new Set<string>();
  const memo = new Map<string, number>();
  const visit = (id: string): number => {
    if (visiting.has(id)) return nodes.length + 1;
    const cached = memo.get(id);
    if (cached !== undefined) return cached;
    visiting.add(id);
    const depth = 1 + Math.max(0, ...(outgoing.get(id) ?? []).map(visit));
    visiting.delete(id);
    memo.set(id, depth);
    return depth;
  };
  return Math.max(0, ...nodes.map((node) => visit(node.id)));
}

export interface ValidateFlowOptions {
  /** Alias conocidos del Connection Manager (se resuelven en runtime). */
  knownDatabaseAliases?: string[];
  knownKafkaAliases?: string[];
}

export function validateFlow(
  meta: FlowMeta,
  nodes: ProcessorNode[],
  edges: ConnectionEdge[],
  options: ValidateFlowOptions = {},
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const knownDatabaseAliases = new Set(
    (options.knownDatabaseAliases ?? []).map((name) => name.trim()).filter(Boolean),
  );
  const knownKafkaAliases = new Set(
    (options.knownKafkaAliases ?? []).map((name) => name.trim()).filter(Boolean),
  );

  if (!meta.id.trim()) {
    issues.push({ level: "error", message: "El flujo necesita un identificador." });
  }
  if (nodes.length === 0) {
    issues.push({ level: "error", message: "Agrega al menos un procesador." });
  }
  if (meta.schedule.enabled) {
    if (meta.schedule.triggerType === "interval" && meta.schedule.everySeconds < 1) {
      issues.push({
        level: "error",
        message: "La agenda por intervalo necesita every_seconds ≥ 1.",
      });
    }
    if (
      meta.schedule.triggerType === "cron" &&
      !meta.schedule.cronExpression.trim()
    ) {
      issues.push({
        level: "error",
        message: "La agenda cron necesita una expresión.",
      });
    }
  }
  if (meta.engine.queue_capacity < 1 || meta.engine.max_concurrency < 1) {
    issues.push({ level: "error", message: "Las capacidades del motor deben ser mayores que cero." });
  }
  if (
    meta.engine.memory_maximum_percent < 1 ||
    meta.engine.memory_maximum_percent > 90
  ) {
    issues.push({ level: "error", message: "La memoria máxima debe estar entre 1% y 90%." });
  }
  if (
    meta.engine.admin_enabled &&
    meta.engine.admin_authentication === "bearer" &&
    !meta.engine.admin_token_env.trim()
  ) {
    issues.push({ level: "error", message: "La administración bearer necesita token_env." });
  }

  const databaseNames = meta.databaseConnections.map((connection) => connection.name);
  const kafkaNames = meta.kafkaConnections.map((connection) => connection.name);
  for (const name of duplicateNames([...databaseNames, ...kafkaNames])) {
    issues.push({ level: "error", message: `Nombre de conexión duplicado: '${name}'.` });
  }
  for (const connection of meta.databaseConnections) {
    if (!connection.name.trim() || !connection.url_env.trim()) {
      issues.push({ level: "error", message: "Cada conexión de base de datos necesita nombre y url_env." });
    }
    if (connection.max_connections < 1) {
      issues.push({ level: "error", message: `La conexión '${connection.name}' necesita al menos una conexión.` });
    }
  }
  for (const connection of meta.kafkaConnections) {
    if (!connection.name.trim() || !connection.brokers_env.trim()) {
      issues.push({ level: "error", message: "Cada conexión Kafka necesita nombre y brokers_env." });
    }
  }

  const seen = new Set<string>();
  for (const node of nodes) {
    const id = node.data.processorId.trim();
    if (!id) {
      issues.push({ level: "error", message: "Hay un procesador sin identificador." });
      continue;
    }
    if (seen.has(id)) {
      issues.push({ level: "error", message: `Identificador de procesador duplicado: '${id}'.` });
    }
    seen.add(id);

    const def = CATALOG_BY_TYPE[node.data.type];
    if (!def) continue;
    for (const field of def.fields) {
      if (!field.required) continue;
      const value = node.data.config[field.key];
      const empty =
        isBlank(value) ||
        (Array.isArray(value) && value.length === 0) ||
        (typeof value === "object" &&
          value !== null &&
          !Array.isArray(value) &&
          Object.keys(value as object).length === 0);
      if (empty) {
        issues.push({
          level: "error",
          message: `'${id}' requiere el campo '${field.label}'.`,
        });
      }
    }

    if (node.data.scheduling.concurrent_tasks < 1) {
      issues.push({ level: "error", message: `'${id}' necesita al menos una tarea concurrente.` });
    }
    if (
      node.data.scheduling.maximum_in_flight !== null &&
      node.data.scheduling.maximum_in_flight < node.data.scheduling.concurrent_tasks
    ) {
      issues.push({
        level: "error",
        message: `'${id}' tiene máximo en vuelo menor que sus tareas concurrentes.`,
      });
    }
    if (
      node.data.scheduling.ordering === "partitioned" &&
      !node.data.scheduling.partition_by?.trim()
    ) {
      issues.push({ level: "error", message: `'${id}' usa orden particionado sin selector.` });
    }
    if (node.data.retry.maximum_delay_ms < node.data.retry.initial_delay_ms) {
      issues.push({ level: "error", message: `'${id}' tiene retardo máximo menor que el inicial.` });
    }
    if (node.data.simulation.mode === "replay" && !meta.engine.repository_enabled) {
      issues.push({
        level: "error",
        message: `'${id}' usa Replay, pero el repositorio persistente está deshabilitado.`,
      });
    }
    if (node.data.simulation.mode !== "real") {
      issues.push({
        level: "warning",
        message: `'${id}' usa ${node.data.simulation.mode}; debe ejecutarse mediante jaiba-simulator.`,
      });
    }

    const connection = String(node.data.config.connection ?? "").trim();
    const connectionField = def.fields.find((field) => field.key === "connection");
    const kind = connectionField?.connectionKind;
    const localDb = databaseNames.includes(connection);
    const aliasDb = knownDatabaseAliases.has(connection);
    const localKafka = kafkaNames.includes(connection);
    const aliasKafka = knownKafkaAliases.has(connection);

    if (
      connection &&
      kind &&
      kind !== "kafka" &&
      !localDb &&
      !aliasDb
    ) {
      issues.push({
        level: "error",
        message: `'${id}' referencia una conexión inexistente: '${connection}'. Créala en Conexiones o en Configuración del flujo.`,
      });
    }
    if (
      connection &&
      kind === "postgres" &&
      !aliasDb &&
      !meta.databaseConnections.some(
        (candidate) => candidate.name === connection && candidate.type === "postgres",
      )
    ) {
      // Si el alias viene del Connection Manager, el tipo se valida en runtime.
      if (localDb) {
        issues.push({
          level: "error",
          message: `'${id}' necesita una conexión PostgreSQL: '${connection}'.`,
        });
      }
    }
    if (connection && kind === "oracle" && localDb && !aliasDb) {
      const local = meta.databaseConnections.find((candidate) => candidate.name === connection);
      if (local && local.type && local.type !== "oracle") {
        issues.push({
          level: "error",
          message: `'${id}' necesita una conexión Oracle: '${connection}'.`,
        });
      }
    }
    if (connection && kind === "kafka" && !localKafka && !aliasKafka) {
      issues.push({
        level: "error",
        message: `'${id}' referencia una conexión Kafka inexistente: '${connection}'. Defínela en Configuración del flujo.`,
      });
    }

    if (
      (node.data.type === "put_database" || node.data.type === "auto_destination") &&
      node.data.config.mode === "upsert" &&
      (!Array.isArray(node.data.config.conflict_columns) ||
        (node.data.config.conflict_columns as unknown[]).length === 0)
    ) {
      issues.push({
        level: "error",
        message: `'${id}' usa upsert y necesita columnas de conflicto.`,
      });
    }
    if (
      node.data.type === "put_mongodb" &&
      node.data.config.mode === "upsert" &&
      (!Array.isArray(node.data.config.key_fields) ||
        (node.data.config.key_fields as unknown[]).length === 0)
    ) {
      issues.push({
        level: "error",
        message: `'${id}' usa upsert MongoDB y necesita campos clave.`,
      });
    }
  }

  const nodeIds = new Set(nodes.map((node) => node.id));
  for (const edge of edges) {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) {
      issues.push({ level: "error", message: "Hay una conexión que referencia un nodo inexistente." });
    }
    if ((edge.data?.queueCapacity ?? 100) < 1) {
      issues.push({ level: "error", message: "La capacidad de cada conexión debe ser mayor que cero." });
    }
  }

  const targets = new Set(edges.map((e) => e.target));
  const hasStart = nodes.some((n) => !targets.has(n.id));
  if (nodes.length > 0 && !hasStart) {
    issues.push({
      level: "error",
      message: "El flujo no tiene un procesador inicial (posible ciclo).",
    });
  }
  const requiredConcurrency = longestPath(nodes, edges);
  if (nodes.length > 0 && meta.engine.max_concurrency < requiredConcurrency) {
    issues.push({
      level: "error",
      message: `La concurrencia máxima debe ser al menos ${requiredConcurrency} para la ruta más larga.`,
    });
  }

  return issues;
}

export interface ImportedFlow {
  meta: FlowMeta;
  nodes: ProcessorNode[];
  edges: ConnectionEdge[];
}

function object(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function numberOr(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function stringOr(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

/** Converts an existing Jaiva YAML into the visual model without resolving secrets. */
export function parseFlowYaml(content: string): ImportedFlow {
  const root = object(parse(content));
  const processors = Array.isArray(root.processors) ? root.processors.map(object) : [];
  if (processors.length === 0) throw new Error("El YAML no contiene procesadores.");

  const engine = object(root.engine);
  const memory = object(engine.memory);
  const repository = object(engine.repository);
  const circuitBreaker = object(engine.circuit_breaker);
  const admin = object(engine.admin);
  const parameters = object(root.parameters);
  const databases = object(root.database_connections);
  const kafka = object(root.kafka_connections);

  const databaseConnections: DatabaseConnection[] = Object.entries(databases).map(
    ([name, raw]) => {
      const value = object(raw);
      return {
        name,
        type: stringOr(value.type, "postgres"),
        url_env: stringOr(value.url_env),
        max_connections: numberOr(value.max_connections, 5),
      };
    },
  );
  const kafkaConnections: KafkaConnection[] = Object.entries(kafka).map(([name, raw]) => {
    const value = object(raw);
    return {
      name,
      brokers_env: stringOr(value.brokers_env),
      client_id: stringOr(value.client_id, "jaiva"),
    };
  });
  const scheduleRoot = object(root.schedule);
  const trigger = object(scheduleRoot.trigger);
  const triggerType = (["interval", "cron", "webhook"].includes(stringOr(trigger.type))
    ? stringOr(trigger.type)
    : SCHEDULE_DEFAULTS.triggerType) as ScheduleTriggerType;
  const overlap = (["skip", "queue", "replace"].includes(stringOr(scheduleRoot.overlap))
    ? stringOr(scheduleRoot.overlap)
    : SCHEDULE_DEFAULTS.overlap) as OverlapPolicy;
  const catchUp = (["none", "one"].includes(stringOr(scheduleRoot.catch_up))
    ? stringOr(scheduleRoot.catch_up)
    : SCHEDULE_DEFAULTS.catchUp) as CatchUpPolicy;

  const meta: FlowMeta = {
    id: stringOr(root.id, "flujo"),
    parameters: Object.entries(parameters).map(([name, value]) => ({
      name,
      value: stringOr(value, String(value ?? "")),
    })),
    databaseConnections,
    kafkaConnections,
    schedule: {
      enabled: Boolean(scheduleRoot.enabled),
      triggerType,
      everySeconds: numberOr(trigger.every_seconds, SCHEDULE_DEFAULTS.everySeconds),
      cronExpression: stringOr(trigger.expression, SCHEDULE_DEFAULTS.cronExpression),
      timezone: stringOr(scheduleRoot.timezone, SCHEDULE_DEFAULTS.timezone),
      overlap,
      catchUp,
    },
    engine: {
      queue_capacity: numberOr(engine.queue_capacity, ENGINE_DEFAULTS.queue_capacity),
      max_concurrency: numberOr(engine.max_concurrency, ENGINE_DEFAULTS.max_concurrency),
      memory_maximum_percent: numberOr(
        memory.maximum_percent,
        ENGINE_DEFAULTS.memory_maximum_percent,
      ),
      repository_enabled:
        typeof repository.enabled === "boolean"
          ? repository.enabled
          : ENGINE_DEFAULTS.repository_enabled,
      circuit_breaker_enabled:
        typeof circuitBreaker.enabled === "boolean"
          ? circuitBreaker.enabled
          : ENGINE_DEFAULTS.circuit_breaker_enabled,
      admin_enabled:
        typeof admin.enabled === "boolean" ? admin.enabled : ENGINE_DEFAULTS.admin_enabled,
      admin_authentication:
        admin.authentication === "none" ? "none" : ENGINE_DEFAULTS.admin_authentication,
      admin_token_env: stringOr(admin.token_env, ENGINE_DEFAULTS.admin_token_env),
    },
  };

  const nodeByProcessor = new Map<string, string>();
  const levelByProcessor = new Map<string, number>();
  const connectionValues = Array.isArray(root.connections) ? root.connections.map(object) : [];
  for (let pass = 0; pass < processors.length; pass += 1) {
    for (const connection of connectionValues) {
      const from = stringOr(connection.from);
      const to = stringOr(connection.to);
      levelByProcessor.set(
        to,
        Math.max(levelByProcessor.get(to) ?? 0, (levelByProcessor.get(from) ?? 0) + 1),
      );
    }
  }
  const rowsByLevel = new Map<number, number>();
  const nodes: ProcessorNode[] = processors.map((processor, index) => {
    const id = stringOr(processor.id, `processor_${index + 1}`);
    const internalId = `import_${index}_${id.replace(/[^a-zA-Z0-9_-]/g, "_")}`;
    nodeByProcessor.set(id, internalId);
    const level = levelByProcessor.get(id) ?? 0;
    const row = rowsByLevel.get(level) ?? 0;
    rowsByLevel.set(level, row + 1);
    const retry = object(processor.retry);
    const scheduling = object(processor.scheduling);
    const simulation = object(processor.simulation);
    return {
      id: internalId,
      type: "processor",
      position: { x: 80 + level * 260, y: 70 + row * 180 },
      data: {
        processorId: id,
        type: stringOr(processor.type),
        config: object(processor.config),
        retry: {
          maximum_attempts: numberOr(retry.maximum_attempts, RETRY_DEFAULTS.maximum_attempts),
          initial_delay_ms: numberOr(retry.initial_delay_ms, RETRY_DEFAULTS.initial_delay_ms),
          maximum_delay_ms: numberOr(retry.maximum_delay_ms, RETRY_DEFAULTS.maximum_delay_ms),
        },
        scheduling: {
          concurrent_tasks: numberOr(
            scheduling.concurrent_tasks,
            SCHEDULING_DEFAULTS.concurrent_tasks,
          ),
          maximum_in_flight:
            scheduling.maximum_in_flight === undefined
              ? null
              : numberOr(scheduling.maximum_in_flight, 1),
          timeout_ms:
            scheduling.timeout_ms === undefined ? null : numberOr(scheduling.timeout_ms, 1),
          execution_mode:
            scheduling.execution_mode === "async_io" ||
            scheduling.execution_mode === "blocking_io" ||
            scheduling.execution_mode === "cpu"
              ? scheduling.execution_mode
              : "auto",
          ordering:
            scheduling.ordering === "preserve" || scheduling.ordering === "partitioned"
              ? scheduling.ordering
              : "unordered",
          partition_by:
            typeof scheduling.partition_by === "string" ? scheduling.partition_by : null,
        },
        simulation: {
          mode:
            simulation.mode === "mock" || simulation.mode === "replay"
              ? simulation.mode
              : "real",
          options: object(simulation.options),
        },
      },
    };
  });
  const edges: ConnectionEdge[] = connectionValues.flatMap((connection, index) => {
    const source = nodeByProcessor.get(stringOr(connection.from));
    const target = nodeByProcessor.get(stringOr(connection.to));
    if (!source || !target) return [];
    const queue = object(connection.queue);
    const relationship = connection.relationship === "failure" ? "failure" : "success";
    return [{
      id: `import_edge_${index}`,
      source,
      target,
      sourceHandle: relationship,
      targetHandle: "in",
      type: "default",
      style: {
        stroke: relationship === "failure" ? "#c2603f" : "#2f8f83",
        strokeWidth: 2,
      },
      data: {
        relationship,
        queueCapacity: numberOr(queue.capacity, 100),
      },
    }];
  });

  return { meta, nodes, edges };
}

export function downloadYaml(filename: string, content: string): void {
  const blob = new Blob([content], { type: "application/x-yaml;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename.endsWith(".yaml") ? filename : `${filename}.yaml`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  addEdge,
  Background,
  Controls,
  Handle,
  MiniMap,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type EdgeChange,
  type EdgeMouseHandler,
  type NodeChange,
  type NodeMouseHandler,
  type NodeProps,
  type NodeTypes,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { jaivaApi } from "../api";
import type { FlowAction, FlowLifecycle, FlowSnapshot } from "../types";
import {
  CATALOG_BY_TYPE,
  CATEGORY_LABEL,
  CATEGORY_TAG,
  PROCESSOR_CATALOG,
  UPCOMING_COMPONENTS,
} from "./catalog";
import {
  BottomPanel,
  type BottomTab,
  type FlowStats,
  type LogChannel,
  type LogEntry,
} from "./BottomPanel";
import { Inspector } from "./Inspector";
import { Modal } from "./Modal";
import { SettingsPanel } from "./SettingsPanel";
import { TopMenu } from "./TopMenu";
import {
  createProcessorNode,
  defaultFlowMeta,
  SIMULATION_DEFAULTS,
  type ConnectionEdge,
  type FlowMeta,
  type ProcessorNode,
  type Relationship,
  type RetrySettings,
  type SchedulingSettings,
  type SimulationSettings,
} from "./model";
import { downloadYaml, parseFlowYaml, toYaml, validateFlow } from "./yaml";

const DRAG_TYPE = "application/jaiba-processor";
const DRAFT_KEY = "jaiba.visual.builder.v1";
const LEGACY_DRAFT_KEY = "jaiva.visual.builder.v1";
const RUN_ACTIONS: FlowAction[] = ["start", "pause", "resume", "drain", "stop"];
const ACTION_LABEL: Record<FlowAction, string> = {
  start: "Iniciar",
  pause: "Pausar",
  resume: "Reanudar",
  drain: "Drenar",
  stop: "Detener",
};
const VALID_ACTIONS: Record<FlowLifecycle, FlowAction[]> = {
  STOPPED: ["start"],
  STARTING: ["drain", "stop"],
  RUNNING: ["pause", "drain", "stop"],
  PAUSED: ["resume", "drain", "stop"],
  DRAINING: ["stop"],
  FAILED: ["start"],
};

interface StoredDraft {
  version: 1;
  meta: FlowMeta;
  nodes: ProcessorNode[];
  edges: ConnectionEdge[];
}

function loadDraft(): StoredDraft | null {
  try {
    const raw =
      window.localStorage.getItem(DRAFT_KEY) ??
      window.localStorage.getItem(LEGACY_DRAFT_KEY);
    if (!raw) return null;
    const value = JSON.parse(raw) as StoredDraft;
    if (value.version !== 1) return null;
    return {
      ...value,
      nodes: value.nodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          simulation: node.data.simulation ?? {
            ...SIMULATION_DEFAULTS,
            options: {},
          },
        },
      })),
    };
  } catch {
    return null;
  }
}

function ProcessorNodeView({ data, selected }: NodeProps<ProcessorNode>) {
  const def = CATALOG_BY_TYPE[data.type];
  const category = def?.category ?? "transform";
  return (
    <div className={`rf-node ${category} ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} id="in" />
      <span className={`rf-node-tag ${category}`}>
        {def ? CATEGORY_TAG[category] : "Proceso"}
      </span>
      <strong className="rf-node-id">{data.processorId}</strong>
      <small className="rf-node-type">{data.type}</small>
      <Handle type="source" position={Position.Right} id="success" className="handle-success" style={{ top: "42%" }} />
      <Handle type="source" position={Position.Right} id="failure" className="handle-failure" style={{ top: "72%" }} />
      <span className="rf-handle-hint success">success</span>
      <span className="rf-handle-hint failure">failure</span>
    </div>
  );
}

const nodeTypes: NodeTypes = { processor: ProcessorNodeView };

function edgeStyle(relationship: Relationship) {
  return {
    stroke: relationship === "failure" ? "#c2603f" : "#2f8f83",
    strokeWidth: 2,
  };
}

type DrawerMode = "none" | "node" | "settings";

function FlowBuilderInner() {
  const initialDraft = useRef<StoredDraft | null>(loadDraft());
  const [nodes, setNodes, onNodesChange] = useNodesState<ProcessorNode>(
    initialDraft.current?.nodes ?? [],
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState<ConnectionEdge>(
    initialDraft.current?.edges ?? [],
  );
  const [meta, setMeta] = useState<FlowMeta>(
    initialDraft.current?.meta ?? defaultFlowMeta(),
  );
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [drawer, setDrawer] = useState<DrawerMode>("none");
  const [bottomTab, setBottomTab] = useState<BottomTab>("consola");
  const [bottomCollapsed, setBottomCollapsed] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [engineOnline, setEngineOnline] = useState(false);
  const [engineFlow, setEngineFlow] = useState<FlowSnapshot | null>(null);
  const [modal, setModal] = useState<"yaml" | "run" | null>(null);
  const [runBusy, setRunBusy] = useState<FlowAction | null>(null);
  const [deployBusy, setDeployBusy] = useState<"validate" | "deploy" | "start" | null>(null);
  const [saveState, setSaveState] = useState<"saved" | "saving" | "unsaved">(
    initialDraft.current ? "saved" : "unsaved",
  );

  const logSeq = useRef(0);
  const importInput = useRef<HTMLInputElement>(null);
  const { screenToFlowPosition, fitView } = useReactFlow();

  const addLog = useCallback(
    (channel: LogChannel, level: LogEntry["level"], message: string) => {
      logSeq.current += 1;
      const entry: LogEntry = {
        id: logSeq.current,
        channel,
        level,
        time: new Date().toLocaleTimeString("es-MX"),
        message,
      };
      setLogs((current) => [entry, ...current].slice(0, 200));
    },
    [],
  );

  const databaseConnectionNames = useMemo(
    () => meta.databaseConnections.map((connection) => connection.name).filter(Boolean),
    [meta.databaseConnections],
  );
  const kafkaConnectionNames = useMemo(
    () => meta.kafkaConnections.map((connection) => connection.name).filter(Boolean),
    [meta.kafkaConnections],
  );
  const postgresConnectionNames = useMemo(
    () =>
      meta.databaseConnections
        .filter((connection) => connection.type === "postgres")
        .map((connection) => connection.name)
        .filter(Boolean),
    [meta.databaseConnections],
  );

  const selectedNode = useMemo(
    () => nodes.find((node) => node.id === selectedNodeId) ?? null,
    [nodes, selectedNodeId],
  );
  const selectedEdge = useMemo(
    () => edges.find((edge) => edge.id === selectedEdgeId) ?? null,
    [edges, selectedEdgeId],
  );

  const issues = useMemo(() => validateFlow(meta, nodes, edges), [meta, nodes, edges]);
  const errorCount = issues.filter((issue) => issue.level === "error").length;

  const yaml = useMemo(() => {
    try {
      return toYaml(meta, nodes, edges);
    } catch (error) {
      return `# Error al generar YAML: ${error instanceof Error ? error.message : error}`;
    }
  }, [meta, nodes, edges]);

  const persistDraft = useCallback(() => {
    const draft: StoredDraft = { version: 1, meta, nodes, edges };
    window.localStorage.setItem(DRAFT_KEY, JSON.stringify(draft));
    setSaveState("saved");
  }, [meta, nodes, edges]);

  useEffect(() => {
    setSaveState("saving");
    const timer = window.setTimeout(persistDraft, 600);
    return () => window.clearTimeout(timer);
  }, [persistDraft]);

  const stats = useMemo<FlowStats>(() => {
    const category = (type: string) => CATALOG_BY_TYPE[type]?.category;
    return {
      processors: nodes.length,
      connections: edges.length,
      sources: nodes.filter((n) => category(n.data.type) === "source").length,
      transforms: nodes.filter((n) => category(n.data.type) === "transform").length,
      sinks: nodes.filter((n) => category(n.data.type) === "sink").length,
      parameters: meta.parameters.filter((p) => p.name.trim() !== "").length,
      databaseConnections: meta.databaseConnections.filter((c) => c.name.trim() !== "").length,
      kafkaConnections: meta.kafkaConnections.filter((c) => c.name.trim() !== "").length,
      successEdges: edges.filter((e) => (e.data?.relationship ?? "success") === "success").length,
      failureEdges: edges.filter((e) => e.data?.relationship === "failure").length,
    };
  }, [nodes, edges, meta]);

  // Engine health and served-flow polling. Runtime control never assumes that
  // the draft ID is already loaded by the independent Rust process.
  const prevOnline = useRef<boolean | null>(null);
  useEffect(() => {
    let active = true;
    const check = async () => {
      let online = false;
      try {
        await jaivaApi.health();
        const flows = await jaivaApi.flows();
        if (active) setEngineFlow(flows[0] ?? null);
        online = true;
      } catch {
        online = false;
        if (active) setEngineFlow(null);
      }
      if (!active) return;
      setEngineOnline(online);
      if (prevOnline.current !== online) {
        addLog("console", online ? "info" : "warn", online ? "Motor Jaiba conectado." : "Motor Jaiba desconectado.");
        prevOnline.current = online;
      }
    };
    void check();
    const timer = window.setInterval(() => void check(), 4000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [addLog]);

  const handleNodesChange = useCallback(
    (changes: NodeChange<ProcessorNode>[]) => onNodesChange(changes),
    [onNodesChange],
  );
  const handleEdgesChange = useCallback(
    (changes: EdgeChange<ConnectionEdge>[]) => onEdgesChange(changes),
    [onEdgesChange],
  );

  const handleNodeClick = useCallback<NodeMouseHandler<ProcessorNode>>((_event, node) => {
    setSelectedNodeId(node.id);
    setSelectedEdgeId(null);
    setDrawer("node");
  }, []);

  const handleEdgeClick = useCallback<EdgeMouseHandler<ConnectionEdge>>((_event, edge) => {
    setSelectedEdgeId(edge.id);
    setSelectedNodeId(null);
    setDrawer("node");
  }, []);

  const onConnect = useCallback(
    (connection: Connection) => {
      const relationship: Relationship =
        connection.sourceHandle === "failure" ? "failure" : "success";
      setEdges((current: ConnectionEdge[]) =>
        addEdge<ConnectionEdge>(
          {
            ...connection,
            id: `edge_${Date.now()}_${Math.round(Math.random() * 1e6)}`,
            type: "smoothstep",
            animated: relationship === "success",
            label: relationship,
            data: { relationship, queueCapacity: 100 },
            style: edgeStyle(relationship),
          },
          current,
        ),
      );
      addLog("evento", "info", `Conexión '${relationship}' creada.`);
    },
    [setEdges, addLog],
  );

  const addNode = useCallback(
    (type: string, position: { x: number; y: number }) => {
      let createdId = "";
      setNodes((current: ProcessorNode[]) => {
        const ids = new Set(current.map((node) => node.data.processorId));
        const node = createProcessorNode(type, position, ids);
        createdId = node.data.processorId;
        return [...current, node];
      });
      const label = CATALOG_BY_TYPE[type]?.label ?? type;
      addLog("evento", "info", `Componente agregado: ${label} (${createdId}).`);
      window.setTimeout(() => {
        void fitView({ padding: 0.3, maxZoom: 1.2, duration: 250 });
      }, 40);
    },
    [setNodes, addLog, fitView],
  );

  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const type = event.dataTransfer.getData(DRAG_TYPE);
      if (!type || !CATALOG_BY_TYPE[type]) return;
      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      addNode(type, position);
    },
    [addNode, screenToFlowPosition],
  );

  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = "move";
  }, []);

  const updateSelectedNode = useCallback(
    (updater: (data: ProcessorNode["data"]) => ProcessorNode["data"]) => {
      if (!selectedNodeId) return;
      setNodes((current: ProcessorNode[]) =>
        current.map((node) =>
          node.id === selectedNodeId ? { ...node, data: updater(node.data) } : node,
        ),
      );
    },
    [selectedNodeId, setNodes],
  );

  const deleteSelectedNode = useCallback(() => {
    if (!selectedNodeId) return;
    const removed = nodes.find((node) => node.id === selectedNodeId)?.data.processorId;
    setNodes((current: ProcessorNode[]) => current.filter((node) => node.id !== selectedNodeId));
    setEdges((current: ConnectionEdge[]) =>
      current.filter((edge) => edge.source !== selectedNodeId && edge.target !== selectedNodeId),
    );
    setSelectedNodeId(null);
    setDrawer("none");
    if (removed) addLog("evento", "info", `Componente eliminado: ${removed}.`);
  }, [selectedNodeId, nodes, setNodes, setEdges, addLog]);

  const updateSelectedEdge = useCallback(
    (relationship: Relationship, queueCapacity: number) => {
      if (!selectedEdgeId) return;
      setEdges((current: ConnectionEdge[]) =>
        current.map((edge) =>
          edge.id === selectedEdgeId
            ? {
                ...edge,
                sourceHandle: relationship,
                animated: relationship === "success",
                label: relationship,
                style: edgeStyle(relationship),
                data: { relationship, queueCapacity },
              }
            : edge,
        ),
      );
    },
    [selectedEdgeId, setEdges],
  );

  const deleteSelectedEdge = useCallback(() => {
    if (!selectedEdgeId) return;
    setEdges((current: ConnectionEdge[]) => current.filter((edge) => edge.id !== selectedEdgeId));
    setSelectedEdgeId(null);
    setDrawer("none");
    addLog("evento", "info", "Conexión eliminada.");
  }, [selectedEdgeId, setEdges, addLog]);

  const download = useCallback(() => {
    downloadYaml(meta.id || "flujo", yaml);
    addLog("console", "info", `YAML descargado: ${meta.id || "flujo"}.yaml`);
  }, [meta.id, yaml, addLog]);

  const importYaml = useCallback(
    async (file: File) => {
      try {
        const imported = parseFlowYaml(await file.text());
        setMeta(imported.meta);
        setNodes(imported.nodes);
        setEdges(imported.edges);
        setSelectedNodeId(null);
        setSelectedEdgeId(null);
        setDrawer("none");
        addLog("console", "info", `YAML importado: ${file.name}.`);
        window.setTimeout(() => void fitView({ padding: 0.15 }), 50);
      } catch (error) {
        addLog(
          "console",
          "error",
          `No se pudo importar '${file.name}': ${error instanceof Error ? error.message : error}`,
        );
        setBottomCollapsed(false);
        setBottomTab("consola");
      } finally {
        if (importInput.current) importInput.current.value = "";
      }
    },
    [setNodes, setEdges, fitView, addLog],
  );

  const clearCanvas = useCallback(() => {
    setNodes(() => []);
    setEdges(() => []);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setDrawer("none");
    addLog("evento", "warn", "Lienzo limpiado.");
  }, [setNodes, setEdges, addLog]);

  const runAction = useCallback(
    async (action: FlowAction) => {
      if (!engineFlow || engineFlow.flow_id !== meta.id) return;
      setRunBusy(action);
      try {
        const snapshot = await jaivaApi.mutate(engineFlow.flow_id, action);
        setEngineFlow(snapshot);
        addLog("console", "info", `Acción '${ACTION_LABEL[action]}' aceptada. Estado: ${snapshot.control.state}.`);
      } catch (error) {
        addLog(
          "console",
          "error",
          `Acción '${ACTION_LABEL[action]}' falló: ${error instanceof Error ? error.message : error}`,
        );
      } finally {
        setRunBusy(null);
      }
    },
    [meta.id, engineFlow, addLog],
  );

  const validateInEngine = useCallback(async () => {
    setDeployBusy("validate");
    try {
      const result = await jaivaApi.validateFlow(yaml);
      addLog(
        "console",
        "info",
        `Motor validó '${result.flow_id}': ${result.processors} procesadores y ${result.connections} conexiones.`,
      );
    } catch (error) {
      addLog("console", "error", `Validación Rust falló: ${error instanceof Error ? error.message : error}`);
      setBottomCollapsed(false);
      setBottomTab("consola");
    } finally {
      setDeployBusy(null);
    }
  }, [yaml, addLog]);

  const deployToEngine = useCallback(
    async (start: boolean) => {
      setDeployBusy(start ? "start" : "deploy");
      try {
        await jaivaApi.validateFlow(yaml);
        const snapshot = await jaivaApi.deployFlow(meta.id, yaml, start);
        setEngineFlow(snapshot);
        addLog(
          "console",
          "info",
          `Flujo '${meta.id}' publicado${start ? " e iniciado" : " en estado detenido"}.`,
        );
      } catch (error) {
        addLog("console", "error", `Publicación falló: ${error instanceof Error ? error.message : error}`);
        setBottomCollapsed(false);
        setBottomTab("consola");
      } finally {
        setDeployBusy(null);
      }
    },
    [meta.id, yaml, addLog],
  );

  const runCommand = `cargo run -- serve ${meta.id || "flujo"}.yaml`;

  const showLogs = useCallback(() => {
    setBottomCollapsed(false);
    setBottomTab("consola");
  }, []);

  const openSettings = useCallback(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setDrawer("settings");
  }, []);

  const closeDrawer = useCallback(() => {
    setDrawer("none");
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
  }, []);

  return (
    <div className="ide-shell">
      <TopMenu
        engineOnline={engineOnline}
        downloadDisabled={errorCount > 0}
        saveState={saveState}
        onViewYaml={() => setModal("yaml")}
        onDownload={download}
        onImport={() => importInput.current?.click()}
        onSave={persistDraft}
        onClear={clearCanvas}
        onRun={() => setModal("run")}
        onShowLogs={showLogs}
        onOpenSettings={openSettings}
      />
      <input
        ref={importInput}
        type="file"
        accept=".yaml,.yml,application/yaml,text/yaml"
        hidden
        onChange={(event) => {
          const file = event.target.files?.[0];
          if (file) void importYaml(file);
        }}
      />

      <div className="ide-middle">
        <aside className="components">
          <h3>Componentes</h3>
          {(["source", "transform", "sink"] as const).map((category) => (
            <div className="palette-group" key={category}>
              <span className="palette-group-title">{CATEGORY_LABEL[category]}</span>
              {PROCESSOR_CATALOG.filter((def) => def.category === category).map((def) => (
                <button
                  key={def.type}
                  type="button"
                  className={`palette-item ${category}`}
                  draggable
                  onDragStart={(event) => {
                    event.dataTransfer.setData(DRAG_TYPE, def.type);
                    event.dataTransfer.effectAllowed = "move";
                  }}
                  onClick={() =>
                    addNode(def.type, { x: 80 + Math.random() * 120, y: 60 + Math.random() * 220 })
                  }
                  title={def.description}
                >
                  <strong>{def.label}</strong>
                  <small>{def.type}</small>
                </button>
              ))}
            </div>
          ))}
          <div className="palette-group">
            <span className="palette-group-title">Próximamente</span>
            {UPCOMING_COMPONENTS.map((component) => (
              <div className="palette-item upcoming" key={component.label} title={component.note}>
                <strong>{component.label}</strong>
                <small>no disponible en el motor</small>
              </div>
            ))}
          </div>
        </aside>

        <div className="canvas-area" onDrop={onDrop} onDragOver={onDragOver}>
          <ReactFlow<ProcessorNode, ConnectionEdge>
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={handleNodesChange}
            onEdgesChange={handleEdgesChange}
            onConnect={onConnect}
            onNodeClick={handleNodeClick}
            onEdgeClick={handleEdgeClick}
            onPaneClick={() => {
              setSelectedNodeId(null);
              setSelectedEdgeId(null);
              if (drawer === "node") setDrawer("none");
            }}
            fitView
            proOptions={{ hideAttribution: true }}
          >
            <Background color="#2f8f8322" gap={20} />
            <MiniMap pannable zoomable className="rf-minimap" />
            <Controls />
          </ReactFlow>
          {nodes.length === 0 ? (
            <div className="canvas-empty">
              <p>Arrastra un componente aquí para comenzar tu flujo.</p>
            </div>
          ) : null}
        </div>

        {drawer !== "none" ? (
          <aside className="ide-drawer">
            <div className="drawer-head">
              <span>{drawer === "settings" ? "Configuración" : "Propiedades"}</span>
              <button
                type="button"
                className="builder-icon-btn"
                aria-label="Cerrar panel"
                onClick={closeDrawer}
              >
                ×
              </button>
            </div>
            <div className="drawer-body">
              {drawer === "settings" ? (
                <SettingsPanel meta={meta} onChange={setMeta} />
              ) : selectedEdge ? (
                <EdgeInspector
                  relationship={selectedEdge.data?.relationship ?? "success"}
                  queueCapacity={selectedEdge.data?.queueCapacity ?? 100}
                  onChange={updateSelectedEdge}
                  onDelete={deleteSelectedEdge}
                />
              ) : (
                <Inspector
                  node={selectedNode}
                  databaseConnectionNames={databaseConnectionNames}
                  postgresConnectionNames={postgresConnectionNames}
                  kafkaConnectionNames={kafkaConnectionNames}
                  onChangeProcessorId={(id) =>
                    updateSelectedNode((data) => ({ ...data, processorId: id }))
                  }
                  onChangeConfig={(key, value) =>
                    updateSelectedNode((data) => ({
                      ...data,
                      config: { ...data.config, [key]: value },
                    }))
                  }
                  onChangeRetry={(retry: RetrySettings) =>
                    updateSelectedNode((data) => ({ ...data, retry }))
                  }
                  onChangeScheduling={(scheduling: SchedulingSettings) =>
                    updateSelectedNode((data) => ({ ...data, scheduling }))
                  }
                  onChangeSimulation={(simulation: SimulationSettings) =>
                    updateSelectedNode((data) => ({ ...data, simulation }))
                  }
                  onDelete={deleteSelectedNode}
                />
              )}
            </div>
          </aside>
        ) : null}
      </div>

      <BottomPanel
        tab={bottomTab}
        onTab={setBottomTab}
        logs={logs}
        stats={stats}
        issues={issues}
        collapsed={bottomCollapsed}
        onToggleCollapsed={() => setBottomCollapsed((value) => !value)}
      />

      {modal === "yaml" ? (
        <Modal
          title={`${meta.id || "flujo"}.yaml`}
          onClose={() => setModal(null)}
          footer={
            <button
              type="button"
              className="button primary"
              disabled={errorCount > 0}
              onClick={download}
            >
              Descargar
            </button>
          }
        >
          <pre className="yaml-preview">{yaml}</pre>
        </Modal>
      ) : null}

      {modal === "run" ? (
        <Modal title="Ejecutar flujo" onClose={() => setModal(null)}>
          <div className="run-modal">
            <p className="run-hint">
              El motor Jaiba es un binario Rust independiente. Descarga el YAML y ejecútalo en tu
              terminal:
            </p>
            <div className="run-command">
              <code>{runCommand}</code>
              <button
                type="button"
                className="button subtle"
                onClick={() => {
                  void navigator.clipboard?.writeText(runCommand);
                  addLog("console", "info", "Comando copiado al portapapeles.");
                }}
              >
                Copiar
              </button>
            </div>
            <button
              type="button"
              className="button primary"
              disabled={errorCount > 0}
              onClick={download}
            >
              Descargar {meta.id || "flujo"}.yaml
            </button>

            <div className="run-divider" />

            <div className="run-api-head">
              <span>Validar y publicar en el motor</span>
              <span className={`engine-pill small ${engineOnline ? "online" : "offline"}`}>
                <i />
                {engineOnline ? "en línea" : "desconectado"}
              </span>
            </div>
            <p className="run-hint">
              Publicar reemplaza de forma coordinada el flujo activo de esta instancia. El YAML
              se valida antes de detenerlo.
            </p>
            <div className="run-actions deploy-actions">
              <button
                type="button"
                className="button"
                disabled={!engineOnline || errorCount > 0 || deployBusy !== null}
                onClick={() => void validateInEngine()}
              >
                {deployBusy === "validate" ? "Validando…" : "Validar con Rust"}
              </button>
              <button
                type="button"
                className="button"
                disabled={!engineOnline || errorCount > 0 || deployBusy !== null}
                onClick={() => void deployToEngine(false)}
              >
                {deployBusy === "deploy" ? "Publicando…" : "Publicar detenido"}
              </button>
              <button
                type="button"
                className="button primary"
                disabled={!engineOnline || errorCount > 0 || deployBusy !== null}
                onClick={() => void deployToEngine(true)}
              >
                {deployBusy === "start" ? "Publicando…" : "Publicar e iniciar"}
              </button>
            </div>

            <div className="run-divider" />

            <div className="run-api-head">
              <span>Control del flujo publicado</span>
              <code>{engineFlow?.flow_id ?? "ninguno"}</code>
            </div>
            {engineFlow?.flow_id !== meta.id ? (
              <p className="run-warning">
                El borrador <code>{meta.id}</code> no coincide con el flujo cargado en el motor.
                Publícalo antes de usar sus controles.
              </p>
            ) : null}
            <div className="run-actions">
              {RUN_ACTIONS.map((action) => (
                <button
                  key={action}
                  type="button"
                  className="button"
                  disabled={
                    !engineOnline ||
                    engineFlow?.flow_id !== meta.id ||
                    runBusy !== null ||
                    !VALID_ACTIONS[engineFlow?.control.state ?? "STOPPED"].includes(action)
                  }
                  onClick={() => void runAction(action)}
                >
                  {runBusy === action ? "…" : ACTION_LABEL[action]}
                </button>
              ))}
            </div>
          </div>
        </Modal>
      ) : null}
    </div>
  );
}

function EdgeInspector({
  relationship,
  queueCapacity,
  onChange,
  onDelete,
}: {
  relationship: Relationship;
  queueCapacity: number;
  onChange: (relationship: Relationship, queueCapacity: number) => void;
  onDelete: () => void;
}) {
  return (
    <div className="inspector">
      <div className="inspector-head">
        <span className="node-tag transform">Conexión</span>
        <button type="button" className="button subtle danger" onClick={onDelete}>
          Eliminar conexión
        </button>
      </div>
      <h3>Relación entre procesadores</h3>
      <label className="builder-field">
        <span className="builder-field-label">Relación</span>
        <select
          className="builder-input"
          value={relationship}
          onChange={(event) => onChange(event.target.value as Relationship, queueCapacity)}
        >
          <option value="success">success</option>
          <option value="failure">failure</option>
        </select>
      </label>
      <label className="builder-field">
        <span className="builder-field-label">Capacidad de cola</span>
        <input
          className="builder-input"
          type="number"
          min={1}
          value={queueCapacity}
          onChange={(event) => onChange(relationship, Number(event.target.value) || 1)}
        />
      </label>
    </div>
  );
}

export function FlowBuilder() {
  return (
    <ReactFlowProvider>
      <FlowBuilderInner />
    </ReactFlowProvider>
  );
}

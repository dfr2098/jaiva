import { lazy, Suspense, useCallback, useEffect, useMemo, useState } from "react";
import { jaivaApi } from "./api";
import {
  CrabMark,
  AdminAccess,
  FlowCanvas,
  FlowControls,
  LifecycleBadge,
  MetricCard,
  OperationsConsole,
  RuntimeDetails,
  Status,
} from "./components";
import type { FlowAction, FlowRecord, FlowSnapshot } from "./types";

const FlowBuilder = lazy(() =>
  import("./builder/FlowBuilder").then((module) => ({ default: module.FlowBuilder })),
);
const ConnectionManagerView = lazy(() =>
  import("./connections/ConnectionManagerView").then((module) => ({
    default: module.ConnectionManagerView,
  })),
);

type AppView = "monitor" | "builder" | "connections";

const formatNumber = (value = 0) =>
  new Intl.NumberFormat("es-MX").format(value);

function pickFlow(
  flows: FlowSnapshot[],
  selectedId: string | null,
  fallback: FlowSnapshot | null,
): FlowSnapshot | null {
  if (selectedId) {
    const match = flows.find((item) => item.flow_id === selectedId);
    if (match) return match;
  }
  if (fallback && flows.some((item) => item.flow_id === fallback.flow_id)) {
    return fallback;
  }
  return flows[0] ?? fallback;
}

export default function App() {
  const [flows, setFlows] = useState<FlowSnapshot[]>([]);
  const [registry, setRegistry] = useState<FlowRecord[]>([]);
  const [selectedFlowId, setSelectedFlowId] = useState<string | null>(null);
  const [online, setOnline] = useState(false);
  const [message, setMessage] = useState("Conectando con el motor");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<FlowAction | null>(null);
  const [view, setView] = useState<AppView>("monitor");
  const [transport, setTransport] = useState<"socket" | "polling">("polling");

  const flow = useMemo(
    () => pickFlow(flows, selectedFlowId, null),
    [flows, selectedFlowId],
  );

  const refresh = useCallback(async () => {
    try {
      await jaivaApi.health();
      const [primary, records] = await Promise.all([
        jaivaApi.runtime(),
        jaivaApi.listFlows().catch(() => [] as FlowRecord[]),
      ]);
      const runtimeById = new Map<string, FlowSnapshot>();
      if (primary) runtimeById.set(primary.flow_id, primary);
      await Promise.all(
        records.map(async (record) => {
          try {
            const detail = await jaivaApi.getFlow(record.id);
            if (detail.runtime) runtimeById.set(detail.id, detail.runtime);
          } catch {
            // Flow may exist in registry without a live supervisor.
          }
        }),
      );
      const next = [...runtimeById.values()].sort((left, right) =>
        left.flow_id.localeCompare(right.flow_id),
      );
      setFlows(next);
      setRegistry(records);
      setSelectedFlowId((current) => {
        if (current && (runtimeById.has(current) || records.some((r) => r.id === current))) {
          return current;
        }
        return next[0]?.flow_id ?? records[0]?.id ?? null;
      });
      setOnline(true);
      setMessage(
        next.length > 1
          ? `${next.length} flujos en runtime`
          : "Motor Jaiba disponible",
      );
      setError(null);
    } catch (requestError) {
      setOnline(false);
      setMessage("Motor Jaiba desconectado");
      setError(
        requestError instanceof Error
          ? requestError.message
          : "No fue posible consultar el motor",
      );
    }
  }, []);

  useEffect(() => {
    if (view !== "monitor") return;
    void refresh();
    const socket = jaivaApi.runtimeSocket((event) => {
      if (event.kind !== "runtime_snapshot") return;
      const next =
        event.flows && event.flows.length > 0
          ? [...event.flows].sort((left, right) =>
              left.flow_id.localeCompare(right.flow_id),
            )
          : event.flow
            ? [event.flow]
            : [];
      setFlows(next);
      setSelectedFlowId((current) => {
        if (current && next.some((item) => item.flow_id === current)) return current;
        return next[0]?.flow_id ?? current;
      });
      setOnline(true);
      setMessage(
        next.length > 1
          ? `${next.length} flujos · tiempo real`
          : "Motor Jaiba · tiempo real",
      );
      setError(null);
    });
    socket.addEventListener("open", () => {
      setTransport("socket");
      setOnline(true);
      setMessage("Motor Jaiba · tiempo real");
    });
    socket.addEventListener("close", () => {
      setTransport("polling");
    });
    socket.addEventListener("error", () => {
      setTransport("polling");
    });
    const timer = window.setInterval(() => {
      if (socket.readyState !== WebSocket.OPEN) void refresh();
    }, 10000);
    return () => {
      window.clearInterval(timer);
      socket.close();
    };
  }, [refresh, view]);

  const mutate = async (action: FlowAction) => {
    if (!flow) return;
    setBusy(action);
    setError(null);
    try {
      const snapshot = await jaivaApi.mutate(flow.flow_id, action);
      setFlows((current) => {
        const others = current.filter((item) => item.flow_id !== snapshot.flow_id);
        if (action === "stop") return others;
        return [...others, snapshot].sort((left, right) =>
          left.flow_id.localeCompare(right.flow_id),
        );
      });
      setMessage(`Acción ${action} aceptada`);
      window.setTimeout(() => void refresh(), 350);
    } catch (requestError) {
      setError(
        requestError instanceof Error
          ? requestError.message
          : "La operación no pudo completarse",
      );
    } finally {
      setBusy(null);
    }
  };

  const metrics = flow?.metrics;
  const flowOptions = useMemo(() => {
    const ids = new Set<string>();
    const options: { id: string; label: string }[] = [];
    for (const item of flows) {
      ids.add(item.flow_id);
      options.push({
        id: item.flow_id,
        label: `${item.flow_id} · ${item.control.state}`,
      });
    }
    for (const record of registry) {
      if (ids.has(record.id)) continue;
      options.push({
        id: record.id,
        label: `${record.id} · registro${
          record.active_version != null ? ` v${record.active_version}` : ""
        }`,
      });
    }
    return options.sort((left, right) => left.id.localeCompare(right.id));
  }, [flows, registry]);

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="/" aria-label="Jaiba">
          <CrabMark />
          <span>
            <strong>Jaiba</strong>
            <small>Data Flow Platform</small>
          </span>
        </a>
        <nav className="top-nav">
          <button
            type="button"
            className={view === "monitor" ? "active" : ""}
            onClick={() => setView("monitor")}
          >
            Monitor
          </button>
          <button
            type="button"
            className={view === "builder" ? "active" : ""}
            onClick={() => setView("builder")}
          >
            Diseñar flujo
          </button>
          <button
            type="button"
            className={view === "connections" ? "active" : ""}
            onClick={() => setView("connections")}
          >
            Conexiones
          </button>
        </nav>
        <Status online={online} label={message} />
        <AdminAccess />
      </header>

      {view === "builder" ? (
        <main className="builder-main">
          <Suspense fallback={<div className="builder-loading">Cargando diseñador…</div>}>
            <FlowBuilder />
          </Suspense>
        </main>
      ) : view === "connections" ? (
        <main className="connections-main">
          <Suspense fallback={<div className="builder-loading">Cargando conexiones…</div>}>
            <ConnectionManagerView onCreateQueryNode={() => setView("builder")} />
          </Suspense>
        </main>
      ) : (
      <main>
        <section className="hero">
          <div>
            <p className="eyebrow">CONTROL PLANE · FASE 5</p>
            <h1>
              La fuerza del motor.
              <br />
              <em>La claridad del flujo.</em>
            </h1>
            <p className="hero-copy">
              Supervisa varios flujos a la vez: elige cuál observar, controla su
              ciclo de vida y revisa provenance o dead-letter por flujo.
            </p>
          </div>
          <aside className="hero-state">
            <span>Flujo seleccionado</span>
            <label className="monitor-flow-switcher">
              <select
                value={selectedFlowId ?? ""}
                onChange={(event) =>
                  setSelectedFlowId(event.target.value || null)
                }
                disabled={flowOptions.length === 0}
              >
                {flowOptions.length === 0 ? (
                  <option value="">Sin flujos</option>
                ) : (
                  flowOptions.map((option) => (
                    <option key={option.id} value={option.id}>
                      {option.label}
                    </option>
                  ))
                )}
              </select>
            </label>
            <LifecycleBadge state={flow?.control?.state} />
            <small>
              {flow
                ? `${flows.length} en runtime · ${registry.length} en registro`
                : "Esperando un flujo"}
            </small>
          </aside>
        </section>

        <section className="metrics-grid" aria-label="Métricas principales">
          <MetricCard
            label="Procesados"
            value={formatNumber(metrics?.processed)}
            detail="paquetes correctos"
            tone="foam"
          />
          <MetricCard
            label="En cola"
            value={formatNumber(metrics?.queue_depth)}
            detail="paquetes pendientes"
            tone="blue"
          />
          <MetricCard
            label="Tareas activas"
            value={formatNumber(metrics?.active_tasks)}
            detail="workers ocupados"
            tone="sand"
          />
          <MetricCard
            label="Fallidos"
            value={formatNumber(metrics?.failed)}
            detail="revisar dead-letter"
            tone="rust"
          />
        </section>

        <section className="workspace">
          <article className="panel flow-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">OPERACIÓN</p>
                <h2>Mapa del flujo</h2>
              </div>
              <button className="button subtle" onClick={refresh} type="button">
                Actualizar
              </button>
            </div>
            <FlowCanvas processors={metrics?.processors ?? {}} />
            <FlowControls
              busy={busy}
              disabled={!flow || !online}
              onAction={(action) => void mutate(action)}
              state={flow?.control?.state}
            />
            <p className={`operation-message ${error ? "error" : ""}`}>
              {error ?? "Los controles actúan directamente sobre el supervisor del motor."}
            </p>
          </article>

          <aside className="panel runtime-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">RUNTIME</p>
                <h2>Capacidad</h2>
              </div>
              <span className="shell-chip">RUST</span>
            </div>
            <RuntimeDetails metrics={metrics} />
          </aside>
        </section>

        <OperationsConsole flow={flow} online={online} />

        <section className="separation-note">
          <CrabMark />
          <div>
            <p className="eyebrow">ARQUITECTURA DESACOPLADA</p>
            <h2>Una interfaz, dos formas de distribución.</h2>
            <p>
              El mismo build de React funciona hoy dentro de Nginx y podrá
              integrarse después en Tauri. El motor seguirá siendo un binario
              Rust independiente o un sidecar opcional de escritorio.
            </p>
          </div>
          <div className="distribution">
            <span>WEB</span>
            <i />
            <span>DESKTOP</span>
          </div>
        </section>
      </main>
      )}

      <footer>
        <span>Jaiba UI · React + TypeScript</span>
        <span>
          Motor desacoplado · {transport === "socket" ? "WebSocket en tiempo real" : "sondeo de respaldo"}
        </span>
      </footer>
    </div>
  );
}

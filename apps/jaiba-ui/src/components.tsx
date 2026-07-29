import { useCallback, useEffect, useState } from "react";
import { jaivaApi } from "./api";
import jaibaLogo from "./img/jaiba-logo.png";
import type {
  DeadLetterEntry,
  FlowAction,
  FlowLifecycle,
  FlowMetrics,
  FlowSnapshot,
  ProcessorMetrics,
  ProvenanceRecord,
} from "./types";

export function CrabMark() {
  return (
    <img
      className="crab-mark"
      src={jaibaLogo}
      alt="Cangrejo jarocho, logo de Jaiba"
    />
  );
}

export function Status({
  online,
  label,
}: {
  online: boolean;
  label: string;
}) {
  return (
    <div className="connection-status">
      <span className={`status-light ${online ? "online" : "offline"}`} />
      <span>{label}</span>
    </div>
  );
}

export function AdminAccess() {
  const [token, setToken] = useState(
    () =>
      window.sessionStorage.getItem("jaiba.admin.token") ??
      window.sessionStorage.getItem("jaiva.admin.token") ??
      "",
  );
  const [saved, setSaved] = useState(token !== "");

  return (
    <details className="token-control">
      <summary title="Credencial administrativa">Acceso</summary>
      <div className="token-popover">
        <label>
          <span>Bearer token</span>
          <input
            autoComplete="off"
            className="builder-input"
            onChange={(event) => {
              setToken(event.target.value);
              setSaved(false);
            }}
            placeholder="Solo se conserva en esta pestaña"
            type="password"
            value={token}
          />
        </label>
        <div>
          <button
            className="button subtle"
            onClick={() => {
              if (token) window.sessionStorage.setItem("jaiba.admin.token", token);
              else window.sessionStorage.removeItem("jaiba.admin.token");
              setSaved(token !== "");
            }}
            type="button"
          >
            Guardar
          </button>
          <button
            className="button subtle"
            onClick={() => {
              window.sessionStorage.removeItem("jaiba.admin.token");
              window.sessionStorage.removeItem("jaiva.admin.token");
              setToken("");
              setSaved(false);
            }}
            type="button"
          >
            Limpiar
          </button>
        </div>
        <small>{saved ? "Token activo en sessionStorage." : "Modo sin token."}</small>
      </div>
    </details>
  );
}

export function MetricCard({
  label,
  value,
  detail,
  tone = "blue",
}: {
  label: string;
  value: string;
  detail: string;
  tone?: "blue" | "rust" | "sand" | "foam";
}) {
  return (
    <article className={`metric-card tone-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </article>
  );
}

function processorRole(index: number, total: number) {
  if (index === 0) return "Origen";
  if (index === total - 1) return "Destino";
  return "Proceso";
}

function processorWeight(id: string) {
  const normalized = id.toLowerCase();
  if (/source|origen|read|query|generate/.test(normalized)) return 0;
  if (/destination|destino|sink|write|put|publish|log/.test(normalized)) return 2;
  return 1;
}

export function FlowCanvas({
  processors,
}: {
  processors: Record<string, ProcessorMetrics>;
}) {
  const entries = Object.entries(processors).sort(([left], [right]) => {
    const weight = processorWeight(left) - processorWeight(right);
    return weight === 0 ? left.localeCompare(right) : weight;
  });
  if (entries.length === 0) {
    return <div className="empty-canvas">El flujo todavía no reporta nodos.</div>;
  }
  return (
    <div className="flow-canvas">
      {entries.map(([id, metric], index) => (
        <div className="flow-step" key={id}>
          <article className="processor-node">
            <div className="node-top">
              <span>{String(index + 1).padStart(2, "0")}</span>
              <i className={metric.active_tasks > 0 ? "active" : ""} />
            </div>
            <strong>{id}</strong>
            <small>{processorRole(index, entries.length)}</small>
            <div className="node-load">
              <span>
                {metric.active_tasks}/{metric.concurrency_limit} workers
              </span>
              <span>{metric.completed} completos</span>
            </div>
          </article>
          {index < entries.length - 1 && (
            <div className="flow-edge" aria-hidden="true">
              <i />
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

const actions: Array<{ action: FlowAction; label: string }> = [
  { action: "start", label: "Iniciar" },
  { action: "pause", label: "Pausar" },
  { action: "resume", label: "Reanudar" },
  { action: "drain", label: "Drenar" },
  { action: "stop", label: "Detener" },
];

const validActions: Record<FlowLifecycle, FlowAction[]> = {
  STOPPED: ["start"],
  STARTING: ["drain", "stop"],
  RUNNING: ["pause", "drain", "stop"],
  PAUSED: ["resume", "drain", "stop"],
  DRAINING: ["stop"],
  FAILED: ["start"],
};

export function FlowControls({
  disabled,
  busy,
  state,
  onAction,
}: {
  disabled: boolean;
  busy: FlowAction | null;
  state?: FlowLifecycle;
  onAction: (action: FlowAction) => void;
}) {
  const lifecycle = state ?? "STOPPED";
  const allowed = validActions[lifecycle];
  return (
    <>
      <div className="flow-actions">
        {actions.map(({ action, label }) => {
          const valid = allowed.includes(action);
          return (
            <button
              className={`button action-${action}`}
              disabled={disabled || busy !== null || !valid}
              key={action}
              onClick={() => onAction(action)}
              title={
                valid
                  ? `${label} flujo`
                  : `${label} no está disponible en estado ${lifecycle}`
              }
              type="button"
            >
              {busy === action ? "Procesando…" : label}
            </button>
          );
        })}
      </div>
      <span className="action-hint">
        Acciones disponibles para <strong>{lifecycle}</strong>:{" "}
        {allowed.length > 0 ? allowed.join(", ") : "ninguna"}
      </span>
    </>
  );
}

export function RuntimeDetails({ metrics }: { metrics?: FlowMetrics }) {
  const formatBytes = (value = 0) => {
    const units = ["B", "KB", "MB", "GB", "TB"];
    let amount = value;
    let unit = 0;
    while (amount >= 1024 && unit < units.length - 1) {
      amount /= 1024;
      unit += 1;
    }
    return `${amount.toFixed(unit < 2 ? 0 : 1)} ${units[unit]}`;
  };
  const values = [
    ["CPU visibles", metrics?.available_parallelism ?? "—"],
    ["Workers CPU", metrics?.cpu_worker_limit ?? "—"],
    ["Workers bloqueantes", metrics?.blocking_worker_limit ?? "—"],
    [
      "Memoria de paquetes",
      metrics
        ? `${formatBytes(metrics.memory_used_bytes)} / ${formatBytes(metrics.memory_budget_bytes)}`
        : "—",
    ],
    ["Circuitos abiertos", metrics?.circuits_open ?? "—"],
    ["Dead-letter", metrics?.repository_dead_letter ?? "—"],
  ];
  return (
    <dl className="runtime-list">
      {values.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

export function LifecycleBadge({ state }: { state?: FlowLifecycle }) {
  const value = state ?? "STOPPED";
  return <strong className={`lifecycle state-${value}`}>{value}</strong>;
}

function eventDate(epoch: number): string {
  return new Date(epoch * 1000).toLocaleString("es-MX");
}

export function OperationsConsole({
  flow,
  online,
}: {
  flow: FlowSnapshot | null;
  online: boolean;
}) {
  const [tab, setTab] = useState<"provenance" | "dead-letter">("provenance");
  const [provenance, setProvenance] = useState<ProvenanceRecord[]>([]);
  const [deadLetters, setDeadLetters] = useState<DeadLetterEntry[]>([]);
  const [packetId, setPacketId] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!online || !flow) return;
    setBusy(true);
    setError(null);
    try {
      if (tab === "provenance") {
        setProvenance(await jaivaApi.provenance(100, packetId.trim() || undefined));
      } else {
        setDeadLetters(await jaivaApi.deadLetters(100));
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "No fue posible consultar el repositorio.");
    } finally {
      setBusy(false);
    }
  }, [flow, online, packetId, tab]);

  useEffect(() => {
    if ((flow?.metrics.repository_pending ?? 0) > 0 || (flow?.metrics.repository_dead_letter ?? 0) > 0) {
      void load();
    }
  }, [flow?.flow_id, tab]); // Refresh explicitly after the initial operational load.

  const replay = async (queueId: string) => {
    setBusy(true);
    setError(null);
    try {
      await jaivaApi.replayDeadLetter(queueId);
      await load();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : "No fue posible reencolar el paquete.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="panel operations-panel">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">TRAZABILIDAD</p>
          <h2>Provenance y dead-letter</h2>
        </div>
        <button className="button subtle" disabled={!online || !flow || busy} onClick={() => void load()} type="button">
          {busy ? "Consultando…" : "Actualizar"}
        </button>
      </div>
      <div className="operations-tabs">
        <button className={tab === "provenance" ? "active" : ""} onClick={() => setTab("provenance")} type="button">
          Provenance
        </button>
        <button className={tab === "dead-letter" ? "active" : ""} onClick={() => setTab("dead-letter")} type="button">
          Dead-letter ({flow?.metrics.repository_dead_letter ?? 0})
        </button>
      </div>
      {tab === "provenance" ? (
        <>
          <div className="operations-filter">
            <input
              className="builder-input"
              onChange={(event) => setPacketId(event.target.value)}
              placeholder="Filtrar por packet_id (opcional)"
              value={packetId}
            />
            <button className="button subtle" disabled={busy} onClick={() => void load()} type="button">Buscar</button>
          </div>
          <div className="operations-table-wrap">
            <table className="operations-table">
              <thead><tr><th>Fecha</th><th>Paquete</th><th>Procesador</th><th>Evento</th></tr></thead>
              <tbody>
                {provenance.map((record) => (
                  <tr key={record.id}>
                    <td>{eventDate(record.created_at)}</td>
                    <td title={record.packet_id}>{record.packet_id}</td>
                    <td>{record.processor_id}</td>
                    <td><span className={`event-chip event-${record.event_type.toLowerCase()}`}>{record.event_type}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
            {provenance.length === 0 ? <p className="operations-empty">No hay eventos cargados.</p> : null}
          </div>
        </>
      ) : (
        <div className="operations-table-wrap">
          <table className="operations-table">
            <thead><tr><th>Fallo</th><th>Paquete</th><th>Procesador</th><th>Intento</th><th>Error</th><th /></tr></thead>
            <tbody>
              {deadLetters.map((entry) => (
                <tr key={entry.queue_id}>
                  <td>{eventDate(entry.failed_at)}</td>
                  <td title={entry.packet_id}>{entry.packet_id}</td>
                  <td>{entry.processor_id}</td>
                  <td>{entry.attempt}</td>
                  <td title={entry.error ?? ""}>{entry.error ?? "—"}</td>
                  <td><button className="button subtle" disabled={busy} onClick={() => void replay(entry.queue_id)} type="button">Reencolar</button></td>
                </tr>
              ))}
            </tbody>
          </table>
          {deadLetters.length === 0 ? <p className="operations-empty">No hay paquetes en dead-letter.</p> : null}
        </div>
      )}
      {error ? <p className="operation-message error">{error}</p> : null}
      {!flow?.metrics.repository_pending && !flow?.metrics.repository_dead_letter ? (
        <p className="operations-note">El repositorio debe estar habilitado en el YAML para consultar trazabilidad.</p>
      ) : null}
    </section>
  );
}

import type { ValidationIssue } from "./yaml";

export type BottomTab = "consola" | "eventos" | "estadisticas" | "errores";

export type LogChannel = "console" | "evento";

export interface LogEntry {
  id: number;
  channel: LogChannel;
  time: string;
  level: "info" | "warn" | "error";
  message: string;
}

export interface FlowStats {
  processors: number;
  connections: number;
  sources: number;
  transforms: number;
  sinks: number;
  parameters: number;
  databaseConnections: number;
  kafkaConnections: number;
  successEdges: number;
  failureEdges: number;
}

interface BottomPanelProps {
  tab: BottomTab;
  onTab: (tab: BottomTab) => void;
  logs: LogEntry[];
  stats: FlowStats;
  issues: ValidationIssue[];
  collapsed: boolean;
  onToggleCollapsed: () => void;
}

const TABS: Array<{ id: BottomTab; label: string }> = [
  { id: "consola", label: "Consola" },
  { id: "eventos", label: "Eventos" },
  { id: "estadisticas", label: "Estadísticas" },
  { id: "errores", label: "Errores" },
];

export function BottomPanel({
  tab,
  onTab,
  logs,
  stats,
  issues,
  collapsed,
  onToggleCollapsed,
}: BottomPanelProps) {
  const errorCount = issues.filter((issue) => issue.level === "error").length;
  const consoleLogs = logs.filter((entry) => entry.channel === "console");
  const eventLogs = logs.filter((entry) => entry.channel === "evento");

  return (
    <section className={`bottom-panel ${collapsed ? "collapsed" : ""}`}>
      <div className="bottom-tabs">
        {TABS.map((item) => (
          <button
            key={item.id}
            type="button"
            className={tab === item.id ? "active" : ""}
            onClick={() => onTab(item.id)}
          >
            {item.label}
            {item.id === "errores" && errorCount > 0 ? (
              <span className="tab-badge">{errorCount}</span>
            ) : null}
          </button>
        ))}
        <button
          type="button"
          className="bottom-collapse"
          aria-label={collapsed ? "Expandir" : "Colapsar"}
          onClick={onToggleCollapsed}
        >
          {collapsed ? "▲" : "▼"}
        </button>
      </div>

      {!collapsed ? (
        <div className="bottom-body">
          {tab === "consola" ? <LogList entries={consoleLogs} empty="Sin mensajes." /> : null}
          {tab === "eventos" ? <LogList entries={eventLogs} empty="Sin eventos de edición." /> : null}
          {tab === "estadisticas" ? <StatsView stats={stats} /> : null}
          {tab === "errores" ? (
            issues.length > 0 ? (
              <ul className="issues">
                {issues.map((issue, index) => (
                  <li key={index} className={issue.level}>
                    {issue.message}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="issues-ok">El flujo es válido.</p>
            )
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

function LogList({ entries, empty }: { entries: LogEntry[]; empty: string }) {
  if (entries.length === 0) {
    return <p className="bottom-empty">{empty}</p>;
  }
  return (
    <ul className="log-list">
      {entries.map((entry) => (
        <li key={entry.id} className={`log-line ${entry.level}`}>
          <span className="log-time">{entry.time}</span>
          <span className="log-message">{entry.message}</span>
        </li>
      ))}
    </ul>
  );
}

function StatsView({ stats }: { stats: FlowStats }) {
  const items: Array<[string, number]> = [
    ["Procesadores", stats.processors],
    ["Conexiones", stats.connections],
    ["Fuentes", stats.sources],
    ["Transformaciones", stats.transforms],
    ["Destinos", stats.sinks],
    ["Relaciones success", stats.successEdges],
    ["Relaciones failure", stats.failureEdges],
    ["Parámetros", stats.parameters],
    ["Conexiones BD", stats.databaseConnections],
    ["Conexiones Kafka", stats.kafkaConnections],
  ];
  return (
    <div className="stats-grid">
      {items.map(([label, value]) => (
        <div className="stat-cell" key={label}>
          <span>{label}</span>
          <strong>{value}</strong>
        </div>
      ))}
    </div>
  );
}

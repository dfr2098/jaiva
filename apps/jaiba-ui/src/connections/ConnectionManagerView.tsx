import { useCallback, useEffect, useMemo, useState } from "react";
import { jaivaApi } from "../api";
import { Modal } from "../builder/Modal";
import type {
  ConnectionDriver,
  ConnectionType,
  DatabaseConnection,
  DatabaseConnectionInput,
  DiagnosticCheck,
} from "../types";
import { SqlQueryBuilder } from "./SqlQueryBuilder";
import { MetadataExplorer } from "./MetadataExplorer";

const EMPTY: DatabaseConnectionInput = {
  name: "",
  connection_type: "postgres",
  host: "127.0.0.1",
  port: 5432,
  database: "",
  username: "",
  password: "",
  url: "",
  ssl: false,
  pool_min: 1,
  pool_max: 10,
  timeout_ms: 10_000,
};

/** Rellena host/puerto/base/usuario/contraseña a partir de una URI MongoDB. */
function applyMongoConnectionUrl(
  form: DatabaseConnectionInput,
  raw: string,
): DatabaseConnectionInput {
  const trimmed = raw.trim();
  if (!trimmed) {
    return { ...form, url: "" };
  }
  try {
    const parsed = new URL(trimmed);
    if (parsed.protocol !== "mongodb:" && parsed.protocol !== "mongodb+srv:") {
      return { ...form, url: trimmed };
    }
    const database = decodeURIComponent(parsed.pathname.replace(/^\/+/, ""));
    const tls = parsed.searchParams.get("tls") ?? parsed.searchParams.get("ssl");
    const ssl =
      parsed.protocol === "mongodb+srv:" || tls === "true" || tls === "1";
    return {
      ...form,
      url: trimmed,
      host: parsed.hostname || form.host,
      port: parsed.port ? Number(parsed.port) : form.port || 27017,
      database: database || form.database,
      username: decodeURIComponent(parsed.username || form.username),
      password: parsed.password
        ? decodeURIComponent(parsed.password)
        : form.password,
      ssl,
    };
  } catch {
    return { ...form, url: trimmed };
  }
}

const marks: Record<string, string> = {
  postgres: "PG",
  mysql: "MY",
  maria_db: "MA",
  mongodb: "MO",
  oracle: "OR",
  sql_server: "MS",
  kafka: "KF",
  opc_ua: "OP",
  rest: "API",
};

function driverMark(id: ConnectionType): string {
  const generated = id.replace(/[^a-z0-9]/gi, "").slice(0, 3).toUpperCase();
  return marks[id] ?? (generated || "DB");
}

function statusLabel(status: DatabaseConnection["status"]["availability"]) {
  return {
    unknown: "Sin probar",
    testing: "Probando",
    available: "Disponible",
    degraded: "Degradada",
    unavailable: "No disponible",
  }[status];
}

function dateLabel(timestamp: number | null) {
  return timestamp
    ? new Intl.DateTimeFormat("es-MX", {
        dateStyle: "short",
        timeStyle: "medium",
      }).format(new Date(timestamp * 1000))
    : "Nunca";
}

export function ConnectionManagerView({
  onCreateQueryNode,
}: {
  onCreateQueryNode?: () => void;
}) {
  const [drivers, setDrivers] = useState<ConnectionDriver[]>([]);
  const [connections, setConnections] = useState<DatabaseConnection[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [modal, setModal] = useState<"drivers" | "form" | null>(null);
  const [editing, setEditing] = useState<string | null>(null);
  const [form, setForm] = useState<DatabaseConnectionInput>(EMPTY);
  const [search, setSearch] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState("Los secretos permanecen en el servidor.");
  const [error, setError] = useState<string | null>(null);
  const [diagnostics, setDiagnostics] = useState<DiagnosticCheck[]>([]);

  const refresh = useCallback(async () => {
    try {
      const [driverList, connectionList] = await Promise.all([
        jaivaApi.connectionTypes(),
        jaivaApi.connections(),
      ]);
      setDrivers(driverList);
      setConnections(connectionList);
      setSelected((current) =>
        current && connectionList.some((item) => item.id === current)
          ? current
          : connectionList[0]?.id ?? null,
      );
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "No se pudieron cargar las conexiones");
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const current = connections.find((item) => item.id === selected) ?? null;
  const currentDriver = drivers.find((driver) => driver.id === current?.connection_type) ?? null;
  const filteredDrivers = useMemo(
    () =>
      drivers.filter((driver) =>
        `${driver.name} ${driver.category}`.toLowerCase().includes(search.toLowerCase()),
      ),
    [drivers, search],
  );

  const chooseDriver = (driver: ConnectionDriver) => {
    setEditing(null);
    setForm({
      ...EMPTY,
      connection_type: driver.id,
      port: driver.default_port,
      name: `${driver.name} `,
    });
    setModal("form");
  };

  const edit = (connection: DatabaseConnection) => {
    setEditing(connection.id);
    setForm({
      name: connection.name,
      connection_type: connection.connection_type,
      host: connection.host,
      port: connection.port,
      database: connection.database ?? "",
      username: connection.username,
      password: "",
      ssl: connection.ssl,
      pool_min: connection.pool_min,
      pool_max: connection.pool_max,
      timeout_ms: connection.timeout_ms,
    });
    setModal("form");
  };

  const save = async (testAfter = false) => {
    setBusy("save");
    setError(null);
    try {
      const payload: DatabaseConnectionInput = {
        ...form,
        url: form.connection_type === "mongodb" && form.url?.trim()
          ? form.url.trim()
          : undefined,
      };
      const saved = editing
        ? await jaivaApi.updateConnection(editing, payload)
        : await jaivaApi.createConnection(payload);
      setForm(EMPTY);
      setModal(null);
      await refresh();
      setSelected(saved.id);

      if (!testAfter) {
        setMessage(`Conexión ${saved.name} guardada.`);
        return;
      }

      try {
        const tested = await jaivaApi.testConnection(saved.id);
        setMessage(`Conexión ${tested.name} validada.`);
        await refresh();
        setSelected(tested.id);
      } catch (reason) {
        const detail = reason instanceof Error ? reason.message : "La prueba falló";
        setMessage(`Conexión ${saved.name} guardada.`);
        await refresh();
        setSelected(saved.id);
        setError(`El perfil se guardó, pero la prueba falló: ${detail}`);
      }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "No se pudo guardar la conexión");
    } finally {
      setBusy(null);
    }
  };

  const test = async (connection: DatabaseConnection) => {
    setBusy(connection.id);
    setError(null);
    try {
      const tested = await jaivaApi.testConnection(connection.id);
      setMessage(`${tested.name}: conexión validada en ${tested.status.latency_ms ?? 0} ms.`);
      await refresh();
      setSelected(tested.id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "La prueba falló");
      await refresh();
    } finally {
      setBusy(null);
    }
  };

  const diagnose = async (connection: DatabaseConnection) => {
    setBusy(`diagnostics:${connection.id}`);
    setError(null);
    try {
      const checks = await jaivaApi.diagnoseConnection(connection.id);
      setDiagnostics(checks);
      setMessage(`${connection.name}: ${checks.length} diagnósticos ejecutados por el adaptador.`);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "El diagnóstico falló");
    } finally {
      setBusy(null);
    }
  };

  const remove = async (connection: DatabaseConnection) => {
    if (!window.confirm(`¿Eliminar la conexión "${connection.name}"?`)) return;
    setBusy(connection.id);
    try {
      await jaivaApi.deleteConnection(connection.id);
      setMessage(`Conexión ${connection.name} eliminada.`);
      await refresh();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "No se pudo eliminar");
    } finally {
      setBusy(null);
    }
  };

  const duplicate = async (connection: DatabaseConnection) => {
    const name = window.prompt("Nombre de la copia", `${connection.name} copia`);
    if (!name?.trim()) return;
    setBusy(connection.id);
    try {
      const copy = await jaivaApi.duplicateConnection(connection.id, name.trim());
      setMessage(`Se creó ${copy.name}.`);
      await refresh();
      setSelected(copy.id);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "No se pudo duplicar");
    } finally {
      setBusy(null);
    }
  };

  return (
    <section className="connection-manager" data-testid="connection-manager">
      <header className="connections-hero">
        <div>
          <p className="eyebrow">CONNECTION MANAGER</p>
          <h1>Conexiones reutilizables</h1>
          <p>
            Configura, valida y comparte perfiles entre nodos. El YAML guarda solamente el
            nombre de la conexión; nunca la contraseña.
          </p>
        </div>
        <button
          className="button primary"
          type="button"
          data-testid="connection-new"
          onClick={() => setModal("drivers")}
        >
          + Nueva conexión
        </button>
      </header>

      <div className="connections-layout">
        <aside className="connection-list">
          <div className="connection-list-head">
            <strong>Perfiles</strong>
            <span>{connections.length}</span>
          </div>
          {connections.length === 0 ? (
            <div className="connection-empty">Aún no hay conexiones guardadas.</div>
          ) : (
            connections.map((connection) => (
              <button
                className={`connection-list-item ${selected === connection.id ? "active" : ""}`}
                key={connection.id}
                type="button"
                onClick={() => setSelected(connection.id)}
              >
                <span className={`db-mark db-${connection.connection_type}`}>
                  {driverMark(connection.connection_type)}
                </span>
                <span>
                  <strong>{connection.name}</strong>
                  <small>{connection.host}:{connection.port}</small>
                </span>
                <i className={`availability ${connection.status.availability}`} />
              </button>
            ))
          )}
        </aside>

        <article className="connection-detail">
          {current ? (
            <>
              <div className="connection-title">
                <div>
                  <span className={`db-mark large db-${current.connection_type}`}>
                    {driverMark(current.connection_type)}
                  </span>
                  <div>
                    <p className="eyebrow">{current.connection_type.replace("_", " ")}</p>
                    <h2>{current.name}</h2>
                  </div>
                </div>
                <div className="connection-actions">
                  <button type="button" className="button subtle" onClick={() => edit(current)}>
                    Editar
                  </button>
                  <button type="button" className="button subtle" onClick={() => void duplicate(current)}>
                    Duplicar
                  </button>
                  <button type="button" className="button danger" onClick={() => void remove(current)}>
                    Eliminar
                  </button>
                </div>
              </div>

              <div className="connection-health">
                <div>
                  <span>Estado</span>
                  <strong
                    className={`health-${current.status.availability}`}
                    data-testid="connection-availability"
                  >
                    {statusLabel(current.status.availability)}
                  </strong>
                </div>
                <div><span>Respuesta</span><strong>{current.status.latency_ms ?? "—"}{current.status.latency_ms !== null ? " ms" : ""}</strong></div>
                <div><span>Pool</span><strong>{current.status.pool_active ?? "—"} / {current.status.pool_maximum ?? current.pool_max}</strong></div>
                <div><span>Última prueba</span><strong>{dateLabel(current.status.tested_at)}</strong></div>
              </div>

              <div className="connection-info-grid">
                <div><span>Servidor</span><strong>{current.host}:{current.port}</strong></div>
                <div><span>Base / servicio</span><strong>{current.database || "Predeterminada"}</strong></div>
                <div><span>Usuario</span><strong>{current.username}</strong></div>
                <div><span>SSL</span><strong>{current.ssl ? "Habilitado" : "Deshabilitado"}</strong></div>
                <div className="wide"><span>Versión detectada</span><strong>{current.status.version || "Ejecuta una prueba para detectarla"}</strong></div>
              </div>

              <div className="connection-test-row">
                <p>{current.status.message ?? "La prueba abre un pool temporal y ejecuta SELECT version()."}</p>
                {currentDriver?.capabilities.includes("diagnostics") ? (
                  <button
                    className="button subtle"
                    disabled={busy === `diagnostics:${current.id}`}
                    type="button"
                    onClick={() => void diagnose(current)}
                  >
                    {busy === `diagnostics:${current.id}` ? "Diagnosticando…" : "Diagnóstico"}
                  </button>
                ) : null}
                <button
                  className="button primary"
                  data-testid="connection-test"
                  disabled={busy === current.id}
                  type="button"
                  onClick={() => void test(current)}
                >
                  {busy === current.id ? "Probando…" : "Probar conexión"}
                </button>
              </div>
              {diagnostics.length > 0 ? (
                <div className="connection-info-grid">
                  {diagnostics.map((check) => (
                    <div key={check.code}>
                      <span>{check.label}</span>
                      <strong className={`health-${check.status}`}>
                        {statusLabel(check.status)}
                        {check.latency_ms !== null ? ` · ${check.latency_ms} ms` : ""}
                      </strong>
                    </div>
                  ))}
                </div>
              ) : null}
              {currentDriver?.capabilities.includes("query_builder") ? (
                <SqlQueryBuilder connection={current} onCreateQueryNode={onCreateQueryNode} />
              ) : null}
              {currentDriver?.capabilities.includes("schema_explorer") &&
              !currentDriver.capabilities.includes("query_builder") ? (
                <MetadataExplorer connection={current} />
              ) : null}
            </>
          ) : (
            <div className="connection-welcome">
              <span className="db-stack">DB</span>
              <h2>Administrador de conexiones</h2>
              <p>Agrega un perfil para comprobar disponibilidad, latencia, versión y pool.</p>
              <button className="button primary" type="button" onClick={() => setModal("drivers")}>
                Elegir base de datos
              </button>
            </div>
          )}
        </article>
      </div>

      <div
        className={`connection-message ${error ? "error" : ""}`}
        data-testid="connection-message"
      >
        {error ?? message}
      </div>

      {modal === "drivers" ? (
        <Modal title="Conectar a una base de datos" onClose={() => setModal(null)}>
          <div className="driver-picker" data-testid="driver-picker">
            <div>
              <strong>Selecciona el motor</strong>
              <p>Solo los drivers activos pueden crear perfiles comprobables.</p>
            </div>
            <input
              autoFocus
              className="driver-search"
              data-testid="driver-search"
              placeholder="Buscar PostgreSQL, Oracle, SQL Server…"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            <div className="driver-grid">
              {filteredDrivers.map((driver) => (
                <button
                  key={driver.id}
                  className="driver-card"
                  type="button"
                  data-testid={`driver-${driver.id}`}
                  onClick={() => chooseDriver(driver)}
                >
                  <span className={`db-mark large db-${driver.id}`}>{driverMark(driver.id)}</span>
                  <strong>{driver.name}</strong>
                  <small>{driver.category} · {driver.default_port}</small>
                  <em>{driver.capabilities.join(" · ")}</em>
                </button>
              ))}
            </div>
          </div>
        </Modal>
      ) : null}

      {modal === "form" ? (
        <Modal
          title={editing ? "Editar conexión" : "Nueva conexión"}
          onClose={() => setModal(null)}
          footer={
            <>
              <button className="button subtle" type="button" onClick={() => setModal(null)}>Cancelar</button>
              <button
                className="button subtle"
                data-testid="connection-save"
                disabled={busy === "save"}
                type="button"
                onClick={() => void save(false)}
              >
                Guardar
              </button>
              <button
                className="button primary"
                data-testid="connection-save-test"
                disabled={busy === "save"}
                type="button"
                onClick={() => void save(true)}
              >
                {busy === "save" ? "Validando…" : "Guardar y probar"}
              </button>
            </>
          }
        >
          <ConnectionForm form={form} editing={Boolean(editing)} onChange={setForm} />
        </Modal>
      ) : null}
    </section>
  );
}

function ConnectionForm({
  form,
  editing,
  onChange,
}: {
  form: DatabaseConnectionInput;
  editing: boolean;
  onChange: (value: DatabaseConnectionInput) => void;
}) {
  const field = <K extends keyof DatabaseConnectionInput>(
    key: K,
    value: DatabaseConnectionInput[K],
  ) => onChange({ ...form, [key]: value });
  const isMongo = form.connection_type === "mongodb";
  const hasMongoUrl = Boolean(form.url?.trim());
  return (
    <form
      className="connection-form"
      data-testid="connection-form"
      onSubmit={(event) => event.preventDefault()}
    >
      <label className="wide">
        Nombre del perfil
        <input
          required
          data-testid="connection-name"
          value={form.name}
          onChange={(event) => field("name", event.target.value)}
        />
      </label>
      {isMongo ? (
        <label className="wide">
          URL de conexión
          <textarea
            rows={3}
            value={form.url ?? ""}
            placeholder="mongodb://usuario:clave@host:27017/base?authSource=admin"
            onChange={(event) => onChange(applyMongoConnectionUrl(form, event.target.value))}
          />
          <small>Opcional. Al pegar una URI se rellenan host, puerto, base y credenciales. También admite mongodb+srv://.</small>
        </label>
      ) : null}
      <label>
        Host
        <input
          required={!hasMongoUrl}
          data-testid="connection-host"
          value={form.host}
          onChange={(event) => field("host", event.target.value)}
        />
      </label>
      <label>
        Puerto
        <input
          required={!hasMongoUrl}
          min={1}
          max={65535}
          type="number"
          data-testid="connection-port"
          value={form.port}
          onChange={(event) => field("port", Number(event.target.value))}
        />
      </label>
      <label>
        Base / servicio
        <input
          data-testid="connection-database"
          value={form.database}
          onChange={(event) => field("database", event.target.value)}
        />
      </label>
      <label>
        Usuario
        <input
          required={!hasMongoUrl}
          autoComplete="username"
          data-testid="connection-username"
          value={form.username}
          onChange={(event) => field("username", event.target.value)}
        />
      </label>
      <label className="wide">
        Contraseña
        <input
          required={!editing && !hasMongoUrl}
          autoComplete="new-password"
          type="password"
          data-testid="connection-password"
          value={form.password ?? ""}
          placeholder={
            editing
              ? "Vacío conserva la contraseña actual"
              : hasMongoUrl
                ? "Vacío usa la de la URL"
                : ""
          }
          onChange={(event) => field("password", event.target.value)}
        />
      </label>
      <label>Pool mínimo<input min={0} type="number" value={form.pool_min} onChange={(event) => field("pool_min", Number(event.target.value))} /></label>
      <label>Pool máximo<input min={1} type="number" value={form.pool_max} onChange={(event) => field("pool_max", Number(event.target.value))} /></label>
      <label>Timeout (ms)<input min={250} type="number" value={form.timeout_ms} onChange={(event) => field("timeout_ms", Number(event.target.value))} /></label>
      <label className="check-field"><input type="checkbox" checked={form.ssl} onChange={(event) => field("ssl", event.target.checked)} />Usar SSL/TLS</label>
      <p className="form-security wide">La contraseña se envía al motor para guardarse en su SecretStore. Nunca vuelve en una respuesta ni se escribe en el YAML.</p>
    </form>
  );
}

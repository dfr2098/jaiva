import { useCallback, useEffect, useMemo, useState } from "react";
import { jaivaApi } from "../api";
import { Modal } from "../builder/Modal";
import type {
  ConnectionDriver,
  ConnectionType,
  DatabaseConnection,
  DatabaseConnectionInput,
} from "../types";

const EMPTY: DatabaseConnectionInput = {
  name: "",
  connection_type: "postgres",
  host: "127.0.0.1",
  port: 5432,
  database: "",
  username: "",
  password: "",
  ssl: false,
  pool_min: 1,
  pool_max: 10,
  timeout_ms: 10_000,
};

const marks: Record<ConnectionType, string> = {
  postgres: "PG",
  mysql: "MY",
  maria_db: "MA",
  oracle: "OR",
  sql_server: "MS",
  kafka: "KF",
  opc_ua: "OP",
  rest: "API",
};

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

export function ConnectionManagerView() {
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
  const filteredDrivers = useMemo(
    () =>
      drivers.filter((driver) =>
        `${driver.name} ${driver.category}`.toLowerCase().includes(search.toLowerCase()),
      ),
    [drivers, search],
  );

  const chooseDriver = (driver: ConnectionDriver) => {
    if (!driver.enabled || !driver.test_supported) return;
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
      const saved = editing
        ? await jaivaApi.updateConnection(editing, form)
        : await jaivaApi.createConnection(form);
      const final = testAfter ? await jaivaApi.testConnection(saved.id) : saved;
      setMessage(
        testAfter ? `Conexión ${final.name} validada.` : `Conexión ${final.name} guardada.`,
      );
      setForm(EMPTY);
      setModal(null);
      await refresh();
      setSelected(final.id);
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
    <section className="connection-manager">
      <header className="connections-hero">
        <div>
          <p className="eyebrow">CONNECTION MANAGER</p>
          <h1>Conexiones reutilizables</h1>
          <p>
            Configura, valida y comparte perfiles entre nodos. El YAML guarda solamente el
            nombre de la conexión; nunca la contraseña.
          </p>
        </div>
        <button className="button primary" type="button" onClick={() => setModal("drivers")}>
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
                  {marks[connection.connection_type]}
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
                    {marks[current.connection_type]}
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
                  <strong className={`health-${current.status.availability}`}>
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
                <button
                  className="button primary"
                  disabled={busy === current.id}
                  type="button"
                  onClick={() => void test(current)}
                >
                  {busy === current.id ? "Probando…" : "Probar conexión"}
                </button>
              </div>
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

      <div className={`connection-message ${error ? "error" : ""}`}>{error ?? message}</div>

      {modal === "drivers" ? (
        <Modal title="Conectar a una base de datos" onClose={() => setModal(null)}>
          <div className="driver-picker">
            <div>
              <strong>Selecciona el motor</strong>
              <p>Solo los drivers activos pueden crear perfiles comprobables.</p>
            </div>
            <input
              autoFocus
              className="driver-search"
              placeholder="Buscar PostgreSQL, Oracle, SQL Server…"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
            <div className="driver-grid">
              {filteredDrivers.map((driver) => (
                <button
                  key={driver.id}
                  className="driver-card"
                  disabled={!driver.enabled || !driver.test_supported}
                  type="button"
                  onClick={() => chooseDriver(driver)}
                >
                  <span className={`db-mark large db-${driver.id}`}>{marks[driver.id]}</span>
                  <strong>{driver.name}</strong>
                  <small>{driver.category} · {driver.default_port}</small>
                  <em>{driver.enabled && driver.test_supported ? "Disponible" : driver.note}</em>
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
              <button className="button subtle" disabled={busy === "save"} type="button" onClick={() => void save(false)}>Guardar</button>
              <button className="button primary" disabled={busy === "save"} type="button" onClick={() => void save(true)}>
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
  return (
    <form className="connection-form" onSubmit={(event) => event.preventDefault()}>
      <label className="wide">Nombre del perfil<input required value={form.name} onChange={(event) => field("name", event.target.value)} /></label>
      <label>Host<input required value={form.host} onChange={(event) => field("host", event.target.value)} /></label>
      <label>Puerto<input required min={1} max={65535} type="number" value={form.port} onChange={(event) => field("port", Number(event.target.value))} /></label>
      <label>Base / servicio<input value={form.database} onChange={(event) => field("database", event.target.value)} /></label>
      <label>Usuario<input required autoComplete="username" value={form.username} onChange={(event) => field("username", event.target.value)} /></label>
      <label className="wide">Contraseña<input required={!editing} autoComplete="new-password" type="password" value={form.password ?? ""} placeholder={editing ? "Vacío conserva la contraseña actual" : ""} onChange={(event) => field("password", event.target.value)} /></label>
      <label>Pool mínimo<input min={0} type="number" value={form.pool_min} onChange={(event) => field("pool_min", Number(event.target.value))} /></label>
      <label>Pool máximo<input min={1} type="number" value={form.pool_max} onChange={(event) => field("pool_max", Number(event.target.value))} /></label>
      <label>Timeout (ms)<input min={250} type="number" value={form.timeout_ms} onChange={(event) => field("timeout_ms", Number(event.target.value))} /></label>
      <label className="check-field"><input type="checkbox" checked={form.ssl} onChange={(event) => field("ssl", event.target.checked)} />Usar SSL/TLS</label>
      <p className="form-security wide">La contraseña se envía al motor para guardarse en su SecretStore. Nunca vuelve en una respuesta ni se escribe en el YAML.</p>
    </form>
  );
}

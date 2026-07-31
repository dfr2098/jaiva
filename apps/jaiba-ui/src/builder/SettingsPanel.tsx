import type {
  DatabaseConnection,
  EngineSettings,
  FlowMeta,
  KafkaConnection,
  ParameterEntry,
} from "./model";

interface SettingsPanelProps {
  meta: FlowMeta;
  onChange: (meta: FlowMeta) => void;
}

const DATABASE_TYPES = ["postgres", "mysql", "mariadb", "mongodb", "oracle", "sqlserver"];

export function SettingsPanel({ meta, onChange }: SettingsPanelProps) {
  const patch = (partial: Partial<FlowMeta>) => onChange({ ...meta, ...partial });
  const patchEngine = (partial: Partial<EngineSettings>) =>
    patch({ engine: { ...meta.engine, ...partial } });

  return (
    <div className="settings-panel">
      <fieldset className="inspector-group">
        <legend>Flujo</legend>
        <label className="builder-field">
          <span className="builder-field-label">Identificador del flujo</span>
          <input
            className="builder-input"
            value={meta.id}
            onChange={(event) => patch({ id: event.target.value })}
          />
        </label>
      </fieldset>

      <fieldset className="inspector-group">
        <legend>Parámetros</legend>
        {meta.parameters.map((parameter, index) => (
          <div className="builder-pair" key={index}>
            <input
              className="builder-input"
              value={parameter.name}
              placeholder="nombre"
              onChange={(event) =>
                patch({
                  parameters: replaceAt(meta.parameters, index, {
                    ...parameter,
                    name: event.target.value,
                  }),
                })
              }
            />
            <input
              className="builder-input"
              value={parameter.value}
              placeholder="valor"
              onChange={(event) =>
                patch({
                  parameters: replaceAt(meta.parameters, index, {
                    ...parameter,
                    value: event.target.value,
                  }),
                })
              }
            />
            <button
              type="button"
              className="builder-icon-btn"
              aria-label="Eliminar"
              onClick={() => patch({ parameters: removeAt(meta.parameters, index) })}
            >
              ×
            </button>
          </div>
        ))}
        <button
          type="button"
          className="builder-add-btn"
          onClick={() =>
            patch({ parameters: [...meta.parameters, emptyParameter()] })
          }
        >
          + Parámetro
        </button>
      </fieldset>

      <fieldset className="inspector-group">
        <legend>Conexiones de base de datos</legend>
        {meta.databaseConnections.map((connection, index) => (
          <div className="connection-card" key={index}>
            <div className="connection-card-head">
              <input
                className="builder-input"
                value={connection.name}
                placeholder="nombre (ej. main)"
                onChange={(event) =>
                  patch({
                    databaseConnections: replaceAt(meta.databaseConnections, index, {
                      ...connection,
                      name: event.target.value,
                    }),
                  })
                }
              />
              <button
                type="button"
                className="builder-icon-btn"
                aria-label="Eliminar"
                onClick={() =>
                  patch({ databaseConnections: removeAt(meta.databaseConnections, index) })
                }
              >
                ×
              </button>
            </div>
            <div className="connection-card-grid">
              <select
                className="builder-input"
                value={connection.type}
                onChange={(event) =>
                  patch({
                    databaseConnections: replaceAt(meta.databaseConnections, index, {
                      ...connection,
                      type: event.target.value,
                    }),
                  })
                }
              >
                {DATABASE_TYPES.map((type) => (
                  <option key={type} value={type}>
                    {type}
                  </option>
                ))}
              </select>
              <input
                className="builder-input"
                value={connection.url_env}
                placeholder="url_env (DATABASE_URL)"
                onChange={(event) =>
                  patch({
                    databaseConnections: replaceAt(meta.databaseConnections, index, {
                      ...connection,
                      url_env: event.target.value,
                    }),
                  })
                }
              />
              <input
                className="builder-input"
                type="number"
                min={1}
                value={connection.max_connections}
                title="max_connections"
                onChange={(event) =>
                  patch({
                    databaseConnections: replaceAt(meta.databaseConnections, index, {
                      ...connection,
                      max_connections: Number(event.target.value),
                    }),
                  })
                }
              />
            </div>
          </div>
        ))}
        <button
          type="button"
          className="builder-add-btn"
          onClick={() =>
            patch({ databaseConnections: [...meta.databaseConnections, emptyDatabase()] })
          }
        >
          + Conexión de base de datos
        </button>
      </fieldset>

      <fieldset className="inspector-group">
        <legend>Conexiones de Kafka</legend>
        {meta.kafkaConnections.map((connection, index) => (
          <div className="connection-card" key={index}>
            <div className="connection-card-head">
              <input
                className="builder-input"
                value={connection.name}
                placeholder="nombre (ej. events)"
                onChange={(event) =>
                  patch({
                    kafkaConnections: replaceAt(meta.kafkaConnections, index, {
                      ...connection,
                      name: event.target.value,
                    }),
                  })
                }
              />
              <button
                type="button"
                className="builder-icon-btn"
                aria-label="Eliminar"
                onClick={() =>
                  patch({ kafkaConnections: removeAt(meta.kafkaConnections, index) })
                }
              >
                ×
              </button>
            </div>
            <div className="connection-card-grid two">
              <input
                className="builder-input"
                value={connection.brokers_env}
                placeholder="brokers_env (KAFKA_BROKERS)"
                onChange={(event) =>
                  patch({
                    kafkaConnections: replaceAt(meta.kafkaConnections, index, {
                      ...connection,
                      brokers_env: event.target.value,
                    }),
                  })
                }
              />
              <input
                className="builder-input"
                value={connection.client_id}
                placeholder="client_id"
                onChange={(event) =>
                  patch({
                    kafkaConnections: replaceAt(meta.kafkaConnections, index, {
                      ...connection,
                      client_id: event.target.value,
                    }),
                  })
                }
              />
            </div>
          </div>
        ))}
        <button
          type="button"
          className="builder-add-btn"
          onClick={() => patch({ kafkaConnections: [...meta.kafkaConnections, emptyKafka()] })}
        >
          + Conexión de Kafka
        </button>
      </fieldset>

      <fieldset className="inspector-group">
        <legend>Motor</legend>
        <div className="connection-card-grid two">
          <label className="builder-field">
            <span className="builder-field-label">Capacidad de cola</span>
            <input
              className="builder-input"
              type="number"
              min={1}
              value={meta.engine.queue_capacity}
              onChange={(event) => patchEngine({ queue_capacity: Number(event.target.value) })}
            />
          </label>
          <label className="builder-field">
            <span className="builder-field-label">Concurrencia máxima</span>
            <input
              className="builder-input"
              type="number"
              min={1}
              value={meta.engine.max_concurrency}
              onChange={(event) => patchEngine({ max_concurrency: Number(event.target.value) })}
            />
          </label>
        </div>
        <label className="builder-field">
          <span className="builder-field-label">Memoria máxima (%)</span>
          <input
            className="builder-input"
            type="number"
            min={1}
            max={90}
            value={meta.engine.memory_maximum_percent}
            onChange={(event) =>
              patchEngine({ memory_maximum_percent: Number(event.target.value) })
            }
          />
        </label>
        <label className="builder-check">
          <input
            type="checkbox"
            checked={meta.engine.repository_enabled}
            onChange={(event) => patchEngine({ repository_enabled: event.target.checked })}
          />
          <span>Repositorio persistente (durabilidad, dead-letter)</span>
        </label>
        <label className="builder-check">
          <input
            type="checkbox"
            checked={meta.engine.circuit_breaker_enabled}
            onChange={(event) => patchEngine({ circuit_breaker_enabled: event.target.checked })}
          />
          <span>Circuit breaker</span>
        </label>
        <label className="builder-check">
          <input
            type="checkbox"
            checked={meta.engine.admin_enabled}
            onChange={(event) => patchEngine({ admin_enabled: event.target.checked })}
          />
          <span>API administrativa</span>
        </label>
        {meta.engine.admin_enabled ? (
          <div className="connection-card-grid two">
            <label className="builder-field">
              <span className="builder-field-label">Autenticación</span>
              <select
                className="builder-input"
                value={meta.engine.admin_authentication}
                onChange={(event) =>
                  patchEngine({
                    admin_authentication: event.target.value as EngineSettings["admin_authentication"],
                  })
                }
              >
                <option value="bearer">bearer</option>
                <option value="none">none (solo loopback)</option>
              </select>
            </label>
            <label className="builder-field">
              <span className="builder-field-label">Token env</span>
              <input
                className="builder-input"
                value={meta.engine.admin_token_env}
                onChange={(event) => patchEngine({ admin_token_env: event.target.value })}
              />
            </label>
          </div>
        ) : null}
      </fieldset>
    </div>
  );
}

function replaceAt<T>(list: T[], index: number, value: T): T[] {
  const next = [...list];
  next[index] = value;
  return next;
}

function removeAt<T>(list: T[], index: number): T[] {
  return list.filter((_, i) => i !== index);
}

function emptyParameter(): ParameterEntry {
  return { name: "", value: "" };
}

function emptyDatabase(): DatabaseConnection {
  return { name: "", type: "postgres", url_env: "", max_connections: 5 };
}

function emptyKafka(): KafkaConnection {
  return { name: "", brokers_env: "", client_id: "jaiba" };
}

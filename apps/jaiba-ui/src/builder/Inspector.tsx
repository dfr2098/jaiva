import { CATALOG_BY_TYPE, CATEGORY_TAG } from "./catalog";
import { FieldEditor } from "./fields";
import {
  EXECUTION_MODES,
  ORDERING_MODES,
  type ProcessorNode,
  type RetrySettings,
  type SchedulingSettings,
  type SimulationSettings,
} from "./model";

interface InspectorProps {
  node: ProcessorNode | null;
  databaseConnectionNames: string[];
  postgresConnectionNames: string[];
  kafkaConnectionNames: string[];
  onChangeProcessorId: (id: string) => void;
  onChangeConfig: (key: string, value: unknown) => void;
  onChangeRetry: (retry: RetrySettings) => void;
  onChangeScheduling: (scheduling: SchedulingSettings) => void;
  onChangeSimulation: (simulation: SimulationSettings) => void;
  onDelete: () => void;
}

export function Inspector({
  node,
  databaseConnectionNames,
  postgresConnectionNames,
  kafkaConnectionNames,
  onChangeProcessorId,
  onChangeConfig,
  onChangeRetry,
  onChangeScheduling,
  onChangeSimulation,
  onDelete,
}: InspectorProps) {
  if (!node) {
    return (
      <div className="inspector empty">
        <p>Selecciona un procesador del lienzo para configurarlo, o arrastra uno desde la paleta.</p>
      </div>
    );
  }

  const def = CATALOG_BY_TYPE[node.data.type];
  const { retry, scheduling, simulation } = node.data;

  return (
    <div className="inspector">
      <div className="inspector-head">
        <span className={`node-tag ${def?.category ?? "transform"}`}>
          {def ? CATEGORY_TAG[def.category] : "Proceso"}
        </span>
        <button type="button" className="button subtle danger" onClick={onDelete}>
          Eliminar nodo
        </button>
      </div>
      <h3>{def?.label ?? node.data.type}</h3>
      <p className="inspector-desc">{def?.description}</p>

      <label className="builder-field">
        <span className="builder-field-label">
          Identificador <em className="required">*</em>
        </span>
        <input
          className="builder-input"
          value={node.data.processorId}
          onChange={(event) => onChangeProcessorId(event.target.value)}
        />
      </label>

      {def && def.fields.length > 0 ? (
        <fieldset className="inspector-group">
          <legend>Configuración</legend>
          {def.fields.map((field) => (
            <FieldEditor
              key={field.key}
              field={field}
              value={node.data.config[field.key]}
              connectionNames={
                field.connectionKind === "kafka"
                  ? kafkaConnectionNames
                  : field.connectionKind === "postgres"
                    ? postgresConnectionNames
                  : databaseConnectionNames
              }
              onChange={(value) => onChangeConfig(field.key, value)}
            />
          ))}
        </fieldset>
      ) : (
        <p className="inspector-desc">Este procesador no requiere configuración.</p>
      )}

      <details className="inspector-details">
        <summary>Reintentos</summary>
        <label className="builder-field">
          <span className="builder-field-label">Intentos máximos</span>
          <input
            className="builder-input"
            type="number"
            min={0}
            value={retry.maximum_attempts}
            onChange={(event) =>
              onChangeRetry({ ...retry, maximum_attempts: Number(event.target.value) })
            }
          />
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Retardo inicial (ms)</span>
          <input
            className="builder-input"
            type="number"
            min={0}
            value={retry.initial_delay_ms}
            onChange={(event) =>
              onChangeRetry({ ...retry, initial_delay_ms: Number(event.target.value) })
            }
          />
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Retardo máximo (ms)</span>
          <input
            className="builder-input"
            type="number"
            min={0}
            value={retry.maximum_delay_ms}
            onChange={(event) =>
              onChangeRetry({ ...retry, maximum_delay_ms: Number(event.target.value) })
            }
          />
        </label>
      </details>

      <details className="inspector-details">
        <summary>Simulación</summary>
        <label className="builder-field">
          <span className="builder-field-label">Modo de datos</span>
          <select
            className="builder-input"
            value={simulation.mode}
            onChange={(event) =>
              onChangeSimulation({
                ...simulation,
                mode: event.target.value as SimulationSettings["mode"],
              })
            }
          >
            <option value="real">Real</option>
            <option value="mock">Mock</option>
            <option value="replay">Replay</option>
          </select>
        </label>
        {simulation.mode !== "real" ? (
          <label className="builder-field">
            <span className="builder-field-label">Opciones JSON</span>
            <textarea
              className="builder-input builder-textarea"
              value={JSON.stringify(simulation.options, null, 2)}
              onChange={(event) => {
                try {
                  onChangeSimulation({
                    ...simulation,
                    options: JSON.parse(event.target.value) as Record<string, unknown>,
                  });
                } catch {
                  // Keep the last valid object while the user is typing JSON.
                }
              }}
            />
          </label>
        ) : null}
        <small className="builder-help">
          Real usa el plugin; Mock genera datos; Replay utiliza provenance.
        </small>
      </details>

      <details className="inspector-details">
        <summary>Planificación</summary>
        <label className="builder-field">
          <span className="builder-field-label">Tareas concurrentes</span>
          <input
            className="builder-input"
            type="number"
            min={1}
            value={scheduling.concurrent_tasks}
            onChange={(event) =>
              onChangeScheduling({
                ...scheduling,
                concurrent_tasks: Number(event.target.value),
              })
            }
          />
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Máx. en vuelo (opcional)</span>
          <input
            className="builder-input"
            type="number"
            min={1}
            value={scheduling.maximum_in_flight ?? ""}
            onChange={(event) =>
              onChangeScheduling({
                ...scheduling,
                maximum_in_flight:
                  event.target.value === "" ? null : Number(event.target.value),
              })
            }
          />
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Timeout (ms, opcional)</span>
          <input
            className="builder-input"
            type="number"
            min={1}
            value={scheduling.timeout_ms ?? ""}
            onChange={(event) =>
              onChangeScheduling({
                ...scheduling,
                timeout_ms: event.target.value === "" ? null : Number(event.target.value),
              })
            }
          />
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Modo de ejecución</span>
          <select
            className="builder-input"
            value={scheduling.execution_mode}
            onChange={(event) =>
              onChangeScheduling({
                ...scheduling,
                execution_mode: event.target.value as SchedulingSettings["execution_mode"],
              })
            }
          >
            {EXECUTION_MODES.map((mode) => (
              <option key={mode} value={mode}>
                {mode}
              </option>
            ))}
          </select>
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Orden</span>
          <select
            className="builder-input"
            value={scheduling.ordering}
            onChange={(event) =>
              onChangeScheduling({
                ...scheduling,
                ordering: event.target.value as SchedulingSettings["ordering"],
              })
            }
          >
            {ORDERING_MODES.map((mode) => (
              <option key={mode} value={mode}>
                {mode}
              </option>
            ))}
          </select>
        </label>
        {scheduling.ordering === "partitioned" ? (
          <label className="builder-field">
            <span className="builder-field-label">Particionar por</span>
            <input
              className="builder-input"
              value={scheduling.partition_by ?? ""}
              placeholder="attribute.customer_id"
              onChange={(event) =>
                onChangeScheduling({
                  ...scheduling,
                  partition_by: event.target.value === "" ? null : event.target.value,
                })
              }
            />
          </label>
        ) : null}
      </details>
    </div>
  );
}

import { useCallback, useEffect, useState } from "react";
import { jaivaApi } from "../api";
import type {
  DatabaseConnection,
  DatabaseObject,
  ObjectDescription,
} from "../types";

/**
 * Explorador neutral para adaptadores que publican metadatos pero no un
 * constructor SQL. MongoDB lo usa para mostrar colecciones y los campos BSON
 * inferidos por el servidor a partir de un documento de muestra.
 */
export function MetadataExplorer({
  connection,
}: {
  connection: DatabaseConnection;
}) {
  const [objects, setObjects] = useState<DatabaseObject[]>([]);
  const [selectedIndex, setSelectedIndex] = useState("");
  const [description, setDescription] = useState<ObjectDescription | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadObjects = useCallback(async () => {
    setLoading(true);
    try {
      const result = await jaivaApi.connectionMetadata(connection.id);
      setObjects(result.filter((object) => object.kind !== "schema"));
      setSelectedIndex("");
      setDescription(null);
      setError(null);
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : "No se pudieron cargar los metadatos",
      );
    } finally {
      setLoading(false);
    }
  }, [connection.id]);

  useEffect(() => {
    void loadObjects();
  }, [loadObjects]);

  const selectObject = async (index: string) => {
    setSelectedIndex(index);
    setDescription(null);
    if (index === "") return;
    const object = objects[Number(index)];
    if (!object?.schema) return;
    setLoading(true);
    try {
      setDescription(
        await jaivaApi.describeConnectionObject(
          connection.id,
          object.schema,
          object.name,
        ),
      );
      setError(null);
    } catch (reason) {
      setError(
        reason instanceof Error
          ? reason.message
          : "No se pudo describir el objeto",
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="sql-workbench sqlb">
      <div className="sql-workbench-head">
        <div>
          <strong>Explorador de metadatos</strong>
          <small>Colecciones y campos detectados por el servidor</small>
        </div>
        <button
          className="button subtle"
          type="button"
          disabled={loading}
          onClick={() => void loadObjects()}
        >
          {loading ? "Cargando…" : "Actualizar"}
        </button>
      </div>

      <label className="builder-field">
        <span className="builder-field-label">Colección / objeto</span>
        <select
          className="builder-input"
          value={selectedIndex}
          onChange={(event) => void selectObject(event.target.value)}
        >
          <option value="">Selecciona un objeto…</option>
          {objects.map((object, index) => (
            <option
              key={`${object.schema}.${object.name}.${object.kind}`}
              value={String(index)}
            >
              {object.name} ({object.kind})
            </option>
          ))}
        </select>
      </label>

      {description ? (
        <div className="sqlb-section">
          <div className="sqlb-section-head">
            <strong>Campos detectados</strong>
            <small>{description.columns.length}</small>
          </div>
          <div className="sqlb-columns">
            {description.columns.length === 0 ? (
              <small className="sqlb-muted">
                La colección está vacía; no hay un documento para inferir campos.
              </small>
            ) : (
              description.columns.map((column) => (
                <span className="sqlb-column-chip" key={column.name}>
                  <span>{column.name}</span>
                  <em>{column.data_type}</em>
                </span>
              ))
            )}
          </div>
        </div>
      ) : null}

      {error ? <p className="sqlb-muted">{error}</p> : null}
    </section>
  );
}

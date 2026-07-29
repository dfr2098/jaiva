import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { jaivaApi } from "../api";
import { stashPendingQueryNode } from "../builder/pendingQueryNode";
import type {
  CompiledQuery,
  ColumnMetadata,
  DatabaseConnection,
  DatabaseObject,
  FilterOperator,
  JoinKind,
  QueryFilter,
  QuerySpec,
  SortDirection,
} from "../types";

const OPERATOR_LABEL: Record<FilterOperator, string> = {
  eq: "=",
  not_eq: "≠",
  greater_than: ">",
  greater_or_equal: "≥",
  less_than: "<",
  less_or_equal: "≤",
  contains: "contiene",
  starts_with: "empieza con",
  in: "en lista",
  is_null: "es nulo",
  is_not_null: "no es nulo",
};

const JOIN_LABEL: Record<JoinKind, string> = {
  inner: "INNER",
  left: "LEFT",
  right: "RIGHT",
  full: "FULL",
};

const NO_VALUE: FilterOperator[] = ["is_null", "is_not_null"];

interface FilterRow {
  field: string;
  operator: FilterOperator;
  value: string;
}

interface OrderRow {
  field: string;
  direction: SortDirection;
}

interface JoinRow {
  kind: JoinKind;
  tableIndex: string;
  left: string;
  right: string;
}

function coerceScalar(raw: string): unknown {
  const trimmed = raw.trim();
  if (trimmed === "") return "";
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  if (/^-?\d+$/.test(trimmed)) return Number(trimmed);
  if (/^-?\d*\.\d+$/.test(trimmed)) return Number(trimmed);
  return raw;
}

/** El procesador query_postgres espera una sola columna JSONB por fila. */
function wrapForPostgres(statement: string): string {
  return `SELECT to_jsonb(t) AS record FROM (\n${statement}\n) AS t`;
}

export function SqlQueryBuilder({
  connection,
  onCreateQueryNode,
}: {
  connection: DatabaseConnection;
  onCreateQueryNode?: () => void;
}) {
  const [tables, setTables] = useState<DatabaseObject[]>([]);
  const [sourceIndex, setSourceIndex] = useState<string>("");
  const [sourceColumns, setSourceColumns] = useState<ColumnMetadata[]>([]);
  const [selectAll, setSelectAll] = useState(true);
  const [columns, setColumns] = useState<string[]>([]);
  const [filters, setFilters] = useState<FilterRow[]>([]);
  const [groupBy, setGroupBy] = useState<string[]>([]);
  const [orderBy, setOrderBy] = useState<OrderRow[]>([]);
  const [joins, setJoins] = useState<JoinRow[]>([]);
  const [limit, setLimit] = useState<string>("");
  const [compiled, setCompiled] = useState<CompiledQuery | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadingTables, setLoadingTables] = useState(false);
  const [created, setCreated] = useState<string | null>(null);

  const isPostgres = connection.connection_type === "postgres";

  const loadTables = useCallback(async () => {
    setLoadingTables(true);
    try {
      const objects = await jaivaApi.connectionMetadata(connection.id);
      setTables(objects.filter((object) => object.kind === "table" || object.kind === "view"));
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "No se pudieron listar las tablas");
    } finally {
      setLoadingTables(false);
    }
  }, [connection.id]);

  useEffect(() => {
    setSourceIndex("");
    setSourceColumns([]);
    setSelectAll(true);
    setColumns([]);
    setFilters([]);
    setGroupBy([]);
    setOrderBy([]);
    setJoins([]);
    setLimit("");
    setCompiled(null);
    setCreated(null);
    void loadTables();
  }, [loadTables]);

  const source = sourceIndex === "" ? null : tables[Number(sourceIndex)] ?? null;

  useEffect(() => {
    if (!source?.schema) {
      setSourceColumns([]);
      return;
    }
    let cancelled = false;
    jaivaApi
      .describeConnectionObject(connection.id, source.schema, source.name)
      .then((description) => {
        if (!cancelled) setSourceColumns(description.columns);
      })
      .catch(() => {
        if (!cancelled) setSourceColumns([]);
      });
    return () => {
      cancelled = true;
    };
  }, [connection.id, source?.schema, source?.name]);

  const columnNames = useMemo(() => sourceColumns.map((column) => column.name), [sourceColumns]);

  const spec = useMemo<QuerySpec | null>(() => {
    if (!source?.schema) return null;
    const selectedColumns = selectAll ? ["*"] : columns;
    if (selectedColumns.length === 0) return null;
    const compiledFilters: QueryFilter[] = filters
      .filter((row) => row.field.trim() !== "")
      .map((row) => {
        if (NO_VALUE.includes(row.operator)) {
          return { field: row.field.trim(), operator: row.operator, value: null };
        }
        if (row.operator === "in") {
          const items = row.value
            .split(",")
            .map((part) => part.trim())
            .filter((part) => part !== "")
            .map((part) => coerceScalar(part));
          return { field: row.field.trim(), operator: row.operator, value: items };
        }
        return { field: row.field.trim(), operator: row.operator, value: coerceScalar(row.value) };
      });
    const compiledJoins = joins
      .filter((row) => row.tableIndex !== "" && row.left.trim() !== "" && row.right.trim() !== "")
      .map((row) => {
        const target = tables[Number(row.tableIndex)];
        return {
          kind: row.kind,
          source: { schema: target?.schema ?? null, table: target?.name ?? "" },
          left: row.left.trim(),
          right: row.right.trim(),
        };
      });
    return {
      source: { schema: source.schema, table: source.name },
      columns: selectedColumns,
      joins: compiledJoins,
      filters: compiledFilters,
      group_by: groupBy,
      order_by: orderBy.filter((row) => row.field.trim() !== ""),
      limit: limit.trim() === "" ? null : Number(limit),
    };
  }, [source?.schema, source?.name, selectAll, columns, filters, joins, groupBy, orderBy, limit, tables]);

  // Compila en vivo (con retardo) mientras la especificación sea válida.
  const specKey = useMemo(() => (spec ? JSON.stringify(spec) : ""), [spec]);
  useEffect(() => {
    if (!spec) {
      setCompiled(null);
      return;
    }
    let cancelled = false;
    const timer = window.setTimeout(() => {
      jaivaApi
        .compileQuery(connection.id, spec)
        .then((result) => {
          if (cancelled) return;
          setCompiled(result);
          setError(null);
        })
        .catch((reason) => {
          if (cancelled) return;
          setCompiled(null);
          setError(reason instanceof Error ? reason.message : "No se pudo compilar la consulta");
        });
    }, 400);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [connection.id, specKey, spec]);

  const toggleColumn = (name: string) => {
    setColumns((current) =>
      current.includes(name) ? current.filter((item) => item !== name) : [...current, name],
    );
  };

  const toggleGroupBy = (name: string) => {
    setGroupBy((current) =>
      current.includes(name) ? current.filter((item) => item !== name) : [...current, name],
    );
  };

  const createNode = () => {
    if (!compiled || !isPostgres) return;
    stashPendingQueryNode({
      connectionName: connection.name,
      connectionType: connection.connection_type,
      query: wrapForPostgres(compiled.statement),
      parameters: compiled.parameters,
      table: source ? `${source.schema}.${source.name}` : "consulta",
    });
    setCreated("Nodo Query preparado. Abriendo el diseñador…");
    onCreateQueryNode?.();
  };

  const datalistId = `sqlb-cols-${connection.id}`;

  return (
    <section className="sql-workbench sqlb">
      <div className="sql-workbench-head">
        <div>
          <strong>Constructor visual SQL</strong>
          <small>{connection.connection_type.replace("_", " ")} · el servidor genera el SQL seguro</small>
        </div>
        <button className="button subtle" type="button" disabled={loadingTables} onClick={() => void loadTables()}>
          {loadingTables ? "Cargando…" : "Actualizar tablas"}
        </button>
      </div>

      <datalist id={datalistId}>
        {columnNames.map((name) => (
          <option key={name} value={name} />
        ))}
      </datalist>

      <div className="sqlb-grid">
        <label className="builder-field">
          <span className="builder-field-label">Tabla / vista</span>
          <select
            className="builder-input"
            value={sourceIndex}
            onChange={(event) => {
              setSourceIndex(event.target.value);
              setColumns([]);
              setSelectAll(true);
              setGroupBy([]);
            }}
          >
            <option value="">Selecciona una tabla…</option>
            {tables.map((table, index) => (
              <option key={`${table.schema}.${table.name}`} value={String(index)}>
                {table.schema}.{table.name} {table.kind === "view" ? "(vista)" : ""}
              </option>
            ))}
          </select>
        </label>
        <label className="builder-field">
          <span className="builder-field-label">Límite</span>
          <input
            className="builder-input"
            type="number"
            min={1}
            placeholder="sin límite"
            value={limit}
            onChange={(event) => setLimit(event.target.value)}
          />
        </label>
      </div>

      {source ? (
        <>
          <div className="sqlb-section">
            <div className="sqlb-section-head">
              <strong>Columnas</strong>
              <label className="sqlb-inline-check">
                <input type="checkbox" checked={selectAll} onChange={(event) => setSelectAll(event.target.checked)} />
                Todas (*)
              </label>
            </div>
            {!selectAll ? (
              <div className="sqlb-columns">
                {sourceColumns.length === 0 ? (
                  <small className="sqlb-muted">Describiendo columnas…</small>
                ) : (
                  sourceColumns.map((column) => (
                    <label key={column.name} className="sqlb-column-chip">
                      <input
                        type="checkbox"
                        checked={columns.includes(column.name)}
                        onChange={() => toggleColumn(column.name)}
                      />
                      <span>{column.name}</span>
                      <em>{column.data_type}</em>
                    </label>
                  ))
                )}
              </div>
            ) : null}
          </div>

          <div className="sqlb-section">
            <div className="sqlb-section-head">
              <strong>Filtros</strong>
              <button
                className="button subtle"
                type="button"
                onClick={() => setFilters((current) => [...current, { field: "", operator: "eq", value: "" }])}
              >
                + Filtro
              </button>
            </div>
            {filters.map((row, index) => (
              <div className="sqlb-row" key={index}>
                <input
                  className="builder-input"
                  list={datalistId}
                  placeholder="columna"
                  value={row.field}
                  onChange={(event) =>
                    setFilters((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, field: event.target.value } : item,
                      ),
                    )
                  }
                />
                <select
                  className="builder-input"
                  value={row.operator}
                  onChange={(event) =>
                    setFilters((current) =>
                      current.map((item, position) =>
                        position === index
                          ? { ...item, operator: event.target.value as FilterOperator }
                          : item,
                      ),
                    )
                  }
                >
                  {Object.entries(OPERATOR_LABEL).map(([value, label]) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
                <input
                  className="builder-input"
                  placeholder={row.operator === "in" ? "a, b, c" : "valor"}
                  disabled={NO_VALUE.includes(row.operator)}
                  value={row.value}
                  onChange={(event) =>
                    setFilters((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, value: event.target.value } : item,
                      ),
                    )
                  }
                />
                <button
                  className="button subtle danger"
                  type="button"
                  onClick={() => setFilters((current) => current.filter((_, position) => position !== index))}
                >
                  ×
                </button>
              </div>
            ))}
          </div>

          <div className="sqlb-section">
            <div className="sqlb-section-head">
              <strong>Joins</strong>
              <button
                className="button subtle"
                type="button"
                onClick={() =>
                  setJoins((current) => [...current, { kind: "inner", tableIndex: "", left: "", right: "" }])
                }
              >
                + Join
              </button>
            </div>
            {joins.map((row, index) => (
              <div className="sqlb-row join" key={index}>
                <select
                  className="builder-input"
                  value={row.kind}
                  onChange={(event) =>
                    setJoins((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, kind: event.target.value as JoinKind } : item,
                      ),
                    )
                  }
                >
                  {Object.entries(JOIN_LABEL).map(([value, label]) => (
                    <option key={value} value={value}>
                      {label}
                    </option>
                  ))}
                </select>
                <select
                  className="builder-input"
                  value={row.tableIndex}
                  onChange={(event) =>
                    setJoins((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, tableIndex: event.target.value } : item,
                      ),
                    )
                  }
                >
                  <option value="">tabla…</option>
                  {tables.map((table, tableIndex) => (
                    <option key={`${table.schema}.${table.name}`} value={String(tableIndex)}>
                      {table.schema}.{table.name}
                    </option>
                  ))}
                </select>
                <input
                  className="builder-input"
                  placeholder="col. izquierda"
                  value={row.left}
                  onChange={(event) =>
                    setJoins((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, left: event.target.value } : item,
                      ),
                    )
                  }
                />
                <input
                  className="builder-input"
                  placeholder="col. derecha"
                  value={row.right}
                  onChange={(event) =>
                    setJoins((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, right: event.target.value } : item,
                      ),
                    )
                  }
                />
                <button
                  className="button subtle danger"
                  type="button"
                  onClick={() => setJoins((current) => current.filter((_, position) => position !== index))}
                >
                  ×
                </button>
              </div>
            ))}
          </div>

          <div className="sqlb-section">
            <div className="sqlb-section-head">
              <strong>Agrupar por</strong>
            </div>
            <div className="sqlb-columns">
              {sourceColumns.map((column) => (
                <label key={column.name} className="sqlb-column-chip">
                  <input
                    type="checkbox"
                    checked={groupBy.includes(column.name)}
                    onChange={() => toggleGroupBy(column.name)}
                  />
                  <span>{column.name}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="sqlb-section">
            <div className="sqlb-section-head">
              <strong>Ordenar por</strong>
              <button
                className="button subtle"
                type="button"
                onClick={() => setOrderBy((current) => [...current, { field: "", direction: "asc" }])}
              >
                + Orden
              </button>
            </div>
            {orderBy.map((row, index) => (
              <div className="sqlb-row" key={index}>
                <input
                  className="builder-input"
                  list={datalistId}
                  placeholder="columna"
                  value={row.field}
                  onChange={(event) =>
                    setOrderBy((current) =>
                      current.map((item, position) =>
                        position === index ? { ...item, field: event.target.value } : item,
                      ),
                    )
                  }
                />
                <select
                  className="builder-input"
                  value={row.direction}
                  onChange={(event) =>
                    setOrderBy((current) =>
                      current.map((item, position) =>
                        position === index
                          ? { ...item, direction: event.target.value as SortDirection }
                          : item,
                      ),
                    )
                  }
                >
                  <option value="asc">ASC</option>
                  <option value="desc">DESC</option>
                </select>
                <button
                  className="button subtle danger"
                  type="button"
                  onClick={() => setOrderBy((current) => current.filter((_, position) => position !== index))}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        </>
      ) : (
        <p className="sqlb-muted">Elige una tabla para comenzar a construir la consulta.</p>
      )}

      <div className="sqlb-preview">
        <div className="sqlb-preview-head">
          <span>SQL generado por el servidor</span>
          {compiled ? (
            <button
              className="button subtle"
              type="button"
              onClick={() => void navigator.clipboard?.writeText(compiled.statement)}
            >
              Copiar
            </button>
          ) : null}
        </div>
        <pre className="sqlb-sql">{compiled?.statement ?? "-- construye la consulta arriba --"}</pre>
        {compiled && compiled.parameters.length > 0 ? (
          <small className="sqlb-params">Parámetros: {JSON.stringify(compiled.parameters)}</small>
        ) : null}
      </div>

      {error ? <div className="sqlb-error">{error}</div> : null}

      <div className="sqlb-actions">
        <button
          className="button primary"
          type="button"
          disabled={!compiled || !isPostgres}
          onClick={createNode}
          title={isPostgres ? "Crea un nodo query_postgres en el lienzo" : "Solo PostgreSQL puede ejecutarse hoy en el motor"}
        >
          Crear nodo Query
        </button>
        {!isPostgres ? (
          <small className="sqlb-muted">
            El motor solo ejecuta consultas PostgreSQL por ahora; puedes copiar el SQL generado.
          </small>
        ) : null}
        {created ? <small className="sqlb-ok">{created}</small> : null}
      </div>
    </section>
  );
}

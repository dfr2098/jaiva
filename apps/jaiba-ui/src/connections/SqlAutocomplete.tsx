import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { jaivaApi } from "../api";
import type {
  DatabaseConnection,
  DatabaseObject,
  ObjectDescription,
} from "../types";

const COMMON = [
  "SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
  "ON", "AS", "AND", "OR", "GROUP BY", "ORDER BY", "HAVING", "INSERT INTO",
  "UPDATE", "DELETE FROM", "VALUES", "CREATE TABLE", "ALTER TABLE", "WITH",
  "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT", "NULL", "IS NULL",
];

const DIALECT: Record<string, string[]> = {
  postgres: ["ILIKE", "RETURNING", "LIMIT", "OFFSET", "JSONB_BUILD_OBJECT", "NOW()"],
  mysql: ["LIMIT", "SHOW TABLES", "IFNULL", "DATE_FORMAT", "NOW()"],
  maria_db: ["LIMIT", "SHOW TABLES", "IFNULL", "DATE_FORMAT", "NOW()"],
  oracle: ["FETCH FIRST", "ROWNUM", "NVL", "SYSDATE", "MERGE INTO", "DUAL"],
  sql_server: ["TOP", "OFFSET", "FETCH NEXT", "ISNULL", "GETDATE()", "MERGE"],
};

interface Suggestion {
  value: string;
  detail: string;
}

export function SqlAutocomplete({ connection }: { connection: DatabaseConnection }) {
  const [sql, setSql] = useState("");
  const [objects, setObjects] = useState<DatabaseObject[]>([]);
  const [descriptions, setDescriptions] = useState<ObjectDescription[]>([]);
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [loading, setLoading] = useState(false);
  const [metadataError, setMetadataError] = useState<string | null>(null);
  const editor = useRef<HTMLTextAreaElement>(null);
  // Claves ya descritas y peticiones en curso: evita describir tablas de forma
  // anticipada. Sólo se consulta el detalle de una tabla cuando aparece en la
  // consulta (FROM/JOIN o alias), no todo el catálogo al abrir la conexión.
  const describedKeys = useRef<Set<string>>(new Set());
  const inflightKeys = useRef<Set<string>>(new Set());

  const loadMetadata = useCallback(async (signal?: { cancelled: boolean }) => {
    setLoading(true);
    setMetadataError(null);
    describedKeys.current.clear();
    inflightKeys.current.clear();
    setDescriptions([]);
    try {
      const nextObjects = await jaivaApi.connectionMetadata(connection.id);
      if (signal?.cancelled) return;
      setObjects(nextObjects);
    } catch (reason) {
      if (signal?.cancelled) return;
      setMetadataError(reason instanceof Error ? reason.message : "No se pudieron explorar metadatos");
      setObjects([]);
    } finally {
      if (!signal?.cancelled) setLoading(false);
    }
  }, [connection.id]);

  useEffect(() => {
    setSql("");
    setOpen(false);
    const signal = { cancelled: false };
    void loadMetadata(signal);
    return () => {
      signal.cancelled = true;
    };
  }, [loadMetadata]);

  // Describe bajo demanda las tablas/vistas referenciadas en la consulta actual.
  useEffect(() => {
    const targets = referencedTables(sql)
      .map((raw) => matchObject(raw, objects))
      .filter((object): object is DatabaseObject => Boolean(object?.schema))
      .filter((object) => {
        const key = `${object.schema}.${object.name}`.toLowerCase();
        return !describedKeys.current.has(key) && !inflightKeys.current.has(key);
      });
    if (targets.length === 0) return;
    let cancelled = false;
    for (const object of targets) {
      const key = `${object.schema}.${object.name}`.toLowerCase();
      inflightKeys.current.add(key);
      jaivaApi
        .describeConnectionObject(connection.id, object.schema as string, object.name)
        .then((described) => {
          if (cancelled) return;
          describedKeys.current.add(key);
          setDescriptions((previous) =>
            previous.some(
              (item) => `${item.object.schema}.${item.object.name}`.toLowerCase() === key,
            )
              ? previous
              : [...previous, described],
          );
        })
        .catch(() => {
          /* un error al describir una tabla no debe romper el editor */
        })
        .finally(() => {
          inflightKeys.current.delete(key);
        });
    }
    return () => {
      cancelled = true;
    };
  }, [sql, objects, connection.id]);

  const suggestions = useMemo(
    () => suggestionsAtCursor(sql, editor.current?.selectionStart ?? sql.length, connection, objects, descriptions),
    [sql, connection, objects, descriptions, open],
  );

  const show = () => {
    setActive(0);
    setOpen(true);
  };

  const accept = (suggestion: Suggestion) => {
    const element = editor.current;
    if (!element) return;
    const cursor = element.selectionStart;
    const prefix = sql.slice(0, cursor);
    const match = prefix.match(/(?:[A-Za-z_][\w$]*\.[\w$]*|[A-Za-z_][\w$]*)$/);
    const start = match ? cursor - match[0].length : cursor;
    const next = `${sql.slice(0, start)}${suggestion.value}${sql.slice(cursor)}`;
    const nextCursor = start + suggestion.value.length;
    setSql(next);
    setOpen(false);
    requestAnimationFrame(() => {
      element.focus();
      element.setSelectionRange(nextCursor, nextCursor);
    });
  };

  return (
    <section className="sql-workbench">
      <div className="sql-workbench-head">
        <div>
          <strong>Editor SQL inteligente</strong>
          <small>{connection.connection_type.replace("_", " ")} · Ctrl+Espacio para sugerencias</small>
        </div>
        <button className="button subtle" type="button" disabled={loading} onClick={() => void loadMetadata()}>
          {loading ? "Explorando…" : "Actualizar metadatos"}
        </button>
      </div>
      <div className="sql-editor-wrap">
        <textarea
          ref={editor}
          className="builder-input builder-textarea code sql-editor"
          value={sql}
          rows={8}
          spellCheck={false}
          placeholder="SELECT t. FROM schema.tabla AS t"
          onChange={(event) => {
            setSql(event.target.value);
            if (open) setActive(0);
          }}
          onKeyDown={(event) => {
            if (event.ctrlKey && event.code === "Space") {
              event.preventDefault();
              show();
              return;
            }
            if (!open) return;
            if (event.key === "ArrowDown" || event.key === "ArrowUp") {
              event.preventDefault();
              const direction = event.key === "ArrowDown" ? 1 : -1;
              setActive((current) => (current + direction + suggestions.length) % suggestions.length);
            } else if (event.key === "Enter" && suggestions[active]) {
              event.preventDefault();
              accept(suggestions[active]);
            } else if (event.key === "Escape") {
              event.preventDefault();
              setOpen(false);
            }
          }}
        />
        {open ? (
          <div className="sql-suggestions" role="listbox">
            {suggestions.length ? suggestions.slice(0, 80).map((suggestion, index) => (
              <button
                type="button"
                role="option"
                aria-selected={index === active}
                className={index === active ? "active" : ""}
                key={`${suggestion.detail}:${suggestion.value}`}
                onMouseDown={(event) => event.preventDefault()}
                onClick={() => accept(suggestion)}
              >
                <span>{suggestion.value}</span><small>{suggestion.detail}</small>
              </button>
            )) : <div className="sql-no-suggestions">Sin coincidencias</div>}
          </div>
        ) : null}
      </div>
      <div className="sql-metadata-summary">
        {metadataError
          ? <span className="error">{metadataError}</span>
          : `${objects.filter((item) => item.kind === "table").length} tablas · ${objects.filter((item) => item.kind === "view").length} vistas · ${objects.filter((item) => item.kind === "procedure" || item.kind === "function").length} rutinas · ${objects.filter((item) => item.kind === "sequence").length} secuencias · ${descriptions.length} descritas`}
      </div>
    </section>
  );
}

/** Extrae las tablas referenciadas en cláusulas FROM/JOIN de la consulta. */
function referencedTables(sql: string): string[] {
  const result: string[] = [];
  const pattern = /\b(?:FROM|JOIN)\s+((?:["`[\]\w$]+\.)?["`[\]\w$]+)/gi;
  for (const match of sql.matchAll(pattern)) {
    result.push(match[1].replace(/["`[\]]/g, ""));
  }
  return result;
}

/** Localiza en el catálogo la tabla o vista cuyo nombre coincide con `raw`. */
function matchObject(raw: string, objects: DatabaseObject[]): DatabaseObject | undefined {
  const target = raw.toLowerCase();
  return objects.find((object) => {
    if (object.kind !== "table" && object.kind !== "view") return false;
    const full = object.schema
      ? `${object.schema}.${object.name}`.toLowerCase()
      : object.name.toLowerCase();
    return full === target || object.name.toLowerCase() === target;
  });
}

function suggestionsAtCursor(
  sql: string,
  cursor: number,
  connection: DatabaseConnection,
  objects: DatabaseObject[],
  descriptions: ObjectDescription[],
): Suggestion[] {
  const prefixText = sql.slice(0, cursor);
  const token = prefixText.match(/((?:[A-Za-z_][\w$]*\.[\w$]*|[A-Za-z_][\w$]*))$/)?.[1] ?? "";
  const [qualifier, partial = ""] = token.includes(".") ? token.split(".", 2) : ["", token];
  const aliases = extractAliases(sql);
  let candidates: Suggestion[];
  if (qualifier) {
    const tableName = aliases.get(qualifier.toLowerCase()) ?? qualifier;
    candidates = descriptions
      .filter((item) =>
        item.object.name.toLowerCase() === tableName.toLowerCase() ||
        `${item.object.schema}.${item.object.name}`.toLowerCase() === tableName.toLowerCase())
      .flatMap((item) => {
        const primaryKeys = new Set(
          item.keys
            .filter((key) => key.kind.toUpperCase().includes("PRIMARY"))
            .flatMap((key) => key.columns.map((column) => column.toLowerCase())),
        );
        return item.columns.map((column) => ({
          value: `${qualifier}.${column.name}`,
          detail: primaryKeys.has(column.name.toLowerCase())
            ? `${column.data_type} · PK`
            : column.data_type,
        }));
      });
  } else {
    candidates = [
      ...COMMON.map((value) => ({ value, detail: "SQL" })),
      ...(DIALECT[connection.connection_type] ?? []).map((value) => ({ value, detail: connection.connection_type })),
      ...objects.map((object) => ({
        value: object.schema ? `${object.schema}.${object.name}` : object.name,
        detail: object.kind,
      })),
      ...descriptions.flatMap((item) => item.columns.map((column) => ({
        value: column.name,
        detail: `${item.object.name} · ${column.data_type}`,
      }))),
    ];
  }
  const needle = partial.toLowerCase();
  return candidates
    .filter((item) => !needle || item.value.split(".").at(-1)?.toLowerCase().startsWith(needle))
    .filter((item, index, all) => all.findIndex((candidate) => candidate.value === item.value) === index)
    .sort((left, right) => left.value.localeCompare(right.value));
}

function extractAliases(sql: string): Map<string, string> {
  const aliases = new Map<string, string>();
  const pattern = /\b(?:FROM|JOIN)\s+((?:["`[\]\w$]+\.)?["`[\]\w$]+)(?:\s+(?:AS\s+)?([A-Za-z_][\w$]*))?/gi;
  for (const match of sql.matchAll(pattern)) {
    const table = match[1].replace(/["`[\]]/g, "");
    const alias = match[2];
    if (alias && !["WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "ON", "GROUP", "ORDER"].includes(alias.toUpperCase())) {
      aliases.set(alias.toLowerCase(), table);
    }
  }
  return aliases;
}

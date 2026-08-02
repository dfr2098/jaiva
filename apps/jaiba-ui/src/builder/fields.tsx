import { useEffect, useState } from "react";
import type { FieldDef } from "./catalog";

interface FieldEditorProps {
  field: FieldDef;
  value: unknown;
  connectionNames: string[];
  onChange: (value: unknown) => void;
}

export function FieldEditor({
  field,
  value,
  connectionNames,
  onChange,
}: FieldEditorProps) {
  return (
    <label className="builder-field">
      <span className="builder-field-label">
        {field.label}
        {field.required ? <em className="required">*</em> : null}
      </span>
      <FieldControl
        field={field}
        value={value}
        connectionNames={connectionNames}
        onChange={onChange}
      />
      {field.help ? <small className="builder-help">{field.help}</small> : null}
    </label>
  );
}

function FieldControl({ field, value, connectionNames, onChange }: FieldEditorProps) {
  switch (field.kind) {
    case "text":
      return (
        <input
          className="builder-input"
          type="text"
          value={asString(value)}
          placeholder={field.placeholder}
          onChange={(event) => onChange(event.target.value)}
        />
      );
    case "textarea":
      return (
        <textarea
          className="builder-input builder-textarea"
          value={asString(value)}
          placeholder={field.placeholder}
          rows={4}
          onChange={(event) => onChange(event.target.value)}
        />
      );
    case "number":
      return (
        <input
          className="builder-input"
          type="number"
          value={value === undefined || value === null ? "" : String(value)}
          placeholder={field.placeholder}
          onChange={(event) =>
            onChange(event.target.value === "" ? undefined : Number(event.target.value))
          }
        />
      );
    case "boolean":
      return (
        <span className="builder-switch">
          <input
            type="checkbox"
            checked={Boolean(value)}
            onChange={(event) => onChange(event.target.checked)}
          />
          <i />
        </span>
      );
    case "select":
      return (
        <select
          className="builder-input"
          value={asString(value)}
          onChange={(event) => onChange(event.target.value)}
        >
          {(field.options ?? []).map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      );
    case "connectionRef": {
      const current = asString(value);
      const names = [...connectionNames];
      if (current && !names.includes(current)) names.unshift(current);
      if (names.length === 0) {
        return (
          <>
            <input
              className="builder-input"
              value={current}
              placeholder="alias del perfil (p. ej. postgres_dma)"
              onChange={(event) => onChange(event.target.value)}
            />
            <small className="builder-help">
              No hay perfiles cargados. Crea uno en Conexiones o escribe el alias
              manualmente.
            </small>
          </>
        );
      }
      return (
        <select
          className="builder-input"
          value={current}
          onChange={(event) => onChange(event.target.value)}
        >
          <option value="">— Elegir conexión —</option>
          {names.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
      );
    }
    case "keyValue":
      return <KeyValueEditor value={asRecord(value)} onChange={onChange} />;
    case "stringList":
      return <StringListEditor value={asStringArray(value)} onChange={onChange} />;
    case "jsonArray":
      return <JsonArrayEditor value={value} onChange={onChange} />;
    case "jsonObject":
      return <JsonObjectEditor value={value} onChange={onChange} />;
    default:
      return null;
  }
}

function asString(value: unknown): string {
  return typeof value === "string" ? value : value === undefined || value === null ? "" : String(value);
}

function asRecord(value: unknown): Record<string, string> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, string>;
  }
  return {};
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? (value as string[]) : [];
}

function KeyValueEditor({
  value,
  onChange,
}: {
  value: Record<string, string>;
  onChange: (value: Record<string, string>) => void;
}) {
  const [entries, setEntries] = useState<Array<[string, string]>>(() => Object.entries(value));

  useEffect(() => {
    const persisted = Object.fromEntries(entries.filter(([key]) => key.trim() !== ""));
    if (JSON.stringify(persisted) !== JSON.stringify(value)) {
      setEntries(Object.entries(value));
    }
  }, [value]); // A blank draft remains local until the user gives it a key.

  const update = (next: Array<[string, string]>) => {
    setEntries(next);
    onChange(Object.fromEntries(next.filter(([key]) => key.trim() !== "")));
  };
  return (
    <div className="builder-pairs">
      {entries.map(([key, val], index) => (
        <div className="builder-pair" key={index}>
          <input
            className="builder-input"
            value={key}
            placeholder="clave"
            onChange={(event) => {
                const next = [...entries];
              next[index] = [event.target.value, val];
              update(next);
            }}
          />
          <input
            className="builder-input"
            value={val}
            placeholder="valor"
            onChange={(event) => {
                const next = [...entries];
              next[index] = [key, event.target.value];
              update(next);
            }}
          />
          <button
            type="button"
            className="builder-icon-btn"
            aria-label="Eliminar"
            onClick={() => update(entries.filter((_, i) => i !== index))}
          >
            ×
          </button>
        </div>
      ))}
      <button
        type="button"
        className="builder-add-btn"
        onClick={() => update([...entries, ["", ""]])}
      >
        + Agregar
      </button>
    </div>
  );
}

function StringListEditor({
  value,
  onChange,
}: {
  value: string[];
  onChange: (value: string[]) => void;
}) {
  return (
    <div className="builder-pairs">
      {value.map((item, index) => (
        <div className="builder-pair single" key={index}>
          <input
            className="builder-input"
            value={item}
            onChange={(event) => {
              const next = [...value];
              next[index] = event.target.value;
              onChange(next);
            }}
          />
          <button
            type="button"
            className="builder-icon-btn"
            aria-label="Eliminar"
            onClick={() => onChange(value.filter((_, i) => i !== index))}
          >
            ×
          </button>
        </div>
      ))}
      <button
        type="button"
        className="builder-add-btn"
        onClick={() => onChange([...value, ""])}
      >
        + Agregar
      </button>
    </div>
  );
}

function JsonArrayEditor({
  value,
  onChange,
}: {
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const [text, setText] = useState(() => stringifyJson(value));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setText(stringifyJson(value));
  }, [value]);

  return (
    <div className="builder-json">
      <textarea
        className={`builder-input builder-textarea code ${error ? "invalid" : ""}`}
        value={text}
        rows={6}
        spellCheck={false}
        onChange={(event) => {
          const next = event.target.value;
          setText(next);
          if (next.trim() === "") {
            setError(null);
            onChange([]);
            return;
          }
          try {
            const parsed = JSON.parse(next);
            if (!Array.isArray(parsed)) {
              setError("Debe ser un arreglo JSON []");
              return;
            }
            setError(null);
            onChange(parsed);
          } catch (parseError) {
            setError(parseError instanceof Error ? parseError.message : "JSON inválido");
          }
        }}
      />
      {error ? <small className="builder-help error">{error}</small> : null}
    </div>
  );
}

function JsonObjectEditor({
  value,
  onChange,
}: {
  value: unknown;
  onChange: (value: unknown) => void;
}) {
  const [text, setText] = useState(() => stringifyJsonObject(value));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setText(stringifyJsonObject(value));
  }, [value]);

  return (
    <div className="builder-json">
      <textarea
        className={`builder-input builder-textarea code ${error ? "invalid" : ""}`}
        value={text}
        rows={6}
        spellCheck={false}
        onChange={(event) => {
          const next = event.target.value;
          setText(next);
          if (next.trim() === "") {
            setError(null);
            onChange({});
            return;
          }
          try {
            const parsed: unknown = JSON.parse(next);
            if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
              setError("Debe ser un objeto JSON {}");
              return;
            }
            setError(null);
            onChange(parsed);
          } catch (parseError) {
            setError(parseError instanceof Error ? parseError.message : "JSON inválido");
          }
        }}
      />
      {error ? <small className="builder-help error">{error}</small> : null}
    </div>
  );
}

function stringifyJson(value: unknown): string {
  if (value === undefined || value === null) return "[]";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "[]";
  }
}

function stringifyJsonObject(value: unknown): string {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "{}";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "{}";
  }
}

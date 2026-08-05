export type ProcessorCategory = "source" | "transform" | "ai_prep" | "sink";

/** Mirrors `Relationship` in model.ts (evitar import circular catalog ↔ model). */
export type OutgoingRelationship =
  | "success"
  | "failure"
  | "train"
  | "validation"
  | "test";

export type FieldKind =
  | "text"
  | "textarea"
  | "number"
  | "boolean"
  | "select"
  | "connectionRef"
  | "keyValue"
  | "stringList"
  | "jsonArray"
  | "jsonObject";

export interface FieldDef {
  key: string;
  label: string;
  kind: FieldKind;
  required?: boolean;
  placeholder?: string;
  help?: string;
  options?: string[];
  /** Filtro de perfiles ofrecidos en el selector de conexión. */
  connectionKind?:
    | "database"
    | "postgres"
    | "oracle"
    | "mongodb"
    | "mysql"
    | "sqlserver"
    | "kafka";
}

export interface ProcessorDef {
  type: string;
  label: string;
  category: ProcessorCategory;
  description: string;
  fields: FieldDef[];
  defaultConfig: Record<string, unknown>;
  /**
   * Handles de salida en el lienzo. Por defecto `success` + `failure`.
   * `ai_split_dataset` usa `train` / `validation` / `test`.
   */
  outgoingRelationships?: readonly OutgoingRelationship[];
}

/**
 * Catalog of built-in processors. Each field mirrors the exact configuration
 * accepted by the Rust engine so the generated YAML deserializes without edits.
 */
export const PROCESSOR_CATALOG: ProcessorDef[] = [
  {
    type: "generate_records",
    label: "Generar registros",
    category: "source",
    description: "Emite registros fijos, útil para pruebas.",
    fields: [
      {
        key: "records",
        label: "Registros (arreglo JSON)",
        kind: "jsonArray",
        help: "Lista de objetos JSON que se emitirán como un paquete.",
      },
    ],
    defaultConfig: { records: [] },
  },
  {
    type: "query_postgres",
    label: "Leer PostgreSQL",
    category: "source",
    description: "Lee PostgreSQL por lotes con un pool compartido.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "postgres",
        required: true,
        help: "Nombre de una conexión definida en 'Conexiones'.",
      },
      {
        key: "query",
        label: "Consulta SQL",
        kind: "textarea",
        required: true,
        placeholder: "SELECT to_jsonb(t) FROM (SELECT * FROM public.tabla) AS t",
      },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
    ],
    defaultConfig: { connection: "", query: "", batch_size: 1000 },
  },
  {
    type: "query_mysql",
    label: "Leer MySQL",
    category: "source",
    description: "Lee MySQL/MariaDB por lotes y emite cada fila como objeto JSON.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "mysql",
        required: true,
        help: "Alias de un perfil MySQL o MariaDB en Conexiones.",
      },
      {
        key: "query",
        label: "Consulta SQL",
        kind: "textarea",
        required: true,
        placeholder: "SELECT id, name FROM schema.tabla WHERE active = ?",
        help: "Usa placeholders `?`. Puedes crearla desde Conexiones → Constructor SQL.",
      },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
    ],
    defaultConfig: { connection: "", query: "", batch_size: 1000 },
  },
  {
    type: "query_oracle",
    label: "Leer Oracle",
    category: "source",
    description: "Ejecuta una consulta SELECT de Oracle y emite objetos por lotes.",
    fields: [
      {
        key: "connection",
        label: "Conexión Oracle",
        kind: "connectionRef",
        connectionKind: "oracle",
        required: true,
        help: "Alias de un perfil Oracle en Conexiones (o nombre en Configuración del flujo).",
      },
      {
        key: "query",
        label: "Consulta SQL",
        kind: "textarea",
        required: true,
        placeholder: "SELECT ID, NAME FROM SCHEMA.TABLE",
        help: "Solo se permiten consultas SELECT o WITH. Puedes crearla desde Conexiones → Constructor SQL.",
      },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
    ],
    defaultConfig: { connection: "", query: "", batch_size: 1000 },
  },
  {
    type: "query_mongodb",
    label: "Leer MongoDB",
    category: "source",
    description: "Lee documentos MongoDB por lotes conservando tipos BSON.",
    fields: [
      {
        key: "connection",
        label: "Conexión MongoDB",
        kind: "connectionRef",
        connectionKind: "mongodb",
        required: true,
        help: "Alias de un perfil MongoDB en Conexiones.",
      },
      {
        key: "collection",
        label: "Colección",
        kind: "text",
        required: true,
        placeholder: "customers",
      },
      {
        key: "filter",
        label: "Filtro JSON",
        kind: "jsonObject",
        help: 'Ejemplo: { "active": true, "age": { "$gte": 18 } }',
      },
      {
        key: "projection",
        label: "Proyección JSON",
        kind: "jsonObject",
        help: 'Ejemplo: { "name": 1, "email": 1 }',
      },
      {
        key: "sort",
        label: "Orden JSON",
        kind: "jsonObject",
        help: 'Ejemplo: { "created_at": -1 }',
      },
      { key: "skip", label: "Omitir documentos", kind: "number" },
      { key: "limit", label: "Límite (opcional)", kind: "number" },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
    ],
    defaultConfig: {
      connection: "",
      collection: "",
      filter: {},
      skip: 0,
      batch_size: 1000,
    },
  },
  {
    type: "put_database",
    label: "Escribir base de datos",
    category: "sink",
    description: "Escritura transaccional insert/upsert multi-base.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "database",
        required: true,
        help: "Alias de un perfil en Conexiones (Postgres, MySQL, Oracle, SQL Server…).",
      },
      { key: "table", label: "Tabla", kind: "text", required: true, placeholder: "public.customers" },
      { key: "mode", label: "Modo", kind: "select", options: ["insert", "upsert"], required: true },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
      {
        key: "columns",
        label: "Columnas (origen → destino)",
        kind: "keyValue",
        required: true,
        help: "Pulsa «+ Agregar» y mapea campo del registro → columna destino (ej. id → customer_id).",
      },
      {
        key: "conflict_columns",
        label: "Columnas de conflicto (upsert)",
        kind: "stringList",
        help: "Requerido cuando el modo es upsert.",
      },
    ],
    defaultConfig: {
      connection: "",
      table: "",
      mode: "insert",
      batch_size: 1000,
      columns: {},
      conflict_columns: [],
    },
  },
  {
    type: "auto_destination",
    label: "Destino automático",
    category: "sink",
    description: "Detecta el motor y selecciona una estrategia de carga compatible.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "database",
        required: true,
        help: "Alias de un perfil en Conexiones.",
      },
      { key: "table", label: "Tabla", kind: "text", required: true, placeholder: "public.customers" },
      {
        key: "mode",
        label: "Modo",
        kind: "select",
        options: ["auto", "insert", "upsert"],
        required: true,
        help: "Auto usa upsert cuando hay columnas de conflicto; en otro caso usa insert.",
      },
      { key: "batch_size", label: "Tamaño de lote solicitado", kind: "number" },
      {
        key: "columns",
        label: "Columnas (origen → destino)",
        kind: "keyValue",
        required: true,
        help: "Pulsa «+ Agregar» y mapea campo del registro → columna destino.",
      },
      {
        key: "conflict_columns",
        label: "Columnas de conflicto",
        kind: "stringList",
        help: "Activa upsert en modo auto.",
      },
    ],
    defaultConfig: {
      connection: "",
      table: "",
      mode: "auto",
      batch_size: 1000,
      columns: {},
      conflict_columns: [],
    },
  },
  {
    type: "put_mongodb",
    label: "Escribir MongoDB",
    category: "sink",
    description: "Inserta documentos o realiza upsert por campos clave.",
    fields: [
      {
        key: "connection",
        label: "Conexión MongoDB",
        kind: "connectionRef",
        connectionKind: "mongodb",
        required: true,
        help: "Alias de un perfil MongoDB en Conexiones.",
      },
      {
        key: "collection",
        label: "Colección",
        kind: "text",
        required: true,
        placeholder: "customers_loaded",
      },
      {
        key: "mode",
        label: "Modo",
        kind: "select",
        options: ["insert", "upsert"],
        required: true,
      },
      {
        key: "key_fields",
        label: "Campos clave",
        kind: "stringList",
        help: "Obligatorios para upsert; admite rutas como customer.id.",
      },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
      {
        key: "ordered",
        label: "Escritura ordenada",
        kind: "boolean",
        help: "Detiene el lote insert al encontrar el primer error.",
      },
    ],
    defaultConfig: {
      connection: "",
      collection: "",
      mode: "insert",
      key_fields: ["_id"],
      batch_size: 1000,
      ordered: true,
    },
  },
  {
    type: "publish_kafka",
    label: "Publicar en Kafka",
    category: "sink",
    description: "Publica mensajes con confirmación e idempotencia.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "kafka",
        required: true,
        help: "Nombre de una conexión Kafka definida en Configuración del flujo.",
      },
      { key: "topic", label: "Topic", kind: "text", required: true, placeholder: "events.customers" },
      { key: "key_field", label: "Campo de clave (registros)", kind: "text" },
      { key: "key_attribute", label: "Atributo de clave (codificado)", kind: "text" },
      { key: "queue_timeout_ms", label: "Timeout de cola (ms)", kind: "number" },
    ],
    defaultConfig: { connection: "", topic: "", queue_timeout_ms: 5000 },
  },
  {
    type: "consume_kafka",
    label: "Consumir Kafka",
    category: "source",
    description:
      "Lee mensajes con auto-commit desactivado y confirma el offset tras emitir el paquete.",
    fields: [
      {
        key: "connection",
        label: "Conexión",
        kind: "connectionRef",
        connectionKind: "kafka",
        required: true,
        help: "Nombre de una conexión Kafka definida en Configuración del flujo.",
      },
      { key: "topic", label: "Topic", kind: "text", required: true, placeholder: "events.customers" },
      {
        key: "group_id",
        label: "Grupo de consumidores",
        kind: "text",
        required: true,
        placeholder: "jaiva-readers",
      },
      {
        key: "auto_offset_reset",
        label: "Offset inicial",
        kind: "select",
        options: ["earliest", "latest"],
        help: "Solo aplica cuando el grupo aún no tiene offsets comprometidos.",
      },
      { key: "max_poll_messages", label: "Máx. mensajes por ciclo", kind: "number" },
      { key: "max_poll_ms", label: "Timeout de poll (ms)", kind: "number" },
      { key: "max_idle_ms", label: "Idle máximo (ms)", kind: "number" },
      {
        key: "decode",
        label: "Decodificación",
        kind: "select",
        options: ["json", "bytes"],
      },
    ],
    defaultConfig: {
      connection: "",
      topic: "",
      group_id: "",
      auto_offset_reset: "earliest",
      max_poll_messages: 100,
      max_poll_ms: 1000,
      max_idle_ms: 2000,
      decode: "json",
    },
  },
  {
    type: "rename_fields",
    label: "Renombrar campos",
    category: "transform",
    description: "Renombra campos de objetos.",
    fields: [
      {
        key: "fields",
        label: "Campos (actual → nuevo)",
        kind: "keyValue",
        required: true,
      },
    ],
    defaultConfig: { fields: {} },
  },
  {
    type: "ai_select_fields",
    label: "AI: seleccionar campos",
    category: "ai_prep",
    description: "Keep/drop de columnas sobre registros JSON.",
    fields: [
      { key: "keep", label: "Keep", kind: "stringList" },
      { key: "drop", label: "Drop", kind: "stringList" },
    ],
    defaultConfig: { keep: [], drop: [] },
  },
  {
    type: "ai_drop_nulls",
    label: "AI: drop nulls",
    category: "ai_prep",
    description: "Elimina filas con null/vacío en los campos dados.",
    fields: [
      { key: "fields", label: "Campos", kind: "stringList", required: true },
    ],
    defaultConfig: { fields: [] },
  },
  {
    type: "ai_fill_missing",
    label: "AI: fill missing",
    category: "ai_prep",
    description: "Rellena nulos: previous, constant, mean o median.",
    fields: [
      { key: "fields", label: "Campos", kind: "stringList", required: true },
      {
        key: "strategy",
        label: "Estrategia",
        kind: "select",
        options: ["previous", "constant", "mean", "median"],
      },
      { key: "constant", label: "Constante", kind: "text" },
      { key: "cumulative", label: "Stats acumulados", kind: "boolean" },
    ],
    defaultConfig: { fields: [], strategy: "previous", cumulative: false },
  },
  {
    type: "ai_remove_duplicates",
    label: "AI: dedupe",
    category: "ai_prep",
    description: "Elimina duplicados por clave(s); window opcional.",
    fields: [
      { key: "key_fields", label: "Claves", kind: "stringList", required: true },
      { key: "window", label: "Ventana", kind: "number", placeholder: "opcional" },
    ],
    defaultConfig: { key_fields: [] },
  },
  {
    type: "ai_filter_range",
    label: "AI: filter range",
    category: "ai_prep",
    description: "Filtra outliers por min/max o IQR.",
    fields: [
      { key: "field", label: "Campo", kind: "text", required: true },
      {
        key: "mode",
        label: "Modo",
        kind: "select",
        options: ["min_max", "iqr"],
      },
      { key: "min", label: "Mín", kind: "number" },
      { key: "max", label: "Máx", kind: "number" },
      { key: "iqr_multiplier", label: "IQR k", kind: "number" },
    ],
    defaultConfig: { field: "", mode: "min_max", iqr_multiplier: 1.5 },
  },
  {
    type: "ai_cast_types",
    label: "AI: cast types",
    category: "ai_prep",
    description: "Cast a number/string/bool/timestamp.",
    fields: [
      {
        key: "fields",
        label: "Campo → tipo",
        kind: "keyValue",
        required: true,
        help: "Valores: number, string, bool, timestamp",
      },
      {
        key: "on_error",
        label: "On error",
        kind: "select",
        options: ["drop", "fail"],
      },
    ],
    defaultConfig: { fields: {}, on_error: "drop" },
  },
  {
    type: "ai_normalize",
    label: "AI: normalize",
    category: "ai_prep",
    description: "min-max o z-score; cumulative opcional entre paquetes.",
    fields: [
      { key: "fields", label: "Campos", kind: "stringList", required: true },
      {
        key: "method",
        label: "Método",
        kind: "select",
        options: ["min_max", "z_score"],
      },
      { key: "cumulative", label: "Stats acumulados", kind: "boolean" },
    ],
    defaultConfig: { fields: [], method: "min_max", cumulative: false },
  },
  {
    type: "ai_encode_categories",
    label: "AI: encode categories",
    category: "ai_prep",
    description: "Label encoding con mapa fijo por campo.",
    fields: [
      {
        key: "fields",
        label: "Mapas (JSON)",
        kind: "jsonObject",
        required: true,
        help: '{"status":{"OK":0,"WARN":1}}',
      },
      {
        key: "on_error",
        label: "On error",
        kind: "select",
        options: ["drop", "fail"],
      },
    ],
    defaultConfig: { fields: {}, on_error: "drop" },
  },
  {
    type: "ai_compute_fields",
    label: "AI: compute fields",
    category: "ai_prep",
    description: "Features aritméticas simples (a + b * 2).",
    fields: [
      {
        key: "fields",
        label: "Campo → expresión",
        kind: "keyValue",
        required: true,
      },
      {
        key: "on_error",
        label: "On error",
        kind: "select",
        options: ["drop", "fail"],
      },
    ],
    defaultConfig: { fields: {}, on_error: "drop" },
  },
  {
    type: "ai_split_dataset",
    label: "AI: split dataset",
    category: "ai_prep",
    description:
      "Emite por las relaciones train / validation / test (no usa success). Cablea cada handle a encode/write.",
    fields: [
      { key: "train", label: "Train", kind: "number" },
      { key: "validation", label: "Validation", kind: "number" },
      { key: "test", label: "Test", kind: "number" },
    ],
    defaultConfig: { train: 0.7, validation: 0.2, test: 0.1 },
    outgoingRelationships: ["train", "validation", "test"],
  },
  {
    type: "ai_lookup_join",
    label: "AI: lookup join",
    category: "ai_prep",
    description: "Enriquece por clave con lookup en memoria.",
    fields: [
      { key: "key", label: "Clave", kind: "text", required: true },
      {
        key: "lookup_records",
        label: "Lookup records",
        kind: "jsonArray",
      },
      {
        key: "lookup_path",
        label: "Lookup path (JSON)",
        kind: "text",
        placeholder: "data/lookup.json",
      },
      { key: "copy_fields", label: "Campos a copiar", kind: "stringList" },
    ],
    defaultConfig: { key: "", lookup_records: [], copy_fields: [] },
  },
  {
    type: "ai_export_manifest",
    label: "AI: export manifest",
    category: "ai_prep",
    description: "Escribe manifest.json para hand-off ML.",
    fields: [
      {
        key: "path",
        label: "Ruta",
        kind: "text",
        required: true,
        placeholder: "output/ai-prep/manifest.json",
      },
      { key: "dataset_name", label: "Dataset", kind: "text" },
      {
        key: "collect_splits",
        label: "Recolectar train / validation / test",
        kind: "boolean",
      },
      { key: "train_path", label: "CSV train", kind: "text" },
      { key: "validation_path", label: "CSV validation", kind: "text" },
      { key: "test_path", label: "CSV test", kind: "text" },
    ],
    defaultConfig: {
      path: "output/ai-prep/manifest.json",
      dataset_name: "dataset",
      collect_splits: false,
      train_path: "",
      validation_path: "",
      test_path: "",
    },
  },
  {
    type: "ai_trigger_webhook",
    label: "AI: trigger webhook",
    category: "ai_prep",
    description: "POST/PUT HTTP al job ML externo (sin train in-process).",
    fields: [
      { key: "url", label: "URL", kind: "text", required: true },
      {
        key: "method",
        label: "Método",
        kind: "select",
        options: ["POST", "PUT", "GET"],
      },
      { key: "include_records", label: "Incluir records", kind: "boolean" },
      { key: "timeout_ms", label: "Timeout ms", kind: "number" },
    ],
    defaultConfig: {
      url: "",
      method: "POST",
      include_records: false,
      timeout_ms: 10000,
    },
  },
  {
    type: "encode_json",
    label: "Codificar JSON",
    category: "transform",
    description: "Convierte registros a JSON.",
    fields: [{ key: "pretty", label: "Formato legible", kind: "boolean" }],
    defaultConfig: { pretty: false },
  },
  {
    type: "encode_yaml",
    label: "Codificar YAML",
    category: "transform",
    description: "Convierte registros a YAML.",
    fields: [],
    defaultConfig: {},
  },
  {
    type: "encode_csv",
    label: "Codificar CSV",
    category: "transform",
    description: "Convierte objetos planos a CSV.",
    fields: [
      { key: "headers", label: "Incluir encabezados", kind: "boolean" },
      { key: "delimiter", label: "Delimitador", kind: "text", placeholder: "," },
    ],
    defaultConfig: { headers: true, delimiter: "," },
  },
  {
    type: "encode_xml",
    label: "Codificar XML",
    category: "transform",
    description: "Convierte objetos planos a XML.",
    fields: [
      { key: "root", label: "Elemento raíz", kind: "text", placeholder: "records" },
      { key: "item", label: "Elemento por registro", kind: "text", placeholder: "record" },
    ],
    defaultConfig: { root: "records", item: "record" },
  },
  {
    type: "write_file",
    label: "Escribir archivo",
    category: "sink",
    description: "Guarda contenido codificado en disco.",
    fields: [
      { key: "path", label: "Ruta", kind: "text", required: true, placeholder: "output/salida.csv" },
    ],
    defaultConfig: { path: "" },
  },
  {
    type: "load_checkpoint",
    label: "Cargar checkpoint",
    category: "source",
    description: "Carga un valor persistente como atributo.",
    fields: [
      { key: "key", label: "Clave", kind: "text", required: true, placeholder: "customers.updated_at" },
      { key: "attribute", label: "Atributo", kind: "text", placeholder: "checkpoint.value" },
      { key: "default", label: "Valor por defecto", kind: "text" },
    ],
    defaultConfig: { key: "", attribute: "checkpoint.value" },
  },
  {
    type: "save_checkpoint",
    label: "Guardar checkpoint",
    category: "sink",
    description: "Guarda un atributo como checkpoint.",
    fields: [
      { key: "key", label: "Clave", kind: "text", required: true, placeholder: "customers.updated_at" },
      { key: "attribute", label: "Atributo", kind: "text", placeholder: "checkpoint.value" },
    ],
    defaultConfig: { key: "", attribute: "checkpoint.value" },
  },
  {
    type: "log_records",
    label: "Registrar (log)",
    category: "sink",
    description: "Muestra registros o contenido codificado.",
    fields: [],
    defaultConfig: {},
  },
];

export const CATALOG_BY_TYPE: Record<string, ProcessorDef> = Object.fromEntries(
  PROCESSOR_CATALOG.map((def) => [def.type, def]),
);

export const CATEGORY_LABEL: Record<ProcessorCategory, string> = {
  source: "Fuentes",
  transform: "Transformaciones",
  ai_prep: "AI Prep",
  sink: "Destinos",
};

export const CATEGORY_TAG: Record<ProcessorCategory, string> = {
  source: "Origen",
  transform: "Proceso",
  ai_prep: "AI Prep",
  sink: "Destino",
};

export interface UpcomingComponent {
  label: string;
  note: string;
}

/**
 * Components present in the engine roadmap but not yet implemented. They are
 * shown disabled so the intended architecture is visible without producing
 * YAML the engine would reject.
 */
export const UPCOMING_COMPONENTS: UpcomingComponent[] = [
  { label: "OPC-UA", note: "Conector industrial en el roadmap del motor." },
  { label: "MQTT", note: "Mensajería IoT en el roadmap del motor." },
  { label: "REST", note: "Origen/destino HTTP en el roadmap del motor." },
];

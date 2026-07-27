export type ProcessorCategory = "source" | "transform" | "sink";

export type FieldKind =
  | "text"
  | "textarea"
  | "number"
  | "boolean"
  | "select"
  | "connectionRef"
  | "keyValue"
  | "stringList"
  | "jsonArray";

export interface FieldDef {
  key: string;
  label: string;
  kind: FieldKind;
  required?: boolean;
  placeholder?: string;
  help?: string;
  options?: string[];
  connectionKind?: "database" | "postgres" | "kafka";
}

export interface ProcessorDef {
  type: string;
  label: string;
  category: ProcessorCategory;
  description: string;
  fields: FieldDef[];
  defaultConfig: Record<string, unknown>;
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
      },
      { key: "table", label: "Tabla", kind: "text", required: true, placeholder: "public.customers" },
      { key: "mode", label: "Modo", kind: "select", options: ["insert", "upsert"], required: true },
      { key: "batch_size", label: "Tamaño de lote", kind: "number" },
      {
        key: "columns",
        label: "Columnas (origen → destino)",
        kind: "keyValue",
        required: true,
        help: "Mapa de campo del registro al nombre de columna.",
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
      },
      { key: "topic", label: "Topic", kind: "text", required: true },
      { key: "key_field", label: "Campo de clave (registros)", kind: "text" },
      { key: "key_attribute", label: "Atributo de clave (codificado)", kind: "text" },
      { key: "queue_timeout_ms", label: "Timeout de cola (ms)", kind: "number" },
    ],
    defaultConfig: { connection: "", topic: "", queue_timeout_ms: 5000 },
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
  sink: "Destinos",
};

export const CATEGORY_TAG: Record<ProcessorCategory, string> = {
  source: "Origen",
  transform: "Proceso",
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

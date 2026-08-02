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

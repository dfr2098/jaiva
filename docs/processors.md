# Procesadores incluidos

## `generate_records`

Genera registros de prueba desde YAML.

```yaml
type: generate_records
config:
  records:
    - id: 1
      name: Ada
```

## `query_postgres`

Ejecuta una consulta streaming y emite paquetes según `batch_size`. La consulta
debe devolver una columna JSON (habitualmente `to_jsonb(...)`).

```yaml
type: query_postgres
config:
  connection: main
  batch_size: 1000
  query: |
    SELECT to_jsonb(row_data)
    FROM (SELECT * FROM public.customers) row_data
```

## `query_mysql`

Lee MySQL/MariaDB por lotes. Cada fila se convierte a un objeto JSON en el
runtime (no hace falta envolver la SELECT). Placeholders `?`. Compatible con el
constructor visual (`processor_type: query_mysql`).

```yaml
type: query_mysql
config:
  connection: main
  batch_size: 1000
  query: SELECT id, name FROM shop.customers WHERE active = ?
  parameters: [true]
```

Ver [`examples/mysql-query.yaml`](../examples/mysql-query.yaml).

## `query_sqlserver`

Requiere `--features sqlserver-driver`. Lee SQL Server (Tiberius) por lotes y
emite objetos JSON. Solo `SELECT`/`WITH`. Placeholders `@P1`, `@P2`, …. El
compilador usa `TOP (n)` en lugar de `LIMIT`.

```yaml
type: query_sqlserver
config:
  connection: main
  batch_size: 1000
  query: SELECT TOP (100) [id], [name] FROM [dbo].[customers] WHERE [active] = @P1
  parameters: [true]
```

Ver [`examples/sqlserver-query.yaml`](../examples/sqlserver-query.yaml).

## `query_mongodb`

Requiere `--features mongodb-driver`. Lee una colección mediante cursor y emite
documentos en paquetes acotados por `batch_size`. `filter`, `projection` y
`sort` son documentos MongoDB expresados como JSON o Extended JSON.

La conexión puede ser un alias del Connection Manager (perfil con host/puerto o
con URI `mongodb://` / `mongodb+srv://`) o una entrada `database_connections`
con `url_env`. Ver [connection-manager.md](connection-manager.md).

```yaml
type: query_mongodb
config:
  connection: mongo
  collection: customers
  filter:
    active: true
    age:
      $gte: 18
  projection:
    name: 1
    email: 1
  sort:
    created_at: -1
  skip: 0
  limit: 10000
  batch_size: 500
```

Los valores BSON que no existen de forma nativa en JSON conservan Extended JSON;
por ejemplo, un `ObjectId` se transporta como `{"$oid":"..."}`.

## `rename_fields`

Renombra propiedades de registros JSON.

```yaml
type: rename_fields
config:
  fields:
    customer_id: id
    customer_name: name
```

## AI Data Prep

Toolkit tabular 100% Rust (limpieza, features, split, hand-off). Ver
[ai-data-prep.md](ai-data-prep.md) y el ejemplo
[`examples/ai-prep-conveyor.yaml`](../examples/ai-prep-conveyor.yaml).

Tipos: `ai_select_fields`, `ai_drop_nulls`, `ai_fill_missing`,
`ai_remove_duplicates`, `ai_filter_range`, `ai_cast_types`, `ai_normalize`,
`ai_encode_categories`, `ai_compute_fields`, `ai_split_dataset`,
`ai_lookup_join`, `ai_export_manifest`, `ai_trigger_webhook`.

## Codificadores

Tipos:

- `encode_json`
- `encode_yaml`
- `encode_csv`
- `encode_xml`

Ejemplo:

```yaml
type: encode_csv
config:
  headers: true
  delimiter: ","
```

CSV y XML requieren objetos. Los valores anidados se representan actualmente
como JSON textual.

## `write_file`

Escribe contenido previamente codificado.

```yaml
type: write_file
config:
  path: output/data.csv
```

## `put_database`

Escribe registros mediante el `DatabaseWriter` asociado con la conexión. La
implementación actual incluye PostgreSQL, MySQL/MariaDB, Oracle y SQL Server.
Los dos últimos requieren `oracle-driver` y `sqlserver-driver`, respectivamente.

```yaml
type: put_database
config:
  connection: destination
  table: public.customers
  mode: upsert
  batch_size: 1000
  columns:
    id: customer_id
    name: customer_name
  conflict_columns:
    - customer_id
```

Todo el paquete se escribe dentro de una transacción. Si un sublote falla, la
transacción completa se revierte y el paquete sigue la política de reintentos y
la ruta `failure`.

## `put_mongodb`

Carga objetos JSON o Extended JSON en una colección MongoDB:

```yaml
type: put_mongodb
config:
  connection: mongo
  collection: customers_loaded
  mode: upsert
  key_fields:
    - _id
  batch_size: 500
  ordered: true
```

- `insert` usa inserciones múltiples por lote.
- `upsert` busca por `key_fields` y reemplaza el documento completo; admite
  rutas punteadas como `customer.id`.
- `_id` es el campo clave predeterminado.

En un MongoDB independiente las escrituras de un paquete no son una transacción
global. `insert` puede confirmar documentos antes de encontrar un error; para
flujos reintentables se recomienda `upsert` con claves estables. Las
transacciones multi-documento requieren un replica set y quedan fuera de esta
fase.

## `auto_destination`

Detecta el motor asociado con `connection` y genera un plan de carga usando las
capacidades declaradas por su writer. Admite actualmente PostgreSQL,
MySQL/MariaDB, Oracle y SQL Server.

```yaml
type: auto_destination
config:
  connection: destination
  table: public.customers
  mode: auto
  batch_size: 1000
  columns:
    id: customer_id
    name: customer_name
  conflict_columns:
    - customer_id
```

En modo `auto`, la presencia de `conflict_columns` selecciona `upsert`; sin
ellas se selecciona `insert`. Antes de escribir, el motor calcula una estrategia
como `multi_row_insert`, `native_upsert` o `transactional_upsert`, limita el lote
según las capacidades del driver y registra el plan en los atributos
`write.*` del paquete.

## `query_oracle`

Ejecuta una consulta de solo lectura en Oracle y emite cada fila como un objeto
JSON. Los nombres de columnas se normalizan a minúsculas para facilitar el
mapeo al destino. Requiere compilar Jaiba con `oracle-driver`.

```yaml
type: query_oracle
config:
  connection: Oracle
  query: SELECT ID, NAME FROM DMA_TEST.CUSTOMERS
  batch_size: 1000
```

Solo acepta sentencias cuyo primer término sea `SELECT` o `WITH`.

## `publish_kafka`

Publica registros JSON o contenido binario y espera confirmación del broker:

```yaml
type: publish_kafka
config:
  connection: bus
  topic: events.batch.v1
  key_field: batch_id
  queue_timeout_ms: 5000
```

Requiere `--features kafka-driver`. Consulta
`docs/history/priority-4-3-kafka.md` para garantías y observabilidad.

## `consume_kafka`

Fuente Kafka: lee un lote de mensajes con auto-commit desactivado y confirma el
offset tras emitir cada paquete por `success` (at-least-once en el MVP).

```yaml
type: consume_kafka
config:
  connection: bus
  topic: events.batch.v1
  group_id: jaiva-readers
  auto_offset_reset: earliest
  max_poll_messages: 50
  max_poll_ms: 1000
  max_idle_ms: 8000
  decode: json
```

Requiere `--features kafka-driver`. Detalle en `docs/history/priority-4-3-kafka.md`.

## Checkpoints

`load_checkpoint` carga un valor en los atributos del paquete:

```yaml
type: load_checkpoint
config:
  key: customers.updated_at
  attribute: checkpoint.value
  default: "1970-01-01T00:00:00Z"
```

`save_checkpoint` lo persiste:

```yaml
type: save_checkpoint
config:
  key: customers.updated_at
  attribute: checkpoint.value
```

El guardado debe colocarse después del commit del destino.

## Estado de dominio JME

Estos procesadores requieren `engine.domain_memory.enabled: true` y una
`policy_file` válida. La clase debe existir en esa política.

```yaml
type: memory_upsert
config:
  class: carrier
  id_attribute: carrier.id
  # value_attribute: carrier.json  # opcional; si falta usa records[0]
```

```yaml
type: memory_get
config:
  class: carrier
  id_attribute: carrier.id
  attribute: memory.value
  miss_as_null: false
```

```yaml
type: memory_remove
config:
  class: carrier
  id_attribute: carrier.id
```

`memory_get` consulta Hot → Warm → Cold → Frozen → rebuild y promueve a Hot un
objeto encontrado fuera de RAM. `memory_remove` escribe también el tombstone de
Cold para que una eliminación sobreviva al reinicio. Detalle en
[jme-cold-memory.md](jme-cold-memory.md).

## `log_records`

Registra contenido estructurado o codificado mediante `tracing`. Es útil para
desarrollo y rutas de error; no debe utilizarse para volúmenes grandes en
producción.

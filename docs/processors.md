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
debe devolver una columna JSON.

```yaml
type: query_postgres
config:
  connection: main
  batch_size: 1000
  query: |
    SELECT to_jsonb(row_data)
    FROM (SELECT * FROM public.customers) row_data
```

## `rename_fields`

Renombra propiedades de registros JSON.

```yaml
type: rename_fields
config:
  fields:
    customer_id: id
    customer_name: name
```

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

## `publish_kafka`

Publica registros JSON o contenido binario y espera confirmación del broker:

```yaml
type: publish_kafka
config:
  connection: dma
  topic: dma.journal.batch.v1
  key_field: batch_id
  queue_timeout_ms: 5000
```

Requiere `--features kafka-driver`. Consulta
`docs/priority-4-3-kafka.md` para garantías y observabilidad.

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

## `log_records`

Registra contenido estructurado o codificado mediante `tracing`. Es útil para
desarrollo y rutas de error; no debe utilizarse para volúmenes grandes en
producción.

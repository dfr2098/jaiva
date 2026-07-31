# Jaiba

Plataforma open source en Rust para diseñar, ejecutar, monitorear y simular
flujos de procesamiento de datos. Jaiba no intenta copiar Apache NiFi: utiliza
un núcleo DAG, un runtime desacoplado, plugins y una UI sin lógica de negocio.

## Documentación

- [Arquitectura](docs/architecture.md)
- [Visión y límites del proyecto](docs/project-vision.md)
- [Ruta de modularización](docs/modular-roadmap.md)
- [Bitácora técnica y memoria de implementación](docs/implementation-notes.md)
- [Configuración](docs/configuration.md)
- [Procesadores](docs/processors.md)
- [Operación y observabilidad](docs/operations.md)
- [Diseño de escritura multi-base](docs/priority-4-database-writes.md)
- [Fase 7: control y endurecimiento operativo](docs/priority-7-control-plane.md)
- [Fase 7.8: ejecución paralela por procesador](docs/priority-7-8-parallel-workers.md)

## Diagrama de flujo

```mermaid
flowchart LR
    USER["Usuario"]

    subgraph UI["jaiba-ui"]
        CMUI["Conexiones y<br/>constructor SQL"]
        DESIGNER["Diseñador DAG"]
        OPS["Operación y<br/>trazabilidad"]
    end

    subgraph SERVER["jaiba-server"]
        API["REST + WebSocket"]
        CM["Connection Manager"]
        SQL["Compilador SQL seguro"]
        VERSIONS[("Registro versionado<br/>DRAFT · VALIDATED<br/>DEPLOYED · ARCHIVED")]
    end

    subgraph RUNTIME["jaiba-runtime"]
        ENGINE["FlowEngine"]
        QUEUE[["Colas limitadas<br/>Backpressure"]]
        PROCESSORS["Procesadores"]
        REPOSITORY[("Packet + Content<br/>SQLite WAL · SHA-256")]
        OBS["Métricas · Provenance<br/>Dead-letter"]
    end

    SYSTEMS[("PostgreSQL · MySQL · Oracle<br/>SQL Server · Kafka · archivos")]

    USER --> CMUI
    USER --> DESIGNER
    USER --> OPS
    CMUI -->|QuerySpec| API
    API --> CM
    CM --> SQL
    SQL -->|SQL + parámetros| CMUI
    CMUI -->|nodo query_postgres| DESIGNER
    DESIGNER -->|YAML| API
    API --> VERSIONS
    VERSIONS -->|desplegar / rollback| ENGINE
    ENGINE --> QUEUE --> PROCESSORS
    PROCESSORS <--> SYSTEMS
    QUEUE <--> REPOSITORY
    ENGINE --> OBS
    OBS -->|WebSocket / REST / Prometheus| OPS
```

La UI describe operaciones; no carga drivers ni recibe contraseñas. El servidor
compila consultas y versiona los YAML. El runtime es el único componente que
ejecuta el DAG. Cuando una cola alcanza su límite, la fuente espera para evitar
consumir memoria sin control.

## Capacidades actuales

- Grafo de procesadores definido en YAML.
- Ejecución concurrente global y límite de tareas por procesador.
- Colas limitadas y backpressure global y por conexión.
- Streaming real: los procesadores emiten paquetes mientras continúan trabajando.
- Relaciones `success` y `failure`.
- Reintentos exponenciales y timeout por procesador.
- Métricas finales de paquetes procesados, fallidos, reintentados y emitidos.
- Parámetros `${nombre}` y secretos `${env:NOMBRE_VARIABLE}`.
- Checkpoints persistentes mediante archivo JSON con escritura atómica.
- Registro extensible de procesadores sin modificar el motor.
- Pools PostgreSQL compartidos.
- Lectura PostgreSQL incremental por lotes.
- Contenido de paquete como registros o bytes codificados.
- Esquema lógico para decimales, fechas, timestamps, UUID y binarios.
- Codificación JSON, YAML, CSV y XML.
- Ciclo de vida `STARTING`, `RUNNING`, `PAUSED`, `DRAINING`, `STOPPED` y
  `FAILED`.
- Apagado coordinado y recuperación de trabajo persistido.
- Circuit breaker independiente por conexión de base de datos y Kafka.
- API administrativa autenticada para control, provenance y dead-letter.
- Límite global estricto, workers CPU/bloqueantes y orden por partición.
- Interfaz opcional `visualisa_jaiva` en un contenedor separado.

## Ejecutar

```bash
cargo run -- examples/basic-flow.yaml
```

Para leer PostgreSQL:

```bash
export DATABASE_URL='postgres://usuario:contraseña@localhost:5432/base'
cargo run -- examples/postgres-read.yaml
```

Para exportar PostgreSQL a CSV:

```bash
cargo run -- examples/postgres-to-csv.yaml
```

El resultado se escribe en `output/aisles.csv`.

Para probar escritura MySQL/MariaDB:

```bash
export MYSQL_DATABASE_URL='mysql://usuario:contraseña@localhost:3306/base'
cargo run -- examples/mysql-write.yaml
```

Para ejecutar la consola visual opcional de la fase 8:

```bash
cargo run -- serve examples/visualisa-flow.yaml
cd visualisa_jaiva
docker compose up -d --build
```

La interfaz se abre en `http://127.0.0.1:9080`. Detener su contenedor no detiene
Jaiva ni sus flujos. El alcance del monitor, diseñador, publicación y
trazabilidad está documentado en
[`docs/priority-8-visual-console.md`](docs/priority-8-visual-console.md).

## Observabilidad, Grafana y WebSocket

Inicia el servidor de observabilidad y, opcionalmente, un flujo:

```bash
cargo run -- serve examples/basic-flow.yaml
```

La dirección predeterminada es `127.0.0.1:9090`. Puede cambiarse:

```bash
export JAIBA_SERVER_ADDR='0.0.0.0:9090'
cargo run -- serve examples/basic-flow.yaml
```

Endpoints:

| Endpoint | Uso |
|---|---|
| `GET /health` | Estado del servicio |
| `GET /ready` | Readiness del flujo |
| `GET /metrics` | Métricas Prometheus para Grafana |
| `GET /ws` | WebSocket con un snapshot JSON por segundo |
| `GET /ws/v1` | WebSocket de estado completo para Visualisa |
| `/api/v1/*` | Administración autenticada |

Grafana normalmente no consume el WebSocket directamente. La integración
recomendada es Grafana → Prometheus → `/metrics`. Configuración de Prometheus:

```yaml
scrape_configs:
  - job_name: jaiva
    scrape_interval: 5s
    static_configs:
      - targets: ["jaiva:9090"]
```

El WebSocket queda disponible para la futura interfaz de Jaiva:

```json
{
  "processed": 3,
  "failed": 0,
  "retried": 0,
  "emitted": 2,
  "queue_depth": 0,
  "active_tasks": 0,
  "memory_used_bytes": 0,
  "memory_budget_bytes": 7516192768,
  "backpressure_total": 0,
  "repository_pending": 0,
  "repository_running": 0,
  "repository_dead_letter": 0,
  "repository_content_bytes": 1024,
  "recovered_packets": 0
}
```

Las métricas Prometheus actuales son:

```text
jaiva_packets_processed_total
jaiva_packets_failed_total
jaiva_packet_retries_total
jaiva_packets_emitted_total
jaiva_queue_depth
jaiva_active_tasks
jaiva_memory_used_bytes
jaiva_memory_budget_bytes
jaiva_backpressure_total
jaiva_repository_pending_packets
jaiva_repository_running_packets
jaiva_repository_dead_letter_packets
jaiva_repository_content_bytes
jaiva_recovered_packets_total
jaiva_database_rows_written_total
jaiva_database_batches_written_total
jaiva_database_write_errors_total
jaiva_database_transaction_rollbacks_total
jaiva_database_write_duration_milliseconds_total
jaiva_circuit_breaker_rejections_total
jaiva_circuit_breakers_open
jaiva_processor_active_tasks
jaiva_processor_queue_depth
jaiva_processor_completed_total
jaiva_processor_failed_total
jaiva_processor_execution_milliseconds_total
jaiva_processor_saturation_ratio
jaiva_available_parallelism
jaiva_cpu_worker_limit
jaiva_blocking_worker_limit
```

## Configuración principal

```yaml
id: example

parameters:
  table: public.customers

database_connections:
  main:
    type: postgres
    url_env: DATABASE_URL
    max_connections: 10

engine:
  queue_capacity: 100
  max_concurrency: 4
  state_file: .jaiva/state.json
  memory:
    maximum_percent: 70
  repository:
    enabled: true
    database_path: .jaiva/repository.db
    content_path: .jaiva/content
    abandoned_after_seconds: 0
    completed_retention_hours: 24
  shutdown:
    drain_timeout_seconds: 60
    force_after_timeout: true
  circuit_breaker:
    enabled: true
    failure_threshold: 5
    open_seconds: 30
    half_open_requests: 1
  admin:
    enabled: true
    token_env: JAIBA_ADMIN_TOKEN
    max_request_body_bytes: 1048576

processors:
  - id: read
    type: query_postgres
    config:
      connection: main
      batch_size: 1000
      query: |
        SELECT to_jsonb(row_data)
        FROM (SELECT * FROM ${table}) AS row_data
    retry:
      maximum_attempts: 5
      initial_delay_ms: 500
      maximum_delay_ms: 30000
    scheduling:
      concurrent_tasks: 2
      timeout_ms: 60000

connections:
  - from: read
    relationship: success
    to: destination
    queue:
      capacity: 50
```

Las contraseñas no deben escribirse en YAML. Las conexiones usan `url_env`, y
cualquier configuración de procesador puede referenciar `${env:VARIABLE}`.

### Presupuesto de memoria

Jaiva detecta el menor valor entre la memoria física y el límite cgroup del
contenedor. El 70% se reserva para paquetes; el 30% restante queda disponible
para el sistema operativo, el ejecutable, los drivers y otras estructuras:

```yaml
engine:
  memory:
    maximum_percent: 70
```

Cada paquete reserva memoria aproximada antes de entrar al canal. Si no existe
presupuesto suficiente, el productor espera mediante backpressure y continúa
cuando los paquetes anteriores salen de la cola. Un paquete individual mayor
que todo el presupuesto se rechaza con un error explícito para evitar un OOM.

### Repositorio persistente

Cuando `repository.enabled` está activo, Jaiva persiste cada paquete antes de
encaminarlo al siguiente procesador:

```text
.jaiva/
├── repository.db
└── content/
    └── ab/
        └── ab12...bin
```

SQLite utiliza WAL para estados `PENDING`, `RUNNING`, `COMPLETED` y
`DEAD_LETTER`. El contenido se escribe primero en un archivo temporal, se
renombra atómicamente y se verifica mediante SHA-256. Al iniciar, Jaiva recupera
trabajo abandonado y reconstruye las colas. Esto ofrece entrega
`at-least-once`; los destinos deben utilizar claves idempotentes o `upsert`.

Los elementos `DEAD_LETTER` conservan el error y el intento final. Se pueden
consultar y preparar para reproceso con `dead-letter list` y
`dead-letter replay`. La fase completa está en
[`docs/priority-5-dead-letter.md`](docs/priority-5-dead-letter.md).

La procedencia registra la ruta, intentos, duración, tamaños, errores y estados
de cada paquete. Puede consultarse con `provenance recent` o
`provenance packet`; consulta
[`docs/priority-6-provenance.md`](docs/priority-6-provenance.md).

El repositorio es infraestructura interna y no depende del conector utilizado.
Los orígenes y destinos pueden ser PostgreSQL, Oracle, MySQL/MariaDB, SQL
Server, archivos u otros plugins.

## Procesadores incluidos

| Procesador | Función |
|---|---|
| `generate_records` | Genera registros para pruebas |
| `query_postgres` | Lee PostgreSQL mediante un pool compartido y crea lotes |
| `query_oracle` | Lee Oracle por lotes y normaliza las filas como objetos JSON |
| `put_database` | Escritura transaccional `insert`/`upsert` para PostgreSQL, MySQL/MariaDB, Oracle y SQL Server |
| `auto_destination` | Detecta el motor y selecciona el plan de carga para los destinos de base de datos disponibles |
| `publish_kafka` | Publica JSON o bytes con confirmación e idempotencia del productor |
| `rename_fields` | Renombra campos de objetos |
| `encode_json` | Convierte registros a JSON |
| `encode_yaml` | Convierte registros a YAML |
| `encode_csv` | Convierte objetos planos a CSV |
| `encode_xml` | Convierte objetos planos a XML |
| `write_file` | Guarda contenido codificado |
| `load_checkpoint` | Carga un valor persistente como atributo |
| `save_checkpoint` | Guarda un atributo como checkpoint |
| `log_records` | Muestra registros o contenido codificado |

CSV y XML convierten objetos anidados a su representación JSON textual. Para
mapeos complejos se deberá incorporar posteriormente un procesador de
aplanamiento o una transformación con esquema.

## Checkpoints

```yaml
- id: checkpoint
  type: load_checkpoint
  config:
    key: customers.updated_at
    attribute: checkpoint.value
    default: "1970-01-01T00:00:00Z"

- id: save
  type: save_checkpoint
  config:
    key: customers.updated_at
    attribute: checkpoint.value
```

El valor solo debe guardarse después de que el destino confirme la escritura.

## Extender el motor

La biblioteca expone `Processor`, `ProcessorRegistry`, `PacketContent`,
`ConnectionManager`, `RecordSchema` y `StateStore`. Un ejecutable externo puede
registrar procesadores propios:

```rust
let mut registry = ProcessorRegistry::default();
registry.register("my_processor", |config| {
    Ok(Arc::new(MyProcessor::from_config(config)?))
});

let engine = FlowEngine::new(config)?.with_registry(registry);
```

Para conservar los procesadores incorporados, se puede partir de
`processors::default_registry()`.

## Protobuf

El motor ya transporta bytes y esquemas, pero Protobuf no se genera de manera
genérica porque necesita un contrato `.proto`. La implementación correcta será
un procesador/plugin que reciba el descriptor Protobuf, valide el esquema y
codifique el mensaje correspondiente.

## Verificación

```bash
cargo fmt --check
cargo test
cargo test --features oracle-driver
cargo test --features sqlserver-driver
cargo test --features kafka-driver
cargo test --features mongodb-driver
```

## Oracle

Oracle se compila de forma opcional para que PostgreSQL y MySQL no arrastren
dependencias nativas:

```bash
cargo run --features oracle-driver -- examples/oracle-write.yaml
```

La variable indicada por `url_env` usa el formato
`oracle://usuario:contraseña@host:1521/servicio`. El proceso necesita Oracle
Instant Client Basic o Basic Light en la ruta del cargador dinámico. Consulta
la [instalación oficial de Instant Client](https://docs.oracle.com/en/database/oracle/oracle-database/26/lacli/install-instant-client-using-zip.html).
El ejemplo completo está en
[`examples/oracle-write.yaml`](examples/oracle-write.yaml).

Para probar una extracción y carga completa Oracle → PostgreSQL:

```bash
bash scripts/test-oracle-to-postgres.sh
```

El procedimiento, los usuarios técnicos, el mapeo y el diagnóstico están en
[`docs/oracle-to-postgres.md`](docs/oracle-to-postgres.md).

## MongoDB

MongoDB incluye perfiles, prueba de conexión, diagnósticos, exploración de
colecciones y los procesadores `query_mongodb`/`put_mongodb` mediante el driver
oficial de Rust:

```bash
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml
```

Configure host `127.0.0.1`, puerto `27017`, base `pruebas`, el usuario raíz del
contenedor y SSL desactivado. Consulte los detalles y el alcance de esta fase en
[`docs/connection-manager.md`](docs/connection-manager.md#mongodb-primera-fase).

Ejecute la carga de ejemplo:

```bash
export MONGODB_URL='mongodb://admin:admin123@127.0.0.1:27017/pruebas?authSource=admin'
cargo run --features mongodb-driver -- examples/mongodb-copy.yaml
```

## SQL Server

El adaptador TDS se habilita con `--features sqlserver-driver` y acepta
`sqlserver://usuario:contraseña@host:1433/base`. El upsert usa bloqueos
`UPDLOCK`/`SERIALIZABLE` dentro de una transacción, sin depender de `MERGE`.

```bash
cargo run --features sqlserver-driver -- examples/sqlserver-write.yaml
```

## Kafka

```bash
export KAFKA_BROKERS=127.0.0.1:29092
cargo run --features kafka-driver -- examples/kafka-publish.yaml
```

El productor espera confirmación del broker, usa `acks=all` e idempotencia y
expone topic, partición y offset en atributos de procedencia. Consulta
[`docs/priority-4-3-kafka.md`](docs/priority-4-3-kafka.md).

## Próximas integraciones

- Lectores genéricos para MySQL y SQL Server.
- Pool de conexiones SQL Server.
- Array binding y pool de sesiones para acelerar Oracle.
- Cancelación coordinada y circuit breaker.
- Almacenamiento de estado PostgreSQL/Redis.
- API administrativa y editor visual.

El diseño previo de escritura multi-base está documentado en
[`docs/priority-4-database-writes.md`](docs/priority-4-database-writes.md).

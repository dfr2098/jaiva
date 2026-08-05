# Configuración de Jaiva

## Estructura mínima

```yaml
id: example

processors:
  - id: source
    type: generate_records

connections: []
```

## Motor

```yaml
engine:
  queue_capacity: 100
  max_concurrency: 4
  state_file: .jaiva/state.json

  memory:
    maximum_percent: 42

  repository:
    enabled: true
    database_path: .jaiva/repository.db
    content_path: .jaiva/content
    abandoned_after_seconds: 0
    completed_retention_hours: 24
    provenance_retention_hours: 2160

  logging:
    enabled: true
    directory: .jaiva/logs
    rotation: daily
    retention_hours: 720
    cleanup_interval_seconds: 3600

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
    authentication: bearer
    token_env: JAIBA_ADMIN_TOKEN
    max_request_body_bytes: 1048576

  workers:
    cpu_threads: 0
    blocking_threads: 0
```

- `queue_capacity`: máximo global de paquetes en espera.
- `max_concurrency`: objetivo global de tareas concurrentes.
- `state_file`: checkpoints simples.
- `maximum_percent`: presupuesto de memoria para paquetes.
- `abandoned_after_seconds`: edad para recuperar trabajo `RUNNING`; cero es
  apropiado para el worker único actual.
- `completed_retention_hours`: retención de paquetes completados.
- `provenance_retention_hours`: retención del historial por paquete; el valor
  predeterminado equivale a 90 días.
- `logging.directory`: carpeta de logs de ejecución.
- `logging.rotation`: `hourly`, `daily` o `never`.
- `logging.retention_hours`: edad a partir de la cual se eliminan logs de
  Jaiva; `720` equivale a 30 días.
- `logging.cleanup_interval_seconds`: frecuencia con la que Jaiva ejecuta la
  depuración. Tanto este valor como la retención deben ser mayores que cero.
- `shutdown.drain_timeout_seconds`: tiempo máximo para que terminen las tareas
  activas durante el apagado.
- `shutdown.force_after_timeout`: permite cancelar la ejecución al agotar el
  plazo; los paquetes persistidos se recuperan después.

- `circuit_breaker.failure_threshold`: errores consecutivos antes de abrir el
  circuito de esa conexión.
- `circuit_breaker.open_seconds`: espera antes de probar el destino otra vez.
- `circuit_breaker.half_open_requests`: pruebas concurrentes permitidas.
- `admin.token_env`: variable que contiene el Bearer token administrativo.
- `admin.authentication`: `bearer` para operación normal o `none` únicamente
  para desarrollo local. Jaiva rechaza `none` al escuchar fuera de loopback.
- `admin.max_request_body_bytes`: límite global de los cuerpos HTTP.
- `workers.cpu_threads`: concurrencia de transformaciones CPU; cero detecta
  automáticamente la mitad de los CPU visibles.
- `workers.blocking_threads`: concurrencia para archivo y drivers bloqueantes;
  cero selecciona automáticamente aproximadamente una cuarta parte.

### Memoria de dominio JME

`engine.memory` limita la RAM de paquetes. `engine.domain_memory` habilita, de
forma independiente, el ciclo de vida de objetos de negocio:

```yaml
engine:
  domain_memory:
    enabled: true
    policy_file: examples/jme-cold-policy.yaml
```

El archivo de política define clases, TTL, prioridad y niveles Hot, Warm, Cold
y Frozen. Para Cold local segmentado:

```yaml
memory:
  cold:
    backend: segmented
    path: data/jme/cold
    segment_max_bytes: 67108864
    max_disk_bytes: 10737418240
    compression: lz4
    mmap: true
  classes:
    carrier:
      policy: cache
      temperature: cold
      demote_after: 30m
      ttl: 24h
```

Consulta [jme-cold-memory.md](jme-cold-memory.md) para el formato, recuperación,
métricas y límites de durabilidad.

`max_disk_bytes` limita el espacio Cold del flujo (el runtime crea un
subdirectorio por `flow_id`). Si una degradación excedería la cuota, JME no
publica el registro y conserva el objeto en Hot. Este límite no sustituye la
compactación: las versiones antiguas siguen ocupando espacio hasta el Paso 9.

Los eventos continúan apareciendo en consola y se escriben de forma no
bloqueante en archivos. La depuración solo elimina archivos `jaiva.log` o con
prefijo `jaiva.log.` dentro de la carpeta configurada.

## Parámetros

```yaml
parameters:
  source_table: public.customers

processors:
  - id: read
    type: query_postgres
    config:
      query: SELECT * FROM ${source_table}
```

Los secretos pueden proceder del entorno:

```yaml
config:
  token: ${env:EXTERNAL_API_TOKEN}
```

## Conexiones de base

```yaml
database_connections:
  main:
    type: postgres
    url_env: DATABASE_URL
    max_connections: 10
```

Las contraseñas no deben escribirse en YAML.

## Kafka

```yaml
kafka_connections:
  bus:
    brokers_env: KAFKA_BROKERS
    client_id: jaiva-publisher
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
```

Kafka se compila mediante `--features kafka-driver`. Solo admite
`PLAINTEXT` por ahora; los brokers siempre proceden de una variable de
entorno. Detalle de `publish_kafka` / `consume_kafka` en
[priority-4-3-kafka.md](priority-4-3-kafka.md).

## Ejecución continua (`schedule`)

Opcional. Si se omite, el flujo corre una sola pasada.

```yaml
schedule:
  enabled: true
  trigger:
    type: interval
    every_seconds: 60
  overlap: skip          # skip | queue | replace
  catch_up: none         # none | one
```

Otros disparadores:

```yaml
# Cron (6 campos: seg min hora día mes dow) + zona IANA
schedule:
  enabled: true
  timezone: America/Mexico_City
  trigger:
    type: cron
    expression: "0 0 2 * * *"
  overlap: skip
  catch_up: one

# Solo disparo manual: POST /api/v1/flows/{id}/trigger
schedule:
  enabled: true
  trigger:
    type: webhook
```

La agenda se arma al desplegar/iniciar el flujo y se desarma al detenerlo.
`overlap: skip` omite un disparo si aún corre una ejecución; `catch_up: one`
permite un disparo inmediato tras reinicio si se perdió la ventana.

MySQL y MariaDB usan el mismo formato:

```yaml
database_connections:
  destination:
    type: mysql # o mariadb
    url_env: MYSQL_DATABASE_URL
    max_connections: 10
```

Oracle se habilita al compilar con `--features oracle-driver`:

```yaml
database_connections:
  destination:
    type: oracle
    url_env: ORACLE_DATABASE_URL
    max_connections: 1
```

`ORACLE_DATABASE_URL` tiene la forma
`oracle://usuario:contraseña@host:1521/servicio`; para Oracle Free el servicio
de aplicación habitual es `FREEPDB1`. La aplicación requiere las bibliotecas de
Oracle Instant Client disponibles en `LD_LIBRARY_PATH` o en la configuración del
cargador del sistema. En esta primera versión Oracle abre una sesión por
operación; `max_connections` queda reservado para el pool de sesiones.

SQL Server se habilita con `--features sqlserver-driver`:

```yaml
database_connections:
  destination:
    type: sqlserver
    url_env: SQLSERVER_DATABASE_URL
    max_connections: 1
```

La URL usa `sqlserver://usuario:contraseña@host:1433/base`. El ejemplo está en
`examples/sqlserver-write.yaml`. La conexión TDS usa TLS y acepta el certificado
autofirmado habitual del contenedor local; en producción deberá configurarse
validación estricta del certificado.

MongoDB se habilita con `--features mongodb-driver`:

```yaml
database_connections:
  source:
    type: mongodb
    url_env: MONGODB_URL
    max_connections: 4
```

`MONGODB_URL` tiene la forma
`mongodb://usuario:contraseña@host:27017/base?authSource=admin` (también
`mongodb+srv://` para Atlas). Ejemplo: `examples/mongodb-copy.yaml`.

En el Connection Manager (UI/API) un perfil Mongo puede crearse con campos
sueltos o con el campo `url` (misma familia de URI). Ver
[connection-manager.md](connection-manager.md). Los flujos en ejecución pueden
usar el **alias** del perfil (`connection: mi_mongo`) en lugar de `url_env`.

Ejemplo de fan-out Oracle → PostgreSQL + MongoDB:
[`examples/multi-db-fanout.yaml`](../examples/multi-db-fanout.yaml)
(documentado en [oracle-to-postgres.md](oracle-to-postgres.md)).

## Procesadores

```yaml
processors:
  - id: read
    type: query_postgres
    config:
      connection: main
      batch_size: 1000
      query: |
        SELECT to_jsonb(row_data)
        FROM (
          SELECT * FROM public.customers
        ) row_data

    scheduling:
      concurrent_tasks: 2
      maximum_in_flight: 8
      execution_mode: auto
      ordering: unordered
      timeout_ms: 60000

    retry:
      maximum_attempts: 5
      initial_delay_ms: 500
      maximum_delay_ms: 30000
```

`engine.max_concurrency` es el límite estricto de todo el flujo.
`concurrent_tasks` limita un procesador. `execution_mode` admite `auto`,
`async_io`, `blocking_io` y `cpu`. `ordering` admite `unordered`, `preserve` y
`partitioned`; el último requiere `partition_by`. El límite global debe ser al
menos igual a la cantidad de procesadores del camino streaming más largo;
Jaiva rechaza configuraciones menores para evitar un interbloqueo.

## Conexiones del grafo

```yaml
connections:
  - from: read
    relationship: success
    to: transform
    queue:
      capacity: 50

  - from: read
    relationship: failure
    to: errors
```

Las relaciones actuales más comunes son `success` y `failure`.

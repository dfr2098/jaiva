# Bitácora técnica de Jaiba

Este documento explica qué se implementó, por qué existe y dónde debe
modificarse. Su objetivo es servir como memoria del proyecto, no sustituir la
documentación detallada de cada módulo.

## Estado actual

Jaiba ya dispone de:

- motor de flujos DAG con colas limitadas y backpressure;
- ejecución concurrente, reintentos, pausado, drenado y apagado coordinado;
- repositorio persistente, provenance y dead-letter;
- conexiones PostgreSQL, MySQL/MariaDB, Oracle y SQL Server;
- publicación en Kafka;
- API administrativa, métricas y WebSocket;
- diseñador visual e importación/exportación YAML;
- administrador de conexiones;
- explorador de metadatos y constructor visual de consultas SQL;
- creación automática de nodos `query_postgres`.

Las fases 9.1–9.8 están terminadas para el alcance de la versión 0.2. El
recorrido de 9.5 y 9.6 se validó contra PostgreSQL real; 9.7 dispone de
proveedores seleccionables Real, Mock y Replay; 9.8 ofrece transporte de
procesos aislados. WebAssembly queda como transporte futuro opcional.

## Mapa del repositorio

| Ruta | Responsabilidad |
|---|---|
| `crates/jaiba-core` | Configuración y validación del grafo DAG |
| `crates/jaiba-runtime` | Ejecución, procesadores, colas, métricas y repositorios |
| `crates/jaiba-server` | REST, WebSocket, seguridad y registro de flujos |
| `crates/jaiba-cli` | Comandos `jaiba` y compatibilidad con `jaiva-flow` |
| `crates/jaiba-plugin-sdk` | Contratos estables para conexiones y procesadores |
| `crates/jaiba-connection-manager` | Perfiles, secretos, diagnóstico y metadatos |
| `apps/jaiba-ui` | Monitor, diseñador y administrador de conexiones |
| `plugins` | Manifiestos de plugins oficiales |
| `simulator` | Base de los modos Mock y Replay |

La UI nunca debe conectarse directamente a una base de datos. Toda operación
con drivers o secretos pasa por el servidor y el Connection Manager.

El catálogo de motores tampoco se mantiene en la UI ni en handlers REST. Se
deriva de los adaptadores registrados y sus capacidades. Los identificadores
son cadenas extensibles, por lo que un motor nuevo no requiere agregar una
variante al núcleo.

## Consulta visual: recorrido completo

1. `ConnectionManagerView.tsx` abre el explorador de una conexión.
2. `SqlQueryBuilder.tsx` permite seleccionar tabla, columnas, joins, filtros,
   agrupación, orden y límite.
3. La UI construye un `QuerySpec`; no concatena texto SQL.
4. `POST /api/v1/connections/{id}/query/compile` entrega el `QuerySpec` al
   servidor.
5. `connection_api.rs` selecciona el plugin correspondiente.
6. `sql_builder.rs` valida y cita identificadores, genera placeholders y
   mantiene los valores separados en `parameters`.
7. El adaptador puede devolver `processor_type` y `execution_statement`; para
   PostgreSQL prepara el objeto JSONB que requiere `query_postgres`.
8. `pendingQueryNode.ts` guarda temporalmente la consulta compilada en
   `localStorage`.
9. `FlowBuilder.tsx` consume ese dato, registra la conexión si hace falta y
   crea el tipo de nodo indicado por el adaptador.
10. Al ejecutar el flujo, `query_postgres.rs` liga los parámetros con SQLx y
    emite los resultados por lotes.

MySQL puede explorar metadatos y compilar consultas, pero todavía no crea un
nodo ejecutable porque el runtime no contiene un procesador `query_mysql`.

## Decisiones de seguridad SQL

- Los identificadores aceptan únicamente letras ASCII, números, `_` y `$`;
  cada segmento se cita según PostgreSQL o MySQL.
- Los valores siempre viajan como parámetros ligados.
- `null` con igualdad se convierte en `IS NULL`; con desigualdad se convierte
  en `IS NOT NULL`.
- Una lista `IN` vacía o que contenga `null` se rechaza.
- `query_postgres` rechaza parámetros `null` sin tipo.
- Los enteros mayores que PostgreSQL `BIGINT` se rechazan para impedir pérdida
  silenciosa de precisión.

Si se añade un operador SQL nuevo, debe implementarse en `FilterOperator`, en
`sql_builder.rs` y en la UI, acompañado de pruebas de compilación segura.

## Versionado y despliegue de flujos

`flow_registry.rs` conserva versiones inmutables:

```mermaid
stateDiagram-v2
    [*] --> DRAFT: importar o publicar YAML
    DRAFT --> VALIDATED: validación correcta
    VALIDATED --> DEPLOYED: deploy
    DEPLOYED --> ARCHIVED: desplegar otra versión
    ARCHIVED --> DEPLOYED: rollback
    DRAFT --> ARCHIVED: archivar manualmente
    VALIDATED --> ARCHIVED: archivar manualmente
```

Cada versión guarda el YAML original, checksum SHA-256 y fechas de transición.
Un despliegue prepara la configuración, detiene de forma coordinada la versión
anterior, activa la nueva y permite rollback. Los despliegues y rollbacks están
serializados para evitar que dos solicitudes modifiquen simultáneamente el
mismo estado.

El registro se guarda atómicamente en `flows.json`. Un fallo de persistencia se
devuelve como error en vez de responder que la operación fue exitosa.

Al desplegar o restaurar una versión también se recalcula la política
administrativa: habilitación, método de autenticación y Bearer token. Esto
evita conservar por accidente la seguridad configurada por una versión
anterior.

## Archivos clave de la implementación reciente

| Archivo | Motivo |
|---|---|
| `crates/jaiba-plugin-sdk/src/lib.rs` | Define `QuerySpec`, metadatos y contratos de plugins |
| `crates/jaiba-server/src/connection_api.rs` | API de conexiones, metadatos y compilación |
| `crates/jaiba-server/src/sql_builder.rs` | Compilador SQL seguro por dialecto |
| `crates/jaiba-server/src/flow_registry.rs` | Versiones, despliegue y rollback |
| `crates/jaiba-runtime/src/processors/query_postgres.rs` | Ejecución parametrizada de consultas |
| `apps/jaiba-ui/src/connections/SqlQueryBuilder.tsx` | Constructor visual |
| `apps/jaiba-ui/src/builder/pendingQueryNode.ts` | Traspaso temporal hacia el diseñador |
| `apps/jaiba-ui/src/builder/FlowBuilder.tsx` | Creación automática del nodo |

## Cómo comprobar que no se rompió

Desde la raíz:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
```

Para el frontend:

```bash
cd apps/jaiba-ui
npm run typecheck
npm run build
```

Prueba manual recomendada para cerrar 9.5 y 9.6:

1. iniciar PostgreSQL y crear una tabla pequeña;
2. registrar y probar la conexión desde la UI;
3. explorar la tabla y sus columnas;
4. construir una consulta con filtro parametrizado;
5. enviarla al diseñador;
6. guardar o publicar el flujo;
7. ejecutarlo y comprobar paquetes, métricas y provenance;
8. repetir la exploración y compilación con MySQL.

### Integración real con MySQL

La prueba `mysql_real_connection_metadata_and_query_compilation` cubre conexión,
diagnóstico, exploración, descripción de columnas/llaves/índices y compilación
segura de un `QuerySpec`. Solo se activa cuando existe la contraseña:

```bash
export JAIBA_TEST_MYSQL_HOST=127.0.0.1
export JAIBA_TEST_MYSQL_PORT=13306
export JAIBA_TEST_MYSQL_DATABASE=dma_test
export JAIBA_TEST_MYSQL_USER=dma_test
export JAIBA_TEST_MYSQL_PASSWORD='contraseña-del-entorno-de-pruebas'
cargo test -p jaiba-server mysql_real_connection_metadata_and_query_compilation -- --nocapture
```

La prueba crea y elimina `jaiba_phase_9_3_probe`. Si la variable de contraseña
no está definida, se omite sin fallar la suite normal.

Estado comprobado el 29 de julio de 2026:

- conexión real contra MySQL 8.4: correcta;
- diagnóstico de conectividad, versión y metadatos: correcto;
- exploración de tablas: correcta;
- descripción de columnas, llave primaria e índice: correcta;
- compilación SQL parametrizada para MySQL: correcta;
- limpieza de la tabla temporal: correcta.

Durante esta prueba se detectó que `diagnose` todavía devolvía
`Unsupported`. Se implementaron comprobaciones reales de conectividad, versión
y acceso a metadatos para los plugins PostgreSQL y MySQL.

### Integración real con Oracle

La prueba `oracle_real_connection_diagnostics_and_metadata` requiere la feature
`oracle-driver`, Oracle Client y una contraseña proporcionada mediante el
entorno:

```bash
export JAIBA_TEST_ORACLE_HOST=127.0.0.1
export JAIBA_TEST_ORACLE_PORT=11521
export JAIBA_TEST_ORACLE_SERVICE=FREEPDB1
export JAIBA_TEST_ORACLE_USER=dma_test
export JAIBA_TEST_ORACLE_PASSWORD='contraseña-del-entorno-de-pruebas'
cargo test -p jaiba-server --features oracle-driver \
  oracle_real_connection_diagnostics_and_metadata -- --nocapture
```

La prueba crea y elimina `JAIBA_PHASE_9_3_PROBE`, comprueba conexión,
diagnóstico y exploración de columnas. Sin contraseña se omite. El ejecutable
necesita encontrar `libclntsh` mediante `LD_LIBRARY_PATH` o la configuración
del cargador del sistema.

Estado comprobado el 29 de julio de 2026 contra Oracle Free/FREEPDB1:

- conexión y lectura de versión: correctas;
- diagnóstico de conectividad, versión y acceso a `all_objects`: correcto;
- exploración de tablas del esquema `DMA_TEST`: correcta;
- descripción ordenada de columnas: correcta;
- creación, inserción y limpieza de la tabla temporal: correctas.

Para esta validación se utilizó Oracle Instant Client Basic Light 23.26,
descargado desde la página oficial y verificado mediante SHA-256 antes de
extraerlo. No se instaló software globalmente en el host.

Limitación vigente: el descriptor Oracle devuelve columnas, pero todavía no
incluye llaves e índices, y su constructor visual SQL permanece deshabilitado.

### Integración real con SQL Server

La prueba `sqlserver_real_connection_diagnostics_and_metadata` requiere la
feature `sqlserver-driver` y credenciales proporcionadas mediante el entorno:

```bash
export JAIBA_TEST_SQLSERVER_HOST=127.0.0.1
export JAIBA_TEST_SQLSERVER_PORT=11433
export JAIBA_TEST_SQLSERVER_DATABASE=master
export JAIBA_TEST_SQLSERVER_USER=sa
export JAIBA_TEST_SQLSERVER_PASSWORD='contraseña-del-entorno-de-pruebas'
cargo test -p jaiba-server --features sqlserver-driver \
  sqlserver_real_connection_diagnostics_and_metadata -- --nocapture
```

La prueba crea y elimina `dbo.JAIBA_PHASE_9_3_PROBE`, y se omite si no existe
la variable de contraseña.

Estado comprobado el 29 de julio de 2026 contra SQL Server 2022:

- conexión TDS y lectura de versión 16.x: correctas;
- diagnóstico de conectividad, edición y acceso a `sys.objects`: correcto;
- exploración de tablas del esquema `dbo`: correcta;
- descripción ordenada de columnas: correcta;
- creación, inserción y limpieza de la tabla temporal: correctas.

Limitación vigente: el descriptor SQL Server todavía no devuelve llaves ni
índices, y el constructor visual SQL permanece deshabilitado.

### Fase 8 — suite con entorno de pruebas

Tras `consume_kafka`, la suite de integración/fallos/rendimiento contra Postgres,
Kafka, MongoDB y SQL Server del entorno de pruebas se ejecuta con:

```bash
export JAIBA_TEST_POSTGRES_PASSWORD='...'
export JAIBA_TEST_MONGODB_PASSWORD='...'
export JAIBA_TEST_SQLSERVER_PASSWORD='...'
./scripts/phase8-integration.sh
```

Realiza: smoke publish/consume Kafka (incluido el procesador `consume_kafka`),
throughput de 100 mensajes, fallo controlado de broker, retry→DLQ→requeue, el
flujo Postgres del Connection Manager, MongoDB (metadatos +
`query_mongodb`→`put_mongodb`) y SQL Server (diagnóstico + metadatos). Detalle en
[priority-8-integration-tests.md](priority-8-integration-tests.md).

Estado comprobado el 1 de agosto de 2026: harness verde con Postgres `:55432`,
Kafka `:29092`, MongoDB `:27018` y SQL Server `:11433`.

### Fan-out Oracle → PostgreSQL + MongoDB (validado)

El 2 de agosto de 2026 se validó en el entorno de pruebas:

- [`examples/multi-db-fanout.yaml`](../examples/multi-db-fanout.yaml): 2 filas,
  `failed=0` hacia Postgres y Mongo.
- [`examples/oracle-fanout-stress.yaml`](../examples/oracle-fanout-stress.yaml):
  ~10 000 filas, `failed=0` tras crear `public.jaiva_oracle_stress`; comprobado
  en Compass y DBeaver.

Requisitos prácticos: Instant Client en el host (`LD_LIBRARY_PATH`, p. ej.
copiado desde `dma_test_oracle_client` a `$HOME/oracle/instantclient_23_26`),
Oracle `healthy` en `:11521`, tablas destino en Postgres. Runbook en
[oracle-to-postgres.md](oracle-to-postgres.md#fan-out-multi-db-prueba-oracle--postgresql--mongodb).

### MongoDB — URL de conexión en Connection Manager

Además de host/puerto/usuario, `POST/PUT /api/v1/connections` acepta `url` para
`connection_type: mongodb` (`mongodb://` o `mongodb+srv://`). La URI se guarda
en `ConnectionSecret.options["connection_url"]` y tiene prioridad al conectar
(plugin y `ProfileConnectionResolver`). La UI muestra el campo **URL de
conexión** y rellena los campos al pegar.

Pruebas:

```bash
cargo test -p jaiba-server --features mongodb-driver mongo_url_unit_tests
cargo test -p jaiba-server --features mongodb-driver \
  mongodb_real_connection_from_url -- --nocapture
cargo test -p jaiba-runtime prefers_stored_mongodb_connection_url
```

Documentación de usuario: [connection-manager.md](connection-manager.md).

### Integración real con Kafka

La prueba `kafka_real_publish_is_acknowledged_and_consumable` requiere la
feature `kafka-driver` y la dirección del broker:

```bash
export JAIBA_TEST_KAFKA_BROKERS=127.0.0.1:29092
cargo test -p jaiba-runtime --features kafka-driver \
  kafka_real_publish_is_acknowledged_and_consumable -- --nocapture
```

La prueba crea un tópico único `jaiba.phase-9-3.*`, publica dos registros con
claves diferentes, espera la confirmación del broker, consume y valida ambos
JSON, y elimina el tópico. Sin la variable de entorno se omite.

Estado comprobado el 29 de julio de 2026 contra Apache Kafka 4.3.1:

- creación explícita de tópico con auto-creación deshabilitada: correcta;
- productor idempotente con `acks=all`: correcto;
- confirmación de partición y offset por el broker: correcta;
- claves y JSON de los dos mensajes consumidos: correctos;
- eliminación del tópico temporal: correcta.

Durante la asignación inicial del grupo, el broker puede devolver
`BrokerTransportFailure` de forma transitoria. La prueba lo tolera dentro de un
límite total de 20 segundos y falla si los mensajes no llegan; los errores
persistentes no se ocultan.

### Integración real con PostgreSQL

La prueba `postgres_real_connection_query_builder_and_flow_execution` cubre el
recorrido completo del Connection Manager hasta el runtime:

```bash
export JAIBA_TEST_POSTGRES_HOST=127.0.0.1
export JAIBA_TEST_POSTGRES_PORT=55432
export JAIBA_TEST_POSTGRES_DATABASE=dma
export JAIBA_TEST_POSTGRES_USER=dma
export JAIBA_TEST_POSTGRES_PASSWORD='contraseña-del-entorno-de-pruebas'
export JAIBA_TEST_POSTGRES_URL='postgres://usuario:contraseña@127.0.0.1:55432/dma'
cargo test -p jaiba-server \
  postgres_real_connection_query_builder_and_flow_execution -- --nocapture
```

La prueba crea `public.jaiba_phase_9_3_probe`, valida conexión, diagnóstico,
columnas, llaves e índices, compila un `QuerySpec`, ejecuta
`query_postgres → encode_json → write_file`, comprueba el registro resultante y
elimina tabla y archivo. Sin las variables requeridas se omite.

Estado comprobado el 29 de julio de 2026 contra PostgreSQL 16.14:

- conexión, versión y diagnóstico: correctos;
- exploración de tabla, columnas, llave primaria e índice: correcta;
- compilación de SQL con parámetro booleano: correcta;
- enlace real del parámetro `$1` por `query_postgres`: correcto;
- ejecución del flujo y contenido JSON de salida: correctos;
- eliminación de tabla y archivo temporal: correcta.

Con esta validación se cierra la fase 9.3 para los conectores actuales:
PostgreSQL, MySQL, Oracle, SQL Server y Kafka cuentan con pruebas reales opt-in.

## Cierre de 9.5 y 9.6

La prueba real PostgreSQL cubre el mismo contrato que usa la UI: exploración,
descripción, compilación segura y ejecución del nodo `query_postgres`. La
compilación TypeScript valida el traspaso del constructor al diseñador. La
creación automática se limita deliberadamente a PostgreSQL porque el runtime
todavía no contiene un procesador `query_mysql`.

## Proveedores Real, Mock y Replay (9.7)

`jaiba-simulator` contiene un `ProviderRegistry` que selecciona el proveedor a
partir del modo del procesador:

```mermaid
flowchart LR
    REQ["SimulationRequest<br/>mismo DAG"] --> REG["ProviderRegistry"]
    REG --> REAL["RealProvider<br/>callback del runtime"]
    REG --> MOCK["MockProvider<br/>paquetes deterministas"]
    REG --> REPLAY["ReplayProvider<br/>referencias de provenance"]
    REPLAY --> STORE["ReplayStore"]
```

Mock acepta registros JSON o bytes y rechaza opciones inválidas. Replay solo
guarda referencias en el YAML y solicita el contenido a `ReplayStore`. Real
recibe un callback del host, evitando que el simulador dependa del runtime.

## Plugins externos aislados (9.8)

`jaiba-plugin-sdk::isolated` define solicitudes y respuestas JSON Lines con
versión de protocolo, correlación por `request_id`, límite de 8 MiB y una sola
alternativa entre resultado y error. `IsolatedPluginProcess` inicia únicamente
un ejecutable previamente elegido por el catálogo y se comunica por
stdin/stdout:

```mermaid
sequenceDiagram
    participant H as Jaiba host
    participant P as Plugin process
    H->>P: PluginRequest v1 + request_id
    P-->>H: PluginResponse v1 + mismo request_id
    H->>H: valida versión, tamaño y resultado/error
```

Esto evita depender de la ABI inestable de Rust. Un plugin incompatible,
malformado, demasiado grande o con un identificador de respuesta distinto se
rechaza. WASM Component Model puede agregarse después como transporte adicional
sin cambiar `ProcessorPlugin` ni los sobres del protocolo.

## AI Data Prep Toolkit (2026-08)

**Problema:** preparar datasets tabulares para plataformas ML externas sin
embeber Python ni entrenar modelos en el worker.

**Cambio:** módulo `crates/jaiba-runtime/src/processors/ai_prep/` con
procesadores `ai_*` (limpieza, normalize/encode/features/split, lookup join,
manifest, webhook HTTP vía `reqwest`). Catálogo UI categoría **AI Prep**.
Docs: [ai-data-prep.md](ai-data-prep.md). Ejemplo:
`examples/ai-prep-conveyor.yaml` → CSV train/val/test + `manifest.json`.

**Decisión:** stats por lote en MVP; `cumulative` / `window` para estado entre
paquetes (Fase B). Sin Parquet ni train in-process.

**Prueba:** `cargo test -p jaiba-runtime --lib processors::ai_prep` y
`cargo run -- examples/ai-prep-conveyor.yaml`.

## Trabajo posterior a la fase 9

- procesadores ejecutables de consulta para MySQL, Oracle y SQL Server;
- catálogo firmado y política de permisos para plugins de terceros;
- transporte WebAssembly opcional;
- automatización de pruebas reales en CI con servicios efímeros.

## Regla para futuras implementaciones

Al terminar una característica, añadir aquí:

1. qué problema resuelve;
2. qué archivos cambió;
3. qué decisión importante se tomó;
4. cómo se prueba;
5. qué limitación permanece.

Conservar esta bitácora hace que el motivo de una implementación sobreviva
aunque pasen meses entre cambios.

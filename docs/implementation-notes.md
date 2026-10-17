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
5. `connection_api/` selecciona el plugin correspondiente
   (`connection_api/plugins/<motor>.rs`).
6. `sql_builder.rs` valida y cita identificadores, genera placeholders y
   mantiene los valores separados en `parameters`.
7. El adaptador puede devolver `processor_type` y `execution_statement`:
   PostgreSQL envuelve con `to_jsonb`; MySQL y SQL Server usan la sentencia
   compilada tal cual (filas → JSON en el runtime).
8. `pendingQueryNode.ts` guarda temporalmente la consulta compilada en
   `localStorage`.
9. `FlowBuilder.tsx` consume ese dato, registra la conexión si hace falta y
   crea el tipo de nodo indicado por el adaptador (`query_postgres`,
   `query_mysql` o `query_sqlserver`).
10. Al ejecutar el flujo, el procesador correspondiente liga los parámetros y
    emite los resultados por lotes.

## Decisiones de seguridad SQL

- Los identificadores aceptan únicamente letras ASCII, números, `_` y `$`;
  cada segmento se cita según el dialecto (`"…"`, `` `…` `` o `[…]`).
- Los valores siempre viajan como parámetros ligados (`$n`, `?`, `@Pn`).
- `null` con igualdad se convierte en `IS NULL`; con desigualdad se convierte
  en `IS NOT NULL`.
- Una lista `IN` vacía o que contenga `null` se rechaza.
- `query_postgres`, `query_mysql` y `query_sqlserver` rechazan parámetros
  `null` sin tipo.
- Los enteros mayores que `BIGINT` con signo se rechazan para impedir pérdida
  silenciosa de precisión.
- SQL Server usa `SELECT TOP (n)` en lugar de `LIMIT`.

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

La política administrativa (habilitación, Bearer/`none`, token) se fija al
**arranque del proceso** (`JAIBA_ADMIN_*` + YAML semilla de `jaiba serve`) y no
se reescribe al desplegar o restaurar otra versión. Así un YAML publicado no
puede bajar el nivel de seguridad del servidor. Ver fase 9A.

## Archivos clave de la implementación reciente

| Archivo | Motivo |
|---|---|
| `crates/jaiba-plugin-sdk/src/lib.rs` | Define `QuerySpec`, metadatos y contratos de plugins |
| `crates/jaiba-server/src/connection_api/mod.rs` | API de conexiones, metadatos y compilación |
| `crates/jaiba-server/src/connection_api/plugins/` | Un plugin de conexión por motor |
| `crates/jaiba-server/src/sql_builder.rs` | Compilador SQL seguro por dialecto |
| `crates/jaiba-server/src/flow_registry.rs` | Versiones, despliegue y rollback |
| `crates/jaiba-runtime/src/processors/query_postgres.rs` | Lectura PostgreSQL parametrizada |
| `crates/jaiba-runtime/src/processors/query_mysql.rs` | Lectura MySQL/MariaDB → JSON |
| `crates/jaiba-runtime/src/processors/query_sqlserver.rs` | Lectura SQL Server (feature `sqlserver-driver`) |
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
índices. El constructor visual y `query_sqlserver` ya están disponibles con
`--features sqlserver-driver`.

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
[priority-8-integration-tests.md](history/priority-8-integration-tests.md).

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
descripción, compilación segura y ejecución del nodo `query_postgres`. MySQL y
SQL Server compilan al mismo contrato (`query_mysql` / `query_sqlserver`) y
habilitan “Crear nodo Query” en la UI. La compilación TypeScript valida el
traspaso del constructor al diseñador.

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

## Fase 9A — Endurecimiento admin (2026-08)

**Problema:** bypass Bearer→None en loopback, auditoría incompleta en conexiones
y posibles fugas de DSN/password en errores de driver.

**Cambio:** `resolve_server_admin` exige token si `authentication=bearer`;
`authorize` en tiempo constante; `/runtime` y `/ws*` gated fuera de loopback;
`redact_sensitive` + `client_message`; `duplicate` clona `secret_ref`;
`audit_action` en validate y CRUD/test de conexiones.
Docs: [priority-9a-admin-hardening.md](history/priority-9a-admin-hardening.md).

**Nota:** la política admin se fija al **arranque del proceso** (env + YAML
semilla) y no se reescribe al desplegar otro flujo.

## Fase 9B — Tauri desktop MVP (2026-08)

**Problema:** empaquetar la UI operativa como app de escritorio sin reescribir
el frontend ni embeber el motor en el WebView.

**Cambio:** `apps/jaiba-ui/src-tauri/` (crate `jaiba-desktop`, Tauri 2). Modo
remoto por defecto a `http://127.0.0.1:9090`; comando `api_base` y resolución
en `src/api.ts`. Scripts `npm run desktop:dev` / `desktop:build`.
Docs: [priority-9b-tauri-desktop.md](history/priority-9b-tauri-desktop.md).

**Decisión:** sidecar de `jaiba serve` aplazado; CSP limitada a loopback.

**Prueba:** con `jaiba serve` en `:9090`, `cd apps/jaiba-ui && npm run desktop:dev`.

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

## Fase 10A — Sidecar Tauri (local / remoto)

**Problema:** el desktop 9B exigía un `jaiba serve` arrancado a mano.

**Cambio:** `apps/jaiba-ui/src-tauri/src/sidecar.rs` gestiona spawn/stop del
binario `jaiba`, conmutación local/remoto, YAML embebido y `externalBin`.
UI: `EngineControl` en el topbar. Docs:
[priority-10a-tauri-sidecar.md](history/priority-10a-tauri-sidecar.md).

**Decisión:** proceso hijo vía `std::process` (no plugin-shell); si el puerto
ya escucha, se reutiliza sin segundo sidecar. Auth local `none` en loopback.

**Prueba:** `scripts/prepare-desktop-sidecar.sh` + `npm run desktop:dev` →
Motor · local → Arrancar → health en `:9090`.

### Compatibilidad Windows nativo + WSL (2026-08-05)

**Problema:** el flujo desktop dependía de `bash`, `env` y nombres de binario
sin `.exe`; Tauri no encontraba el sidecar Windows. Además, `sync_all()` sobre
un temporal abierto solo para lectura fallaba con `os error 5`, y Clippy
rechazaba `Dialect::SqlServer` cuando el feature estaba desactivado.

**Cambio:** preparación/lanzamiento multiplataforma en
`scripts/prepare-desktop-sidecar.mjs` y `scripts/run-desktop.mjs`; resolución
de `jaiba.exe` en `sidecar.rs`; temporal `frozen` abierto para escritura;
`Dialect::SqlServer` condicionado a tests/feature; `*.sh` fijado a LF mediante
`.gitattributes`. Guía operativa:
[windows-native-and-wsl.md](windows-native-and-wsl.md).

**Validación Windows:** 138 tests, Clippy `-D warnings`, SQL Server opcional,
Tauri `cargo check`, typecheck y build Vite correctos. El sidecar se prepara
como `jaiba-x86_64-pc-windows-msvc.exe`.

**Validación WSL2:** 138 tests y preparación del sidecar Linux correctos. El
check Tauri requiere instalar GTK/WebKitGTK en la distribución.

## Fase 10B — TLS, roles y proyectos

**Problema:** un solo Bearer sin roles; HTTP sin TLS nativo; sin alcance por flujo.

**Cambio:** `crates/jaiba-server/src/auth.rs` (roles viewer/operator/admin,
`JAIBA_ADMIN_USERS_FILE`, allowlist `projects`), HTTPS opcional con
`JAIBA_TLS_*` vía `axum-server`, `GET /api/v1/whoami`. UI muestra actor/rol.
Docs: [priority-10b-security.md](history/priority-10b-security.md). Ejemplo:
`examples/admin-users.json`.

**Decisión:** sin SSO; token de entorno sigue siendo admin global; permisos por
`flow_id` (proyecto), no multi-tenant de datos.

**Prueba:** `cargo test -p jaiba-server --lib` y serve con users file + curl whoami.

## Fase 10C — Flujo de planta AI Prep

**Problema:** el conveyor sintético no demostraba origen DB ni hand-off usable.

**Cambio:** `examples/ai-prep-plant.yaml` (Postgres VALUES → `ai_*` → CSV +
manifest con paths); `shuffle`/`seed` en split; `optional` en webhook;
`scripts/mock-ml-webhook.py`. Docs:
[priority-10c-plant-prep.md](history/priority-10c-plant-prep.md).

**Decisión:** VALUES evita DDL en demos; Oracle se documenta como sustitución
del nodo de lectura.

**Prueba:** con `DATABASE_URL`, `cargo run -- examples/ai-prep-plant.yaml` y
`failed=0`; opcionalmente el mock en `:8099`.

## CI mínimo (GitHub Actions)

**Problema:** no había gate automático de fmt/test.

**Cambio:** `.github/workflows/ci.yml` (fmt + `cargo test --workspace` +
typecheck UI) y `.github/workflows/phase8-integration.yml` (manual / label
`phase8`). Docs: [ci.md](ci.md).

**Decisión:** Phase 8 sigue siendo opt-in (necesita entorno real / self-hosted);
el CI de PR no levanta contenedores.

**Prueba:** push/PR dispara CI; localmente los mismos comandos de `docs/ci.md`.

## JME Paso 8 — Cold Memory segmentado (2026-08-05)

**Problema:** JME podía degradar a Warm/Frozen o persistir mediante un sink,
pero no tenía un nivel SSD local eficiente y consciente de objetos entre RAM y
la base autoritativa.

**Cambio:** `jaiba-memory` incorpora `ColdStore` y `SegmentedColdStore`,
segmentos append-only por clase, compresión LZ4, checksum SHA-256, tombstones,
rotación, reconstrucción del índice y lectura bajo demanda (`mmap` opcional). El manager consulta Hot →
Warm → Cold → Frozen → rebuild y selecciona víctimas por prioridad, frecuencia,
tamaño e inactividad (`demote_after`). El runtime aísla el directorio Cold por
`flow_id`, aplica `max_disk_bytes` por flujo y publica métricas Cold.

**Decisión:** Cold es caché local recuperable, no fuente autoritativa. Los datos
críticos conservan `immediate`/`persistent`/`deferred` o un rebuild desde la
base. Si falla una degradación o se agota la cuota, el objeto vuelve a Hot.

**Prueba:** `cargo test -p jaiba-memory`, pruebas de métricas del runtime y suite
completa del workspace. Se cubren reinicio, rotación, cola parcial, tombstones,
promoción, degradación por inactividad y rechazo seguro por cuota.

**Limitación:** la compactación, manifiestos durables y publicación mediante
temporal + rename atómico quedan para el Paso 9. El esquema YAML actual también
conserva compatibilidad histórica y aún agrupa parte de residencia y
durabilidad dentro de `policy`; su separación será una migración versionada.

## JME Beta — integración oficial (2026-10)

**Problema:** JME estaba cableado al runtime pero marcado Experimental. Sus
rutas dependían del directorio de trabajo, el contrato YAML no tenía versión,
Hot rechazaba escrituras al llenar `max_hot_bytes` y ningún flujo de CI ni de
carga lo ejercitaba.

**Archivos:** `jaiba-core/src/config/flow.rs` (política embebida),
`jaiba-memory/src/{policy,hot,manager,cold}.rs`,
`jaiba-runtime/src/engine/domain_memory.rs`, `examples/jme-*.yaml`,
`examples/stable-runtime-stress.yaml`, `scripts/smoke-jme.py`,
`scripts/chaos-jme.py`, `scripts/stress-stable-stack.sh`,
`.github/workflows/ci.yml`.

**Decisiones:**

- La política va embebida en `engine.domain_memory.policy` o en
  `policy_file`, exactamente una de las dos. Ya no se carga un archivo de
  ejemplo por defecto: un flujo con `enabled: true` sin política falla al
  arrancar (ruptura deliberada).
- `memory.version: 1` es opcional; cualquier otro valor se rechaza.
- Cold y Frozen sin `path` van a `$JAIBA_DATA_DIR/jme/{cold,frozen}/<flow_id>`.
- Hot desaloja entradas no `critical` también por bytes, igual que por
  cantidad.
- Si falla la degradación por inactividad (por ejemplo, cuota de Cold llena),
  se cuenta en métricas y el flujo sigue; vaciar `deferred` sigue siendo fatal.
- Daños en Cold: ceros al final del segmento se recortan como escritura
  cortada; un checksum inválido deja `warn` y suma
  `jaiba_memory_cold_read_failures_total`; una cabecera dañada a mitad de
  segmento se rescata (registros válidos conservados, copia
  `segment-<id>.jmc.corrupt`, `jaiba_memory_cold_salvaged_segments_total`).
- JME madura en este repo; AI Prep sigue en el lab `DMA_JAIVA/`.

**Prueba:** `cargo test -p jaiba-memory`; `scripts/smoke-jme.py` (3 PASS) y
`scripts/chaos-jme.py` (11 PASS) en CI; `stress-stable-stack.sh` falla si JME
termina sin objetos.

**Limitación:** falta la compactación de Cold (Paso 9), requisito para
Estable. Las copias `.corrupt` no se borran solas, y un rescate puede dejar
visible una versión anterior de una llave. En el flujo de stress las llaves se
reescriben cada ciclo y casi nunca pasan a Cold; Cold con fallas lo cubre
`chaos-jme.py`, no el soak.

## Runtime — arreglos de la revisión (2026-10)

**Problema:** la revisión y `chaos-jme.py` encontraron fallas del runtime que
no dependen de JME.

**Archivos:** `jaiba-runtime/src/engine/executor.rs` (hoy
`engine/executor/`), `jaiba-runtime/src/engine/supervisor.rs`.

**Decisiones:**

- Memoria de paquetes agotada: antes tumbaba el flujo. Ahora espera hasta
  30 s (`MEMORY_STALL_TIMEOUT`) a que se libere, o falla enseguida si ninguna
  tarea en curso puede liberarla.
- Un flujo se puede detener mientras arranca.
- Con más fuentes que `queue_capacity` el flujo se colgaba en silencio. El
  trabajo inicial de las fuentes ya no cuenta contra la cola, y una fuente solo
  arranca si queda espacio para lo que va a emitir.
- El fallo definitivo de un processor deja un `warn` con el motivo.

**Prueba:** tests de regresión en `engine/executor/tests.rs` y
`scripts/review-regression.py`.

**Limitación:** ninguna conocida.

## Ciclo 2 — partir módulos grandes (2026-10)

**Problema:** `executor.rs` (~1 960 líneas) y `connection_api.rs` (~3 400)
mezclaban responsabilidades en un solo archivo.

**Archivos:**

- `jaiba-runtime/src/engine/executor/`: `mod.rs` (bucle de `FlowEngine`),
  `scheduler.rs`, `retry.rs`, `routing.rs`, `metrics_sync.rs`,
  `partition.rs`, `validation.rs`, `tests.rs`.
- `jaiba-server/src/connection_api/`: `mod.rs` (endpoints y registro),
  `plugins/mod.rs` (helpers comunes), un archivo por motor en `plugins/`,
  `tests.rs` (integración opt-in).

**Decisiones:** movimiento literal con `git mv` para conservar historial; sin
cambios de comportamiento. Visibilidad mínima (`pub(super)` o
`pub(in crate::connection_api)`). Los plugins de drivers opcionales quedan
detrás de su feature a nivel de módulo. De paso se dejó de versionar
`yolo11n/`, `yolo11n.pt` y `runs/` (siguen en el historial de git).

**Prueba:** `cargo fmt`, Clippy del workspace, 191 tests, smoke/chaos JME y
regresiones; `connection_api` además con Clippy y tests por cada feature de
driver.

**Limitación:** faltan `observability.rs` y `FlowBuilder.tsx`. Con
`mongodb-driver` o `kafka-driver`, Clippy marca avisos previos al corte
(`derivable_impls` en `consume_kafka.rs` y `put_mongodb.rs`,
`collapsible_if` en `connection_api/mod.rs`).

## Consola: catálogo según el motor y avisos claros (2026-10)

1. **Problema:** el diseñador ofrecía nodos que el binario no podía ejecutar
   (Oracle, SQL Server, MongoDB, Kafka sin su feature) y fallaban al final; si
   el motor no arrancaba (p. ej. puerto ocupado) la UI se quedaba en
   "Conectando" sin explicación; las conexiones en memoria se perdían al
   reiniciar sin aviso en pantalla; la CLI imprimía errores como
   `Error: Io(Os { … })`.
2. **Archivos:** `jaiba-runtime/src/engine/registry.rs`
   (`processor_types()`), `jaiba-server/src/observability.rs`
   (`GET /api/v1/capabilities`, `listen_error`), `jaiba-cli`
   (`run_and_report`, `load_config` con la ruta del archivo); UI:
   `engineState.ts` (nuevo), `api.ts` (`engineHealth`, `capabilities`),
   `components.tsx` (`EngineNotice`), `builder/catalog.ts`
   (`requiredFeature`, `unavailableHint`), `builder/yaml.ts`
   (`availableProcessorTypes`), `builder/FlowBuilder.tsx`, `vite.config.ts`
   (`JAIBA_API_UPSTREAM`).
3. **Decisión:** el motor es la fuente de verdad de lo que puede ejecutar
   (`default_registry()`, el mismo que usa `/api/v1/flows/validate`); si el
   endpoint no responde (motor anterior), la UI muestra el catálogo completo
   como antes. `requiredFeature` replica la regla de `unknown_processor_hint`.
4. **Prueba:** `processor_types_lists_registered_types_sorted` y
   `busy_port_error_names_address_and_override`; en navegador: motor sin
   features → 6 nodos deshabilitados con su feature; UI apuntando a otro
   servicio en 9090 → aviso "responde otro programa"; Conexiones sin
   `JAIBA_MASTER_KEY` → aviso de memoria.
5. **Limitación:** procesadores añadidos en código con `with_registry` no
   aparecen en `processor_types` (el servidor usa `default_registry()`); el
   catálogo de la UI sigue siendo estático (solo se deshabilitan entradas, no
   se descubren nuevas).

## CLI: ayuda, versión y `jaiba validate` (v0.2.1, 2026-10)

1. **Problema:** con el binario publicado en v0.2.0, `jaiba --help` y
   `--version` fallaban (se tomaban como ruta de archivo) y sin argumentos se
   ejecutaba `examples/basic-flow.yaml` en silencio; no había forma de revisar
   un flujo sin ejecutarlo.
2. **Archivos:** `jaiba-cli/src/lib.rs` (`help_text`, `validate_command`,
   `missing_env_vars`, `read_flow`), `jaiba-server` (`parse_and_validate`
   pasa a ser público), versión 0.2.1 en `Cargo.toml`, `package.json`,
   `tauri.conf.json` y locks.
3. **Decisión:** sin dependencia nueva (se mantiene el parseo manual);
   `validate` reutiliza `parse_and_validate` del servidor para que CLI y
   `POST /api/v1/flows/validate` no diverjan. Las variables de entorno sin
   definir son aviso, no error (pueden definirse en el entorno de ejecución).
   Un YAML sin `processors` se reporta como "no es un flujo" (políticas JME,
   `prometheus.yaml`).
4. **Prueba:** `every_example_flow_validates_or_names_its_missing_feature`
   recorre `examples/` (falla si un ejemplo se rompe), más tests de ayuda,
   errores y variables.
5. **Limitación:** la ayuda es texto fijo; un subcomando nuevo hay que
   añadirlo a mano en `help_text` (el test comprueba los actuales).

## Consola incrustada en `jaiba serve` (2026-10)

1. **Problema:** para usar la consola había que levantar un segundo servicio
   (Nginx en Docker o Vite) y configurar el proxy hacia la API.
2. **Archivos:** `crates/jaiba-server/src/ui.rs` (nuevo; fallback del router),
   `observability.rs` (`.fallback(crate::ui::console)` y log `consola web: …`),
   features `embedded-ui` en `jaiba-server` y `jaiba-cli` (`rust-embed`),
   `release.yml`, `ci.yml`, `deploy/Dockerfile.jaiba-serve` (etapa Node).
3. **Decisiones:**
   - La incrustación es una feature de compilación: `cargo build` sin Node
     sigue funcionando; release y la imagen `jaiba-serve` la activan.
   - Se sirve en el fallback, así que ninguna ruta de API cambia; `api/…`
     desconocido sigue siendo 404 y no cae en `index.html`.
   - `index.html` recibe `window.__JAIBA_API_BASE__=window.location.origin`:
     mismo origen, sin CORS ni proxy. Los estáticos no exigen token (no
     contienen datos); la API sigue con su autenticación.
   - `assets/` (nombres con hash) con caché `immutable`; el resto `no-cache`.
     Rutas sin extensión devuelven `index.html` (navegación del SPA).
   - Sin la feature, `/` responde 404 con una página que explica cómo
     obtener la consola.
4. **Prueba:** tests de `ui::` con y sin la feature (CI corre
   `npm run build` + clippy/test con `embedded-ui`); el release arranca
   `jaiba serve` y verifica que `/` sirve la consola.
5. **Limitación:** `cargo run` normal no incluye la consola; hay que compilar
   `apps/jaiba-ui` y activar `embedded-ui`. El binario crece ~4 MB
   (lo que pesa `dist/`).

## Windows en el release (2026-10)

1. **Problema:** para tener el `.exe` había que instalar Rust, Node, Build
   Tools y CMake y compilar; además ningún build de Windows se probaba.
2. **Archivos:** `.github/workflows/windows.yml` (nuevo; PRs con cambios de
   Rust/desktop, manual y `workflow_call`), `release.yml` (lo llama y sube sus
   artefactos), `apps/jaiba-ui/src-tauri/src/sidecar.rs` (`CREATE_NO_WINDOW`).
3. **Decisiones:**
   - `jaiba.exe` con `all-drivers,embedded-ui`: en Windows no hay imagen
     Docker, así que el binario trae todo.
   - Solo instalador NSIS (un `-setup.exe`); MSI no aporta para este uso.
   - El sidecar se arranca con `CREATE_NO_WINDOW`: la app no tiene consola y
     Windows abriría una ventana negra para `jaiba.exe`.
   - Sin firma de código por ahora (SmartScreen lo advierte; documentado).
4. **Prueba:** el job ejecuta `examples/smoke.yaml`, arranca `jaiba.exe serve`,
   comprueba la consola en `/` y que `/api/v1/capabilities` incluya
   `query_oracle`, `query_sqlserver`, `query_mongodb` y `consume_kafka`.
5. **Limitación:** no se prueba el instalador ni la app ya instalada (solo que
   se genera); Oracle Instant Client lo instala el usuario.

## Plugins externos: nodo `external_plugin` (2026-10)

1. **Problema:** el protocolo de plugins aislados (`jaiba_plugin_sdk::isolated`,
   JSON Lines v1) existía pero ningún nodo lo usaba: un tercero no podía meter
   su plugin en un flujo sin recompilar Jaiba.
2. **Archivos:** `crates/jaiba-runtime/src/processors/external_plugin.rs`
   (nuevo), registro en `processors/mod.rs`, guardas en `write_file.rs` y
   `ai_prep/export_manifest.rs`, `tokio` con `process`/`io-util`, ejemplo
   `examples/plugins/` (Python), catálogo de la UI, ayuda del CLI,
   `docs/plugins.md`.
3. **Decisiones:**
   - El flujo nombra el plugin (`plugin: <id>`), nunca el comando: los
     plugins vienen de `JAIBA_PLUGIN_DIR/<id>/plugin.yaml`, que define el
     operador. Sin esa variable están desactivados. Publicar un flujo por la
     API no equivale a ejecutar programas en el servidor.
   - `write_file` y `ai_export_manifest` rechazan rutas dentro de esa carpeta;
     la defensa real es montarla de solo lectura (Jaiba avisa si es
     escribible).
   - Un proceso por nodo, reutilizado, una petición a la vez; `describe` al
     arrancar valida protocolo e id. Timeout propio por petición: si vence,
     o la respuesta no cumple el protocolo, se mata el proceso y el siguiente
     paquete arranca uno nuevo. Si el motor cancela una petición a medias
     (su `timeout_ms` de nodo), `in_flight` fuerza el reinicio.
   - E/S async con `tokio::process` (el `IsolatedPluginProcess` del SDK es
     síncrono); se reutilizan los tipos y validaciones del SDK.
   - `config` por defecto `{}` para que el plugin no reciba `null`.
4. **Prueba:** tests del módulo con un plugin Python (ok, error del plugin,
   cuelgue con timeout y reinicio), ids inválidos y rutas dentro de la
   carpeta; paso de CI que corre `examples/plugins/mayusculas-flow.yaml`.
5. **Limitación:** sin pool (una petición a la vez por nodo); `retryable`
   del error no cambia los reintentos; sin contenedores, catálogo ni Wasm
   todavía.

## Plugins externos en contenedor (2026-10)

1. **Problema:** un plugin de proceso corre con los permisos de Jaiba y exige
   instalar su lenguaje en el servidor.
2. **Archivos:** `external_plugin.rs` (manifiesto con `container`,
   `container_run_args`, `ContainerGuard`), `examples/plugins/mayusculas/Dockerfile`,
   `examples/plugins-container/`, ayuda del CLI (`JAIBA_CONTAINER_RUNTIME`),
   paso de CI, `docs/plugins.md`.
3. **Decisiones:**
   - `plugin.yaml` lleva `command` o `container` (exactamente uno); el flujo
     no cambia. El runtime lo elige el operador (`JAIBA_CONTAINER_RUNTIME`,
     por defecto `docker`); Podman acepta los mismos argumentos.
   - Encerrado por defecto: `--network=none`, `--read-only` con `/tmp` en
     tmpfs, `--cap-drop=ALL`, `no-new-privileges`, memoria sin swap, CPU y
     pids limitados. `network: bridge` es la única apertura.
   - `--pull=never`: ejecutar un flujo nunca descarga código; instalar el
     plugin es `docker pull`/`build`. El error de arranque sugiere revisar la
     imagen.
   - Matar `docker run` no detiene un contenedor colgado: `ContainerGuard`
     hace `rm -f <nombre>` al soltar el proceso (en un hilo, sin bloquear el
     runtime). Etiqueta `jaiba.plugin` para encontrar restos.
4. **Prueba:** tests de validación del manifiesto y de los argumentos; test
   Unix con un runtime falso que arranca el plugin Python y registra el
   `rm -f` tras un cuelgue; paso de CI con Docker real que construye la
   imagen del ejemplo, corre el flujo y comprueba que no quedan contenedores.
   A mano: Docker 29 y Podman 5 (flujo, imagen inexistente y cuelgue).
5. **Limitación:** sin montajes ni variables de entorno hacia el contenedor;
   no se verifica la firma/digest de la imagen; Windows con Docker Desktop
   no probado en CI; si Jaiba muere con SIGKILL, un contenedor colgado que
   no lea stdin queda hasta limpiarlo por etiqueta.

## Catálogo comunitario de plugins: `jaiba plugins` (2026-10)

1. **Problema:** instalar un plugin era copiar carpetas a mano, sin versiones
   ni verificación de lo descargado.
2. **Archivos:** `crates/jaiba-runtime/src/plugin_catalog.rs` (nuevo),
   `external_plugin.rs` (manifiesto serializable, `PluginManifest::validate`,
   `pinned_by_digest`), `crates/jaiba-cli/src/plugins.rs` (nuevo) y ayuda,
   `catalog/plugins.json` + `catalog/README.md`, `.gitattributes`,
   `scripts/smoke-plugin-catalog.sh`, paso de CI, `docs/plugins.md`.
3. **Decisiones:**
   - Catálogo JSON (`catalog_version: 1`) en este repo; URL por defecto la
     rama `main` en raw.githubusercontent.com, otra con `--catalog` o
     `JAIBA_PLUGIN_CATALOG` (URL o archivo). Puede mudarse a un repo propio
     sin cambiar el formato.
   - Proceso: archivos sueltos con URL y `sha256` (sin `.tar.gz`: nada que
     extraer, sin dependencias nuevas). URLs relativas al catálogo. Contenedor:
     imagen obligatoriamente `@sha256:` y `<runtime> pull` del digest.
   - Jaiba escribe `plugin.yaml` desde el catálogo y lo valida con las mismas
     reglas que `external_plugin`; `.jaiba-install.json` guarda origen,
     versión y checksums.
   - Instalación en `.install-<id>-<uuid>` dentro de `JAIBA_PLUGIN_DIR`; si
     algo falla se borra y la versión previa queda intacta. Reemplazo con
     `--force` (aparta la anterior y la restaura si el movimiento falla).
   - Solo HTTPS (HTTP a loopback para pruebas), también en redirecciones;
     límites de 1 MiB (catálogo) y 64 MiB (archivo). Versiones
     `MAYOR.MENOR.PARCHE`; sin `@versión` se toma la mayor.
   - `.gitattributes`: `examples/plugins/** -text` para que el checkout en
     Windows no cambie los `sha256`.
4. **Prueba:** tests del módulo (entradas inválidas, instalar / listar /
   reemplazar / borrar, checksum incorrecto sin efectos, HTTP en loopback y
   rechazo de HTTP remoto, contenedor con runtime falso que registra el
   `pull`, catálogo del repo contra sus archivos);
   `scripts/smoke-plugin-catalog.sh` en CI: catálogo del repo + flujo, y
   registro local `registry:2` con la imagen del ejemplo fijada por digest.
5. **Limitación:** sin firmas del catálogo (la confianza es la revisión del
   PR), sin `update`, sin API/consola; `remove` no borra la imagen del
   contenedor; la imagen de `mayusculas` no está publicada en GHCR (el
   catálogo solo trae la versión de proceso).

## Trabajo posterior a la fase 9 / 10A–10C

- procesadores ejecutables de consulta para MySQL y SQL Server;
- catálogo firmado y política de permisos para plugins de terceros;
- transporte WebAssembly opcional;
- Phase 8 en CI con servicios efímeros (hoy: self-hosted / manual);
- firmado/auto-update del instalador desktop;
- SSO / gestión de usuarios en UI.

## Desktop con todos los conectores (2026-10)

1. **Problema:** el `.exe` del desktop solo trae los conectores con que se
   compiló el sidecar, y había que recordar cinco `--features`.
2. **Archivos:** feature `all-drivers` en `crates/jaiba-cli/Cargo.toml`,
   script `desktop:build:full` en `apps/jaiba-ui/package.json`,
   `scripts/prepare-desktop-sidecar.mjs` (respeta `CARGO_TARGET_DIR`), guía
   `docs/windows-native-and-wsl.md`.
3. **Decisión:** `all-drivers` solo agrupa features existentes; el release
   Linux y la imagen siguen en `release-core` (sin Oracle/Kafka). Oracle
   Instant Client no se empaqueta: lo instala quien use Oracle.
4. **Prueba:** nuevo job de CI `clippy (all-drivers)` (workspace con
   `--features jaiba-cli/all-drivers`). Al activarlo salieron cuatro avisos
   que el CI no veía (dos `Default` derivables, un `MutexGuard` cruzando un
   `await` en un test de Kafka y un `if` anidado en `connection_api`); ya
   corregidos.
5. **Limitación:** no hay build de Windows en CI; Kafka en Windows requiere
   CMake y no está validado allí.

### Oracle a Beta (v0.8.0)

1. **Problema:** Oracle era un prototipo: una conexión nueva por llamada,
   lectura completa en memoria, una sentencia por fila, `NUMBER` a `f64`
   (perdía precisión), fechas según la configuración regional del servidor,
   ningún timeout y todos los errores se reintentaban igual. Tampoco había
   pruebas reales en CI.
2. **Archivos:** `crates/jaiba-runtime/src/connectors/oracle.rs` (pool, stream,
   array DML, tipos, cancelación, clasificación de errores) y
   `oracle_live_tests.rs`; `processors/query_oracle.rs` (`call_timeout_ms`,
   consumo del stream); `error.rs` (`FlowError::DatabaseRejected` y
   `is_retryable`); `engine/executor/retry.rs`; `processors/put_database.rs`
   (un rechazo no abre el circuit breaker); `engine/connections.rs` (pasa
   `max_connections`/`acquire_timeout` al pool); `scripts/smoke-oracle.sh`;
   `.github/workflows/oracle.yml`; `docs/oracle.md` y criterios de madurez en
   `docs/product-roadmap.md`.
3. **Decisiones:**
   - Pool perezoso: el flow arranca aunque Oracle esté caído; cada llamada
     reintenta crearlo.
   - Streaming con canal acotado a dos lotes desde el hilo bloqueante; al soltar
     el stream o el futuro se llama a `break_execution` en otro hilo.
   - Array DML con un tipo por columna fijado antes de la primera fila
     (`Batch::set_type`); los tipos JSON mezclados se ensanchan a texto y los
     decimales viajan como texto exacto.
   - `NUMBER` se lee como texto y se decide aquí: entero, decimal exacto o
     texto. Fechas y binarios con conversión propia (ISO 8601, base64), sin
     depender de NLS.
   - Los códigos ORA de sesión/red son reintentables; cualquier otro ORA es
     `DatabaseRejected` y va a `failure` sin reintentar. El motor ahora respeta
     `FlowError::is_retryable` para todos los nodos.
   - Las credenciales de la URL se decodifican: `Url::set_password` del
     Connection Manager las codificaba y el conector anterior las enviaba así.
4. **Prueba:** 6 pruebas reales del conector (tipos, `BOOLEAN` en 23ai, streaming de 25 000 filas,
   sesión liberada tras cancelar con un pool de 1, timeout por llamada,
   clasificación de errores, 2 500 filas por array DML con CLOB > 32 KB, MERGE,
   rollback de todo el paquete) más el smoke con el binario, verdes contra XE 21
   y Free 23ai. En CI: `oracle.yml` (nightly, PR que tocan Oracle, label
   `oracle`).
5. **Limitación:** sin claves ni índices en el descriptor, sin escritura de
   `RAW`/`BLOB` desde base64, sin TCPS/wallet, lectura *al menos una vez* tras
   reintento. `rust-oracle` 0.6 no define columnas `JSON`/`VECTOR` de 23ai: el
   error se reporta como rechazo con la indicación de usar `JSON_SERIALIZE` o
   `FROM_VECTOR`.

### Instant Client gestionado: `jaiba oracle-client` (v0.8.0)

1. **Problema:** usar Oracle exigía descargar Instant Client a mano y exportar
   `LD_LIBRARY_PATH`/`PATH` antes de cada `jaiba`; el binario Linux del
   release ni siquiera incluía Oracle.
2. **Archivos:** `crates/jaiba-runtime/src/connectors/oracle_client.rs`
   (paquetes fijos, descarga, extracción, carga), `crates/jaiba-cli/src/oracle_client.rs`
   (`install`/`status`/`remove`), `crates/jaiba-cli/src/lib.rs`
   (`prepare_process`) y los dos `bin/`; `connectors/oracle.rs` y
   `jaiba-server/.../plugins/oracle.rs` inicializan ODPI-C con el cliente;
   `.github/workflows/oracle.yml` y `release.yml`.
3. **Decisiones:**
   - Basic Light 23.26.3 con URL y SHA-256 fijos en el código (Linux x64/arm64,
     Windows x64), solo https, tope de 512 MB; se extrae en una carpeta
     temporal y se cambia de golpe por la anterior.
   - La extracción quita la carpeta `instantclient_*`, ignora `META-INF` y solo
     acepta enlaces simbólicos a archivos hermanos.
   - Linux: `oracle_client_lib_dir` no basta porque las bibliotecas de Oracle no
     traen `RUNPATH` (falla `libnnz.so`). `jaiba` se relanza con `exec` al
     arrancar, antes de crear el runtime de Tokio, con la carpeta al frente de
     `LD_LIBRARY_PATH`; una variable de guarda evita bucles. Windows/macOS usan
     `oracle_client_lib_dir`.
   - La inicialización de ODPI-C no se cachea si falla: en Windows basta
     instalar, sin reiniciar.
   - El binario Linux del release se compila con `oracle-driver` (ODPI-C carga
     el cliente en tiempo de ejecución: sin dependencias nuevas del sistema).
4. **Prueba:** unitarias de extracción y enlaces inseguros; instalación real
   desde oracle.com, `status` con "Cargado: sí, cliente 23.26.3.0.0", smoke
   Oracle → Oracle y prueba de conexión de `jaiba serve`, todo sin
   `LD_LIBRARY_PATH`. CI `oracle.yml` instala el cliente con el comando y corre
   el smoke sin la variable.
5. **Limitación:** sin paquete para macOS (Oracle publica DMG, no ZIP); Linux
   necesita `libaio` del sistema; los binarios de prueba de cargo no se
   relanzan y siguen necesitando `LD_LIBRARY_PATH`; la imagen
   `jaiba-serve` sigue sin Oracle.

### MySQL a Beta (v0.9.0)

1. **Problema:** MySQL figuraba como Beta sin pruebas reales en CI: todos los
   errores se reintentaban igual (una clave duplicada se repetía hasta agotar
   reintentos y abría el circuit breaker), no había timeout ni cancelación en
   el servidor, `DECIMAL` con decimales pasaba por `f64` y un `TIME` negativo
   o mayor de 24 h hacía fallar la lectura.
2. **Archivos:** `crates/jaiba-runtime/src/connectors/mysql.rs` (clasificación
   de errores, `MySqlSession` con `KILL QUERY`, timeout por llamada) y
   `mysql_live_tests.rs`; `processors/query_mysql.rs` (`call_timeout_ms`,
   tipos) y `query_mysql_live_tests.rs`; `connectors/values.rs` (conversión
   de números y base64 compartida con Oracle); `jaiba-server/.../tests.rs`
   (prueba `mysql_real` sin base ni versión fijas); `scripts/smoke-mysql.sh`;
   `.github/workflows/mysql.yml`; `docs/mysql.md`.
3. **Decisiones:**
   - Cada lectura o escritura toma una conexión del pool y guarda su
     `CONNECTION_ID()`. Si se suelta sin terminar (timeout, cancelación, parada
     del flow), la conexión se cierra en lugar de volver al pool y se lanza
     `KILL QUERY` desde otra conexión.
   - Errores de sesión/red y los números de servidor transitorios (lock wait,
     deadlock, demasiadas conexiones…) son reintentables; cualquier otro error
     del servidor o de decodificación es `DatabaseRejected`.
   - Lectura: `DECIMAL` exacto (número si se representa sin pérdida, si no
     texto), `DATETIME`/`TIMESTAMP` como `YYYY-MM-DDTHH:MM:SS[.f]` sin zona
     (MySQL rechaza `Z` y MariaDB rechaza `+00:00`; ambos aceptan la `T`),
     `TIME` como duración con signo. Son **cambios de formato** respecto a
     v0.8, listados con su alternativa en `docs/mysql.md` y
     `docs/actualizar.md`.
   - sqlx 0.8.6 marca como binarias las columnas de texto `_bin` (y el `JSON`
     de MariaDB) sin exponer el charset: se mantiene la lectura de bytes UTF-8
     válidos como texto y el resto como base64; `GEOMETRY` pasa a ser siempre
     base64.
   - `MySqlTime::is_negative()` de sqlx 0.8.6 está invertido; se usa
     `sign().is_negative()`.
4. **Prueba:** pruebas reales del conector y de `query_mysql` (tipos, streaming
   de 25 000 filas, sesión liberada tras cancelar con un pool de 1, timeout por
   llamada que mata la consulta, clasificación de errores, 2 500 filas en
   lotes, upsert, rollback de todo el paquete), `mysql_real` del Connection
   Manager y el smoke con el binario, verdes contra MySQL 8.4 y MariaDB 11.4.
   En CI: `mysql.yml` (nightly, PR que tocan MySQL, label `mysql`).
5. **Limitación:** fechas cero no se leen; un `BLOB` con bytes UTF-8 válidos se
   lee como texto; el upsert usa `VALUES(col)` (obsoleto en MySQL 8.0.20+, pero
   es lo que acepta MariaDB); lectura *al menos una vez* tras reintento.

### MongoDB a Beta (v0.9.0)

1. **Problema:** MongoDB figuraba como Beta sin pruebas reales en CI. Todo error
   era `DatabaseConnector` (una clave duplicada se reintentaba y abría el
   circuit breaker; un registro inválido salía como `Configuration`, que
   también se reintenta), no había timeout, el upsert hacía un `replaceOne`
   por documento, `max_connections` no llegaba al pool y la salida en Extended
   JSON relajado perdía el ancho de los enteros al volver a MongoDB.
2. **Archivos:** `crates/jaiba-runtime/src/processors/mongodb_support.rs`
   (clasificación, timeout, `JsonFormat`), `query_mongodb.rs`
   (`call_timeout_ms`, `max_time_ms`, `json_format`, comentario por nodo) y
   `query_mongodb_live_tests.rs`; `put_mongodb.rs` (upsert con el comando
   `update` por lotes, circuito solo con errores reintentables) y
   `put_mongodb_live_tests.rs`; `engine/connections.rs` (`maxPoolSize` desde
   `max_connections`, `appName`); `jaiba-server/.../tests.rs`;
   `scripts/smoke-mongodb.sh`; `.github/workflows/mongodb.yml`;
   `docs/mongodb.md`.
3. **Decisiones:**
   - Reintentables: errores de red, selección de servidor, etiquetas
     `RetryableWriteError`/`TransientTransactionError`, write concern y una
     lista de códigos (cambio de primario, `WriteConflict`, `MaxTimeMSExpired`,
     cursor perdido…). El resto (`11000`, `121`, consultas inválidas,
     credenciales) es `DatabaseRejected`.
   - Upsert con `run_command({update, updates: [{q, u, upsert}]})`: funciona en
     cualquier versión (el `bulkWrite` del cliente exige 8.0). La respuesta trae
     `writeErrors` con `ok: 1`, así que se interpreta aquí; los lotes se parten
     a ~15 MiB.
   - Cancelación: al soltar el futuro el driver cierra la conexión y `mongod`
     interrumpe la lectura; la prueba lo comprueba con `$currentOp` usando el
     comentario `jaiba:<nodo>`.
   - `json_format`: `relaxed` sigue por defecto (sin cambio para flows
     existentes); `canonical` para copia exacta; `plain` para SQL/CSV con
     `Decimal128` exacto vía `connectors/values.rs`.
4. **Prueba:** 11 pruebas reales (tipos en los tres formatos, streaming de
   25 000 documentos, cancelación y timeout comprobados en el servidor,
   `maxTimeMS`, consulta inválida, upsert de 2 500 documentos idempotente,
   división de lotes grandes, copia exacta, clasificación de errores reales),
   las dos del Connection Manager y el smoke con el binario (5 000 documentos
   idénticos, tipos incluidos), verdes contra MongoDB 7.0 y 8.0. En CI:
   `mongodb.yml` (nightly, PR que tocan MongoDB, label `mongodb`).
5. **Limitación:** sin transacciones (requieren replica set) ni pruebas contra
   replica set; una escritura interrumpida por timeout puede completarse en el
   servidor; sin `aggregate` como fuente; lectura *al menos una vez* tras
   reintento.

### Kafka a Beta (v0.9.0)

1. **Problema:** Kafka figuraba como Beta sin pruebas reales en CI. Un JSON
   inválido en `consume_kafka` abortaba el ciclo sin confirmar el offset (el
   grupo se quedaba atascado en el mensaje envenenado); todo error de broker
   era `MessageConnector` (se reintentaba y abría el circuit breaker); la
   publicación de un paquete con N registros era estrictamente secuencial.
2. **Archivos:** `kafka_support.rs` (clasificación, circuit),
   `consume_kafka.rs` (poison → `failure` + commit), `publish_kafka.rs`
   (confirmaciones en paralelo), `*_live_tests.rs`, `scripts/smoke-kafka.sh`,
   `.github/workflows/kafka.yml`, `docs/kafka.md`.
3. **Decisiones:**
   - `MessageRejected` (nueva variante no reintentable) para tamaño,
     autorización, formato y política; el poison de decode no abre el circuit.
   - Commit tras emitir por `success` **o** `failure`: at-least-once sin
     bloquear el grupo.
   - Publicación: se encola en orden y se espera con `join_all` (idempotencia +
     `acks=all` conservan el orden por clave).
   - CI con Redpanda (API Kafka) y Apache Kafka 3.9.
4. **Prueba:** 17 tests (unitarios + live: publish/consume, poison con segundo
   ciclo vacío, paralelo de 250, oversized → rejected, fail-broker,
   throughput 100) y smoke de 200 mensajes con el binario, verdes en local
   contra Redpanda. En CI: `kafka.yml`.
5. **Limitación:** sin TLS/SASL; commit no espera al destino aguas abajo; sin
   pause por backpressure ni pruebas de rebalanceo.

### Licencias: LICENSE, THIRD_PARTY_LICENSES.md y gate en CI (v0.9.0)

1. **Problema:** el proyecto declaraba Apache-2.0 en `Cargo.toml` pero no tenía
   archivo `LICENSE`, y los binarios publicados (con unos 700 crates, 25
   paquetes npm y librdkafka, curl, zlib, AWS-LC y ODPI-C compilados dentro) no
   incluían los textos de licencia que piden MIT, BSD y Apache-2.0 al
   redistribuir.
2. **Archivos:** `LICENSE`; `THIRD_PARTY_LICENSES.md` (generado);
   `scripts/third-party-licenses.py`; `about.toml`; `deny.toml`;
   `licenses/webview2-sdk-LICENSE.txt`; `ci.yml` (job de licencias);
   `release.yml`, `windows.yml`, `tauri.conf.json` y
   `deploy/Dockerfile.jaiba-serve` (los dos archivos en cada artefacto);
   `apps/jaiba-ui/package.json`; `docs/licencias.md`.
3. **Decisiones:**
   - Un solo archivo para todos los artefactos, generado sobre el superconjunto
     (todos los drivers, Linux y Windows, escritorio y consola).
   - `cargo-about` con `--offline` para que el resultado sea reproducible (no
     consulta ClearlyDefined). A `ring` se le fijan sus dos archivos de
     licencia por checksum; sin eso tomaba encabezados de código como
     licencias.
   - Las bibliotecas de C se listan a mano (`NATIVE_LIBRARIES`) porque
     cargo-about solo ve la licencia del crate que las envuelve. El SDK de
     WebView2 no trae su licencia en el crate: se guardó la del paquete NuGet
     1.0.3650.58.
   - El escritorio instala los archivos como recursos; sin `licenseFile`, para
     no añadir una página de "acepto" que Apache-2.0 no pide.
4. **Prueba:** `python3 scripts/third-party-licenses.py --check` y
   `cargo deny check licenses` (motor y escritorio) en el job
   **Licencias de terceros**; quitar `MPL-2.0` de `deny.toml` hace fallar al
   escritorio.
5. **Limitación:** solo se revisan licencias, no avisos de seguridad
   (`cargo deny check advisories`). La lista de bibliotecas de C y la nota
   sobre OpenSSL/SQLite se mantienen a mano.

### Retry / OutputSender / entrega (core hardening)

1. **Problema:** no estaba explícito que `OutputSender` es aceptación de canal
   (nivel A), no commit de sink (C), ni que emitir y luego fallar reintentable
   puede duplicar emisiones en el reintento.
2. **Archivos:** `crates/jaiba-runtime/src/engine/executor/tests.rs`,
   `crates/jaiba-runtime/src/engine/processor.rs`,
   `docs/core-hardening-delivery.md`, enlaces en `architecture.md` / `README`.
3. **Decisiones:** sin cambiar semántica — sigue at-least-once; no exactly-once.
   Se documentan niveles A/B/C y se añaden tests E1–E3 (duplicado esperado,
   rechazo no reintentable, backoff/`attempt`).
4. **Prueba:** `cargo test -p jaiba-runtime --lib emit_then_retryable
   non_retryable_rejection_does_not_retry retry_backoff_increments_attempt`.
5. **Limitación:** no hay rollback de emisiones; sinks deben ser idempotentes;
   no toca supervisor ni soak.

### Scheduler: particiones, equidad y no-deadlock (core hardening)

1. **Problema:** el contrato de `ordering: partitioned`, la rotación de trabajo
   diferido y el fan-out bajo `queue_capacity` justo no estaban cubiertos por
   tests de aceptación explícitos (solo preserve/global/many-sources).
2. **Archivos:** `crates/jaiba-runtime/src/engine/executor/tests.rs`,
   `docs/core-hardening-scheduler.md`, enlaces en `architecture.md` / `README`.
3. **Decisiones:** sin cambio de algoritmo del scheduler; se documenta el
   contrato y se añaden probes S1–S4. Si un test fallara, el arreglo sería
   mínimo en `scheduler.rs`.
4. **Prueba:** `cargo test -p jaiba-runtime --lib partitioned_ slow_processor fan_out_under`.
5. **Limitación:** no mide fairness cuantitativa a largo plazo (soak); no toca
   retry/OutputSender ni supervisor.

### PacketRepository: transiciones atómicas y reconcile (core hardening)

1. **Problema:** `claim` / `complete` / `fail` / `recover` / `requeue` escribían
   el estado de cola y el evento de provenance en pasos separados; un kill a
   mitad dejaba `RUNNING`/`COMPLETED` sin auditoría. Además `content.put` en
   `enqueue` puede dejar blobs huérfanos si falla el `COMMIT` SQL (C1).
2. **Archivos:** `crates/jaiba-runtime/src/engine/repository.rs`,
   `docs/core-hardening-repository.md`, `docs/architecture.md`, `docs/README.md`.
3. **Decisiones:** misma TX SQLite para transición + provenance; `complete` y
   `fail` exigen `status = RUNNING`; `reconcile_orphans` al `open` (y API
   pública) borra `.bin` sin fila y `.tmp` viejos. La garantía sigue siendo
   at-least-once.
4. **Prueba:** tests T1–T13 en `engine::repository` (`cargo test -p jaiba-runtime --lib engine::repository`).
5. **Limitación:** no hay exactly-once; reconcile no cubre provenance huérfano
   (evento sin fila) porque el INSERT de provenance ya va en la misma TX.

## Regla para futuras implementaciones

Al terminar una característica, añadir aquí:

1. qué problema resuelve;
2. qué archivos cambió;
3. qué decisión importante se tomó;
4. cómo se prueba;
5. qué limitación permanece.

Conservar esta bitácora hace que el motivo de una implementación sobreviva
aunque pasen meses entre cambios.

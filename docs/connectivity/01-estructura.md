# 01 — Estructura del repo

Cada carpeta tiene **un trabajo**. Si no sabes dónde tocar, empieza aquí.

## `crates/` — motor

| Crate | Para qué | Apunta a |
| --- | --- | --- |
| `jaiba-core` | Tipos del flow YAML | config de processors / connections |
| `jaiba-runtime` | Ejecuta el DAG, pools, writers | `src/engine/`, `src/connectors/`, `src/processors/` |
| `jaiba-server` | API admin + Connection Manager | UI y `serve` |
| `jaiba-cli` | Binario `jaiba` / `jaiva-flow` | llama runtime + server |
| `jaiba-connection-manager` | Perfiles y secretos | server + resolver |
| `jaiba-plugin-sdk` | Contratos de plugins | tipos `ConnectionType` |
| `jaiba-memory` | JME (memoria de dominio), **Beta** | `engine.domain_memory` + nodos `memory_*` |

### Dentro de `jaiba-runtime` (lo más tocado)

| Carpeta / archivo | Apunta a |
| --- | --- |
| `engine/connections.rs` | Arma pools/writers según `type:` del YAML |
| `engine/executor/mod.rs` | `FlowEngine`: bucle principal del flujo |
| `engine/executor/scheduler.rs` | Qué trabajo arranca: concurrencia, orden, particiones, admisión de fuentes |
| `engine/executor/retry.rs` | Ejecuta un processor con timeout y reintentos |
| `engine/executor/routing.rs` | Enruta emisiones a las colas de cada conexión |
| `engine/executor/validation.rs` | Parámetros `${...}` y validación del YAML |
| `connectors/database.rs` | Contrato `DatabaseWriter` |
| `connectors/postgres.rs` … | Un writer por motor |
| `connectors/clickhouse.rs` | Sink ClickHouse (`clickhouse-driver`) |
| `processors/put_database.rs` | Nodo YAML que escribe vía writer |
| `processors/query_*.rs` | Nodos de lectura por motor |

### Dentro de `jaiba-server`

| Carpeta / archivo | Apunta a |
| --- | --- |
| `connection_api/mod.rs` | Endpoints `/api/v1/connections`, registro de plugins, validación de entrada |
| `connection_api/plugins/mod.rs` | Helpers comunes (descriptor, diagnóstico, metadatos) |
| `connection_api/plugins/postgres.rs` … | Un `ConnectionPlugin` por motor (prueba, exploración, compilación de consultas) |
| `connection_api/tests.rs` | Pruebas de integración contra motores reales (por variables `JAIBA_TEST_*`) |

### Dentro de `jaiba-memory` (JME)

| Archivo | Apunta a |
| --- | --- |
| `policy.rs` | Política YAML (`memory.version: 1`, clases, límites, rutas) |
| `manager.rs` | Ciclo de vida: Hot → Warm → Cold → Frozen, promoción y degradación |
| `hot.rs` | RAM local; desaloja por cantidad y por `max_hot_bytes` (nunca `critical`) |
| `cold.rs` | Segmentos en disco: checksum, cuota, recorte de cola y rescate de segmentos dañados |
| `warm.rs` / `redis_warm.rs` | Warm opcional (Redis con feature `redis`) |
| `frozen.rs` | Archivo de largo plazo (Frozen) |
| `sink.rs` / `deferred.rs` | Persistencia `immediate` (`persist.jsonl`) y cola `deferred` |

El runtime lo abre en `jaiba-runtime/src/engine/domain_memory.rs`.

## `apps/jaiba-ui/`

UI Angular/React del diseñador y Connection Manager.  
Habla con `jaiba-server` (API), **no** ejecuta el DAG sola.

## `examples/`

Flows listos para `cargo run -- examples/….yaml`.  
Uno por caso (postgres-write, clickhouse-write, smoke, …).

## `deploy/` + `scripts/`

Stack Docker estable y pruebas de carga. Ver [04-ops-tests.md](04-ops-tests.md).

## Siguiente

- Conexiones DB: [02-conexiones.md](02-conexiones.md)

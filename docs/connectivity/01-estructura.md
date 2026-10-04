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
| `jaiba-memory` | JME (memoria de dominio) | opcional / experimental |

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

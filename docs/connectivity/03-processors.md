# 03 — Processors (nodos del DAG)

Un processor = un paso del flow. Lista completa: [../processors.md](../processors.md).

## Los que más importan para DB

| `type:` | Archivo | Apunta a |
| --- | --- | --- |
| `put_database` | `processors/put_database.rs` | Writer de `connection:` |
| `query_postgres` | `processors/query_postgres.rs` | Pool Postgres |
| `query_mysql` | `processors/query_mysql.rs` | Pool MySQL |
| `query_oracle` | `processors/query_oracle.rs` | Feature `oracle-driver` |
| `query_sqlserver` | `processors/query_sqlserver.rs` | Feature `sqlserver-driver` |
| `generate_records` | (generate) | Datos de prueba sin DB |
| `encode_json` / `write_file` | processors varios | Salida archivo / JSON |

Registro de tipos: `crates/jaiba-runtime/src/processors/mod.rs`.

## Cómo se enlazan

En el YAML, `connections:` une `from` → `to` por `relationship`
(`success` / `failure`). Eso es el grafo; el runtime lo ejecuta en
`engine/executor/` (bucle en `mod.rs`, enrutamiento en `routing.rs`).

## Siguiente

- Ops / tests: [04-ops-tests.md](04-ops-tests.md)

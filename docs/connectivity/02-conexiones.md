# 02 — Conexiones a bases de datos

Cómo un humano sigue el hilo de una conexión de punta a punta.

## Camino feliz

```text
YAML database_connections
        │
        ▼
engine/connections.rs   (insert_database según type:)
        │
        ▼
connectors/<motor>.rs   (PostgresWriter, ClickHouseWriter, …)
        │
        ▼
processors/put_database.rs   (o query_*)
```

## Declaración en YAML

```yaml
database_connections:
  mi_destino:
    type: clickhouse       # o postgres | mysql | oracle | sqlserver | …
    url_env: CLICKHOUSE_URL
    max_connections: 4

processors:
  - id: sink
    type: put_database
    config:
      connection: mi_destino   # mismo nombre
      table: broder.events
      mode: insert
      columns:
        event_type: event_type
```

## Tabla rápida de motores

| `type:` en YAML | Feature Cargo | Archivo writer | Ejemplo |
| --- | --- | --- | --- |
| `postgres` | (siempre) | `connectors/postgres.rs` | `examples/postgres-write.yaml` |
| `mysql` / `mariadb` | (siempre) | `connectors/mysql.rs` | `examples/mysql-write.yaml` |
| `oracle` | `oracle-driver` | `connectors/oracle.rs` | `examples/oracle-write.yaml` |
| `sqlserver` | `sqlserver-driver` | `connectors/sqlserver.rs` | `examples/sqlserver-write.yaml` |
| `clickhouse` | `clickhouse-driver` | `connectors/clickhouse.rs` | `examples/clickhouse-write.yaml` |

ClickHouse MVP: solo `mode: insert` (sin upsert).

## Connection Manager (UI)

Perfiles guardados (alias) → `engine/resolver.rs` arma la URL → mismo
`insert_database`. Docs: [../connection-manager.md](../connection-manager.md).

## Archivos ancla

- Registro: `crates/jaiba-runtime/src/engine/connections.rs`
- Contrato: `crates/jaiba-runtime/src/connectors/database.rs`
- Sink: `crates/jaiba-runtime/src/processors/put_database.rs`

## Siguiente

- Processors: [03-processors.md](03-processors.md)

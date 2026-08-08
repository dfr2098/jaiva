# Fase 8: pruebas de integración con entorno de pruebas

Suite opt-in de integración, fallos y smoke de rendimiento contra un
**entorno de pruebas ya levantado** (PostgreSQL, Kafka, MongoDB y SQL Server).
No crea contenedores ni usa testcontainers.

> Numeración: esta es la Fase 8 del roadmap de producto (tras `consume_kafka`).
> El documento histórico [priority-8-visual-console.md](priority-8-visual-console.md)
> describe la consola visual (otra numeración) y está cubierta por `apps/jaiba-ui`.

## Precondiciones

Servicios accesibles en el host de pruebas (valores por defecto del harness):

| Servicio | Endpoint por defecto | Feature Cargo |
|---|---|---|
| PostgreSQL | `127.0.0.1:55432` | (incluido) |
| Kafka | `127.0.0.1:29092` | `kafka-driver` |
| MongoDB | `127.0.0.1:27018` | `mongodb-driver` |
| SQL Server | `127.0.0.1:11433` | `sqlserver-driver` |

El script comprueba que los puertos respondan antes de lanzar `cargo test`.

## Qué realiza la suite

1. Comprueba que Postgres, Kafka, MongoDB y SQL Server respondan en los puertos configurados.
2. Ejecuta publicación y consumo Kafka reales (`publish_kafka` / `consume_kafka`).
3. Mide un smoke de 100 mensajes (límite generoso de 30 s).
4. Verifica fallo controlado contra un broker inalcanzable (sin panic).
5. Valida reintentos → dead-letter → requeue en el repositorio local.
6. Ejecuta el recorrido Connection Manager + consulta + flujo contra Postgres.
7. Ejecuta Connection Manager + metadatos MongoDB y el flujo `query_mongodb` → `put_mongodb` (upsert idempotente).
8. Ejecuta Connection Manager + diagnóstico/metadatos SQL Server.

## Ejecución

```bash
# Desde la raíz de Jaiva
export JAIBA_TEST_POSTGRES_PASSWORD='tu-password-postgres'
export JAIBA_TEST_MONGODB_PASSWORD='tu-password-mongo'
export JAIBA_TEST_SQLSERVER_PASSWORD='tu-password-sa'
./scripts/phase8-integration.sh
```

También admite `--password <postgres_password>`.

Si existe un `.env` del entorno de pruebas con `POSTGRES_APP_PASSWORD`, el
script puede tomarlo automáticamente (`JAIBA_TEST_ENV` o rutas conocidas bajo
el árbol del entorno).

Variables relevantes:

| Variable | Default / notas |
|---|---|
| `JAIBA_TEST_KAFKA_BROKERS` | `127.0.0.1:29092` |
| `JAIBA_TEST_KAFKA_FAIL_BROKER` | `127.0.0.1:1` (fallo controlado) |
| `JAIBA_TEST_POSTGRES_HOST` / `PORT` / `DATABASE` / `USER` / `PASSWORD` / `URL` | `127.0.0.1`, `55432`, `dma`, `dma` |
| `JAIBA_TEST_MONGODB_HOST` / `PORT` / `DATABASE` / `USER` / `PASSWORD` / `URL` | `127.0.0.1`, `27018`, `dma_test`, `dma_test` |
| `JAIBA_TEST_SQLSERVER_HOST` / `PORT` / `DATABASE` / `USER` / `PASSWORD` | `127.0.0.1`, `11433`, `master`, `sa` |

Sin las variables necesarias, los tests reales se omiten (`skipping…`) y el
harness **falla** si detecta omisiones.

Salida esperada al final:

```text
Fase 8 OK contra el entorno de pruebas (…, mongo …, sqlserver …).
```

El enlace histórico `scripts/phase8-dma.sh` puede existir como symlink al mismo
script; el nombre canónico es `phase8-integration.sh`.

## Cobertura

| Prueba | Qué valida |
|---|---|
| `postgres_real_connection_query_builder_and_flow_execution` | Connection Manager + query + flujo |
| `mongodb_real_connection_diagnostics_and_collection_metadata` | Perfil Mongo + ping + metadatos |
| `mongodb_real_query_to_upsert_flow_is_idempotent` | `query_mongodb` → `put_mongodb` |
| `mongodb_real_connection_from_url` | Materialización desde URI `mongodb://` (opt-in; no bloquea el harness) |
| `sqlserver_real_connection_diagnostics_and_metadata` | Perfil SQL Server + diagnóstico + metadatos |
| `kafka_real_publish_is_acknowledged_and_consumable` | Publish + consume rdkafka |
| `kafka_real_consume_kafka_processor` | `publish_kafka` → `consume_kafka` |
| `kafka_throughput_smoke` | 100 mensajes en &lt; 30 s |
| `kafka_fail_broker_is_controlled` | Broker caído sin panic |
| `flow_retry_then_dead_letter` | Reintentos → DLQ → requeue |

### Pruebas unitarias relacionadas (sin servicio)

```bash
cargo test -p jaiba-server --features mongodb-driver mongo_url_unit_tests
cargo test -p jaiba-runtime prefers_stored_mongodb_connection_url
```

## Fuera de alcance

- TLS/SASL Kafka, commit tras destino, pause por rebalance
- MySQL / Oracle en este pase del harness (existen tests opt-in aparte)
- Pruebas de carga intensiva; solo smoke acotado

## CI

Workflow opcional: `.github/workflows/phase8-integration.yml`
(`workflow_dispatch` o label `phase8`). Detalle en [ci.md](../ci.md).

## Documentación relacionada

- [ci.md](../ci.md) — CI mínimo y Phase 8 en GitHub Actions
- [connection-manager.md](../connection-manager.md) — perfiles Mongo (URL) y SQL Server
- [operations.md](../operations.md) — features al servir
- [implementation-notes.md](../implementation-notes.md) — bitácora de validación
- [docs/README.md](README.md) — índice general

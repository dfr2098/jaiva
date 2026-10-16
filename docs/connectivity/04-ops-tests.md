# 04 — Ops y tests (este repo)

## Antes de un commit (igual que el CI)

| Quieres… | Comando / ruta |
| --- | --- |
| Formato | `cargo fmt --all -- --check` |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| Tests unitarios | `cargo test --workspace --all-targets` |
| Binario para los scripts | `cargo build -p jaiba-cli --bin jaiba` |
| Regresiones API y memoria | `python3 scripts/review-regression.py [binario]` |
| Smoke JME (política embebida, Cold tras reinicio) | `python3 scripts/smoke-jme.py [binario]` |
| Fallas JME (cuota, disco de solo lectura, segmentos dañados, `kill -9`) | `python3 scripts/chaos-jme.py [binario]` |
| Smoke offline | `cargo run -p jaiba-cli --features release-core -- examples/smoke.yaml` |

Los tres scripts de Python no usan Docker; sin argumento toman
`target/debug/jaiba`.

Si tocas código de un driver opcional (`connection_api/plugins/`, processors
`query_*`), compila también con su feature, por ejemplo
`cargo clippy -p jaiba-server --all-targets --features oracle-driver -- -D warnings`.
El CI pasa Clippy con todas las features (`jaiba-cli/all-drivers`), pero solo
ejecuta tests reales de Oracle (workflow `oracle.yml`).

## Stack Docker y carga

| Quieres… | Comando / ruta |
| --- | --- |
| Stack Docker Estable | `./scripts/release-core-up.sh` |
| Smoke del recorrido Estable (Postgres → CSV) | `./scripts/smoke-stable-path.sh` |
| Stress un ciclo (incluye rama JME) | `./scripts/stress-stable-stack.sh` |
| Soak 1 h | `./scripts/soak-stable-stack.sh` |
| Chaos | `./scripts/chaos-soak-stable-stack.sh` |
| Condiciones PASS/FAIL | [../stable-stack-tests.md](../stable-stack-tests.md) |
| ClickHouse local | `cargo run --features clickhouse-driver -- examples/clickhouse-write.yaml` |

Compose: `deploy/docker-compose.release-core.yml`  
Env de ejemplo: `deploy/.env.example`

## Contra motores reales (opt-in)

Las pruebas de `connection_api/tests.rs` se activan con variables
`JAIBA_TEST_<MOTOR>_PASSWORD` (más host/puerto opcionales); sin ellas se
omiten. Ver [../ci.md § Phase 8](../ci.md#phase-8-opcional).

Oracle tiene además pruebas del conector en `jaiba-runtime` (tipos, streaming,
cancelación, array DML, MERGE, rollback) y un smoke con el binario:

```bash
jaiba oracle-client install                       # o un Instant Client propio
export LD_LIBRARY_PATH="$HOME/.jaiba/oracle/instantclient"   # solo para cargo test
export JAIBA_TEST_ORACLE_PASSWORD=... JAIBA_TEST_ORACLE_PORT=1521 \
       JAIBA_TEST_ORACLE_SERVICE=FREEPDB1 JAIBA_TEST_ORACLE_USER=jaiba
cargo test -p jaiba-runtime --features oracle-driver --lib -- oracle
JAIBA_ORACLE_CONTAINER=<contenedor> ./scripts/smoke-oracle.sh
```

Detalle y contenedor de pruebas: [../oracle.md § Pruebas](../oracle.md#pruebas).

MySQL/MariaDB igual: pruebas del conector y de `query_mysql` en `jaiba-runtime`
(tipos, streaming, cancelación con `KILL QUERY`, timeout, errores, upsert,
rollback) y un smoke con el binario:

```bash
JAIBA_TEST_MYSQL_URL='mysql://jaiba:...@127.0.0.1:13306/jaiba' \
  cargo test -p jaiba-runtime --lib -- mysql
JAIBA_TEST_MYSQL_PASSWORD=... ./scripts/smoke-mysql.sh
```

Detalle: [../mysql.md § Pruebas](../mysql.md#pruebas).

MongoDB igual: pruebas de `query_mongodb`/`put_mongodb` en `jaiba-runtime`
(tipos en los tres formatos, streaming, cancelación, timeouts, errores, upsert
por lotes, copia exacta) y un smoke con el binario:

```bash
JAIBA_TEST_MONGODB_URL='mongodb://jaiba:...@127.0.0.1:27028/jaiba?authSource=admin' \
  cargo test -p jaiba-runtime --features mongodb-driver --lib -- mongo
JAIBA_TEST_MONGODB_PASSWORD=... ./scripts/smoke-mongodb.sh
```

Detalle: [../mongodb.md § Pruebas](../mongodb.md#pruebas).

Kafka igual: pruebas de `publish_kafka`/`consume_kafka` (poison, paralelo,
errores, throughput) y un smoke con el binario:

```bash
export JAIBA_TEST_KAFKA_BROKERS=127.0.0.1:29092
export JAIBA_TEST_KAFKA_FAIL_BROKER=127.0.0.1:1
cargo test -p jaiba-runtime --features kafka-driver --lib -- kafka
./scripts/smoke-kafka.sh
```

Detalle: [../kafka.md § Pruebas](../kafka.md#pruebas).

Volver al índice: [README.md](README.md).

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
El CI solo compila las features por defecto.

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

Volver al índice: [README.md](README.md).

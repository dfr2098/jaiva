# 04 — Ops y tests (este repo)

| Quieres… | Comando / ruta |
| --- | --- |
| Smoke offline | `cargo run -p jaiba-cli --features release-core -- examples/smoke.yaml` |
| Stack Docker Estable | `./scripts/release-core-up.sh` |
| Stress un ciclo | `./scripts/stress-stable-stack.sh` |
| Soak 1 h | `./scripts/soak-stable-stack.sh` |
| Chaos | `./scripts/chaos-soak-stable-stack.sh` |
| Condiciones PASS/FAIL | [../stable-stack-tests.md](../stable-stack-tests.md) |
| ClickHouse local | `cargo run --features clickhouse-driver -- examples/clickhouse-write.yaml` |

Compose: `deploy/docker-compose.release-core.yml`  
Env de ejemplo: `deploy/.env.example`

Volver al índice: [README.md](README.md).

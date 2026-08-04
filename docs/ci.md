# Integración continua

## Workflows

| Workflow | Archivo | Cuándo | Qué hace |
|---|---|---|---|
| **CI** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | `cargo fmt --check`, `cargo test --workspace`, typecheck UI |
| **Phase 8** | `.github/workflows/phase8-integration.yml` | manual (`workflow_dispatch`) o PR con label `phase8` | `scripts/phase8-integration.sh` contra entorno real |

El CI **no** levanta Postgres/Kafka/Mongo/SQL Server. Los tests opt-in que
requieren servicios se omiten en ese job.

## Phase 8 (opcional)

1. Entorno de pruebas accesible desde el runner (recomendado: **self-hosted**
   en la misma red que los contenedores de lab).
2. Secrets del repositorio:
   - `JAIBA_TEST_POSTGRES_PASSWORD`
   - `JAIBA_TEST_MONGODB_PASSWORD`
   - `JAIBA_TEST_SQLSERVER_PASSWORD`
3. Variables opcionales (`vars.*`) para host/puerto; defaults en
   [priority-8-integration-tests.md](priority-8-integration-tests.md).
4. Actions → **Phase 8 integration** → Run workflow  
   o etiqueta el PR con `phase8`.

En local:

```bash
export JAIBA_TEST_POSTGRES_PASSWORD=...
export JAIBA_TEST_MONGODB_PASSWORD=...
export JAIBA_TEST_SQLSERVER_PASSWORD=...
./scripts/phase8-integration.sh
```

## Equivalente local al CI

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cd apps/jaiba-ui && npm ci && npm run typecheck
```

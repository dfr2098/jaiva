# Integración continua

## Workflows

| Workflow | Archivo | Cuándo | Qué hace |
|---|---|---|---|
| **CI · Rust** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | formato, tests del workspace y Clippy con warnings como error |
| **CI · Desktop** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | build del sidecar y `cargo check` del shell Tauri en Linux |
| **CI · UI** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | Node 22, `npm ci`, typecheck y build Vite |
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
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p jaiba-cli --bin jaiba

cd apps/jaiba-ui
npm ci
npm run typecheck
npm run build
npm run desktop:sidecar
cd ../..
cargo check --manifest-path apps/jaiba-ui/src-tauri/Cargo.toml
```

En Windows, `desktop:sidecar` genera un `.exe`; en Linux genera el binario sin
extensión. Consulta [windows-native-and-wsl.md](windows-native-and-wsl.md) para
los requisitos de Tauri y las diferencias entre plataformas.

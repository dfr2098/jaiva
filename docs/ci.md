# Integración continua

## Workflows

| Workflow | Archivo | Cuándo | Qué hace |
|---|---|---|---|
| **CI · Rust** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | formato, tests, **smoke release-core**, regresiones API/memoria, **smoke y fallas JME**, Clippy `-D warnings` |
| **CI · Desktop** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | build del sidecar y `cargo check` del shell Tauri en Linux |
| **CI · UI** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | Node 22, `npm ci`, typecheck y build Vite |
| **CI · drivers** | `.github/workflows/ci.yml` | push/PR a `main`/`master` | Clippy `-D warnings` con `jaiba-cli/all-drivers` (Oracle, SQL Server, Kafka, MongoDB, ClickHouse) |
| **Windows** | `.github/workflows/windows.yml` | push/PR | build nativo con `all-drivers,embedded-ui` y comprobación de los nodos de cada driver |
| **Oracle integration** | `.github/workflows/oracle.yml` | nightly, push a `main` y PR que tocan Oracle, label `oracle`, o manual | Oracle Database Free 23ai en contenedor + Instant Client vía `jaiba oracle-client install`: pruebas reales del conector, diagnóstico del Connection Manager y `scripts/smoke-oracle.sh` ([oracle.md](oracle.md)) |
| **MySQL integration** | `.github/workflows/mysql.yml` | nightly, push a `main` y PR que tocan MySQL, label `mysql`, o manual | MySQL 8.4 y MariaDB 11.4 en contenedores: pruebas reales del conector y de `query_mysql`, metadatos del Connection Manager y `scripts/smoke-mysql.sh` ([mysql.md](mysql.md)) |
| **Stable path** | `.github/workflows/stable-path.yml` | push a `main`/`master`, cron laborable, label `stable-path`, o manual | Compose + smoke CSV + regresión Playwright (recorrido **Estable**) |
| **Release** | `.github/workflows/release.yml` | tag `v*` | binario Linux + imagen GHCR `jaiba-serve` |
| **Phase 8** | `.github/workflows/phase8-integration.yml` | manual (`workflow_dispatch`) o PR con label `phase8` | `scripts/phase8-integration.sh` contra entorno real |

La versión de Rust está fijada en `rust-toolchain.toml` (local y CI usan la
misma; los workflows la instalan con `rustup toolchain install`). Para subirla:
cambiar `channel`, correr `cargo clippy --workspace --all-targets -- -D warnings`
y corregir los lints nuevos en el mismo commit. Las imágenes Docker de
`deploy/` usan su propia versión.

El job **CI · Rust** no levanta bases externas: los tests opt-in que requieren
servicios se omiten ahí. Oracle y MySQL/MariaDB tienen integración real
automática (workflows **Oracle integration** y **MySQL integration**);
Kafka/Mongo/SQL Server siguen en Phase 8.

Las features de drivers opcionales se compilan y pasan Clippy en el job
**CI · drivers**, pero sus tests solo corren donde hay servicio real.

### Scripts offline del job Rust

Tras compilar `jaiba`, el job corre tres scripts sin Docker:

| Script | Qué comprueba |
|---|---|
| `scripts/review-regression.py` | Memoria de paquetes baja sin bloqueo, escrituras Hot rechazadas no persisten, deploy fallido conserva la versión previa, `/metrics` con auth, CORS del escritorio |
| `scripts/smoke-jme.py` | Política JME embebida, Hot → Cold bajo `JAIBA_DATA_DIR`, Cold sobrevive reinicio, `memory.version` no soportada se rechaza |
| `scripts/chaos-jme.py` | Cuota de Cold, disco de solo lectura, registros y cabeceras cortados o dañados, ceros al final, `kill -9` durante escrituras |

**Freeze de roadmap:** mientras smoke + release-core no lleven 2 semanas
seguidas en verde, no se abren fases `priority-11+`. Ver
[release-core.md § Congelar roadmap](release-core.md#congelar-roadmap).

## Phase 8 (opcional)

1. Entorno de pruebas accesible desde el runner (recomendado: **self-hosted**
   en la misma red que los contenedores de lab).
2. Secrets del repositorio:
   - `JAIBA_TEST_POSTGRES_PASSWORD`
   - `JAIBA_TEST_MONGODB_PASSWORD`
   - `JAIBA_TEST_SQLSERVER_PASSWORD`
3. Variables opcionales (`vars.*`) para host/puerto; defaults en
   [priority-8-integration-tests.md](history/priority-8-integration-tests.md).
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
./scripts/smoke-release-core.sh
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p jaiba-cli --bin jaiba
python3 scripts/review-regression.py
python3 scripts/smoke-jme.py
python3 scripts/chaos-jme.py

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

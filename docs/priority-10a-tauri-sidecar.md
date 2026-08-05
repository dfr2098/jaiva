# Fase 10A — Sidecar Tauri (motor local / remoto)

Extiende el cliente desktop de la [fase 9B](priority-9b-tauri-desktop.md) con un
**sidecar opcional** de `jaiba serve` y conmutación **local ↔ remoto**.

## Alcance

| Incluido | Fuera (después) |
|---|---|
| Spawn/stop de `jaiba serve` desde el shell | Firmado / notarización / auto-update |
| Modo **local** (sidecar) y **remoto** (API externa) | Sidecar con features opcionales (Oracle, Kafka…) |
| YAML mínimo embebido `desktop-local-flow.yaml` | Empaque Flatpak / tiendas |
| `externalBin` + script de preparación | TLS mutuo UI↔motor |
| UI: menú **Motor · local/remoto** en el topbar | |

## Requisitos

Los mismos de 9B (Node, Rust, WebKitGTK / deps Fedora), más un binario `jaiba`:

```bash
# Desde la raíz del repo
cargo build -p jaiba-cli
scripts/prepare-desktop-sidecar.sh
```

En Windows nativo usa PowerShell o CMD; los scripts de npm detectan la
plataforma y preparan automáticamente el nombre `.exe` requerido por Tauri:

```powershell
cargo build -p jaiba-cli --bin jaiba
cd apps/jaiba-ui
npm run desktop:dev
```

La preparación multiplataforma también puede ejecutarse directamente con
`node scripts/prepare-desktop-sidecar.mjs` desde la raíz del repositorio. La
guía completa de plataforma y diagnóstico está en
[windows-native-and-wsl.md](windows-native-and-wsl.md).

Variables útiles:

| Variable | Efecto |
|---|---|
| `JAIBA_BIN` | Ruta explícita al binario del motor |
| `JAIBA_FLOW` | YAML a servir en modo local |
| `JAIBA_API_BASE` | URL del API (default `http://127.0.0.1:9090`) |
| `JAIBA_ENGINE_MODE` | `local` o `remote` al arrancar (override del modo guardado) |
| `JAIBA_ADMIN_AUTH` | El sidecar fuerza `none` en local (loopback) |

## Uso

```bash
# Terminal única (sidecar gestionado por la app)
cd apps/jaiba-ui
npm run desktop:dev
# En el topbar: Motor → Local (sidecar) → Arrancar
```

### Wayland (Fedora): Error 71 / protocolo

Si la ventana cierra al instante con:

```text
Gdk-Message: Error 71 (Error de protocolo) dispatching to Wayland display.
```

WebKitGTK choca con Wayland. `npm run desktop:dev` ya fuerza X11
(`GDK_BACKEND=x11` + `WEBKIT_DISABLE_DMABUF_RENDERER=1`). Si lanzaste el
binario a mano:

```bash
cd apps/jaiba-ui
npm run desktop:run
# o:
GDK_BACKEND=x11 WEBKIT_DISABLE_DMABUF_RENDERER=1 \
  ./src-tauri/target/debug/jaiba-desktop
```

Modo remoto (como 9B): deja **Remoto** y arranca el motor aparte:

```bash
cargo run -- serve examples/visualisa-flow.yaml
```

## Empaquetado

```bash
cargo build -p jaiba-cli --release
cd apps/jaiba-ui
npm run desktop:build
```

`npm run desktop:build` llama al preparador Node en modo `release`. En Linux
copia `target/release/jaiba` a `src-tauri/binaries/jaiba-<target-triple>`; en
Windows copia `target/release/jaiba.exe` y conserva `.exe` en el destino. Ese
nombre es requerido por `bundle.externalBin`. El YAML local va en
`bundle.resources`.

## Comandos Tauri

| Comando | Descripción |
|---|---|
| `api_base` | URL efectiva del API |
| `engine_status` | modo, running, pid, binary, flow, error |
| `set_engine_mode` | `local` \| `remote` |
| `start_local_engine` | Arranca (o reutiliza) el sidecar |
| `stop_local_engine` | Mata el proceso hijo |

Al salir de la app se detiene el hijo. Si el puerto ya está ocupado en modo
local, no se lanza un segundo proceso: se reutiliza el motor existente.

## Layout

```text
apps/jaiba-ui/src-tauri/
  src/lib.rs           # setup + shutdown
  src/sidecar.rs       # EngineManager
  resources/desktop-local-flow.yaml
  binaries/jaiba-<triple>[.exe]   # generado, no versionado
```

Los scripts que implementan esta ruta son:

| Archivo | Responsabilidad |
|---|---|
| `scripts/prepare-desktop-sidecar.mjs` | Detectar plataforma/triple y copiar el sidecar |
| `scripts/run-desktop.mjs` | Lanzar Tauri o el binario desktop sin comandos Unix |
| `scripts/prepare-desktop-sidecar.sh` | Wrapper Linux/CI compatible con el flujo anterior |

## Seguridad

- CSP sigue limitado a loopback (igual que 9B).
- Sidecar local usa `authentication: none` + `JAIBA_ADMIN_AUTH=none` (solo
  válido en loopback por las reglas de 9A).
- El modo remoto sigue exigiendo Bearer cuando el servidor no está en loopback.

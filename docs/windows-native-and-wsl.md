# Desarrollo en Windows nativo y WSL

Guía para compilar, probar y ejecutar Jaiba desde PowerShell/CMD o desde WSL2
compartiendo el mismo repositorio.

## Qué ejecutar en cada entorno

| Tarea | Windows nativo | WSL2 |
|---|---:|---:|
| Motor, CLI y pruebas Rust | Sí | Sí |
| Frontend React/Vite | Sí | Sí, con Node ≥ 22.12 |
| Desktop Tauri para Windows | Sí | No; WSL genera un binario Linux |
| Desktop Tauri para Linux/WSLg | No | Sí, con GTK/WebKitGTK instalados |
| Integraciones que dependen de librerías Linux | No | Sí |

No mezcles el binario desktop de una plataforma con el sidecar de la otra.
El preparador detecta el `target triple` y mantiene ambos nombres separados.

## Requisitos

### Windows nativo

- Windows 10/11 con WebView2.
- Rust estable con el target MSVC de la máquina.
- Visual Studio Build Tools con las herramientas de C++.
- Node.js ≥ 22.12 y npm.

Comprueba el entorno:

```powershell
rustc --version
cargo --version
node --version
npm --version
```

### WSL2

- Rust estable instalado dentro de la distribución WSL.
- Node.js ≥ 22.12 dentro de WSL para frontend/Tauri. El Node instalado en
  Windows no reemplaza al de WSL.
- Para comprobar Tauri en Ubuntu/WSL, las dependencias que instala el CI:

```bash
sudo apt-get update
sudo apt-get install -y libwebkit2gtk-4.1-dev libappindicator3-dev \
  librsvg2-dev patchelf
```

Trabajar bajo `/mnt/c` o `/mnt/d` es válido, aunque Cargo puede ser bastante
más lento que sobre el filesystem Linux de WSL.

## Preparación inicial

Desde la raíz del repositorio:

```powershell
cargo build -p jaiba-cli --bin jaiba
cd apps\jaiba-ui
npm ci
npm run desktop:sidecar
```

En Windows, el último comando genera:

```text
apps/jaiba-ui/src-tauri/binaries/jaiba-x86_64-pc-windows-msvc.exe
```

En Linux/WSL genera el equivalente sin `.exe`, por ejemplo:

```text
apps/jaiba-ui/src-tauri/binaries/jaiba-x86_64-unknown-linux-gnu
```

Estos archivos son artefactos generados y no se versionan.

## Ejecutar el desktop en Windows

```powershell
cd apps\jaiba-ui
npm run desktop:dev
```

`desktop:dev` prepara el sidecar correcto y abre Tauri. En la barra superior,
selecciona **Motor → Local → Arrancar**. El motor escucha por defecto en
`http://127.0.0.1:9090`.

Para usar un motor que ya está ejecutándose, selecciona **Remoto**. También
puedes cambiar el endpoint antes de abrir la app:

```powershell
$env:JAIBA_API_BASE = "http://127.0.0.1:9190"
npm run desktop:dev
```

Variables admitidas:

| Variable | Uso |
|---|---|
| `JAIBA_BIN` | Ruta explícita a `jaiba.exe` o `jaiba` |
| `JAIBA_FLOW` | YAML utilizado por el motor local |
| `JAIBA_API_BASE` | URL del motor remoto |
| `JAIBA_ENGINE_MODE` | `local` o `remote` al iniciar |

## Build y ejecución local

Build de release con sidecar:

```powershell
cargo build -p jaiba-cli --bin jaiba --release
cd apps\jaiba-ui
npm run desktop:build
```

Después de haber generado el binario desktop de depuración:

```powershell
npm run desktop:run
```

Los scripts `desktop:*` son multiplataforma. En Linux, el lanzador conserva
automáticamente `GDK_BACKEND=x11` y `WEBKIT_DISABLE_DMABUF_RENDERER=1` para
evitar el fallo conocido de WebKitGTK bajo Wayland.

## Verificación recomendada

Desde la raíz:

```powershell
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo check -p jaiba-server --features sqlserver-driver
cargo check --manifest-path apps\jaiba-ui\src-tauri\Cargo.toml
```

Frontend:

```powershell
cd apps\jaiba-ui
npm run typecheck
npm run build
```

Antes de comprobar Tauri debe existir el sidecar de la plataforma actual;
ejecuta `npm run desktop:sidecar` si aparece un error de `resource path`.

## Compatibilidad implementada

- El almacenamiento `frozen` abre el temporal con permiso de escritura antes
  de `sync_all()`, requerido por Windows.
- El shell busca tanto `jaiba` como `jaiba.exe`, con y sin `target triple`.
- `prepare-desktop-sidecar.mjs` copia el ejecutable y agrega `.exe` únicamente
  en Windows.
- `run-desktop.mjs` inicia Tauri sin depender de `bash` ni del comando Unix
  `env`.
- `.gitattributes` conserva los scripts `*.sh` con finales LF para CI y WSL.
- El dialecto SQL Server solo se compila cuando corresponde a pruebas o al
  feature `sqlserver-driver`; Clippy con features predeterminados queda limpio.

## Solución de problemas

### `resource path binaries\\jaiba-...exe doesn't exist`

```powershell
cargo build -p jaiba-cli --bin jaiba
cd apps\jaiba-ui
npm run desktop:sidecar
```

### `fsync ... Acceso denegado (os error 5)`

La implementación actual ya abre el temporal con acceso de escritura. Si el
error reaparece, confirma que estás usando el código actualizado y que el
antivirus o una política corporativa no esté bloqueando `target/jme-test`.

### `spawn EPERM` al ejecutar Vite o Node

Suele indicar que un sandbox, antivirus o política de ejecución bloqueó un
proceso hijo. Prueba desde una terminal normal del usuario y revisa la política
de seguridad; no es un error de TypeScript.

### `pipefail\r: invalid option name`

El checkout convirtió un `.sh` a CRLF. `.gitattributes` evita esto en nuevos
checkouts. En un árbol antiguo, convierte el archivo a LF con tu editor o con
`dos2unix scripts/prepare-desktop-sidecar.sh` dentro de WSL.

### WSL: falta `gio-2.0`, `gobject-2.0`, `pango` o `gdk-3.0`

Faltan las dependencias GTK/WebKitGTK de Tauri. Instala los paquetes indicados
en la sección de requisitos y repite `cargo check`.

### WSL usa una versión antigua de Node

`node --version` debe devolver 22.12 o posterior. Actualiza Node dentro de la
distribución WSL antes de ejecutar `npm ci`, Vite o Tauri.

### WSL no resuelve `index.crates.io`

El mensaje `Could not resolve host: index.crates.io` es un problema DNS de WSL,
no de Cargo ni de la dependencia mencionada. Con red reflejada y túnel DNS, la
configuración de Windows debe incluir:

```ini
# C:\Users\<usuario>\.wslconfig
[wsl2]
networkingMode=mirrored
dnsTunneling=true
autoProxy=true
firewall=true
```

No se debe bloquear la generación de `resolv.conf` dentro de la distribución:

```ini
# /etc/wsl.conf
[network]
generateResolvConf = true
generateHosts = true
```

Después de cambiarlo, desde PowerShell ejecuta `wsl --shutdown` y vuelve a abrir
WSL. Verifica con:

```bash
getent hosts index.crates.io
cargo fetch --locked
```

En la instalación validada, servidores estáticos como `8.8.8.8` estaban
bloqueados por la red, mientras el túnel de WSL `10.255.255.254` sí resolvía.
No conviene fijar esa dirección permanentemente: debe regenerarla WSL.

Si la red corporativa continúa bloqueando HTTPS, se puede reutilizar
temporalmente el caché Cargo de Windows en modo offline:

```bash
CARGO_HOME=/mnt/c/Users/<usuario>/.cargo \
  cargo run --offline --features oracle-driver -- \
  serve examples/visualisa-flow.yaml
```

Primero completa ese caché desde PowerShell con `cargo fetch --locked`. Las
fuentes Rust son reutilizables; los binarios se compilan nuevamente para Linux.

## Validación registrada

Validado el 5 de agosto de 2026:

- Windows nativo: 150 pruebas Rust, Clippy estricto, feature SQL Server,
  `cargo check` de Tauri, typecheck y build Vite correctos.
- Windows nativo: sidecar generado como
  `jaiba-x86_64-pc-windows-msvc.exe`.
- WSL2: JME y compilación con `oracle-driver` correctos; el flujo visual arrancó
  y la API escuchó en `127.0.0.1:9090`.
- El chequeo Tauri en la instalación WSL examinada quedó condicionado a
  instalar GTK/WebKitGTK; el CI sí instala esas dependencias.

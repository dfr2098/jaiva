# Jaiba UI

Interfaz React + TypeScript desacoplada del motor Jaiba. Node.js se usa para
desarrollo y compilación; la imagen final contiene solamente Nginx y los
archivos estáticos. El contenedor no contiene Rust, conectores, credenciales ni
repositorios de paquetes. Si se detiene, los flujos continúan funcionando.

La paleta visual procede de la jaiba:

- verde petróleo del caparazón;
- azul y turquesa de las pinzas;
- óxido rojizo de las puntas;
- arena y blanco espuma para contraste.

## Identidad visual

El arte original del cangrejo jarocho está en `src/img/jaiba-logo.png`
(1448×1086, 3.2 MB). Es una copia sin modificaciones de la imagen de referencia;
no debe regenerarse ni sobrescribirse, y no se importa en la UI: todo lo que
importa la UI termina en `dist/` y, con ello, en el binario `jaiba`, el `.exe` y
la imagen Docker.

La UI usa derivados reducidos, generados con ImageMagick desde el original:

```bash
cd src/img
magick jaiba-logo.png -resize 256x192 -strip -quality 85 jaiba-logo-256.webp
magick jaiba-logo.png -gravity center -crop 1086x1086+0+0 +repage \
  -resize 64x64 -strip -define png:compression-level=9 jaiba-favicon-64.png
```

- `jaiba-logo-256.webp` (~26 KB): encabezado, importado desde
  `src/components.tsx` (Vite agrega un hash al nombre compilado). Se muestra a
  62 px como máximo; 256 px cubre pantallas de alta densidad.
- `jaiba-favicon-64.png` (~11 KB): favicon de `index.html`, recorte cuadrado
  central.

Para cambiar el tamaño visible ajusta `.crab-mark` en `src/styles.css`. Si el
logo llega a mostrarse más grande, regenera el derivado con más resolución.

## Conexiones en la UI

El módulo **Conexiones** crea perfiles reutilizables. Para MongoDB puede usar
host/puerto o pegar una URI (`mongodb://` / `mongodb+srv://`). Documentación:
[connection-manager.md](../../docs/connection-manager.md).

Drivers opcionales al arrancar el motor (si no, no aparecen en el catálogo):

```bash
cargo run --features mongodb-driver,sqlserver-driver,kafka-driver \
  -- serve examples/visualisa-flow.yaml
```

## Iniciar el motor sin login local

Usa `examples/visualisa-flow.yaml`:

```bash
cargo run -- serve examples/visualisa-flow.yaml
```

Jaiba escucha por defecto en `127.0.0.1:9090`. El modo sin autenticación es
rechazado si se intenta escuchar en una dirección que no sea loopback.

## Iniciar la interfaz

La forma más simple: el binario del release y la imagen `jaiba-serve` ya la
incluyen y `jaiba serve` la sirve en `http://127.0.0.1:9090/`. Desde el repo:

```bash
npm ci && npm run build
cargo run --features release-core,embedded-ui -- serve examples/visualisa-flow.yaml
```

La consola incrustada usa el mismo origen que la API, sin proxy ni CORS.
Como contenedor separado:

```bash
cd apps/jaiba-ui
docker compose up -d --build
```

Abrir:

```text
http://127.0.0.1:9080
```

Compose publica únicamente `127.0.0.1:9080` y Nginx accede a la API mediante
`host.docker.internal:9090`. Así la interfaz funciona en Docker Desktop sin
publicar Jaiba ni la UI en la LAN. En Windows se recomienda ejecutar el backend
nativo; en Linux puede sobreescribirse `JAIBA_API_UPSTREAM` si el host usa otra
dirección alcanzable desde Docker.

## Desarrollo React

```bash
npm install
npm run dev
```

Vite queda en `http://127.0.0.1:5173` y redirige `/jaiba-api` hacia el motor en
`127.0.0.1:9090`. Si el motor escucha en otra dirección:

```bash
JAIBA_API_UPSTREAM=http://127.0.0.1:19090 npm run dev
```

Si el motor no responde (o en esa dirección contesta otro programa), la UI lo
indica en un aviso bajo el encabezado con el comando para arreglarlo.

Validación y build:

```bash
npm run typecheck
npm run build
```

## Detener únicamente la interfaz

```bash
docker compose down
```

Esto no detiene el motor Jaiba.

## Funciones de la fase 8

- Monitor en tiempo real mediante WebSocket con sondeo de respaldo.
- Diseñador visual con importación, exportación y borrador local.
- Validación autoritativa y publicación coordinada en el motor.
- Provenance, dead-letter y reencolado.
- Controles habilitados de acuerdo con el ciclo de vida real.

El diseñador almacena solamente nombres de variables de entorno; nunca guarda
credenciales. La publicación reemplaza el único flujo administrado por la
instancia de Jaiba, por lo que primero drena el supervisor anterior.

## Aplicación de escritorio (fases 9B + 10A)

Tauri 2 en `src-tauri/`: modo **remoto** o **local** (sidecar `jaiba serve`).

- 9B: [docs/history/priority-9b-tauri-desktop.md](../../docs/history/priority-9b-tauri-desktop.md)
- 10A: [docs/history/priority-10a-tauri-sidecar.md](../../docs/history/priority-10a-tauri-sidecar.md)
- Windows/WSL: [docs/windows-native-and-wsl.md](../../docs/windows-native-and-wsl.md)

```bash
# Sidecar gestionado por la app (topbar → Motor · local)
cargo build -p jaiba-cli
cd apps/jaiba-ui
npm run desktop:dev

# O modo remoto (como 9B): motor aparte + Motor · remoto
cargo run -- serve examples/visualisa-flow.yaml
```

Los comandos `desktop:*` son multiplataforma. En Windows preparan y localizan
`jaiba-<target-triple>.exe`; en Linux preparan el binario sin extensión y
aplican el workaround X11 de WebKitGTK. Para un checkout nuevo se recomienda
`npm ci` en lugar de `npm install`.

Build de release:

```bash
cargo build -p jaiba-cli --bin jaiba --release
cd apps/jaiba-ui
npm run desktop:build
```

Los conectores del desktop son los del sidecar: `desktop:build` empaqueta el
`jaiba` que haya en `target/release`. Para incluir Oracle, SQL Server,
MongoDB, ClickHouse y Kafka en un solo paso:

```bash
npm run desktop:build:full   # cargo build --features all-drivers + desktop:build
```

Requisitos y detalles en
[windows-native-and-wsl.md](../../docs/windows-native-and-wsl.md#desktop-con-todas-las-bases).

Si Tauri indica que falta `binaries/jaiba-<target-triple>`, ejecuta primero
`npm run desktop:sidecar` desde este directorio.

```text
React + TypeScript
        │
        ├── Docker: build Node → Nginx
        │
        └── Desktop: Tauri → WebView del sistema
                              │
                              ├── Remoto: API http://127.0.0.1:9090
                              └── Local: sidecar jaiba serve (fase 10A)
```

El contrato React sigue usando la API administrativa y WebSocket; no se importa
código del motor dentro del frontend.

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

El encabezado y el favicon usan el arte original del cangrejo jarocho ubicado
en `src/img/jaiba-logo.png`. El archivo es una copia sin modificaciones de la
imagen de referencia; no debe regenerarse ni sobrescribirse durante el build.

La importación ocurre desde `src/components.tsx`, por lo que Vite agrega un hash
al nombre del recurso compilado. Para cambiar el tamaño visible se debe ajustar
`.crab-mark` en `src/styles.css`, sin alterar los píxeles del archivo original.

## Iniciar el motor sin login local

Usa `examples/visualisa-flow.yaml`:

```bash
cargo run -- serve examples/visualisa-flow.yaml
```

Jaiba escucha por defecto en `127.0.0.1:9090`. El modo sin autenticación es
rechazado si se intenta escuchar en una dirección que no sea loopback.

## Iniciar la interfaz

```bash
cd apps/jaiba-ui
docker compose up -d --build
```

Abrir:

```text
http://127.0.0.1:9080
```

El contenedor usa `network_mode: host` para que Nginx pueda comunicarse con la
API local sin publicar Jaiba en la red. Esta configuración está destinada a
Linux.

## Desarrollo React

```bash
npm install
npm run dev
```

Vite queda en `http://127.0.0.1:5173` y redirige `/jaiba-api` hacia el motor en
`127.0.0.1:9090`.

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

## Aplicación de escritorio futura

El build de Vite en `dist/` puede utilizarse directamente como
`frontendDist` de Tauri. La evolución prevista es:

```text
React + TypeScript
        │
        ├── Docker: build Node → Nginx
        │
        └── Desktop: Tauri → WebView del sistema
                              │
                              └── jaiba como sidecar opcional
```

En modo remoto, Tauri se conectará a una instalación Jaiba existente. En modo
local podrá iniciar `jaiba serve` como sidecar sobre loopback. El contrato
React seguirá usando la API administrativa y WebSocket; no se importará código
del motor dentro del frontend.

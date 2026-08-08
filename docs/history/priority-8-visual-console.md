# Fase 8: consola visual desacoplada

La fase 8 añade una interfaz opcional sin introducir dependencias de React,
Node.js o Nginx en el motor Rust. Detener Visualisa no detiene los flujos.

## 8.1 Monitor local

- React + TypeScript servido por Nginx en un contenedor independiente.
- Operación sin login únicamente cuando Jaiva usa `authentication: none` sobre
  loopback.
- Estado, métricas, capacidad del runtime y controles de ciclo de vida.
- Los botones se habilitan según `STOPPED`, `RUNNING`, `PAUSED`, `DRAINING` o
  `FAILED`, evitando transiciones que producirían `409`.

## 8.2 Actualización en tiempo real

- `GET /ws/v1` publica un `runtime_snapshot` por segundo.
- `GET /runtime` permite recuperar la misma instantánea mediante polling sin
  convertir los estados detenidos en errores HTTP.
- El monitor consume WebSocket y usa sondeo cada 10 segundos como respaldo.
- `GET /ws` conserva el contrato anterior de métricas para clientes existentes.
- Nginx y Vite permiten el upgrade WebSocket mediante `/jaiva-api`.

## 8.3 Diseñador de flujos

- Lienzo visual con fuentes, transformaciones, destinos y rutas
  `success`/`failure`.
- Inspectores de configuración, reintentos, concurrencia, orden y capacidad de
  cola.
- Catálogo limitado a procesadores implementados por Rust. Los componentes del
  roadmap aparecen deshabilitados.
- Referencias separadas para PostgreSQL, escritores multi-base y Kafka.

## 8.4 Proyectos e intercambio

- Guardado automático del borrador en `localStorage`.
- Guardado manual, vista previa y descarga YAML.
- Importación de YAML Jaiva con reconstrucción automática del diagrama.
- Las credenciales nunca se guardan: el proyecto contiene nombres de variables
  de entorno (`url_env`, `brokers_env`, `token_env`).

El borrador local es una comodidad del navegador, no un repositorio de
producción. Los YAML aprobados deben versionarse fuera de Visualisa.

## 8.5 Validación y publicación

- La validación del navegador comprueba grafo, concurrencia, memoria, colas,
  conexiones, orden particionado y campos requeridos.
- `POST /api/v1/flows/validate` ejecuta la validación autoritativa del motor.
- `PUT /api/v1/flows/{id}?start=false` publica un flujo detenido.
- `PUT /api/v1/flows/{id}?start=true` publica e inicia el flujo.

La publicación valida el YAML, la seguridad y el repositorio antes de tocar el
supervisor activo. Después drena el flujo anterior y reemplaza el único flujo
administrado por esa instancia. Cada publicación genera `audit_action:
flow_deploy`.

## 8.6 Trazabilidad operativa

- Consulta de provenance reciente o por `packet_id`.
- Vista de dead-letter con error, intento y procesador.
- Reencolado administrativo desde la interfaz.
- El repositorio debe estar habilitado en la configuración del flujo.

## 8.7 Seguridad y distribución

- Bearer token opcional almacenado solamente en `sessionStorage`.
- El modo sin autenticación continúa restringido a loopback.
- El cuerpo YAML respeta `engine.admin.max_request_body_bytes`.
- Build web dividido: el monitor no descarga React Flow hasta abrir el
  diseñador.
- El frontend está preparado para Nginx y para ser reutilizado posteriormente
  como `frontendDist` de Tauri.

## Arranque

```bash
cargo run -- serve examples/visualisa-flow.yaml
cd visualisa_jaiva
docker compose up -d --build
```

Abrir `http://127.0.0.1:9080`.

## Aceptación

```bash
cargo fmt --check
cargo test --lib
cargo check --all-features
cd visualisa_jaiva
npm run typecheck
npm run build
```

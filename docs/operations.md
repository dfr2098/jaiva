# Operación y observabilidad

## Ejecutar un flujo

```bash
cargo run -- examples/basic-flow.yaml
```

Ejemplos de escritura:

```bash
cargo run -- examples/postgres-write.yaml
cargo run -- examples/mysql-write.yaml
```

## Servidor de observabilidad

```bash
cargo run -- serve examples/basic-flow.yaml
```

Dirección predeterminada:

```text
127.0.0.1:9090
```

Para contenedores:

```bash
export JAIBA_SERVER_ADDR=0.0.0.0:9090
```

## Endpoints

- `GET /health`: disponibilidad.
- `GET /ready`: preparación real del flujo según su ciclo de vida.
- `GET /runtime`: instantánea actual del flujo, siempre con estado HTTP `200`.
- `GET /metrics`: Prometheus.
- `GET /ws`: snapshot JSON cada segundo.
- `/api/v1/*`: control autenticado, provenance y dead-letter.

Grafana debe consultar Prometheus, no el WebSocket directamente.

Las métricas etiquetadas y sus reglas de cardinalidad están documentadas en
[Paso 9: métricas Prometheus](priority-9-metrics.md).

Para habilitar la API administrativa:

```bash
export JAIBA_ADMIN_TOKEN='un-token-largo-generado-de-forma-segura'
cargo run -- serve examples/basic-flow.yaml
curl -H "Authorization: Bearer $JAIBA_ADMIN_TOKEN" \
  http://127.0.0.1:9090/api/v1/flows
```

La referencia completa está en
[Fase 7: control y endurecimiento operativo](priority-7-control-plane.md).

## Interfaz opcional

`apps/jaiba-ui` se ejecuta en un contenedor Nginx separado. El motor no
depende de este contenedor y puede continuar por CLI o API cuando la interfaz
está detenida.

Para desarrollo local sin login:

```bash
cargo run -- serve examples/visualisa-flow.yaml
cd apps/jaiba-ui
docker compose up -d --build
```

La interfaz queda en `http://127.0.0.1:9080`. El proxy usa la red de host para
alcanzar la API enlazada exclusivamente a `127.0.0.1:9090`.

## Archivos internos

```text
.jaiva/
├── repository.db
├── state.json
└── content/
```

No deben editarse mientras Jaiva esté ejecutándose.

## Apagado

El servidor y el modo de consola responden a `Ctrl+C` solicitando un drain. Las
tareas activas disponen del plazo configurado; el trabajo persistido que no
alcanzó a ejecutarse se recupera al reiniciar.

## Verificación

```bash
cargo fmt --check
cargo test
cargo test --all-features
cargo doc --all-features --no-deps
```

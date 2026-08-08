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

Drivers opcionales (hay que habilitarlos al compilar/ejecutar):

```bash
# Kafka (publish / consume)
cargo run --features kafka-driver -- serve examples/visualisa-flow.yaml

# MongoDB (Connection Manager + query_mongodb / put_mongodb)
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml

# Oracle (Connection Manager + query_oracle / put)
cargo run --features oracle-driver -- serve examples/visualisa-flow.yaml

# SQL Server (Connection Manager + put_database)
cargo run --features sqlserver-driver -- serve examples/visualisa-flow.yaml

# Varios a la vez
cargo run --features kafka-driver,mongodb-driver,sqlserver-driver,oracle-driver -- serve examples/visualisa-flow.yaml
```

Sin el feature correspondiente, el tipo no aparece en
`/api/v1/connection-types` ni en el selector de la UI.

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
- `GET /ws` / `GET /ws/v1`: eventos de runtime; sondeo cada
  `JAIBA_WS_POLL_MS` (default 250 ms) y **solo envía si el JSON cambió**
  (throttle + dirty-check). Ver [packaging.md](packaging.md).
- `/api/v1/*`: control autenticado, provenance y dead-letter.

Grafana debe consultar Prometheus, no el WebSocket directamente.

Las métricas etiquetadas y sus reglas de cardinalidad están documentadas en
[Paso 9: métricas Prometheus](history/priority-9-metrics.md).

Para habilitar la API administrativa:

```bash
export JAIBA_ADMIN_TOKEN='un-token-largo-generado-de-forma-segura'
cargo run -- serve examples/basic-flow.yaml
curl -H "Authorization: Bearer $JAIBA_ADMIN_TOKEN" \
  http://127.0.0.1:9090/api/v1/flows
```

La referencia completa está en
[Fase 7: control y endurecimiento operativo](history/priority-7-control-plane.md).

### PC se traba con muchos contenedores

En hosts ~16 GiB, el stack de producto (Angular, backends, Kafka) más Oracle /
SQL Server suele dejar la RAM sin margen. Usa el modo ligero:

```bash
# Solo lo necesario para fan-out / Mongo (para Angular, Kafka, backends pesados)
./scripts/jaiva-light-containers.sh fanout

# Oracle + Postgres + Mongo
./scripts/jaiva-light-containers.sh oracle

# Ver uso
./scripts/jaiva-light-containers.sh status
```

Las DBs de prueba en `compose.test-databases.yml` llevan `mem_limit` (Mongo
512 MiB, SQL Server 1.5 GiB, Oracle 3 GiB + shm 1 GiB). Tras cambiar límites:

```bash
cd ../DMA_CORE/DMA_CORE   # ruta de tu entorno
docker compose -f compose.test-databases.yml up -d --force-recreate
```

Fan-out Oracle → Postgres + Mongo (validado, incluido estrés 10 000 filas):
[oracle-to-postgres.md](oracle-to-postgres.md#fan-out-multi-db-prueba-oracle--postgresql--mongodb).
Requiere Instant Client en el host (`LD_LIBRARY_PATH`, p. ej.
`$HOME/oracle/instantclient_23_26`).

### Suite Fase 8 (entorno de pruebas)

Integra Postgres, Kafka, MongoDB y SQL Server ya levantados en el host:

```bash
export JAIBA_TEST_POSTGRES_PASSWORD='...'
export JAIBA_TEST_MONGODB_PASSWORD='...'
export JAIBA_TEST_SQLSERVER_PASSWORD='...'
./scripts/phase8-integration.sh
```

Detalle, variables y cobertura:
[priority-8-integration-tests.md](history/priority-8-integration-tests.md).

Perfiles Mongo con URI (`mongodb://` / `mongodb+srv://`) y SQL Server en la UI:
[connection-manager.md](connection-manager.md).

Índice de documentación: [docs/README.md](README.md).

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

La interfaz queda en `http://127.0.0.1:9080`. Docker publica solo loopback y el
proxy usa `host.docker.internal:9090` para alcanzar la API nativa enlazada a
`127.0.0.1:9090`.

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

## Operación de JME Cold

El nivel Cold puede limitarse por flujo con `memory.cold.max_disk_bytes`. Al
alcanzar la cuota, JME conserva el objeto en Hot y publica
`jaiba_memory_cold_quota_rejections_total`; no elimina silenciosamente datos ni
segmentos. La guía de dimensionamiento, alertas y recuperación está en
[JME Cold Memory segmentado](jme-cold-memory.md#dimensionamiento-por-flujo).

No se deben borrar archivos `.jmc` mientras Jaiba esté en ejecución. Cold es
una caché reconstruible y nunca debe ser la única copia de datos críticos.

## Verificación

```bash
cargo fmt --check
cargo test
cargo test --all-features
cargo doc --all-features --no-deps
```

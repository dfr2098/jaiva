# Empaquetado (Prioridad 3)

## Imagen `jaiba-serve`

Slim, perfil `release-core` (Postgres/SQLite; sin Oracle/Kafka/Mongo/SQL Server).

```bash
docker build -f deploy/Dockerfile.jaiba-serve -t jaiba-serve:local .
docker run --rm -p 127.0.0.1:9090:9090 \
  -e JAIBA_MASTER_KEY='dev-master-key' \
  -e JAIBA_ADMIN_TOKEN='dev-admin-token' \
  jaiba-serve:local
```

Compose de lab/producto mínimo sigue usando
[`Dockerfile.jaiba`](../deploy/Dockerfile.jaiba) (misma idea, root + volumes).
La imagen publicada en GHCR se llama **`jaiba-serve`**.

## Release GitHub

Workflow [`.github/workflows/release.yml`](../.github/workflows/release.yml):

- Trigger: tag `v*` (o `workflow_dispatch`).
- Artefacto: `jaiba-linux-x86_64.tar.gz` + checksum.
- Imagen: `ghcr.io/<owner>/jaiba-serve:<version>`.

```bash
git tag v0.2.1
git push origin v0.2.1
```

## WebSocket (observabilidad)

`/ws` y `/ws/v1` ya **no** empujan un snapshot cada segundo si no hay cambios:

- Sondeo cada `JAIBA_WS_POLL_MS` (default **250** ms).
- Envía solo si el JSON cambió (dirty-check).
- Un send en vuelo; ticks perdidos se omiten (`MissedTickBehavior::Skip`).

La UI sigue consumiendo `kind: "runtime_snapshot"` (sin cambio de contrato).

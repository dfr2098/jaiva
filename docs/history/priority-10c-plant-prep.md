# Fase 10C — Flujo de planta (DB → AI Prep → CSV + manifest)

Cierra el wedge de prep industrial: origen real (Postgres/Oracle) → limpieza /
features / split → CSV train/val/test + `manifest.json` + webhook opcional al
job ML externo.

Jaiba **prepara**; Azure ML / Fabric / SageMaker **entrenan**.

## Entregables

| Pieza | Detalle |
|---|---|
| Ejemplo canónico | [`examples/ai-prep-plant.yaml`](../examples/ai-prep-plant.yaml) |
| Paths en manifest | `train_path` / `validation_path` / `test_path` |
| Split reproducible | `shuffle` + `seed` en `ai_split_dataset` |
| Webhook mock | [`scripts/mock-ml-webhook.py`](../scripts/mock-ml-webhook.py) |
| Webhook tolerante | `optional: true` en `ai_trigger_webhook` |

## Ejecutar

```bash
export DATABASE_URL=postgres://usuario:clave@127.0.0.1:5432/jaiba

# Terminal A (opcional): receptor del hand-off
python3 scripts/mock-ml-webhook.py

# Terminal B
cargo run -- examples/ai-prep-plant.yaml
```

Salidas:

```text
output/ai-prep-plant/train.csv
output/ai-prep-plant/validation.csv
output/ai-prep-plant/test.csv
output/ai-prep-plant/manifest.json
```

El manifest incluye `splits.train_path` (etc.) y `checksum_sha256` del paquete
train observado.

Sin mock arriba, el flujo **sigue OK** (`optional: true`); verás un WARN de
webhook en el log.

## Origen Postgres vs Oracle

El ejemplo usa `query_postgres` con `VALUES` (no crea tablas). Para planta real,
cambia el `query` a tu vista/tabla de sensores.

Oracle: sustituye el nodo por `query_oracle` + `ORACLE_DATABASE_URL` (patrón
[`examples/multi-db-fanout.yaml`](../examples/multi-db-fanout.yaml)).

## Demo sin base (sintético)

```bash
cargo run -- examples/ai-prep-conveyor.yaml
```

Misma cadena AI Prep; origen `generate_records`.

## Checklist de entrega

1. CSV train/val/test generados.
2. `manifest.json` con columnas, dtypes, checksum y paths de split.
3. (Opcional) mock webhook recibe `event: jaiba.ai_prep.ready`.
4. Mensaje de producto: *Jaiba prepara; la plataforma ML entrena*.

## Relación

- Toolkit: [ai-data-prep.md](../ai-data-prep.md)
- UI split handles: builder (fase punto 4)
- Seguridad / desktop: 10A / 10B (independientes)

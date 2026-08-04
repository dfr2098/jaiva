# AI Data Prep Toolkit

Jaiba prepara datasets tabulares para IA en **Rust puro**. No entrena ni despliega
modelos: el flujo termina en export (CSV/JSON) listo para Azure ML, Fabric,
SageMaker u otra plataforma externa.

## Principio

- Unidad de trabajo: `PacketContent::Records` (arrays de objetos JSON).
- Transforms en `ExecutionMode::Cpu`.
- Sin Python, notebooks, PyO3, sklearn ni train in-process.
- Hand-off opcional vía `ai_export_manifest` + `ai_trigger_webhook` (HTTP).

## Ejemplos

```bash
# Sintético (sin DB)
cargo run -- examples/ai-prep-conveyor.yaml

# Planta: Postgres → prep → CSV + manifest (+ webhook opcional)
# Ver docs/priority-10c-plant-prep.md
export DATABASE_URL=postgres://...
cargo run -- examples/ai-prep-plant.yaml
```

Salidas típicas:

- `output/ai-prep/train.csv` (o `output/ai-prep-plant/…`)
- `output/ai-prep/validation.csv`
- `output/ai-prep/test.csv`
- `output/ai-prep/manifest.json` (incluye `splits.*_path` si se configuran)

## Procesadores

### Limpieza (MVP)

| Tipo | Rol |
|---|---|
| `ai_select_fields` | `keep` / `drop` de columnas |
| `ai_drop_nulls` | Elimina filas con null/vacío en `fields` |
| `ai_fill_missing` | `previous` / `constant` / `mean` / `median`; opcional `cumulative` |
| `ai_remove_duplicates` | Dedup por `key_fields`; opcional `window` entre paquetes |
| `ai_filter_range` | Outliers: `min_max` o `iqr` |
| `ai_cast_types` | `number` / `string` / `bool` / `timestamp`; `on_error: drop\|fail` |

### Features / split

| Tipo | Rol |
|---|---|
| `ai_normalize` | `min_max` o `z_score`; `cumulative: true` acumula stats entre paquetes |
| `ai_encode_categories` | Label encoding con mapa fijo en YAML |
| `ai_compute_fields` | Expresiones `+ - * /` sobre números (`a + b * 2`) |
| `ai_split_dataset` | Emite `train` / `validation` / `test`; opcional `shuffle` + `seed` |

### Join y hand-off

| Tipo | Rol |
|---|---|
| `ai_lookup_join` | Enriquecimiento por clave (`lookup_records` o `lookup_path` JSON) |
| `ai_export_manifest` | `manifest.json` + opcional `train_path` / `validation_path` / `test_path` |
| `ai_trigger_webhook` | POST/PUT HTTP al job externo; `optional: true` no aborta el flujo |

## Configuración rápida

```yaml
- id: normalize
  type: ai_normalize
  config:
    fields: [temperature, vibration]
    method: min_max          # o z_score
    cumulative: false        # true = stats entre paquetes

- id: split
  type: ai_split_dataset
  config:
    train: 0.7
    validation: 0.2
    test: 0.1
    shuffle: true
    seed: 42

# Conexiones del split:
# - { from: split, relationship: train, to: encode_train }
# - { from: split, relationship: validation, to: encode_val }
# - { from: split, relationship: test, to: encode_test }
```

## Errores por fila

La mayoría de nodos de prep usan `on_error: drop` (default) para descartar filas
inválidas. `fail` aborta el procesador ante el primer error (útil en cast estricto).

## Qué queda fuera

- Train / Evaluate / Deploy de modelos dentro del worker.
- AutoML, pandas, notebooks.
- Join shuffle multi-nodo o Parquet (CSV/JSON bastan hasta que el volumen lo pida).
- GPU / deep learning.

## UI

En el builder, categoría **AI Prep** (`apps/jaiba-ui` catálogo).

El nodo `ai_split_dataset` muestra tres handles de salida (`train`,
`validation`, `test`) en lugar de `success`/`failure`. Al importar
`examples/ai-prep-conveyor.yaml` las aristas conservan esas relaciones.
La validación del diseñador avisa si falta algún split o si se cableó
`success` por error.

# Guía para nuevos (nivel junior)

Si acabas de clonar el repo, **esta es la única puerta**. No leas el resto de
`docs/` todavía.

## En una frase

Jaiba mueve datos con un **flujo YAML**: leer → transformar → escribir, con
reintentos si falla.

## Checklist (5 líneas)

1. Instala Rust (`rustup`).
2. En la raíz del repo: `cargo run -- serve examples/basic-flow.yaml`
3. En otra terminal: `curl -fsS http://127.0.0.1:9090/health`
4. Debe responder algo como `{"status":"ok",...}`.
5. Para parar el server: `Ctrl+C` en la primera terminal.

Ese es el **único camino de arranque** el primer día. La primera compilación
tarda; las siguientes son más rápidas.

## Quiero X → feature Y

Sin el feature correcto, el conector **no existe** al compilar. Copia el
comando completo:

| Quiero… | Comando |
| --- | --- |
| Arrancar (Postgres/CSV/smoke, sin extras) | `cargo run -- serve examples/basic-flow.yaml` |
| Oracle | `cargo run --features oracle-driver -- serve examples/basic-flow.yaml` |
| MongoDB | `cargo run --features mongodb-driver -- serve examples/basic-flow.yaml` |
| Kafka | `cargo run --features kafka-driver -- serve examples/basic-flow.yaml` |
| SQL Server | `cargo run --features sqlserver-driver -- serve examples/basic-flow.yaml` |
| Varios a la vez | `cargo run --features oracle-driver,mongodb-driver,kafka-driver -- serve …` |

Si el error dice *unknown processor* y menciona un feature: **activa ese
`--features …`**, no busques un typo primero.

## Ideas mínimas

| Idea | Significado simple |
| --- | --- |
| **Flow** | Archivo YAML del pipeline |
| **Procesador** | Un paso (`log_records`, `query_postgres`, …) |
| **Conexión (YAML)** | Flecha `success` / `failure` entre pasos |
| **Perfil** | Host/user/password de una base (fuera del YAML) |
| **Feature de Cargo** | Flag `--features …` para compilar un conector |

## Carpetas al inicio

```text
jaiva/
  examples/          ← flows de ejemplo
  docs/guia-para-nuevos.md  ← estás aquí
  apps/jaiba-ui/     ← UI (después de `serve`)
  docs/history/      ← notas priority-* (historial; no el día 1)
```

## Después del health (mismo día, opcional)

- Flow canónico offline (sin `serve`): `cargo run -- examples/smoke.yaml`
- Producto **Estable** Postgres→CSV (Docker + CI): `./scripts/release-core-up.sh`
  y `./scripts/smoke-stable-path.sh` → UI en http://127.0.0.1:19080
  (revalidado en GitHub Actions: workflow *Stable path*)

## Cómo se ve un flow (mínimo)

```yaml
id: mi-primer-flow

processors:
  - id: source
    type: generate_records
    config:
      records: []
  - id: log
    type: log_records

connections:
  - from: source
    relationship: success
    to: log
```

Reglas: **nunca** passwords en el YAML; el `type` debe existir; cada `from`/`to`
debe coincidir con un `id`.

## Contraseñas (rápido)

- Con `JAIBA_MASTER_KEY`: secretos en disco (sobreviven al reinicio).
- Sin clave en loopback y **sin** almacén previo: memoria (dev); se pierden al cerrar.
- Si ya existe `data/secrets.enc` y falta la clave: el server **falla** con error claro.
- En red (`0.0.0.0`) o con `JAIBA_REQUIRE_MASTER_KEY=1`: la clave es obligatoria.

## Qué leer después

| Orden | Documento | Para qué |
| --- | --- | --- |
| 1 | [configuration.md](configuration.md) (estructura mínima) | Escribir un flow |
| 2 | [processors.md](processors.md) | Catálogo de nodos |
| 3 | [operations.md](operations.md) | UI, métricas |
| 4 | [product-roadmap.md](product-roadmap.md) | Qué está Estable / Beta (JME y AI Prep = lab) |

Índice: [README.md](README.md). Las notas `docs/history/priority-*` son historial.

## Errores típicos

| Síntoma | Qué hacer |
| --- | --- |
| `activa --features oracle-driver` (o similar) | Recompila con ese feature |
| `JAIBA_MASTER_KEY` / `secrets.enc` | Exporta la clave o borra `data/` solo en dev |
| Conexión OK en UI, flow falla | El alias del YAML no coincide con el perfil |
| Compila eterno / falla link | Features nativos (Oracle/SQL Server); ver README raíz |

## Si te atascas

1. Reproduce con `examples/basic-flow.yaml` o `examples/smoke.yaml`.
2. Pega: comando exacto + error completo.

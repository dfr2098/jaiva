# 05 — Dónde se ejecutan los YAML

Antes de empaquetar el binario/servicio, esta es la **ruta estable** de los flows.

## Dos modos (no confundirlos)

| Modo | Cómo arranca | Dónde está el YAML | Qué se ejecuta |
| --- | --- | --- | --- |
| **CLI un flow** | `jaiba examples/foo.yaml` | El archivo que pasas (ruta relativa al **cwd** del shell) | Ese YAML una vez (o hasta Ctrl+C) |
| **Servidor (`serve`)** | `jaiba serve` o `jaiba serve seed.yaml` | Tras el deploy: cuerpo YAML en **`JAIBA_DATA_DIR/flows.json`** (API), no hace falta el `.yaml` en disco | El flow publicado por API/UI |

En Docker release-core:

| Ruta en contenedor | Origen | Uso |
| --- | --- | --- |
| `/flows` | montaje `examples/` (solo lectura) | Catálogo de ejemplo en disco |
| `/data` | volumen `JAIBA_DATA_DIR` | Estado real: `flows.json`, secretos, repos |
| `/output` | volumen output | Salidas si el YAML escribe ahí |

Los scripts stress/soak/chaos **despliegan por API** (`PUT /api/v1/flows/...`) leyendo un YAML del host; el runtime no vuelve a abrir ese archivo del host en cada ciclo.

## Convención recomendada (destino final)

Fija esto en el servicio Linux/Windows:

```text
JAIBA_DATA_DIR=/var/lib/jaiba          # estado (obligatorio en prod)
WorkingDirectory=/var/lib/jaiba        # cwd del proceso = rutas relativas del YAML
# YAML “de producto” (opcionales, para editar/versionar):
#   /etc/jaiba/flows/*.yaml
# Se publican con API o un script de install; el motor corre lo de flows.json
```

| Qué | Dónde |
| --- | --- |
| Definición activa del flow | `$JAIBA_DATA_DIR/flows.json` |
| YAML fuente (git / ops) | `/etc/jaiba/flows/` (o carpeta que elijas) |
| Repositorio de paquetes (si el YAML usa `.jaiva/...`) | relativo al **cwd** del proceso → por eso `WorkingDirectory` fijo |
| `write_file` paths relativos | igual: relativos al cwd |
| JME: política | Embebida en el flujo (`engine.domain_memory.policy`), sin archivo |
| JME: persistencia y Cold/Frozen sin `path` | `$JAIBA_DATA_DIR/jme/...` por `flow_id` |

## Desarrollo en este repo

Desde la **raíz del repo**:

```bash
# Un flow desde archivo
cargo run -p jaiba-cli --features release-core -- examples/smoke.yaml

# Servidor (API); el YAML inicial es opcional
cargo run -p jaiba-cli --features release-core -- serve examples/basic-flow.yaml
```

- Ruta del YAML CLI = relativa a donde ejecutas (usa siempre la raíz del repo).
- Tras `serve`, cambios reales = deploy API/UI → quedan en `data/flows.json` (o `$JAIBA_DATA_DIR`).

## Checklist antes del ejecutable de producción

1. Definir `JAIBA_DATA_DIR` absoluto (no relativo).
2. Definir cwd del servicio (systemd `WorkingDirectory=` / equivalente Windows).
3. Decidir carpeta de YAML fuente (`/etc/jaiba/flows` o similar) y un script que haga `PUT` al arrancar si hace falta.
4. En Docker: montar YAML en `/flows` solo como referencia; no asumir que el runtime los lee solos en cada trigger.

Volver: [README.md](README.md).

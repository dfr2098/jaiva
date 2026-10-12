# Plugins externos (escribe un nodo sin recompilar Jaiba)

Un plugin externo es un programa aparte, en el lenguaje que quieras (Python,
Node, Go, Java…), que Jaiba arranca y con el que habla por stdin/stdout. En el
flujo se usa como cualquier nodo: `type: external_plugin`.

Estado: **Preview**. El protocolo es la versión 1 de
`jaiba_plugin_sdk::isolated`; un cambio incompatible subirá esa versión.

## Probarlo en un minuto

Desde la raíz del repo (requiere `python3`):

```bash
JAIBA_PLUGIN_DIR=examples/plugins cargo run -- examples/plugins/mayusculas-flow.yaml
```

El plugin [`examples/plugins/mayusculas`](../examples/plugins/mayusculas/)
pasa a mayúsculas los campos indicados; en el log aparece
`record={"city":"VERACRUZ","id":1,"name":"ADA"}`. Cópialo como plantilla.

## Instalar un plugin

1. Elige una carpeta de plugins y exporta `JAIBA_PLUGIN_DIR` con su ruta. Sin
   esa variable los plugins externos están desactivados.
2. Cada plugin vive en `JAIBA_PLUGIN_DIR/<id>/` con un `plugin.yaml`:

```yaml
id: mayusculas            # igual al nombre de la carpeta: a-z, 0-9, '-' o '_'
version: 0.1.0
description: Pasa a mayúsculas los campos de texto indicados en `fields`.
command: python3          # programa del PATH, o ruta relativa: ./mi-plugin
args: [mayusculas.py]     # el proceso arranca con la carpeta del plugin como cwd
```

3. En el flujo (o desde el diseñador de la consola, nodo **Plugin externo**):

```yaml
  - id: upper
    type: external_plugin
    config:
      plugin: mayusculas    # solo el id: el flujo nunca indica comandos
      timeout_ms: 30000     # por petición; por defecto 30000
      config:               # objeto que recibe el plugin (por defecto {})
        fields: [name, city]
```

`jaiba validate flujo.yaml` comprueba que el plugin esté instalado.

## Protocolo v1

Una petición JSON por línea en stdin y exactamente una respuesta JSON por línea
en stdout (máximo 8 MiB por línea). Todo lo demás (logs, trazas) va a stderr,
que Jaiba muestra en su propio log.

Petición:

```json
{"protocol_version": 1, "request_id": "mayusculas-2", "operation": "execute",
 "configuration": {"fields": ["name"]},
 "packet": {"Records": [{"id": 1, "name": "Ada"}]},
 "context": {"flow_id": "plugin-mayusculas", "processor_id": "upper", "attributes": {}}}
```

Respuesta correcta (cero, uno o varios paquetes; van por `success`):

```json
{"protocol_version": 1, "request_id": "mayusculas-2",
 "result": {"kind": "packets", "packets": [{"Records": [{"id": 1, "name": "ADA"}]}]},
 "error": null}
```

Respuesta de error (el paquete se reintenta según `retry` del flujo y después
sale por `failure` con `error.message`):

```json
{"protocol_version": 1, "request_id": "mayusculas-2", "result": null,
 "error": {"code": "invalid_input", "message": "solo acepta registros JSON", "retryable": false}}
```

| `operation` | Cuándo | Respuesta (`result`) |
| --- | --- | --- |
| `describe` | Al arrancar el proceso | `{"kind": "descriptor", "descriptor": {"id", "version", "display_name", "category", "default_port", "capabilities"}}`; `id` debe coincidir con el manifiesto |
| `execute` | Por cada paquete | `{"kind": "packets", "packets": [...]}` |
| `shutdown` | Reservado | `{"kind": "shutdown"}` |

Un paquete es `{"Records": [ ...objetos JSON... ]}` o
`{"Bytes": {"content": [bytes como números], "media_type": "text/csv"}}`.
Los paquetes emitidos conservan los atributos del de entrada y agregan
`plugin.id` y `plugin.version`.

## Ciclo de vida

- Jaiba arranca el proceso con el primer paquete y lo reutiliza; atiende una
  petición a la vez por nodo.
- Si el plugin no responde en `timeout_ms`, se cae, cierra stdout o responde algo
  que no cumple el protocolo, Jaiba lo mata, el paquete cuenta como fallo y el
  siguiente paquete arranca un proceso nuevo.
- Al terminar el flujo, el proceso se mata.

## Seguridad

Un plugin es código con los mismos permisos que el usuario que corre Jaiba.

- Instala solo plugins en los que confíes y revisa su código.
- El flujo solo nombra plugins ya instalados; publicar un flujo por la API no
  permite elegir comandos.
- `write_file` y `ai_export_manifest` rechazan rutas dentro de
  `JAIBA_PLUGIN_DIR`, pero la defensa real es que **Jaiba no pueda escribir en
  esa carpeta**: en producción móntala de solo lectura (en Docker,
  `-v ./plugins:/plugins:ro -e JAIBA_PLUGIN_DIR=/plugins`). Jaiba avisa en el
  log si la carpeta es escribible.

## Plataformas

- **Windows:** usa `command: python` (o `py`) en vez de `python3`, o un `.exe`
  con ruta relativa. La consola del plugin no se muestra.
- **Docker (`jaiba-serve`):** la imagen no trae Python ni Node; para plugins en
  esos lenguajes crea una imagen derivada que los instale.

## Siguientes pasos

Plugins como contenedores (`docker run -i`, sin red por defecto), catálogo
comunitario con versiones y checksums, y WebAssembly como transporte opcional
para transformaciones puras. Ver [product-roadmap.md](product-roadmap.md).

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

## Instalar desde el catálogo comunitario

```bash
export JAIBA_PLUGIN_DIR=$HOME/.jaiba/plugins
jaiba plugins search mayus               # busca por id o descripción
jaiba plugins install mayusculas         # la versión más nueva
jaiba plugins install mayusculas@0.1.0   # una versión concreta
jaiba plugins list                       # lo instalado (del catálogo o a mano)
jaiba plugins remove mayusculas
```

- El catálogo es [`catalog/plugins.json`](../catalog/plugins.json) de este
  repo (rama `main`). Otro catálogo: `--catalog URL|ARCHIVO` o
  `JAIBA_PLUGIN_CATALOG`. Para publicar un plugin, mira
  [`catalog/README.md`](../catalog/README.md).
- **Plugins de proceso:** cada archivo se descarga y se compara con el
  `sha256` del catálogo. **En contenedor:** la imagen viene fijada por digest
  (`…@sha256:…`) e `install` hace `docker pull` de ese digest (`--no-pull` si
  la descargas tú; `JAIBA_CONTAINER_RUNTIME=podman` para Podman).
- Jaiba escribe el `plugin.yaml` a partir del catálogo y deja en
  `.jaiba-install.json` el origen, la versión y los checksums.
- Si falla una descarga, un checksum o el `pull`, no se instala nada y la
  versión anterior queda intacta. Para reemplazar un plugin instalado usa
  `--force`.
- Solo descarga por HTTPS (HTTP únicamente hacia `localhost`), con límites de
  1 MiB para el catálogo y 64 MiB por archivo.
- **Confianza:** los checksums garantizan que instalas exactamente lo que dice
  el catálogo, no que el plugin sea bueno. El filtro es la revisión de cada PR
  al catálogo; revisa el código antes de instalar y prefiere plugins en
  contenedor para código de terceros.
- Instalar es tarea del operador: `jaiba serve` no descarga plugins y puede
  montar la carpeta de solo lectura.

## Instalar un plugin a mano

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

## Plugins en contenedor (Docker o Podman)

En vez de `command`, el manifiesto puede indicar una imagen. Así el plugin no
necesita Python/Node en el servidor y corre aislado: sin red, sin poder
escribir en su sistema de archivos y con memoria, CPU y procesos limitados.
El protocolo y el flujo son los mismos.

Pruébalo con el mismo ejemplo (requiere Docker):

```bash
docker build -t jaiba-plugin-mayusculas:0.1.0 examples/plugins/mayusculas
JAIBA_PLUGIN_DIR=examples/plugins-container cargo run -- examples/plugins/mayusculas-flow.yaml
```

[`examples/plugins-container/mayusculas/plugin.yaml`](../examples/plugins-container/mayusculas/plugin.yaml):

```yaml
id: mayusculas
version: 0.1.0
container:
  image: jaiba-plugin-mayusculas:0.1.0   # debe existir en el host
  network: none    # por defecto; `bridge` si el plugin necesita red
  memory: 256m     # por defecto
  cpus: 1          # por defecto
  pids: 64         # por defecto
args: []           # opcional: argumentos para el ENTRYPOINT de la imagen
```

Exactamente uno de `command` o `container`. Para cada proceso del plugin Jaiba
ejecuta:

```text
docker run -i --rm --pull=never --name=jaiba-plugin-<id>-<aleatorio>
  --label=jaiba.plugin=<id> --network=none --read-only
  --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=ALL
  --security-opt=no-new-privileges --memory=256m --memory-swap=256m
  --cpus=1 --pids-limit=64 <image> <args...>
```

- **Jaiba nunca descarga imágenes** (`--pull=never`): instalar el plugin
  incluye `docker pull` (o `docker build`). Si falta la imagen, el error lo
  dice y el mensaje de Docker aparece en el log.
- **Podman:** `JAIBA_CONTAINER_RUNTIME=podman` (acepta los mismos argumentos).
  También sirve para indicar la ruta completa del ejecutable.
- **Imagen:** la entrada es stdin y la salida stdout, igual que un proceso.
  Usa `python3 -u` (o `flush`) y un usuario sin privilegios (`USER 65534`);
  mira el [`Dockerfile`](../examples/plugins/mayusculas/Dockerfile) del
  ejemplo. Fija la versión de la imagen (`:0.1.0` o `@sha256:…`), no `latest`.
- **Limpieza:** si el plugin se cuelga o termina el flujo, Jaiba ejecuta
  `docker rm -f` del contenedor. Si Jaiba muere de golpe, el contenedor se
  cierra al perder stdin; para revisar restos:
  `docker ps -a --filter label=jaiba.plugin`.
- El primer paquete paga el arranque del contenedor (≈0,5–1 s); después se
  reutiliza.

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

Un plugin con `command` es código con los mismos permisos que el usuario que
corre Jaiba. Un plugin con `container` queda aislado (sin red, solo lectura,
sin capabilities y con límites), pero quien puede usar Docker es en la práctica
root del host: el aislamiento protege de un plugin defectuoso o malicioso, no
de quien edita `plugin.yaml`.

- Instala solo plugins en los que confíes y revisa su código.
- Prefiere `container` para plugins de terceros y `network: none` salvo que
  el plugin necesite red.
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
  Los plugins en contenedor usan Docker Desktop (imágenes Linux; no probado en
  CI de Windows).
- **Jaiba dentro de Docker (`jaiba-serve`):** la imagen no trae Python, Node ni
  el cliente de Docker. Para plugins de proceso crea una imagen derivada que
  instale el lenguaje. No montes `/var/run/docker.sock` para usar plugins en
  contenedor: equivale a dar root del host a Jaiba.

## Siguientes pasos

Firmas del catálogo, `jaiba plugins update`, ver el catálogo desde la consola y
WebAssembly como transporte opcional para transformaciones puras. Ver
[product-roadmap.md](product-roadmap.md).

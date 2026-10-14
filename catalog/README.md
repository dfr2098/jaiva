# Catálogo comunitario de plugins

[`plugins.json`](plugins.json) es el índice que leen `jaiba plugins search` e
`install` (por defecto, desde la rama `main` de este repo). Guía de uso y del
protocolo: [docs/plugins.md](../docs/plugins.md).

## Publicar un plugin

Abre un PR que agregue tu plugin (o una versión nueva) a `plugins.json`:

```json
{
  "id": "mi-plugin",
  "description": "Qué hace, en una línea",
  "license": "Apache-2.0",
  "homepage": "https://github.com/tu-usuario/mi-plugin",
  "versions": [
    {
      "version": "0.1.0",
      "protocol_version": 1,
      "command": "python3",
      "args": ["main.py"],
      "files": [
        {
          "path": "main.py",
          "url": "https://raw.githubusercontent.com/tu-usuario/mi-plugin/v0.1.0/main.py",
          "sha256": "<sha256sum main.py>"
        }
      ]
    },
    {
      "version": "0.2.0",
      "protocol_version": 1,
      "container": {
        "image": "ghcr.io/tu-usuario/mi-plugin@sha256:<digest>",
        "network": "none"
      }
    }
  ]
}
```

Reglas (Jaiba las valida al leer el catálogo):

- `id`: minúsculas, números, `-` o `_`; único en el catálogo.
- `version`: `MAYOR.MENOR.PARCHE`, sin repetir. No cambies una versión ya
  publicada: agrega otra.
- `protocol_version`: la del protocolo de plugins (hoy `1`).
- **Proceso** (`command`, `args`, `files`):
  - `url` con `https://` y fijada a un tag o commit, nunca a una rama.
    Las URLs relativas se resuelven contra este archivo (las usan los ejemplos
    del repo).
  - `sha256`: 64 caracteres hex en minúsculas (`sha256sum archivo`).
  - `path`: ruta relativa dentro de la carpeta del plugin, sin `..` ni nombres
    que empiecen con `.`; no puede ser `plugin.yaml` (lo escribe Jaiba).
  - `executable: true` para binarios o scripts que se ejecutan directo.
- **Contenedor** (`container`, sin `files`):
  - `image` fijada por digest: `registro/imagen@sha256:<64 hex>`. Obtén el
    digest con `docker buildx imagetools inspect registro/imagen:tag`.
  - Opcionales: `network` (`none` | `bridge`), `memory`, `cpus`, `pids`.
  - La imagen debe ser pública y usar un usuario sin privilegios.

## Qué revisamos en el PR

- Que el código (o el `Dockerfile` de la imagen) sea legible y haga lo que
  dice la descripción.
- Que no pida red (`network: bridge`) sin explicar por qué.
- Que `cargo test -p jaiba-runtime plugin_catalog` pase: valida el catálogo y
  los `sha256` de los archivos que viven en este repo.

Los checksums garantizan que se instala exactamente lo revisado; no sustituyen
la revisión.

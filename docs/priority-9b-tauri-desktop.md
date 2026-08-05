# Fase 9B — Cliente desktop Tauri (MVP)

Empaqueta `apps/jaiba-ui` con [Tauri 2](https://v2.tauri.app/) como aplicación
de escritorio. El contrato React sigue siendo la API administrativa + WebSocket;
**no** se embebe el motor Rust dentro del frontend.

> Esta fase describe el MVP remoto original. El sidecar local ya está
> implementado en [priority-10a-tauri-sidecar.md](priority-10a-tauri-sidecar.md)
> y la ejecución multiplataforma en
> [windows-native-and-wsl.md](windows-native-and-wsl.md).

## Alcance del MVP

| Incluido | Fuera (siguiente iteración) |
|---|---|
| Ventana desktop con UI embebida | Sidecar que arranca `jaiba serve` |
| Modo **remoto**: UI → `http://127.0.0.1:9090` | Firmado / notarización de instaladores |
| Override de API vía env / localStorage | Tiendas (Flatpak, App Store, etc.) |
| Icons y `npm run desktop:dev` / `desktop:build` | Endurecimiento 9A completo |

## Requisitos

1. Node ≥ 22.12 y Rust estable.
2. Dependencias de sistema Tauri / WebKitGTK
   ([prerequisites](https://v2.tauri.app/start/prerequisites/)).
   En Fedora, como mínimo:

   ```bash
   sudo dnf install webkit2gtk4.1-devel openssl-devel \
     gtk3-devel libsoup3-devel dbus-devel pkgconf-pkg-config
   ```

3. Un `jaiba serve` escuchando en loopback (por defecto `:9090`).

## Desarrollo

En una terminal, el motor:

```bash
export JAIBA_ADMIN_TOKEN=dev-token   # si authentication=bearer
cargo run --features mongodb-driver -- serve examples/visualisa-flow.yaml
# o el YAML que uses en tu entorno
```

En otra:

```bash
cd apps/jaiba-ui   # o visualisa_jaiva (symlink)
npm install
npm run desktop:dev
```

`tauri dev` levanta Vite en `127.0.0.1:5173` y abre el WebView.

En Fedora/Wayland, si aparece `Gdk-Message: Error 71` y la app sale al
instante, usa `npm run desktop:dev` (ya fuerza `GDK_BACKEND=x11`) o lanza el
binario con esas variables; detalle en
[priority-10a-tauri-sidecar.md](priority-10a-tauri-sidecar.md).

## Build de release

```bash
cd apps/jaiba-ui
npm run desktop:build
```

Artefactos bajo `apps/jaiba-ui/src-tauri/target/release/bundle/`.

## Configurar la URL del API

Orden de resolución en la UI:

1. `window.__JAIBA_API_BASE__`
2. `localStorage.jaiba.api.base`
3. `VITE_JAIBA_API_BASE` (build-time)
4. En Tauri: comando `api_base` → env `JAIBA_API_BASE` / `JAIVA_API_BASE` o
   default `http://127.0.0.1:9090`
5. En navegador: proxy Vite `/jaiba-api`

Ejemplos:

```bash
# Desktop apuntando a otro puerto
JAIBA_API_BASE=http://127.0.0.1:9190 npm run desktop:dev
```

```js
// En la consola del WebView
localStorage.setItem("jaiba.api.base", "http://127.0.0.1:9190");
location.reload();
```

## Layout

```text
apps/jaiba-ui/
  src/                 # React (mismo código web / desktop)
  src-tauri/           # Crate jaiba-desktop (Tauri 2)
    tauri.conf.json
    src/lib.rs         # invoke api_base
    icons/
  dist/                # frontendDist embebido en release
```

El crate `src-tauri` **no** es member del workspace raíz de Jaiba (evita mezclar
perfiles y features del motor con el shell desktop).

## Seguridad (mínimo del MVP)

- CSP permite `connect-src` solo a loopback / localhost (HTTP y WS).
- Sigue aplicando Bearer del control plane (`JAIBA_ADMIN_TOKEN` en la UI).
- El modo `authentication: none` del servidor solo es válido en loopback
  (regla ya existente en `jaiba-server`).

## Próximo

- Sidecar local / remoto: [priority-10a-tauri-sidecar.md](priority-10a-tauri-sidecar.md).

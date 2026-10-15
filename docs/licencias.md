# Licencias

Jaiba es Apache-2.0 ([`LICENSE`](../LICENSE)). Lo que va dentro de los
binarios (crates de Rust, paquetes npm de la consola, bibliotecas de C) está
listado con su texto de licencia en
[`THIRD_PARTY_LICENSES.md`](../THIRD_PARTY_LICENSES.md).

## Qué cubre

| Parte | De dónde sale |
| --- | --- |
| Motor (`jaiba`, Linux y Windows) | `cargo-about` sobre el workspace con `jaiba-cli/all-drivers` y `jaiba-cli/embedded-ui` |
| Escritorio (Tauri) | `cargo-about` sobre `apps/jaiba-ui/src-tauri/Cargo.toml` |
| Consola web | dependencias de producción de `apps/jaiba-ui/package-lock.json`, textos desde `node_modules` |
| Bibliotecas de C (librdkafka, curl, zlib, AWS-LC, ODPI-C, cargador de WebView2) | lista `NATIVE_LIBRARIES` en `scripts/third-party-licenses.py` |
| Avisos `NOTICE` (Apache-2.0 §4d) | cualquier crate o paquete que traiga uno |

Oracle Instant Client no se redistribuye (lo descarga
`jaiba oracle-client install`) y OpenSSL en Linux es el del sistema.

Cada artefacto lleva los dos archivos: en la raíz del `.tar.gz` y del `.zip`,
en la carpeta de instalación del escritorio y en `/usr/share/doc/jaiba/` de la
imagen `jaiba-serve`.

## Licencias permitidas

Solo permisivas: Apache-2.0 (también con LLVM-exception), MIT, MIT-0, BSD-2/3,
ISC, Zlib, Unicode-3.0, UPL-1.0, MPL-2.0, CDLA-Permissive-2.0, BSL-1.0, 0BSD,
CC0-1.0 y Unlicense. La lista vive en dos sitios que deben coincidir:
`accepted` de [`about.toml`](../about.toml) y `allow` de
[`deny.toml`](../deny.toml).

## Regenerar

Requiere `cargo-about` 0.9.2 (`cargo install cargo-about --version 0.9.2
--locked --features cli`) y `npm ci` en `apps/jaiba-ui`:

```bash
python3 scripts/third-party-licenses.py          # reescribe THIRD_PARTY_LICENSES.md
python3 scripts/third-party-licenses.py --check  # lo que corre el CI
cargo deny check licenses                        # motor
cargo deny --manifest-path apps/jaiba-ui/src-tauri/Cargo.toml check licenses
```

## Cuando el job "Licencias de terceros" falla

- **`THIRD_PARTY_LICENSES.md` está desactualizado**: cambiaste dependencias
  (`Cargo.lock`, el lock del escritorio o `package-lock.json`). Regenera y sube
  el archivo.
- **`license is not explicitly allowed`** (cargo-deny) o **no está en
  about.toml** (npm): la dependencia nueva usa una licencia fuera de la lista.
  No la agregues a la lista sin revisarla; copyleft fuerte (GPL, AGPL, LGPL
  estático, SSPL) no se acepta.
- **checksum de `ring`**: subió de versión y cambiaron sus archivos de
  licencia. Revisa `LICENSE-BoringSSL` y `LICENSE-other-bits` del crate nuevo y
  actualiza los checksums en `about.toml`.
- **`… ya no está en el árbol` / `no existe …`**: una biblioteca de
  `NATIVE_LIBRARIES` se quitó o movió su archivo de licencia; ajusta la lista.

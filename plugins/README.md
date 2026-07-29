# Plugins de Jaiba

Los adaptadores actuales siguen compilados dentro de `jaiba-runtime` durante la
migración. Cada carpeta contiene el manifiesto que utilizará el catálogo de
plugins. Las nuevas implementaciones deben depender de `jaiba-plugin-sdk`, no
del servidor ni de la interfaz.

La carga binaria arbitraria de bibliotecas Rust no forma parte del contrato.
Los plugins externos usan el protocolo JSON Lines v1 de
`jaiba_plugin_sdk::isolated` mediante un proceso seleccionado por un catálogo
de confianza. WebAssembly Component Model queda como transporte futuro
opcional.

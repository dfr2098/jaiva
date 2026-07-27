# Plugins de Jaiba

Los adaptadores actuales siguen compilados dentro de `jaiba-runtime` durante la
migración. Cada carpeta contiene el manifiesto que utilizará el catálogo de
plugins. Las nuevas implementaciones deben depender de `jaiba-plugin-sdk`, no
del servidor ni de la interfaz.

La carga binaria arbitraria de bibliotecas Rust no forma parte del contrato:
los plugins externos se aislarán mediante procesos o WebAssembly Component
Model.

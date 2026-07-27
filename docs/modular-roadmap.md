# Ruta modular de Jaiba

| Fase | Resultado | Estado |
|---|---|---|
| 9.1 | Cargo workspace, Core, Runtime, Server y CLI | Implementado |
| 9.2 | SDK y Connection Manager independiente | Base implementada |
| 9.3 | Plugins de diagnóstico y pruebas reales | Siguiente |
| 9.4 | REST del Connection Manager y módulo visual | Base implementada |
| 9.5 | Explorador y constructor SQL en UI | Pendiente |
| 9.6 | Creación automática de nodos Query | Pendiente |
| 9.7 | Proveedores Real, Mock y Replay | Contratos base |
| 9.8 | Plugins externos aislados / WASM | Investigación |

La migración no debe detener la evolución del runtime. Los adaptadores
existentes continúan en `jaiba-runtime` hasta que cada plugin alcance paridad de
pruebas y operación.

# Ruta modular de Jaiba

> **Congelar producto nuevo:** no abrir fases `priority-11+` hasta que smoke +
> release-core lleven **2 semanas seguidas en verde**. Ver
> [release-core.md § Congelar roadmap](release-core.md#congelar-roadmap).

| Fase | Resultado | Estado |
|---|---|---|
| 9.1 | Cargo workspace, Core, Runtime, Server y CLI | Implementado |
| 9.2 | SDK y Connection Manager independiente | Implementado |
| 9.3 | Plugins de diagnóstico y pruebas reales | Implementado; PostgreSQL, MySQL, Oracle, SQL Server, MongoDB y Kafka validados |
| 9.4 | REST del Connection Manager y módulo visual | Implementado |
| 9.5 | Explorador y constructor SQL en UI | Implementado y validado |
| 9.6 | Creación automática de nodos Query | Implementado y validado para PostgreSQL |
| 9.7 | Proveedores Real, Mock y Replay | Implementado |
| 9.8 | Plugins externos aislados / WASM | Proceso aislado implementado; WASM queda como evolución opcional |

La migración no debe detener la evolución del runtime. Los adaptadores
existentes continúan en `jaiba-runtime` hasta que cada plugin alcance paridad de
pruebas y operación.

La fase 9 queda cerrada en el alcance de la versión 0.2. El aislamiento externo
usa procesos con JSON Lines versionado; no carga bibliotecas Rust dentro del
servidor. WebAssembly Component Model no es necesario para el cierre y podrá
añadirse como otro transporte sin modificar los contratos.

La explicación de lo implementado, sus archivos y la lista de comprobación se
mantiene en [la bitácora técnica](implementation-notes.md).

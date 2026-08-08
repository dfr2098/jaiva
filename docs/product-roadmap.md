# Roadmap de producto Jaiba

Nombre público: **Jaiba**. Los alias `jaiva` / `JAIVA_*` se conservan solo por
compatibilidad.

Este documento fija el producto mínimo **Estable** y los ciclos siguientes.
Complementa [release-core.md](release-core.md) (defaults seguros / smoke).

> **Freeze:** nada de nuevas fases `priority-11+` hasta que smoke + release-core
> estén verdes **2 semanas seguidas**. Detalle en
> [release-core.md § Congelar roadmap](release-core.md#congelar-roadmap).

## Matriz de madurez

| Capacidad | Estado |
| --- | --- |
| PostgreSQL → CSV (recorrido oficial) | **Estable** (CI automático) |
| PostgreSQL escritura | Estable (secundario) |
| MySQL | Beta |
| MongoDB | Beta |
| Kafka | Beta |
| Oracle | Experimental |
| SQL Server | Experimental |
| Memoria JME | Experimental (lab; ver política abajo) |
| AI Prep toolkit | Experimental (lab; ver política abajo) |
| Plugins externos (proceso / JSON Lines) | Preview |
| Escritorio Tauri | Beta |
| Imagen / release `jaiba-serve` | Beta (Prioridad 3) |

## Recorrido oficial (Estable)

```text
PostgreSQL → transformación → CSV
```

- Ejemplo Estable: [`examples/stable-postgres-to-csv.yaml`](../examples/stable-postgres-to-csv.yaml)
- Smoke canónico (README/CI offline): [`examples/smoke.yaml`](../examples/smoke.yaml)
- Stack: [`deploy/docker-compose.release-core.yml`](../deploy/docker-compose.release-core.yml)
- Smoke CLI: `./scripts/smoke-stable-path.sh`
- Regresión corta: `./scripts/smoke-regression.sh` (Playwright en `apps/jaiba-ui/e2e`)

### PostgreSQL → CSV es oficialmente **Estable**

El recorrido de producto está declarado Estable con evidencia automatizada:

| Señal | Dónde |
| --- | --- |
| Ejemplo canónico | [`examples/stable-postgres-to-csv.yaml`](../examples/stable-postgres-to-csv.yaml) |
| Local | `./scripts/smoke-regression.sh` (Compose + CSV + e2e) |
| CI cada push a `main`/`master` | workflow **Stable path** |
| CI laborable (cron) | mismo workflow, 06:00 UTC lun–vie |
| Opt-in en PR | label `stable-path` o `workflow_dispatch` |
| Defaults offline | `smoke-release-core` en CI en cada PR |

“Estable” = ese camino es el prometido al usuario y se **revalida solo** en CI.
Un rojo en Stable path o release-core reinicia el contador del
[freeze](release-core.md#congelar-roadmap) (2 semanas verdes antes de
`priority-11+`).

Checklist del recorrido completo (UI + motor):

1. Crear conexión Postgres  
2. Probar credenciales  
3. Diseñar / importar el DAG  
4. Validar  
5. Ejecutar  
6. Pausar y reanudar  
7. Ver errores y métricas  
8. Recuperarse tras reiniciar  

La regresión corta cubre health, conexiones, smoke flow, pause/resume, DLQ y
persistencia de secretos. Import YAML → deploy UI end-to-end sigue en follow-up.

### Deuda conocida (no quita el sello Estable del recorrido)

| Ítem | Notas |
| --- | --- |
| Split módulos grandes | `observability.rs` / `executor.rs` / `FlowBuilder.tsx` → Ciclo 2 |
| `release-core` feature vacío | Hoy es perfil nominal; falta composición Cargo real (deps/features) |
| Logo UI ~3 MB | Optimizar asset en `apps/jaiba-ui/src/img/` |
| Plugin externo real | Referencia REST JSON Lines (Ciclo 4) |

## Prioridad 3 — producto (después del freeze)

Trabajo de producto **sin** abrir `priority-11+`:

| Frente | Estado / norma |
| --- | --- |
| **Observabilidad WS** | Dirty-check + throttle (`JAIBA_WS_POLL_MS`); sin snapshot ciego 1 s |
| **Empaquetado** | Binario en GitHub Release + imagen GHCR `jaiba-serve` ([packaging.md](packaging.md)) |
| **JME / AI Prep** | Madurar en lab **`DMA_JAIVA/`** (fuera de este repo). Al OSS solo se porta lo **estable** y documentado; no forman parte del recorrido Estable ni de la guía junior |

## Ocho frentes

1. **Producto mínimo estable** — Ciclo 1 (Postgres→CSV).  
2. **Pruebas end-to-end** — Playwright + smoke Compose.  
3. **Integraciones reales** — Docker reproducible; nightly para Kafka/Oracle/SQL Server.  
4. **Reducir módulos grandes** — extraer responsabilidades sin rewrite masivo.  
5. **Plugins externos** — adaptador REST JSON Lines de referencia.  
6. **Operación y seguridad** — límites, rate limit, auditoría, TLS, recuperación.  
7. **Rendimiento** — benchmarks rps / p95 / memoria / workers.  
8. **Presentación** — nombre Jaiba + matriz de madurez (esta página).

## Cuatro ciclos

| Ciclo | Foco | Estado |
| --- | --- | --- |
| **1. Estabilidad** | Recorrido oficial, Docker, E2E base | Hecho |
| **2. Mantenibilidad** | Split observability / connection_api / executor / FlowBuilder | Pendiente |
| **3. Producción** | Seguridad, recuperación, benchmarks, soak | Pendiente |
| **4. Extensibilidad** | Plugin externo real, SDK y docs de terceros | Pendiente |

## Comandos útiles

```bash
# Defaults + unit smoke (sin Docker)
./scripts/smoke-release-core.sh

# Stack Estable + flow Postgres→CSV
./scripts/release-core-up.sh
./scripts/smoke-stable-path.sh

# E2E UI (stack arriba)
cd apps/jaiba-ui && npm run e2e
```

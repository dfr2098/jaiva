# Endurecimiento del núcleo: Scheduler / DAG (Fase 0)

Contrato de planificación del motor
(`crates/jaiba-runtime/src/engine/executor/scheduler.rs` y el bucle en
`executor/mod.rs`).

Segunda etapa del plan de estabilidad del núcleo (después de
[PacketRepository](core-hardening-repository.md)). Trabajo en la rama
`cursor/core-hardening-scheduler` hasta que los tests nombrados pasen; luego PR
a `main`.

Relacionado: [architecture.md](architecture.md) § Paralelismo / backpressure,
[dag-mathematical-model.md](dag-mathematical-model.md) § 4–5.

## Alcance

| Dentro | Fuera |
| --- | --- |
| Cupos globales y por processor | Retry / duplicados de `OutputSender` (etapa 3) |
| `ordering`: unordered / preserve / partitioned | FlowSupervisor pause/drain (etapa 4) |
| Evitar deadlock con `queue_capacity` y semillas fuente | Soak largo (etapa 5) |
| Progreso (equidad básica) entre processors con carga desigual | Exactly-once |

## Reglas del scheduler (contrato)

`schedule_available` lanza tareas mientras `running < concurrency_limit`,
recorriendo `pending` en orden. Un ítem que no cabe **vuelve al final**
(`push_back`). Prioridad de comprobaciones:

1. Ciclo de vida = `Running` (si no, no agenda).
2. Semilla de fuente solo si
   `routed_backlog + running < engine.queue_capacity`.
3. Huecos reservados para profundidad aguas abajo
   (`running + 1 + reserved_downstream ≤ concurrency_limit`).
4. Límite por processor:
   - `preserve` → 1 tarea activa;
   - `unordered` / `partitioned` → `concurrent_tasks`.
5. `partitioned`: como máximo **una** tarea activa por
   `(processor_id, partition_key)`.
6. Con repositorio: `claim` antes de spawn; si no gana el claim, el ítem se
   descarta de esta pasada (otro worker lo tendrá o se recuperará).

Backpressure de emisión (fuera del scheduler puro, pero acoplado): el productor
espera si el canal de emisiones, la cola de arista o la memoria están llenos.
Un flow se declara en deadlock si una emisión diferida no puede caber nunca en
`queue_capacity`.

## Qué ya cubre CI hoy

| Área | Test existente |
| --- | --- |
| Límite global estricto | `global_concurrency_is_a_strict_upper_bound` |
| `ordering: preserve` → 1 activo | `preserve_order_forces_one_active_task_for_the_processor` |
| Streaming sin deadlock (`queue_capacity` bajo) | `strict_limit_keeps_a_streaming_pipeline_moving` |
| Salida terminal CPU con cola llena | `terminal_cpu_output_cannot_deadlock_a_full_upstream_queue` |
| Muchas fuentes > capacidad | `more_sources_than_queue_capacity_do_not_deadlock` |
| Fan-out y reserva de memoria | `branches_reserve_independent_copies_before_routing` |
| Clave de partición coherente | `partition_key_requires_one_value_per_packet` |
| Validación `max_concurrency` vs camino | `rejects_a_global_limit_smaller_than_the_streaming_path` |

## Huecos (matriz de aceptación)

| # | Escenario | Riesgo si falla | Deseado |
| --- | --- | --- | --- |
| D1 | Misma `partition_key` en un processor `partitioned` | Dos tareas de la misma clave a la vez → desorden / carreras | Concurrencia por clave = 1 |
| D2 | Claves distintas en el mismo processor | Capacidad infrautilizada | Pueden correr en paralelo hasta `concurrent_tasks` |
| D3 | Un processor lento con cola larga y otro rápido en paralelo | Hambruna del rápido | Ambos terminan en tiempo acotado (ítems diferidos rotan con `push_back`) |
| D4 | Fan-out a dos sinks bajo `queue_capacity` justo | Deadlock o pérdida | Completa sin timeout (regresión de routing + cupos) |
| D5 | Processor lento aguas arriba + sink | Pipeline se atasca | Progreso hasta vaciar (ya parcialmente cubierto; reforzar) |

## Tests nombrados

| ID | Test | Comprueba | Estado |
| --- | --- | --- | --- |
| S1 | `partitioned_same_key_runs_serially` | D1: máximo activo por clave = 1 | ✓ |
| S2 | `partitioned_different_keys_run_in_parallel` | D2: máximo global ≥ 2 con dos claves | ✓ |
| S3 | `slow_processor_does_not_starve_sibling` | D3: sink rápido termina aunque el lento duerma | ✓ |
| S4 | `fan_out_under_tight_capacity_completes` | D4: dos destinos, capacidad baja, sin deadlock | ✓ |
| S5–S12 | (existentes arriba) | Regresiones ya en CI | ✓ |

## Plan en esta rama

1. **Doc (este archivo)** — contrato + huecos + nombres.
2. **Tests S1–S4** — probes en `executor/tests.rs`.
3. **Solo si un test revela bug** — ajuste mínimo en `scheduler.rs` / routing.
4. **PR a `main`** cuando S1–S4 + suite previa estén verdes.

## Criterio de salida

- [x] S1–S4 implementados y verdes (unidad).
- [x] Sin regressión detectada en la batería local de executor.
- [ ] PR fusionado a `main`.

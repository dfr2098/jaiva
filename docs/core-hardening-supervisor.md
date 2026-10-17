# Endurecimiento del núcleo: FlowSupervisor / ciclo de vida (Fase 0)

Cuarta etapa del plan de estabilidad del núcleo (después de
[PacketRepository](core-hardening-repository.md),
[Scheduler](core-hardening-scheduler.md) y
[entrega](core-hardening-delivery.md)).

Código: `engine/supervisor.rs`, `engine/control.rs`, bucle de ciclo de vida en
`executor/mod.rs`. Rama de trabajo: `cursor/core-hardening-supervisor`.

Contrato operativo histórico: [priority-7-control-plane.md](history/priority-7-control-plane.md).

## Estados

```text
STOPPED → STARTING → RUNNING ⇄ PAUSED
                         │         │
                         └──→ DRAINING → STOPPED

STARTING / RUNNING / DRAINING → FAILED
```

| Estado | Comportamiento del scheduler |
| --- | --- |
| `RUNNING` | Programa trabajo nuevo según cupos. |
| `PAUSED` | No programa trabajo nuevo; deja terminar tareas activas; espera señal. |
| `DRAINING` | No programa trabajo nuevo; al vaciar `running` termina el flujo. |
| `STOPPED` / `FAILED` | Terminal; se puede `start` otra vez. |

`snapshot().ready` es `true` solo en `RUNNING` o `PAUSED` (alineado con
`GET /ready`).

## Contrato de API (`FlowSupervisor`)

1. `start` — si ya hay tarea viva, `Ok(false)`. Si no, crea motor, pasa a
   `STARTING`, espera el oneshot de inicialización (`Ok(true)`), o error si se
   detuvo / falló antes.
2. `pause` / `resume` — solo `RUNNING → PAUSED` y `PAUSED → RUNNING`;
   actualizan métrica de estado de flujo.
3. `drain` — desde `RUNNING`, `PAUSED` o `STARTING` → `DRAINING`.
4. `stop_gracefully` — pide `DRAINING`, espera hasta
   `engine.shutdown.drain_timeout_seconds`. Si vence y `force_after_timeout`:
   aborta la tarea y deja `STOPPED`. Si vence y no fuerza: `Err`, **conserva**
   la tarea (sigue en `DRAINING`).
5. `wait_for_terminal` — bloquea hasta `STOPPED` (Ok resumen) o `FAILED` (Err).

## Qué ya cubre CI

| Área | Test |
| --- | --- |
| Transiciones pause/resume/drain en `FlowControl` | `enforces_pause_resume_and_drain_transitions` |
| Pause/resume en el motor (sin Supervisor) | `pause_stops_downstream_scheduling_until_resume` |
| Drain en el motor sin programar pendiente | `drain_finishes_active_work_without_scheduling_pending_work` |
| Reinicio tras completar | `completed_flow_can_be_started_again` |
| Stop mientras `STARTING` (conn. inalcanzable) | `flow_stuck_initializing_can_be_stopped` |

## Huecos → tests nombrados

| ID | Test | Comprueba |
| --- | --- | --- |
| V1 | `supervisor_pause_resume_preserves_work` | Via `FlowSupervisor`: pause no tumba el flujo; resume termina con el trabajo esperado |
| V2 | `supervisor_drain_finishes_active_without_pending` | `drain` vía Supervisor → termina sin programar el pendiente (p. ej. sink) |
| V3 | `stop_gracefully_timeout_without_force_keeps_task` | Timeout + `force_after_timeout: false` → `Err`, estado `DRAINING` |
| V4 | `stop_gracefully_force_aborts_after_timeout` | Timeout + force → `Ok`, `STOPPED` |
| V5 | `start_returns_false_while_already_running` | Segundo `start` con tarea viva → `Ok(false)` |

## Plan en esta rama

1. Este documento + enlaces.
2. Tests V1–V5 (inyección de registry de prueba en Supervisor si hace falta).
3. **No** cambiar el grafo de estados ni exactly-once.
4. PR a `main` cuando CI esté verde.

## Criterio de salida

- [x] V1–V5 verdes.
- [x] Contrato documentado (este archivo + architecture).
- [x] PR fusionado a `main`.

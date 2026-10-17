# Endurecimiento del núcleo: PacketRepository (Fase 0)

Contrato de consistencia y recuperación del repositorio local de paquetes
(`crates/jaiba-runtime/src/engine/repository.rs`).

Esta página es la **Fase 0** del trabajo de estabilidad del núcleo: define
estados, transiciones, matriz de interrupciones y tests nombrados. **No cambia
código de producción.** Las fases 1+ implementan lo marcado como “deseado”.

Garantía pública (sin cambios): **at-least-once**. Tras una recuperación, un
paquete puede reejecutarse; los destinos deben ser idempotentes.

Relacionado: [architecture.md](architecture.md), [dag-mathematical-model.md](dag-mathematical-model.md),
[history/priority-5-dead-letter.md](history/priority-5-dead-letter.md),
[history/priority-6-provenance.md](history/priority-6-provenance.md).

## Alcance

| Dentro | Fuera (otras etapas del núcleo) |
| --- | --- |
| Cola SQLite (`PENDING` / `RUNNING` / `COMPLETED` / `DEAD_LETTER`) | Scheduler / equidad entre procesadores |
| Provenance emparejado a cada transición crítica | Contrato de `OutputSender` / duplicados por emit-antes-de-fallar |
| Contenido en disco (SHA-256) y huérfanos | MemoryLimiter, FlowSupervisor, plugins |
| `recover_abandoned`, cleanup, requeue de DLQ | Exactly-once; unificar con JME |

## Máquina de estados

Estados en `jaiva_packet_queue.status`:

```text
                 claim
    PENDING ──────────────► RUNNING
       ▲                       │
       │ recover_abandoned     │
       │ (RUNNING abandonado)  │
       └───────────────────────┤
                               │ complete (éxito) ──► COMPLETED
                               │ fail / complete(Failed) ──► DEAD_LETTER
                               ▼
                          DEAD_LETTER
                               │
                               │ requeue_dead_letter
                               ▼
                            PENDING
```

### Transiciones permitidas

| Desde | Hacia | Operación | Evento de provenance esperado |
| --- | --- | --- | --- |
| — (nuevo) | `PENDING` | `enqueue` | `ENQUEUED` |
| `PENDING` | `RUNNING` | `claim` | `CLAIMED` |
| `RUNNING` | `PENDING` | `recover_abandoned` | `RECOVERED` |
| `RUNNING` | `COMPLETED` | `complete(Completed)` (u otro éxito) | el evento pasado a `complete` |
| `RUNNING` | `DEAD_LETTER` | `fail` o `complete(Failed)` | `FAILED` (o el evento de `complete`) |
| `DEAD_LETTER` | `PENDING` | `requeue_dead_letter` | `REQUEUED` |

Cualquier otra transición debe ser **no-op** (0 filas afectadas) o **error
explícito**. No debe escribir provenance de “éxito” si el `UPDATE` no movió la fila.

### Invariantes

1. Si existe fila con `content_key = K`, el fichero de contenido para `K` existe
   y su SHA-256 coincide (salvo ventana documentada durante `enqueue`; ver
   matriz).
2. Tras una transición crítica confirmada, hay **al menos** un evento de
   provenance coherente con el nuevo estado (misma transacción en el deseado).
3. `claim` es exclusivo: como máximo un ganador por `queue_id` desde `PENDING`.
4. `COMPLETED` y `DEAD_LETTER` son terminales respecto al scheduler hasta un
   `requeue` explícito (solo desde DLQ).

## Comportamiento actual vs deseado

Resumen del código en `LocalPacketRepository` al documentar esta fase:

| Operación | Hoy | Deseado (Fase 1–2) |
| --- | --- | --- |
| `enqueue` | `content.put` **antes** del `BEGIN`; TX SQL OK | **Fase 2:** `reconcile_orphans` en `open` limpia blobs/`.tmp` huérfanos |
| `claim` | ~~`UPDATE` y provenance separados~~ → **Fase 1:** misma TX; solo si `PENDING` | Hecho |
| `complete` | ~~sin exigir `RUNNING`~~ → **Fase 1:** `WHERE status = 'RUNNING'`; misma TX | Hecho |
| `fail` | ~~provenance aparte~~ → **Fase 1:** misma TX | Hecho |
| `recover_abandoned` | ~~loop suelto~~ → **Fase 1:** una TX; `UPDATE` solo si sigue `RUNNING` | Hecho |
| `requeue_dead_letter` | ~~provenance aparte~~ → **Fase 1:** misma TX; solo desde `DEAD_LETTER` | Hecho |
| Contenido `.tmp` | rename atómico tras `fsync` | Igual; GC de `.tmp` viejos + blobs sin fila |

## Matriz de interrupción (crash)

Cada fila describe un kill del proceso en ese instante. “Hoy” es el riesgo
observado; “Deseado” es el criterio de aceptación de las fases 1–2.

| # | Momento del kill | Efecto hoy (riesgo) | Deseado |
| --- | --- | --- | --- |
| C1 | Tras `content.put` (rename OK), antes del `COMMIT` de `enqueue` | Blob en disco **sin** fila en cola | `reconcile_orphans` borra blobs sin `content_key` (y `.tmp` antiguos) |
| C2 | Tras `COMMIT` de `enqueue`, antes de devolver `queue_id` al caller | Fila `PENDING` + `ENQUEUED` (OK); el caller puede no encolar en memoria | at-least-once: al reiniciar, `pending()` recupera el trabajo |
| C3 | Tras `UPDATE` de `claim` a `RUNNING`, antes de `CLAIMED` | `RUNNING` **sin** evento `CLAIMED` | Misma TX: no existe `RUNNING` sin `CLAIMED` |
| C4 | Tras `UPDATE` de `complete` a `COMPLETED`, antes de provenance | `COMPLETED` sin evento de cierre | Misma TX |
| C5 | Tras `UPDATE` de `fail` a `DEAD_LETTER`, antes de `FAILED` | DLQ sin evento | Misma TX |
| C6 | Tras `UPDATE` de recover a `PENDING`, antes de `RECOVERED` | `PENDING` sin `RECOVERED` | Misma TX |
| C7 | Tras `UPDATE` de requeue, antes de `REQUEUED` | `PENDING` sin `REQUEUED` | Misma TX |
| C8 | Durante `cleanup_completed`: fila borrada, antes de `content.delete` | Blob huérfano temporal | Reconcile o delete en el mismo ciclo; test de ref-count |
| C9 | Kill con filas `RUNNING` y `claimed_at` antiguo | Quedan abandonadas hasta recover | `recover_abandoned` las devuelve a `PENDING` + `RECOVERED` |
| C10 | Fila con `content_key` pero fichero ausente/corrupto | Fallo al materializar el paquete | Error de repositorio claro → ruta failure/DLQ, sin panic |

**No objetivo de esta etapa:** impedir que C2/C9 reejecuten el procesador
(sigue siendo at-least-once).

## Tests nombrados (aceptación)

Nombres canónicos para implementar en Fase 1+ (unidad en
`repository.rs` / módulo de tests; integración con tempdir). Marcar ✓ al
fusionar el PR que los haga pasar.

### Máquina de estados y atomicidad (Fase 1)

| ID | Test | Comprueba | Estado |
| --- | --- | --- | --- |
| T1 | `claim_is_exclusive_and_records_claimed_in_same_commit` | Dos `claim` concurrentes: un ganador; si gana, existe `CLAIMED` en la misma visibilidad post-commit | ✓ Fase 1 |
| T2 | `complete_requires_running_and_is_atomic_with_provenance` | `complete` desde `PENDING` no cambia estado ni escribe cierre; desde `RUNNING` deja `COMPLETED` + evento juntos | ✓ Fase 1 |
| T3 | `fail_requires_running_and_is_atomic_with_provenance` | Igual para `DEAD_LETTER` + `FAILED` | ✓ Fase 1 |
| T4 | `recover_abandoned_is_atomic_per_row` | Tras recover, no hay `PENDING` “abandonado” sin `RECOVERED` | ✓ Fase 1 |
| T5 | `requeue_dead_letter_requires_dead_letter` | Desde `COMPLETED`/`PENDING` → no-op; desde DLQ → `PENDING` + `REQUEUED` atómicos | ✓ Fase 1 |
| T6 | `enqueue_keeps_queue_row_and_enqueued_atomic` | Regresión: ya existe; mantener verde | ✓ Fase 1 |

### Contenido huérfano (Fase 2)

| ID | Test | Comprueba | Estado |
| --- | --- | --- | --- |
| T7 | `reconcile_deletes_content_without_queue_row` | Simula C1: blob sin fila → reconcile lo elimina | ✓ Fase 2 |
| T8 | `reconcile_keeps_content_referenced_by_queue` | Blob con fila → intacto | ✓ Fase 2 |
| T9 | `stale_content_tmp_files_are_removed` | `*.tmp` antiguos se limpian | ✓ Fase 2 |
| T10 | `missing_or_corrupt_content_surfaces_repository_error` | Simula C10 | ✓ Fase 2 |

### Recuperación de punta a punta (Fase 3)

| ID | Test | Comprueba | Estado |
| --- | --- | --- | --- |
| T11 | `restart_requeues_abandoned_running_work` | Filas `RUNNING` viejas → `recover_abandoned` → terminan | ✓ Fase 3 |
| T12 | `pending_after_crash_mid_enqueue_commit_is_visible` | Tras commit de enqueue, reinicio ve el trabajo (C2) | ✓ Fase 3 |
| T13 | `stats_and_invariants_hold_after_recover_and_cleanup` | `pending=running=0` al terminar; sin huérfanos de content | ✓ Fase 3 |

## Integración

Trabajo en una sola rama `cursor/core-hardening-repository` (fases 0–3).
Se abre PR a `main` solo cuando T1–T13 pasan en CI.

| Fase | Contenido | Estado |
| --- | --- | --- |
| **0** | Contrato + matriz + tests nombrados | ✓ |
| **1** | TX en claim/complete/fail/recover/requeue + T1–T6 | ✓ |
| **2** | `reconcile_orphans` en `open` + T7–T10 | ✓ |
| **3** | Reinicio / recover / cleanup + T11–T13 | ✓ |

## Criterio de salida

- [x] Máquina de estados e invariantes escritos.
- [x] Matriz C1–C10 con hoy/deseado.
- [x] Tests T1–T13 implementados y verdes en unidad.
- [x] PR a `main` tras revisión y CI verde (#19).

## Referencia rápida de API

Trait `PacketRepository` (métodos relevantes): `enqueue`, `claim`, `complete`,
`fail`, `requeue_dead_letter`, `recover_abandoned`, `pending`,
`cleanup_completed`, `record_event` / consultas de provenance.

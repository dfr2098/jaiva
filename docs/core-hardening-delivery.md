# Endurecimiento del núcleo: Retry / OutputSender / entrega (Fase 0)

Tercera etapa del plan de estabilidad del núcleo (después de
[PacketRepository](core-hardening-repository.md) y
[Scheduler](core-hardening-scheduler.md)).

Código: `executor/retry.rs`, `engine/processor.rs` (`OutputSender`), enrutado
en `executor/routing.rs`. Rama de trabajo:
`cursor/core-hardening-delivery`.

## Garantía pública

Jaiba ofrece **at-least-once** para trabajo persistido y para emisiones que ya
entraron en el canal de routing:

- Un paquete **puede repetirse** tras reintento o recuperación.
- Los destinos deben ser **idempotentes** (`upsert`, claves únicas, etc.).

No se promete exactly-once ni “emitir = commit en el sink”.

## Dos niveles de “aceptado”

| Nivel | Qué significa | Dónde |
| --- | --- | --- |
| **A — Canal / memoria** | `OutputSender::emit` / `success` devolvió `Ok`: el paquete tiene reserva de memoria (si aplica) y está en el `mpsc` de emisiones (o se contabilizó como terminal sin arista). | `processor.rs` |
| **B — Persistido en cola** | Fila en `PacketRepository` (`PENDING`…) con content en disco. | `repository.rs` |
| **C — Destino externo** | Broker / DB / fichero del sink confirmó. | Conectores (`publish_kafka`, `put_*`, …) |

**A ⇏ C.** El comentario de `OutputSender` ya lo dice: aceptar en el canal no
implica que un destino aguas abajo haya hecho commit.

**A + reintento ⇒ posible duplicado en A (y luego en C si el sink no es
idempotente).** Si un processor emite por `success` y después devuelve error
reintentable, esas emisiones **ya salieron**; el reintento puede emitir otra
vez.

## Contrato de `execute_with_retry`

1. Copia de trabajo: `reserve_working_copy` si el processor retiene el input;
   fallo de memoria → error (suele ir a `failure` sin tumbar el flow).
2. Timeout opcional (`scheduling.timeout_ms`) → error de processor (reintentable
   salvo política futura).
3. `Ok(())` → métricas `processed`; si no hubo emisiones, cuenta records de
   entrada; provenance `Processed`.
4. Error **reintentable** y `attempt < maximum_attempts` → `attempt++`, backoff
   exponencial acotado, provenance `Retried`, **mismo** `OutputSender` (las
   emisiones previas no se revierten).
5. Error no reintentable (`PacketTooLarge`, `DatabaseRejected`,
   `MessageRejected`, …) o intentos agotados → métricas `failed`, emite por
   relación `failure` con `error.processor` / `error.message`,
   `Ok(Some((mensaje, attempt)))`.
6. `PacketTooLarge` se propaga como `Err` (no se envuelve en failure local del
   mismo modo que otros; el caller del scheduler lo trata como fatal de tarea).

## Qué ya cubre CI

| Área | Test |
| --- | --- |
| Memoria agotada en retry → failure del paquete | `exhausted_packet_memory_fails_the_packet_not_the_flow` |
| Retry → DLQ → requeue | `flow_retry_then_dead_letter` |
| Canal acotado espera | `bounded_output_waits_until_capacity_is_available` |
| Input holder no se bloquea a sí mismo | `input_holder_fails_instead_of_waiting_for_its_own_memory` |
| Errores a relación `failure` | `routes_processor_errors_to_failure` |

## Huecos → tests nombrados

| ID | Test | Comprueba |
| --- | --- | --- |
| E1 | `emit_then_retryable_fail_can_duplicate_on_retry` | Emite `success`, falla reintentable, en el 2º intento emite otra vez → el receptor ve **≥ 2** paquetes (duplicado esperado / at-least-once) |
| E2 | `non_retryable_rejection_does_not_retry` | `MessageRejected` (o equivalente) → una sola ejecución; va a failure sin segundo intento |
| E3 | `retry_backoff_increments_attempt_until_success` | Fallos reintentables luego `Ok`; `retried` / attempt finales coherentes |
| E4 | `output_sender_docs_channel_is_not_sink_commit` | Regresión de documentación / comentario en API (sin assert de runtime extra; cubierto por rustdoc + E1) |

## Plan en esta rama

1. Este documento + enlaces.
2. Tests E1–E3 (y reforzar rustdoc de `OutputSender` si hace falta).
3. **No** cambiar la semántica a exactly-once.
4. PR a `main` cuando CI esté verde.

## Criterio de salida

- [x] E1–E3 verdes.
- [x] Contrato documentado (este archivo + architecture).
- [x] PR fusionado a `main`.

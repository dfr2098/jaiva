# Endurecimiento del núcleo: Soak / plugins (Fase 0)

Quinta (y última) etapa del plan de estabilidad del núcleo (después de
[PacketRepository](core-hardening-repository.md),
[Scheduler](core-hardening-scheduler.md),
[entrega](core-hardening-delivery.md) y
[Supervisor](core-hardening-supervisor.md)).

Rama de trabajo: `cursor/core-hardening-soak-plugins`.

## Dos capas

| Capa | Qué valida | Dónde |
| --- | --- | --- |
| **A — Probes CI / local rápido** | Plugin reutilizado bajo ráfaga; N ciclos del flujo ejemplo sin `failed` | Tests Rust + `scripts/mini-soak-plugins.sh` |
| **B — Soak operativo (stack Estable)** | Carga sostenida 1 h (stress → soak → chaos) | [stable-stack-tests.md](stable-stack-tests.md), `soak-stable-stack.sh` |

La etapa 5 **no** mete el soak de 1 h en CI. Documenta el contrato y añade
probes de la capa A; la capa B sigue siendo aceptación del operador.

## Contrato de plugins bajo carga

1. Un nodo `external_plugin` **reutiliza** el proceso hijo entre paquetes
   mientras las respuestas cumplan el protocolo y no venza el timeout.
2. Timeout / respuesta inválida → se mata el proceso; el siguiente paquete
   arranca uno nuevo (sin tumbar el flow por sí solo).
3. El flujo nombra solo `plugin: <id>`; nunca comandos. Sin `JAIBA_PLUGIN_DIR`
   los externos están desactivados.
4. Mini-soak local: N ejecuciones seguidas de
   `examples/plugins/mayusculas-flow.yaml` deben terminar con el registro
   esperado y sin error de proceso.

Plugins (guía): [plugins.md](plugins.md). Catálogo smoke:
`scripts/smoke-plugin-catalog.sh`.

## Qué ya cubre CI

| Área | Test / paso |
| --- | --- |
| Plugin Python ok / error / hang+reinicio | `runs_a_python_plugin_and_recovers_from_errors_and_hangs` |
| Contenedor + `rm -f` tras hang | `container_plugins_run_through_the_runtime_and_are_removed_on_hang` |
| Flujo ejemplo mayúsculas | CI: `JAIBA_PLUGIN_DIR=examples/plugins … mayusculas-flow.yaml` |
| Catálogo install + digest | `scripts/smoke-plugin-catalog.sh` |
| Soak 1 h stack | Manual / ops (`soak-stable-stack.sh`) |

## Huecos → probes nombrados

| ID | Probe | Comprueba |
| --- | --- | --- |
| P1 | `sustained_plugin_reuse_across_many_packets` | Mismo `ExternalPlugin`: muchas peticiones seguidas → todas OK; proceso reutilizado |
| P2 | `plugin_burst_packet_transforms_all_records` | Un paquete con muchos records → todos transformados |
| P3 | `scripts/mini-soak-plugins.sh` | N ciclos del flujo ejemplo (default 20) sin fallo |

## Plan en esta rama

1. Este documento + enlaces.
2. Tests P1–P2 + script P3 (+ paso CI del mini-soak).
3. **No** exigir soak 1 h en CI ni cambiar el protocolo de plugins.
4. PR a `main` cuando las probes locales/CI estén verdes.

## Criterio de salida

- [x] P1–P2 verdes; P3 OK en local.
- [x] Contrato documentado (este archivo + architecture / ops).
- [ ] PR fusionado a `main`.

# Fase JME — Jaiba Memory Engine (Pasos 0–8)

Motor de **ciclo de vida de datos de dominio**: clasifica, decide dónde vive
cada dato y cuándo desaparece, se demote o se persiste.

No es un caché genérico ni un Redis. No sustituye el repositorio de paquetes del
DAG (`PacketRepository`). Es una capa paralela para estado operativo
(telegramas, carriers, alarmas, inventarios, configuración, …).

> **Estado actual:** integrado al runtime con Hot RAM, Warm distribuido
> opcional (Redis es un proveedor), Cold SSD segmentado con LZ4 y lectura bajo
> demanda, Frozen y persistencia/rebuild.

## Motivación

Jaiba ya tiene:


| Pieza                            | Rol                                    |
| -------------------------------- | -------------------------------------- |
| `MemoryLimiter`                  | Presión de RAM del runtime de paquetes |
| Colas + backpressure             | Frenar productores del DAG             |
| `PacketRepository` + content SHA | Persistencia de **paquetes de flujo**  |
| `put_database` batches           | Escritura diferida a destinos          |
| Provenance / DLQ                 | Auditoría de **ejecución**             |


JME aporta el clasificador de **estado de negocio** que responde:

- ¿Dónde vive?
- ¿Cuánto vive?
- ¿Quién lo usa?
- ¿Puede eliminarse?
- ¿Debe persistirse?
- ¿Debe (algún día) replicarse?

Filosofía: **recibir → clasificar → decidir → (tal vez) persistir**.

Analogía con Linux: Hot ≈ RAM activa; Warm/Cold/Frozen ≈ reclaim semántico
(no swap opaco del kernel). El swap del SO sigue siendo red de seguridad; JME
decide el lifecycle del dato con semántica.

## Fuera de alcance (explícito)


| Incluido en JME                            | Fuera de alcance                    |
| ------------------------------------------ | ----------------------------------- |
| Políticas declarativas YAML                | Allocator custom / arenas globales  |
| Hot RAM y selección semántica de víctimas  | Sustituir el swap del sistema       |
| Warm como *trait* (`none` o Redis opcional)| Cluster distribuido transparente    |
| Cold local segmentado y sinks persistentes | Mezclar con `PacketRepository`      |
| Frozen e immediate para datos críticos     | UI completa de gestión              |
| Métricas de presión y lifecycle            | PostgreSQL embebido en el Cold local|


Redis se puede enchufar como proveedor `WarmStore` mediante la feature `redis`;
no es un nivel fijo ni obligatorio. Valkey puede usarse cuando sea compatible
con el protocolo configurado.

## Separación de capas (innegociable)

```text
┌─────────────────────────────────────────────┐
│  FlowEngine / PacketRepository / Provenance │  ← ciclo de vida del DAG
└─────────────────────────────────────────────┘
┌─────────────────────────────────────────────┐
│  Jaiba Memory Engine (JME)                  │  ← ciclo de vida de dominio
│  Hot · Warm · Cold · Frozen                 │
└─────────────────────────────────────────────┘
```

Un paquete de flujo puede *alimentar* JME (`publish` / `upsert_state`), pero
JME no es el almacén de cola del scheduler.

## Vocabulario



### Temperatura (dónde vive)


| Temperatura | Dónde                               | Uso típico                                   |
| ----------- | ----------------------------------- | -------------------------------------------- |
| `hot`       | Solo RAM del proceso                | Último telegrama, posición, sesión           |
| `warm`      | RAM + backend opcional              | Consultas frecuentes (carrier, orden activa) |
| `cold`      | SSD local segmentado                 | Casi no se consulta en caliente              |
| `frozen`    | Archivo / objeto comprimido         | Auditoría, histórico                         |




### Política (qué hacer)


| Política     | Comportamiento                                    |
| ------------ | ------------------------------------------------- |
| `volatile`   | Solo Hot; TTL; se pierde a propósito              |
| `cache`      | Hot (y Warm si hay backend); rebuild si expira    |
| `deferred`   | Hot/buffer → flush periódico a Cold               |
| `immediate`  | Persistir ya; fallo ruidoso si no se puede        |
| `persistent` | Alias semántico de immediate para config/usuarios |




### Prioridad (urgencia bajo presión)


| Prioridad  | Efecto                                           |
| ---------- | ------------------------------------------------ |
| `critical` | Nunca eviction; immediate si la política lo pide |
| `high`     | Flush rápido; demote tarde                       |
| `normal`   | Comportamiento por defecto                       |
| `low`      | Primer candidato a eviction / TTL corto          |


Prioridad **manda** sobre presión de memoria: un `critical` no se tira para
liberar Hot.

## Contrato de API (borrador Paso 1+)

Los módulos de negocio no eligen Redis ni SQL. Solo publican:

```text
MemoryManager
  upsert(key, value, class)
  get(key) -> Option<value>
  remove(key)
  publish(event)          # atajo: class inferida por event.kind
```

`class` referencia una entrada del YAML de lifecycle (abajo). El motor aplica
temperatura, TTL, flush y persistencia.

Clave sugerida: `"{kind}:{id}"` (p. ej. `carrier:A12`, `telegram:last:chute3`).

Valores: bytes o JSON (`serde_json::Value`) en MVP; esquema tipado después.

## Configuración declarativa (contrato)

Las políticas viven fuera del código de módulos. Hot-reload es deseable desde
Paso 1; obligatorio no es.

```yaml
# Runtime (Paso 7): engine.domain_memory.policy_file apunta a este YAML.
# No confundir con engine.memory.maximum_percent (RAM de paquetes).
memory:
  warm:
    backend: none          # none | redis
    # url_env: REDIS_URL   # solo si backend: redis

  defaults:
    priority: normal

  classes:
    telegram:
      policy: volatile
      temperature: hot
      ttl: 5m
      priority: low

    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      priority: high
      # rebuild: opcional (referencia a query/conexión) — Paso 5+

    inventory:
      policy: deferred
      temperature: cold
      flush: 2s
      priority: high

    alarm:
      policy: immediate
      temperature: cold
      priority: critical

    configuration:
      policy: persistent
      temperature: cold
      priority: critical
```



### Reglas de validación (contrato)

- `ttl` / `flush` solo con unidades explícitas (`s`, `m`, `h`).
- `immediate` / `persistent` + `priority: critical` no admiten eviction.
- `deferred` exige `flush` > 0.
- `warm.backend: redis` sin feature/URL → error de configuración.
- Clases desconocidas en `upsert` → error (no silencio).



## Backends (enchufes)

```text
HotStore     → RamStore (Paso 1)          [obligatorio]
WarmStore    → Noop | Redis (feature)     [Pasos 4 y 6]
ColdStore    → Segmented LZ4              [Paso 8; mmap es lectura opcional]
PersistSink  → JSONL / writer durable     [Pasos 2–3]
FrozenStore  → File archive               [Paso 6]
```

Las políticas apuntan a **comportamiento**, no a marca de producto.

## Relación con MemoryLimiter


| Componente      | Pregunta                                            |
| --------------- | --------------------------------------------------- |
| `MemoryLimiter` | ¿Hay presupuesto de RAM para paquetes/estado?       |
| JME             | ¿Qué estado de dominio sacrificar o demote primero? |


Bajo presión: JME evict/demote `low` → `normal` → … y **nunca** `critical`.
El limiter puede señalar presión; JME ejecuta la política.

## Métricas (contrato de observabilidad)

Nombres orientativos (Prometheus):


| Métrica                                 | Significado                     |
| --------------------------------------- | ------------------------------- |
| `jaiba_memory_hot_objects`              | Objetos en Hot                  |
| `jaiba_memory_hot_bytes`                | Bytes JSON estimados en Hot     |
| `jaiba_memory_warm_objects`             | Objetos en Warm (0 si noop)     |
| `jaiba_memory_cold_objects`             | Objetos indexados en Cold local |
| `jaiba_memory_cold_bytes`               | Bytes de segmentos Cold         |
| `jaiba_memory_cold_max_disk_bytes`      | Cuota Cold; cero = ilimitada     |
| `jaiba_memory_cold_quota_rejections_total` | Demotions rechazadas por cuota |
| `jaiba_memory_cold_hits_total`          | Lecturas resueltas desde Cold   |
| `jaiba_memory_cold_misses_total`        | Fallos de búsqueda en Cold      |
| `jaiba_memory_pressure_ratio`           | Uso vs presupuesto JME          |
| `jaiba_memory_evictions_total`          | Evictions por clase/prioridad   |
| `jaiba_memory_persist_queue`            | Pendientes de flush deferred    |
| `jaiba_memory_promotions_total`         | Cold/Warm → Hot                 |
| `jaiba_memory_demotions_total`          | Hot → Warm/Cold                 |
| `jaiba_memory_immediate_failures_total` | Fallos de persistencia critical |


Objetivo Grafana: ver si Hot ~95 % / Warm pequeño / Cold residual — o si las
políticas están mal (todo Hot eterno, o todo yendo a Cold).

## Roadmap controlado


| Paso  | Entrega                                           | Criterio de cierre                         |
| ----- | ------------------------------------------------- | ------------------------------------------ |
| **0** | Este documento                                    | Vocabulario y límites acordados            |
| **1** | `crates/jaiba-memory` Hot + TTL/LRU + YAML mínimo | Tests + ejemplo in-process                 |
| **2** | `immediate` → writer                              | Critical no se pierde en crash de proceso* |
| **3** | `deferred` + flush                                | Batch medible; cola acotada                |
| **4** | Trait `WarmStore` + `backend: none`               | Compila sin Redis                          |
| **5** | Promote/demote + rebuild hooks                    | Métricas promotion/demotion                |
| **6** | Frozen + Redis opcional                           | Solo cuando duela de verdad                |
| **7** | Cablear JME al runtime                            | Context + presión + procesadores finos     |
| **8** | Cold SSD segmentado + política semántica          | Reinicio, cuota, LZ4, métricas y docs      |
| **9** | Compactación + manifiesto durable                 | Rename atómico y recuperación de espacio   |


“No se pierde” = durabilidad del sink configurado (Postgres, etc.), no magia
sin almacenamiento.

## Anti-patrones

1. Meter JME dentro de `PacketRepository`.
2. TTL hardcodeados por módulo en lugar de `classes`.
3. Añadir Redis antes de Hot estable.
4. Allocator custom.
5. Un “cache global” sin `kind` / `priority`.
6. Hacer `immediate` el default (mata el throughput).



## Relación con el resto del proyecto

- Runtime / memoria de paquetes: `[architecture.md](architecture.md)`
- Escrituras / batch: `[priority-4-database-writes.md](priority-4-database-writes.md)`
- Métricas: `[priority-9-metrics.md](priority-9-metrics.md)`
- Visión: `[project-vision.md](project-vision.md)`
- Laboratorio / integración con DMA: carpeta hermana `DMA_JAIVA/` (fuera de este repo)



## Checklist Paso 0

- [x] Separación DAG vs dominio documentada
- [x] Temperaturas, políticas y prioridades definidas
- [x] YAML de ejemplo de clases
- [x] Hueco Warm/`none` sin dependencia obligatoria de Redis
- [x] Métricas y roadmap por pasos
- [x] Aceptación del vocabulario (equipo / producto)

## Checklist Paso 1 (Hot)

- [x] Crate [`crates/jaiba-memory`](../crates/jaiba-memory)
- [x] `MemoryManager::{upsert,get,remove,notify_pressure,snapshot}`
- [x] Políticas YAML `volatile` | `cache` + TTL + prioridad
- [x] Eviction: expirados → low→high LRU; **nunca** `critical`
- [x] Ejemplo [`examples/jme-hot-policy.yaml`](../examples/jme-hot-policy.yaml)
- [x] Tests unitarios (`cargo test -p jaiba-memory`)

## Checklist Paso 2 (Immediate)

- [x] Trait `ImmediateSink` + `RecordingSink` / `JsonlFileSink`
- [x] Políticas `immediate` | `persistent` (default priority `critical`)
- [x] Persist **antes** de Hot; fallo → no Hot + `immediate_failures`
- [x] Construcción exige sink si hay clases immediate/persistent
- [x] Ejemplo [`examples/jme-immediate-policy.yaml`](../examples/jme-immediate-policy.yaml)

## Checklist Paso 3 (Deferred)

- [x] Política `deferred` + `flush` obligatorio
- [x] Cola acotada (`max_pending_deferred`) con coalesce por clave
- [x] Hot inmediato; Cold por intervalo / tope / `flush()` / presión
- [x] Métricas `persist_queue`, `deferred_writes`, `deferred_flushes`
- [x] Ejemplo [`examples/jme-deferred-policy.yaml`](../examples/jme-deferred-policy.yaml)

## Checklist Paso 4 (WarmStore)

- [x] Trait `WarmStore` + `NoopWarmStore` (`warm.backend: none`)
- [x] `MemoryPolicy.warm_backend`; Redis habilitado por feature desde Paso 6
- [x] `cache` + `temperature: warm` espeja a Warm; Hot miss → promote ligero
- [x] Snapshot: `warm_objects`, `warm_hits` / `misses`, `promotions`
- [x] Ejemplo [`examples/jme-warm-policy.yaml`](../examples/jme-warm-policy.yaml)

## Checklist Paso 5 (Promote / Demote / Rebuild)

- [x] Eviction Hot de `cache`+`warm` → demote a `WarmStore` + métrica `demotions`
- [x] Promote Warm→Hot en miss (Paso 4) con contador `promotions`
- [x] YAML `rebuild:` (solo `cache`) + trait `RebuildHook`
- [x] Get path: Hot → Warm → rebuild hook
- [x] Ejemplo [`examples/jme-lifecycle-policy.yaml`](../examples/jme-lifecycle-policy.yaml)

## Checklist Paso 6 (Frozen + Redis opcional)

- [x] Trait `FrozenStore` + `FileFrozenStore` / `NoopFrozenStore`
- [x] YAML `memory.frozen.backend: file` + `path`; `temperature: frozen`
- [x] Get path: Hot → Warm → Frozen → rebuild; demote frozen bajo presión
- [x] `WarmBackend::Redis` + feature `redis` (`RedisWarmStore`); default sigue `none`
- [x] `MemoryManager::open` / `open_with_sink` construyen backends desde YAML
- [x] Ejemplo [`examples/jme-frozen-policy.yaml`](../examples/jme-frozen-policy.yaml)

## Checklist Paso 7 (Runtime)

- [x] `engine.domain_memory` (`DomainMemoryConfig`) distinto de `engine.memory` (limiter)
- [x] `ProcessorContext.domain_memory: Option<DomainMemoryHandle>`
- [x] Backpressure del `MemoryLimiter` → `notify_pressure` (cap)
- [x] Procesadores `memory_upsert` / `memory_get` / `memory_remove` + catalog UI
- [x] Mantenimiento periódico de `deferred` en runtime (tick de 250 ms)
- [x] Métricas `jaiba_memory_*` expuestas por Prometheus
- [x] Persistencia Cold separada por flujo en `data/jme/<flow_id>/persist.jsonl`
- [x] Ejemplo [`examples/jme-runtime-flow.yaml`](../examples/jme-runtime-flow.yaml)

## Checklist Paso 8 (Cold Memory segmentado)

- [x] `ColdStore` y backend append-only segmentado por clase
- [x] Payload LZ4, checksum SHA-256, tombstones y rotación configurable
- [x] Lectura `mmap` opcional e índice reconstruido durante apertura
- [x] Cuota `max_disk_bytes` por flujo; rechazo seguro conserva el objeto Hot
- [x] Recuperación de cola parcial sin publicar registros incompletos
- [x] Degradación por inactividad, frecuencia, tamaño y prioridad
- [x] Lectura Hot → Warm → Cold → Frozen → rebuild con promoción a Hot
- [x] Métricas de objetos, bytes, hits y misses Cold
- [ ] Compactación, manifiesto durable y publicación por rename atómico (Paso 9)
- [x] Guía [`jme-cold-memory.md`](jme-cold-memory.md) y ejemplo
  [`examples/jme-cold-policy.yaml`](../examples/jme-cold-policy.yaml)

Uso rápido (Hot only):

```rust
use jaiba_memory::MemoryManager;
use serde_json::json;

let yaml = std::fs::read_to_string("examples/jme-hot-policy.yaml")?;
let mut mm = MemoryManager::from_yaml(&yaml)?;
mm.upsert_keyed("telegram", "chute-1", json!({"raw": "PING"}))?;
```

Uso con immediate:

```rust
use jaiba_memory::{JsonlFileSink, MemoryManager};
use serde_json::json;

let yaml = std::fs::read_to_string("examples/jme-immediate-policy.yaml")?;
let mut mm = MemoryManager::from_yaml_with_sink(
    &yaml,
    JsonlFileSink::new("output/jme-alarms.jsonl"),
)?;
mm.upsert_keyed("alarm", "STOP_LINE", json!({"code": "E-STOP"}))?;
```

Uso deferred:

```rust
use jaiba_memory::{MemoryManager, RecordingSink};
use serde_json::json;

let yaml = std::fs::read_to_string("examples/jme-deferred-policy.yaml")?;
let mut mm = MemoryManager::from_yaml_with_sink(&yaml, RecordingSink::default())?;
mm.upsert_keyed("inventory", "SKU-1", json!({"qty": 10}))?;
mm.poll()?;   // flush si ya venció el intervalo
mm.flush()?;  // fuerza toda la cola
```

Uso Warm (`backend: none`):

```rust
use jaiba_memory::MemoryManager;
use serde_json::json;

let yaml = std::fs::read_to_string("examples/jme-warm-policy.yaml")?;
let mut mm = MemoryManager::from_yaml(&yaml)?;
mm.upsert_keyed("carrier", "A12", json!({"lane": 3}))?;
// Con noop, get solo ve Hot; un WarmStore real (Paso 6) habilita promote.
assert_eq!(mm.get_keyed("carrier", "A12"), Some(json!({"lane": 3})));
```

Uso lifecycle (demote + rebuild):

```rust
use jaiba_memory::{MapRebuildHook, MemoryManager, RecordingWarmStore};
use serde_json::json;

let yaml = std::fs::read_to_string("examples/jme-lifecycle-policy.yaml")?;
let mut hook = MapRebuildHook::default();
hook.values.insert("carrier:A12".into(), json!({"lane": 3}));
let mut mm = MemoryManager::from_yaml_with_warm_and_rebuild(
    &yaml,
    RecordingWarmStore::default(),
    hook,
)?;
mm.upsert_keyed("carrier", "A12", json!({"lane": 3}))?;
mm.notify_pressure(); // demote Hot → Warm si hace falta espacio
```

Uso Frozen (+ Redis opcional):

```rust
use jaiba_memory::MemoryManager;
use serde_json::json;

// Frozen file: MemoryManager::from_yaml abre FileFrozenStore.
let yaml = std::fs::read_to_string("examples/jme-frozen-policy.yaml")?;
let mut mm = MemoryManager::from_yaml(&yaml)?;
mm.upsert_keyed("audit_event", "E1", json!({"ok": true}))?;

// Redis Warm: cargo build -p jaiba-memory --features redis
// memory.warm.backend: redis  +  env REDIS_URL=redis://127.0.0.1/
```

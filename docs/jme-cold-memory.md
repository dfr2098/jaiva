# JME Cold Memory segmentado

> **Política:** JME es **Experimental**. El lab de integración (DMA) vive en
> `DMA_JAIVA/` fuera de este repo; al OSS solo se porta lo estable. No es el
> recorrido [Estable](product-roadmap.md).

Cold Memory es el nivel SSD local del Jaiba Memory Engine. No es swap del
sistema operativo: JME mueve objetos completos porque conoce su clase,
criticidad, frecuencia y tiempo desde el último acceso.

## Modelo conceptual

```text
                 Política semántica por objeto
                         │
          ┌──────────────┴──────────────┐
          │                             │
    Hot local RAM           Warm distribuido opcional
                              (proveedor enchufable)
          └──────────────┬──────────────┘
                         ▼
                  Cold local SSD
          segmentos, índice, checksum, LZ4
```

Warm no es un escalón obligatorio ni significa necesariamente Redis. Es un
backend opcional para compartir contexto entre instancias; actualmente existe
un proveedor Redis y Valkey puede usarse cuando mantenga compatibilidad con su
protocolo.

Frozen y el sistema de registro tienen responsabilidades diferentes y no son
una continuación obligatoria de la cadena de caché:

```text
                         Evento
                            │
               ┌────────────┴────────────┐
               ▼                         ▼
      sistema autoritativo         Frozen archive
      PostgreSQL/Oracle/etc.     auditoría, replay, retención
```

El sistema autoritativo responde cuál es el estado válido. Frozen conserva su
historia. Cold es una optimización local recuperable. En la implementación
actual, el camino de búsqueda configurable es Hot → Warm → Cold → Frozen →
rebuild, pero eso no convierte a esos componentes en una jerarquía de
durabilidad ni obliga a habilitarlos todos.

## Configuración

```yaml
memory:
  max_entries: 5000
  cold:
    backend: segmented
    path: data/jme/cold
    segment_max_bytes: 67108864
    max_disk_bytes: 10737418240
    compression: lz4
    mmap: true
  classes:
    carrier:
      policy: cache
      temperature: cold
      ttl: 24h
      demote_after: 30m
      priority: normal
      rebuild: query:carrier_by_id
```

Referencia ejecutable: [`examples/jme-cold-policy.yaml`](../examples/jme-cold-policy.yaml).

| Campo | Significado |
|---|---|
| `backend` | `none` o `segmented` (`file` es alias compatible) |
| `path` | Directorio base de segmentos |
| `segment_max_bytes` | Rotación por clase; mínimo 4096, default 64 MiB |
| `max_disk_bytes` | Cuota total por flujo; omitido significa ilimitado |
| `compression` | Actualmente `lz4` |
| `mmap` | Mapea segmentos para lectura bajo demanda, default `true` |
| `demote_after` | Inactividad antes de retirar un objeto de Hot |

`max_disk_bytes` debe ser igual o mayor que `segment_max_bytes`. Al abrir JME
desde `engine.domain_memory`, el runtime añade a `path` un subdirectorio
sanitizado por `flow_id`; por eso la cuota se aplica independientemente a cada
flujo. Al usar `MemoryManager` directamente, `path` se utiliza literalmente.

`demote_after` solo aplica a clases `cache` con temperatura `warm`, `cold` o
`frozen`. Las clases `critical` nunca son víctimas automáticas.

### Qué significa realmente mmap

`mmap` no es RAM gratuita: reserva espacio de direcciones y las páginas activas
usan la caché del sistema operativo. Reduce copias y permite leer solo el rango
necesario, pero recorrer todo el Cold puede generar fallos de página, presión de
memoria y E/S. Puede deshabilitarse con `mmap: false`; JME usará `seek` y una
lectura acotada.

## La política es la unidad de diseño

Los niveles describen residencia, no importancia. Cada clase debe razonarse en
dimensiones independientes:

| Dimensión | Pregunta |
|---|---|
| Residencia | ¿Dónde conviene mantener el objeto ahora? |
| Durabilidad | ¿Qué tan grave es perderlo y cuándo se persiste? |
| Criticidad | ¿Cuánto afecta a la operación? |
| Distribución | ¿Otras instancias deben verlo? |
| Retención | ¿Cuánto tiempo debe existir? |
| Reconstrucción | ¿Cuánto cuesta volver a obtenerlo? |

El YAML actual expresa esas dimensiones mediante `temperature`, `policy`,
`priority`, `ttl`, `rebuild`, la configuración Warm y `demote_after`. Conserva
una limitación histórica: `policy` todavía agrupa decisiones de residencia y
durabilidad, por lo que no representa todas las combinaciones de forma
ortogonal. La migración futura deberá introducir bloques versionados
`lifecycle`, `durability`, `distribution` y `classification`. El objetivo es
permitir, por ejemplo, un objeto Hot volátil y otro Cold crítico con persistencia
inmediata sin confundir ambas decisiones.

## Decisión de degradación

JME degrada objetos cuando el runtime notifica presión, Hot supera
`max_entries`, o vence `demote_after`. La selección ordena por:

1. menor prioridad;
2. menor frecuencia de acceso;
3. mayor tamaño estimado;
4. acceso menos reciente.

Así, un carrier finalizado y poco consultado sale antes de RAM que una alarma
crítica o un objeto activo.

## Formato y recuperación

Cada clase usa su propio directorio y rota archivos append-only
`segment-<id>.jmc`. Cada registro contiene tipo (`value` o tombstone), clave,
clase, longitudes original y comprimida, checksum SHA-256 y payload LZ4.

El índice vive en RAM y se reconstruye al abrir el store. Una cola parcial por
caída durante append se detecta y se trunca al último registro completo. Los
tombstones preservan eliminaciones entre reinicios. Los segmentos sellados son
inmutables; el segmento activo solo recibe appends y su mapa se invalida antes
de escribir. Al rotarlo deja de modificarse.

La publicación actual hace `append` + `fsync` antes de actualizar el índice en
RAM. Si el proceso cae durante el append, el arranque descarta la cola parcial y
reconstruye el índice desde registros completos. El esquema no afirma todavía
que exista un manifiesto durable ni publicación por rename atómico.

## Durabilidad, cuota y límites

Cada append ejecuta `sync_all()` antes de publicar la ubicación en el índice.
Si falla la escritura o el `fsync`, JME restaura la víctima en Hot y reporta el
error. Si falla un lote de degradación, también restaura los candidatos aún no
procesados.

Antes de escribir, JME verifica `max_disk_bytes`. Si el registro excedería la
cuota, rechaza la degradación, incrementa la métrica correspondiente y conserva
el objeto en Hot. Los tombstones se permiten aun al alcanzar la cuota para no
convertir el límite en una imposibilidad de eliminar datos.

Cold es un caché local recuperable, no la única copia de información crítica.
Para datos autoritativos se usa una política `immediate`, `persistent` o
`deferred` con su sink, o un rebuild hook hacia el sistema de registro.

La cuota evita crecimiento nuevo, pero no recupera espacio por sí sola. La
compactación de versiones/tombstones, publicación por segmento temporal con
rename atómico y manifiestos por clase son la siguiente fase.

## Dimensionamiento por flujo

La cuota debe reservar margen para versiones reemplazadas y tombstones, porque
todavía no existe compactación. Una aproximación inicial es:

```text
cuota Cold = objetos lógicos × tamaño comprimido medio × factor de versiones
             + margen operativo
```

Valores prudentes para comenzar:

| Variable | Valor inicial sugerido |
|---|---|
| `segment_max_bytes` | 64 MiB |
| factor de versiones | 1.5–3 según frecuencia de actualización |
| margen operativo | 20–30 % |
| alerta preventiva | 80 % de `max_disk_bytes` |
| alerta crítica | 95 % o primer rechazo por cuota |

Ejemplo: dos millones de objetos de 1 KiB comprimido, factor 2 y margen de 25 %
requieren aproximadamente 5 GiB. Una cuota de 6–8 GiB deja espacio razonable
para variaciones. Debe medirse `jaiba_memory_cold_bytes` en producción y ajustar
con datos reales; la compresión depende mucho del contenido.

`max_entries` limita cantidad de objetos Hot, no bytes exactos. La métrica
`jaiba_memory_hot_bytes` es una estimación del JSON y no incluye todo el
overhead del allocator. Para cargas grandes se deben observar simultáneamente
la RAM residente del proceso, Hot estimado y la caché de páginas del sistema.

## Operación cuando se alcanza la cuota

La secuencia observable es:

```text
seleccionar víctima Hot
        ↓
calcular tamaño del registro Cold
        ↓
¿cabe en max_disk_bytes?
   ├─ sí → escribir + fsync → publicar índice → retirar de Hot
   └─ no → incrementar quota_rejections → restaurar/conservar en Hot → error
```

Respuesta operativa recomendada:

1. Confirmar `cold_bytes`, `cold_max_disk_bytes` y los rechazos.
2. Comprobar espacio libre real del volumen; la cuota no sustituye la
   supervisión del filesystem.
3. Aumentar temporalmente `max_disk_bytes` solo si el volumen tiene margen.
4. Reducir TTL o volumen de clases reconstruibles si la política lo permite.
5. No borrar segmentos manualmente con Jaiba ejecutándose: el índice podría
   apuntar a offsets inexistentes.
6. Hasta disponer de compactación, detener el flujo antes de archivar o retirar
   un directorio Cold completo. Cold es reconstruible; los datos autoritativos
   deben permanecer en su sink o sistema de registro.

## Estado de implementación

| Capacidad | Estado |
|---|---|
| Hot RAM, TTL, prioridad y frecuencia | Implementado |
| Cold segmentado por clase, LZ4 y checksum | Implementado |
| Lectura bajo demanda con `mmap` opcional | Implementado |
| Reconstrucción del índice y cola parcial | Implementado |
| Cuota total por flujo y rechazo seguro | Implementado |
| Warm mediante proveedor Redis opcional | Implementado por feature |
| Esquema ortogonal de residencia/durabilidad | Pendiente, migración versionada |
| Compactación y recuperación automática de espacio | Pendiente, Paso 9 |
| Manifiesto durable y rename atómico de segmento | Pendiente, Paso 9 |
| Replicación de segmentos o consenso | Fuera del alcance actual |

## Métricas

- `jaiba_memory_hot_bytes`
- `jaiba_memory_cold_objects`
- `jaiba_memory_cold_bytes`
- `jaiba_memory_cold_max_disk_bytes`
- `jaiba_memory_cold_quota_rejections_total`
- `jaiba_memory_cold_hits_total`
- `jaiba_memory_cold_misses_total`
- `jaiba_memory_promotions_total`
- `jaiba_memory_demotions_total`
- `jaiba_memory_evictions_total`

## Pruebas

```bash
cargo test -p jaiba-memory
cargo test -p jaiba-runtime engine::metrics::tests --lib
```

La suite cubre rotación, compresión, lectura con y sin `mmap`, checksum,
tombstones, recuperación de cola parcial, cuotas, restauración segura,
promoción y reapertura del `MemoryManager`.

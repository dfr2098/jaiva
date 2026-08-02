# Fase 7.8: ejecución paralela por procesador

Jaiva puede procesar varios paquetes simultáneamente sin perder los límites de
memoria, las colas persistentes ni el apagado coordinado.

## Hardware detectado

En el equipo de desarrollo se detectaron:

```text
AMD Ryzen 7 7435HS
8 núcleos / 16 hilos lógicos
```

Jaiva no fija estos valores en el binario. Usa los CPU visibles mediante
`available_parallelism`, por lo que también respeta los límites asignados a un
contenedor.

Con 16 hilos visibles, los valores automáticos son:

```text
cpu_threads: 8
blocking_threads: 4
```

Los futuros de red y base de datos continúan en Tokio. Los límites anteriores
protegen el ejecutor bloqueante utilizado por transformaciones CPU y drivers o
archivos bloqueantes.

## Configuración recomendada

```yaml
engine:
  max_concurrency: 16
  workers:
    # Cero significa detección automática.
    cpu_threads: 0
    blocking_threads: 0

processors:
  - id: encode
    type: encode_xml
    scheduling:
      concurrent_tasks: 8
      maximum_in_flight: 16
      execution_mode: auto
      ordering: unordered
      timeout_ms: 60000

  - id: write
    type: put_database
    scheduling:
      concurrent_tasks: 4
      maximum_in_flight: 8
      execution_mode: async_io
      ordering: partitioned
      partition_by: customer_id
```

`engine.max_concurrency` es un límite global estricto. La suma de las tareas
activas nunca puede superarlo. `concurrent_tasks` limita un procesador y
`maximum_in_flight` limita sus tareas activas más sus paquetes pendientes.
Para mantener streaming con colas limitadas sin interbloqueos, el valor global
debe ser al menos igual al número de procesadores del camino más largo del
grafo. Jaiva calcula y valida este mínimo al cargar el flujo, y reserva cupos
para que los paquetes puedan avanzar hacia los procesadores posteriores.

## Modos de ejecución

| Modo | Uso |
|---|---|
| `auto` | Usa la clasificación declarada por el procesador |
| `async_io` | Red, pools SQL y Kafka |
| `blocking_io` | Archivos o drivers bloqueantes |
| `cpu` | JSON, YAML, CSV, XML, Protobuf, compresión o cifrado |

Los codificadores, `rename_fields` y `generate_records` se clasifican
automáticamente como CPU. `write_file` y el guardado de checkpoint se
clasifican como I/O bloqueante. Los conectores asíncronos permanecen en Tokio;
Oracle conserva además su aislamiento mediante `spawn_blocking`.

## Orden

- `unordered`: máxima velocidad; los paquetes pueden terminar en otro orden.
- `preserve`: fuerza una sola tarea activa para ese procesador.
- `partitioned`: permite paralelismo entre claves, pero nunca ejecuta
  simultáneamente dos paquetes con la misma clave.

`partition_by` busca primero un atributo. También puede escribirse
`attribute.customer_id`. Si no existe, busca el campo en los registros. Todos
los registros de un paquete deben tener la misma clave; si contienen claves
distintas, el paquete debe dividirse antes de entrar al procesador.

Para checkpoints crecientes se recomienda `preserve`. Para escrituras `upsert`
por cliente o cuenta se recomienda `partitioned`.

## Métricas

```text
jaiva_processor_active_tasks{processor="write"}
jaiva_processor_queue_depth{processor="write"}
jaiva_processor_completed_total{processor="write"}
jaiva_processor_failed_total{processor="write"}
jaiva_processor_execution_milliseconds_total{processor="write"}
jaiva_processor_saturation_ratio{processor="write"}
jaiva_available_parallelism
jaiva_cpu_worker_limit
jaiva_blocking_worker_limit
```

La saturación es `active_tasks / concurrent_tasks`. Un valor próximo a `1`
junto con una cola creciente indica que ese procesador podría usar más
concurrencia, siempre que la base o servicio destino tenga capacidad.

## Ajuste inicial para este Ryzen

Se recomienda comenzar con:

```yaml
engine:
  max_concurrency: 16
  memory:
    maximum_percent: 42
  workers:
    cpu_threads: 8
    blocking_threads: 4
```

No conviene asignar 16 tareas a cada escritor. PostgreSQL, MySQL, Oracle y SQL
Server deben mantenerse por debajo del tamaño de sus pools y de la capacidad
real del servidor. Un punto inicial prudente es:

- transformaciones CPU: 6–8;
- PostgreSQL/MySQL asíncrono: 4–8;
- SQL Server: 2–4;
- Oracle bloqueante: 2–4;
- Kafka: 4–8.

Después debe ajustarse observando latencia, saturación, rollbacks, circuit
breakers y uso de memoria.

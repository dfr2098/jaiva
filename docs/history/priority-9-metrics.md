# Paso 9: métricas Prometheus etiquetadas

Jaiba publica métricas operativas en `GET /metrics`. Las métricas nuevas usan
solamente dimensiones estables del grafo:

```text
jaiva_processor_records_total{flow,processor}
jaiva_processor_duration_seconds{flow,processor}
jaiva_processor_errors_total{flow,processor}
jaiva_queue_packets{flow,connection}
jaiva_queue_bytes{flow,connection}
jaiva_flow_status{flow}
jaiva_flow_last_success_timestamp{flow}
```

`connection` se construye con la arista estable
`origen.relación.destino`. `records_total` cuenta registros emitidos por fuentes
y transformaciones; para procesadores terminales que no emiten salida cuenta
los registros recibidos.

## Estado del flujo

`jaiva_flow_status` es un gauge numérico para no agregar otra etiqueta:

| Valor | Estado |
|---:|---|
| 0 | Stopped |
| 1 | Starting |
| 2 | Running |
| 3 | Paused |
| 4 | Draining |
| 5 | Failed |

`jaiva_flow_last_success_timestamp` contiene segundos Unix y solo cambia cuando
una ejecución completa termina correctamente.

## Cardinalidad

Prometheus nunca recibe `packet_id`, contenido, texto de error, usuario de base
de datos, nombre de tabla ni claves de partición. Esos detalles pertenecen a
Provenance y logs.

La cardinalidad máxima aproximada de estas series por flujo es:

```text
3 × procesadores + 2 × conexiones + 2
```

Los IDs de procesador y las aristas proceden del YAML validado y permanecen
constantes durante la ejecución.

## Consultas iniciales para Grafana

```promql
sum by (flow, processor) (rate(jaiva_processor_records_total[5m]))
sum by (flow, processor) (rate(jaiva_processor_errors_total[5m]))
sum by (flow, connection) (jaiva_queue_packets)
sum by (flow, connection) (jaiva_queue_bytes)
time() - jaiva_flow_last_success_timestamp
```

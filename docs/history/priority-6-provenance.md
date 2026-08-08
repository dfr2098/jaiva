# Prioridad 6: procedencia y trazabilidad

La procedencia responde qué ocurrió con un paquete, dónde, cuándo y por qué.
Se almacena en SQLite WAL cuando `engine.repository.enabled` está activo.

## Eventos

| Evento | Significado |
|---|---|
| `ENQUEUED` | El paquete y su contenido quedaron persistidos |
| `ROUTED` | Recorrió una relación entre dos procesadores |
| `CLAIMED` | Un worker reclamó el trabajo |
| `PROCESSING_STARTED` | Comenzó un intento del procesador |
| `RETRIED` | El intento falló y se programó otro |
| `PROCESSED` | El procesador terminó correctamente |
| `COMPLETED` | El elemento persistente llegó a estado final correcto |
| `FAILED` | Agotó reintentos y pasó a `DEAD_LETTER` |
| `RECOVERED` | Se recuperó trabajo abandonado después de reiniciar |
| `REQUEUED` | Un operador solicitó reprocesar un dead-letter |

Los detalles JSON incluyen, según el evento:

- procesadores de origen y destino;
- relación recorrida;
- intento;
- duración y espera de reintento;
- tamaño del paquete y contenido;
- tipo de contenido y media type;
- cantidad de atributos y presencia de esquema;
- mensaje de error y estado resultante.

## Consultas

Eventos recientes de un flujo:

```bash
cargo run -- provenance recent flow.yaml 100
```

Línea de tiempo ascendente de un paquete:

```bash
cargo run -- provenance packet flow.yaml PACKET_ID 1000
```

Ambos comandos devuelven JSON y limitan las respuestas a un máximo interno de
5,000 eventos. El índice `(flow_id, packet_id, created_at, id)` mantiene
eficiente la consulta por paquete.

## Retención

```yaml
engine:
  repository:
    enabled: true
    provenance_retention_hours: 2160
```

El valor predeterminado es 2,160 horas, equivalentes a 90 días. La limpieza se
ejecuta al finalizar un flujo y no afecta el contenido de paquetes pendientes
o dead-letter; únicamente elimina eventos antiguos de procedencia.

## Garantías y límites

- Los eventos tienen un ID SQLite creciente para desempatar eventos ocurridos
  en el mismo segundo.
- La procedencia acompaña al repositorio persistente; si este se deshabilita,
  solo quedan logs y métricas.
- Un paquete conserva su `packet_id` al pasar por varios procesadores; cada
  tramo tiene un `queue_id` distinto.
- La instrumentación de procedencia añade escrituras SQLite. Una fase posterior
  puede agrupar eventos en lotes para cargas de millones de paquetes por
  segundo.

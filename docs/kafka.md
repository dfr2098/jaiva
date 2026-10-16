# Kafka

Estado: **Beta** desde v0.9.0 (ver [criterios de madurez](product-roadmap.md#criterios-de-madurez)).

| Criterio Beta | Cómo se cumple |
| --- | --- |
| Pruebas reales automáticas | Workflow [`kafka.yml`](../.github/workflows/kafka.yml): cada noche y en cada PR que toca Kafka, contra Redpanda y Apache Kafka 3.9 |
| Memoria acotada | Un mensaje → un paquete; publicación en paralelo sin acumular el lote en un buffer extra |
| Fallos definidos | Errores reintentables vs. rechazos; mensaje envenenado a `failure` con commit |
| Tipos / contrato | JSON o bytes; claves desde campo o atributo |
| Guía de uso | Esta página |

Para pasar a **Estable** falta: TLS/SASL con pruebas reales, commit solo tras el
destino completo, coordinación de rebalanceo y un soak largo verde.

## Cambios respecto a v0.8 (revisa tus flows)

| Qué | Hasta v0.8 | Desde v0.9 | Para conservar lo anterior |
| --- | --- | --- | --- |
| JSON inválido en `consume_kafka` (`decode: json`) | abortaba el ciclo y **no** confirmaba el offset (el grupo se quedaba atascado) | emite por `failure`, confirma el offset y sigue con el siguiente mensaje | — |
| Tamaño excesivo, autorización, formato rechazado por el broker | `MessageConnector` (se reintentaba y abría el circuit breaker) | `MessageRejected`: va a `failure` sin reintentar; el circuit no se abre | — |
| Publicación de varios registros en un paquete | un `send` tras otro | todos encolados y confirmados en paralelo (mismo orden por clave) | — |

## Conexión

Requiere un binario con `--features kafka-driver` (incluido en los binarios de
Windows del release y en `all-drivers`).

```yaml
kafka_connections:
  bus:
    brokers_env: KAFKA_BROKERS   # 127.0.0.1:9092
    client_id: jaiba-publisher
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
```

```bash
export KAFKA_BROKERS=127.0.0.1:29092
cargo run --features kafka-driver -- examples/kafka-publish.yaml
```

- Solo se admite `PLAINTEXT`. TLS y SASL se rechazan al arrancar el flow.
- El cliente fuerza `broker.address.family=v4` para evitar que `localhost`
  resuelva a `::1` cuando el broker solo escucha en IPv4.
- El productor usa `enable.idempotence=true` y `acks=all`.

## Publicación: `publish_kafka`

```yaml
- id: publish
  type: publish_kafka
  config:
    connection: bus
    topic: events.batch.v1
    key_field: batch_id          # clave desde cada registro
    # key_attribute: kafka.key   # clave desde atributos (paquetes encoded)
    queue_timeout_ms: 5000       # espera máxima por mensaje en la cola interna
```

- Un paquete de registros produce un mensaje JSON compacto por registro.
- Un paquete encoded produce un único mensaje con sus bytes.
- `success` se emite solo cuando el broker ha confirmado **todos** los mensajes
  del paquete.
- Los mensajes del paquete se encolan en orden y se confirman en paralelo.

### Garantías

- Idempotencia del productor + `acks=all`: no hay duplicados por reintentos
  internos de librdkafka. Un operador que reprocese el paquete sí puede
  republicar.
- Un rechazo (`MessageRejected`) no abre el circuit breaker.

## Consumo: `consume_kafka`

```yaml
- id: consume
  type: consume_kafka
  config:
    connection: bus
    topic: events.batch.v1
    group_id: jaiva-readers
    auto_offset_reset: earliest   # earliest | latest
    max_poll_messages: 50
    max_poll_ms: 1000
    max_idle_ms: 8000
    decode: json                  # json | bytes
```

| Campo | Rol |
| --- | --- |
| `group_id` | Grupo de consumidores |
| `auto_offset_reset` | Si el grupo aún no tiene offsets |
| `max_poll_messages` | Tope de mensajes por ejecución del nodo |
| `max_poll_ms` | Timeout de cada `recv` |
| `max_idle_ms` | Sin mensajes nuevos → fin del ciclo |
| `decode` | `json` (registros) o `bytes` (contenido encoded) |

- Auto-commit desactivado. El offset se confirma tras emitir por `success` **o**
  por `failure` (at-least-once).
- **Mensaje envenenado** (`decode: json` y payload no JSON): el paquete sale
  por `failure` con `error.kind=poison`, el payload crudo en contenido encoded
  y el offset se confirma para no bloquear el grupo.
- Durante el join inicial, fallos de transporte transitorios se toleran hasta
  agotar `max_idle_ms`.

Conecta siempre una arista `failure` si usas `decode: json`:

```yaml
connections:
  - from: consume
    relationship: success
    to: inspect
  - from: consume
    relationship: failure
    to: dead_letter
```

## Errores

| Situación | Variante | ¿Reintenta? | ¿Abre circuit? |
| --- | --- | --- | --- |
| Red, timeout, líder ausente, cola llena | `MessageConnector` | sí | sí |
| Mensaje demasiado grande, autorización, formato/política | `MessageRejected` | no | no |
| JSON inválido al consumir | emite `failure` + commit | no (por mensaje) | no |

## Observabilidad

- `jaiva_kafka_messages_published_total` / `bytes_published` / `publish_errors`
- `jaiva_kafka_messages_consumed_total` / `bytes_consumed` / `consume_errors`

Atributos del paquete: `kafka.topic`, `kafka.partition`, `kafka.offset`,
`kafka.messages`, `kafka.connection`, `kafka.duration_ms`, y en consumo
`kafka.group_id` / `kafka.key`. En poison: `error.kind`, `error.message`.

## Pruebas

```bash
export JAIBA_TEST_KAFKA_BROKERS=127.0.0.1:29092
export JAIBA_TEST_KAFKA_FAIL_BROKER=127.0.0.1:1
cargo test -p jaiba-runtime --features kafka-driver --lib -- kafka
./scripts/smoke-kafka.sh
```

En CI: [`kafka.yml`](../.github/workflows/kafka.yml) (nightly, PR que tocan
Kafka, label `kafka`).

## Limitaciones

- Sin TLS/SASL.
- El commit es tras emitir el paquete, no tras completar el destino aguas abajo.
- Sin pause por backpressure ni manejo fino de rebalanceos.
- Un solo mensaje por paquete en consumo (no agrupa).

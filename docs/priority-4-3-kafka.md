# Kafka: publicación y consumo

Jaiva incorpora Kafka como bus de eventos, separado del contrato
`DatabaseWriter`. Requiere compilar con `--features kafka-driver`.

## Conexión

```yaml
kafka_connections:
  bus:
    brokers_env: KAFKA_BROKERS
    client_id: jaiva-publisher
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
```

```bash
export KAFKA_BROKERS=127.0.0.1:29092
cargo run --features kafka-driver -- examples/kafka-publish.yaml
```

Solo se admite `PLAINTEXT` por ahora. TLS y SASL se rechazan explícitamente
hasta incorporar y probar sus dependencias criptográficas.

El cliente fuerza `broker.address.family=v4` para evitar fallos cuando el broker
anuncia `localhost` y el puerto solo escucha en IPv4 (`127.0.0.1`).

## Publicación (`publish_kafka`)

```yaml
type: publish_kafka
config:
  connection: bus
  topic: events.batch.v1
  key_field: batch_id
  queue_timeout_ms: 5000
```

- Un paquete de registros produce un mensaje JSON compacto por registro.
- Un paquete codificado produce un único mensaje con sus bytes originales.
- `key_field` obtiene la clave desde cada registro.
- `key_attribute` obtiene la clave de los atributos de un paquete codificado.
- `success` se emite solamente después de recibir la confirmación del broker.
- Un error utiliza los reintentos, dead-letter y logs normales del motor.

### Garantías del productor

- `enable.idempotence=true`
- `acks=all`
- timeout de entrega configurable
- espera limitada cuando la cola interna está llena

Esto evita duplicados causados por reintentos internos del productor. No evita
que un operador reprocese intencionalmente un paquete ya publicado.

### Observabilidad (publish)

- `jaiva_kafka_messages_published_total`
- `jaiva_kafka_bytes_published_total`
- `jaiva_kafka_publish_errors_total`

Atributos del paquete: `kafka.topic`, `kafka.partition`, `kafka.offset`,
`kafka.messages`, `kafka.connection`, `kafka.duration_ms`.

## Consumo (`consume_kafka`)

MVP con auto-commit desactivado. El offset se confirma tras emitir el paquete
por `success` (at-least-once). Rebalanceo con pause por backpressure y commit
tras el destino completo quedan para una fase posterior.

```yaml
type: consume_kafka
config:
  connection: bus
  topic: events.batch.v1
  group_id: jaiva-readers
  auto_offset_reset: earliest
  max_poll_messages: 50
  max_poll_ms: 1000
  max_idle_ms: 8000
  decode: json   # o bytes
```

```bash
export KAFKA_BROKERS=127.0.0.1:29092
cargo run --features kafka-driver -- examples/kafka-consume.yaml
```

Parámetros relevantes:

| Campo | Rol |
|---|---|
| `group_id` | Grupo de consumidores Kafka |
| `auto_offset_reset` | `earliest` o `latest` si el grupo no tiene offsets |
| `max_poll_messages` | Tope de mensajes por ejecución del procesador |
| `max_poll_ms` | Timeout de cada `recv` |
| `max_idle_ms` | Tiempo sin mensajes nuevos antes de terminar el ciclo |
| `decode` | `json` (registros) o `bytes` (contenido codificado) |

Durante el join inicial, errores de transporte transitorios se toleran hasta
agotar `max_idle_ms` (no abortan el ciclo vacío de inmediato).

### Observabilidad (consume)

- `jaiva_kafka_messages_consumed_total`
- `jaiva_kafka_bytes_consumed_total`
- `jaiva_kafka_consume_errors_total`

Atributos del paquete: `kafka.topic`, `kafka.partition`, `kafka.offset`,
`kafka.group_id`, `kafka.connection` y, si aplica, `kafka.key`.

## Pruebas

La suite de integración (entorno de pruebas con Postgres + Kafka) se documenta
en [priority-8-integration-tests.md](priority-8-integration-tests.md).

## Pendiente

- Commit de offsets solo tras completar el destino
- Coordinación de rebalanceos y pause por backpressure
- TLS / SASL con pruebas reales

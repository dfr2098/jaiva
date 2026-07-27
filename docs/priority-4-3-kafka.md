# Fase 4.3.1: publicación en Kafka

Jaiva incorpora Kafka como bus de eventos, separado del contrato
`DatabaseWriter`. El primer procesador es `publish_kafka`.

## Conexión

```yaml
kafka_connections:
  dma:
    brokers_env: KAFKA_BROKERS
    client_id: jaiva-publisher
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
```

```bash
export KAFKA_BROKERS=127.0.0.1:29092
cargo run --features kafka-driver -- examples/kafka-publish.yaml
```

La fase 4.3.1 admite `PLAINTEXT`, que corresponde al contenedor DMA actual.
TLS y SASL se rechazan explícitamente hasta incorporar y probar sus
dependencias criptográficas.

## Procesador

```yaml
type: publish_kafka
config:
  connection: dma
  topic: dma.journal.batch.v1
  key_field: batch_id
  queue_timeout_ms: 5000
```

- Un paquete de registros produce un mensaje JSON compacto por registro.
- Un paquete codificado produce un único mensaje con sus bytes originales.
- `key_field` obtiene la clave desde cada registro.
- `key_attribute` obtiene la clave de los atributos de un paquete codificado.
- `success` se emite solamente después de recibir la confirmación del broker.
- Un error utiliza los reintentos, dead-letter y logs normales del motor.

## Garantías

El productor compartido configura:

- `enable.idempotence=true`;
- `acks=all`;
- timeout de entrega configurable;
- espera limitada cuando la cola interna está llena;
- hilo interno de entrega de `FutureProducer`.

Esto evita duplicados causados por reintentos internos del productor. No evita
que un operador reprocese intencionalmente un paquete ya publicado; el
consumidor debe usar la key o un identificador de evento para deduplicación
de extremo a extremo.

## Observabilidad

Métricas:

- `jaiva_kafka_messages_published_total`;
- `jaiva_kafka_bytes_published_total`;
- `jaiva_kafka_publish_errors_total`.

Después de publicar, el paquete incluye `kafka.topic`, `kafka.partition`,
`kafka.offset`, `kafka.messages`, `kafka.connection` y `kafka.duration_ms`.
Estos atributos operativos se copian al siguiente evento `ENQUEUED` de
procedencia.

## Siguiente fase

`consume_kafka` deberá desactivar auto-commit y confirmar offsets únicamente
después de que el paquete complete el destino. También deberá coordinar
rebalanceos, particiones pausadas por backpressure y recuperación.

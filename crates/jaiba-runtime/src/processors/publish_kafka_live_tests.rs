use std::time::{Duration, Instant};

use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
};

use crate::engine::DataPacket;

use crate::processors::kafka_live::{brokers, delete_topic, ensure_topic, unique};

use super::PublishKafka;

/// Publica dos mensajes con el código del procesador y confirma su consumo.
#[tokio::test]
async fn kafka_real_publish_is_acknowledged_and_consumable() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.publish");
    ensure_topic(&brokers, &topic).await;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("group.id", unique("jaiva-kafka-publish"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("create integration consumer");
    consumer
        .subscribe(&[&topic])
        .expect("subscribe to integration topic");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("client.id", "jaiva-kafka-publish")
        .set("security.protocol", "PLAINTEXT")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "10000")
        .create()
        .expect("create integration producer");
    let processor = PublishKafka::from_config(&serde_json::json!({
        "connection": "integration",
        "topic": topic,
        "key_field": "event_id",
        "queue_timeout_ms": 5000
    }))
    .expect("create Kafka processor");
    let packet = DataPacket::with_records(vec![
        serde_json::json!({"event_id": "event-1", "status": "created"}),
        serde_json::json!({"event_id": "event-2", "status": "completed"}),
    ]);
    let messages = processor.messages(&packet).expect("encode messages");
    let (partition, offset) = processor
        .publish(&producer, &messages)
        .await
        .expect("publish and receive broker acknowledgement");
    assert_eq!(partition, 0);
    assert!(offset >= 1);

    let mut consumed = Vec::new();
    let consume_deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while consumed.len() < 2 {
        let remaining = consume_deadline
            .checked_duration_since(tokio::time::Instant::now())
            .expect("consumer did not receive both messages before the deadline");
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(message)) => consumed.push((
                message.key().map(Vec::from),
                message.payload().map(Vec::from),
            )),
            Ok(Err(error)) => {
                eprintln!("transient Kafka consumer error: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(_) => panic!("consumer did not receive both messages before the deadline"),
        }
    }
    assert_eq!(consumed[0].0.as_deref(), Some(b"event-1".as_slice()));
    assert_eq!(consumed[1].0.as_deref(), Some(b"event-2".as_slice()));
    let first: serde_json::Value = serde_json::from_slice(
        consumed[0]
            .1
            .as_deref()
            .expect("first message has a payload"),
    )
    .expect("first payload is JSON");
    let second: serde_json::Value = serde_json::from_slice(
        consumed[1]
            .1
            .as_deref()
            .expect("second message has a payload"),
    )
    .expect("second payload is JSON");
    assert_eq!(first["status"], "created");
    assert_eq!(second["status"], "completed");

    delete_topic(&brokers, &topic).await;
}

/// Un lote grande se publica en paralelo y conserva el orden por clave.
#[tokio::test]
async fn kafka_parallel_publish_preserves_keyed_order() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.parallel");
    ensure_topic(&brokers, &topic).await;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("group.id", unique("jaiva-kafka-parallel"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer");
    consumer.subscribe(&[&topic]).expect("subscribe");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("client.id", "jaiva-kafka-parallel")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "30000")
        .create()
        .expect("producer");

    let count = 250usize;
    let records: Vec<_> = (0..count)
        .map(|n| serde_json::json!({"event_id": format!("e-{n}"), "n": n}))
        .collect();
    let processor = PublishKafka::from_config(&serde_json::json!({
        "connection": "integration",
        "topic": topic,
        "key_field": "event_id",
        "queue_timeout_ms": 15000
    }))
    .unwrap();
    let packet = DataPacket::with_records(records);
    let messages = processor.messages(&packet).unwrap();
    let started = Instant::now();
    processor
        .publish(&producer, &messages)
        .await
        .expect("parallel publish");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "parallel publish of {count} messages was too slow: {:?}",
        started.elapsed()
    );

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while seen.len() < count {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("only got {} of {count} messages", seen.len()));
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(message)) => {
                let key = String::from_utf8_lossy(message.key().unwrap_or_default()).into_owned();
                seen.push(key);
            }
            Ok(Err(_)) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(_) => panic!("only got {} of {count} messages", seen.len()),
        }
    }
    let expected: Vec<_> = (0..count).map(|n| format!("e-{n}")).collect();
    assert_eq!(seen, expected, "keyed messages must keep enqueue order");
    delete_topic(&brokers, &topic).await;
}

/// Un payload mayor que el límite del broker se clasifica como rechazo.
#[tokio::test]
async fn kafka_oversized_message_is_rejected() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.oversized");
    ensure_topic(&brokers, &topic).await;

    // Redpanda / Kafka default message.max.bytes ≈ 1 MiB. Un payload de 2 MiB
    // debe rechazarse sin reintentar.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("client.id", "jaiba-kafka-oversized")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("message.timeout.ms", "10000")
        // Evita que el cliente recorte antes: fuerza el rechazo del broker.
        .set("message.max.bytes", "3000000")
        .create()
        .expect("producer");
    let processor = PublishKafka::from_config(&serde_json::json!({
        "connection": "integration",
        "topic": topic,
        "queue_timeout_ms": 10000
    }))
    .unwrap();
    let huge = vec![b'x'; 2 * 1024 * 1024];
    let error = processor
        .publish(&producer, &[(huge, None)])
        .await
        .expect_err("oversized message must fail");
    assert!(
        matches!(error, crate::error::FlowError::MessageRejected(_)),
        "expected MessageRejected, got {error}"
    );
    assert!(!error.is_retryable());
    delete_topic(&brokers, &topic).await;
}

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::{
    engine::FlowEngine,
    processors::{
        default_registry,
        kafka_live::{CaptureSink, brokers, delete_topic, ensure_topic, unique},
    },
};

/// Publica con `publish_kafka` y consume con el procesador `consume_kafka`.
#[tokio::test]
async fn kafka_real_consume_kafka_processor() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.consume");
    let group = unique("jaiva-kafka-consume");
    ensure_topic(&brokers, &topic).await;

    let publish_yaml = format!(
        r#"
id: kafka-publish
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-kafka-publish
    security_protocol: PLAINTEXT
    message_timeout_ms: 15000
processors:
  - id: records
    type: generate_records
    config:
      records:
        - batch_id: a
          status: created
        - batch_id: b
          status: completed
  - id: publish
    type: publish_kafka
    config:
      connection: integration
      topic: {topic}
      key_field: batch_id
      queue_timeout_ms: 5000
connections:
  - from: records
    relationship: success
    to: publish
"#
    );
    let publish_summary = FlowEngine::new(serde_yaml::from_str(&publish_yaml).unwrap())
        .unwrap()
        .run()
        .await
        .expect("publish flow");
    assert_eq!(publish_summary.failed, 0);
    assert!(publish_summary.emitted >= 1);

    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let sink = Arc::new(CaptureSink {
        packets: captured.clone(),
    });
    registry.register("capture_sink", move |_| Ok(sink.clone()));

    let consume_yaml = format!(
        r#"
id: kafka-consume
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-kafka-consume
    security_protocol: PLAINTEXT
    message_timeout_ms: 15000
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: integration
      topic: {topic}
      group_id: {group}
      auto_offset_reset: earliest
      max_poll_messages: 20
      max_poll_ms: 1000
      max_idle_ms: 12000
      decode: json
  - id: capture
    type: capture_sink
connections:
  - from: consume
    relationship: success
    to: capture
"#
    );
    let consume_summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("consume flow");
    assert_eq!(consume_summary.failed, 0);

    {
        let packets = captured.lock().expect("capture poisoned");
        assert_eq!(packets.len(), 2, "expected two Kafka messages as packets");
        for packet in packets.iter() {
            assert!(packet.attributes.contains_key("kafka.offset"));
            assert_eq!(
                packet.attributes.get("kafka.topic").map(String::as_str),
                Some(topic.as_str())
            );
            let records = packet.records().expect("records payload");
            assert_eq!(records.len(), 1);
        }
    }
    delete_topic(&brokers, &topic).await;
}

/// Un JSON inválido va a `failure`, se confirma el offset y el mensaje bueno
/// sigue llegando; un segundo ciclo del mismo grupo no lo vuelve a leer.
#[tokio::test]
async fn kafka_poison_message_goes_to_failure_and_is_committed() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.poison");
    let group = unique("jaiva-kafka-poison");
    ensure_topic(&brokers, &topic).await;

    // Publicar con el productor crudo: un poison + un JSON válido.
    use rdkafka::{
        ClientConfig,
        producer::{FutureProducer, FutureRecord},
    };
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("broker.address.family", "v4")
        .set("client.id", "jaiva-poison-seed")
        .set("acks", "all")
        .create()
        .expect("producer");
    for (key, payload) in [
        ("bad", &b"this is not json"[..]),
        ("good", br#"{"batch_id":"ok","status":"ready"}"#),
    ] {
        producer
            .send(
                FutureRecord::to(&topic).key(key).payload(payload),
                Duration::from_secs(10),
            )
            .await
            .expect("seed message");
    }

    let ok = Arc::new(Mutex::new(Vec::new()));
    let bad = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let ok_sink = Arc::new(CaptureSink {
        packets: ok.clone(),
    });
    let bad_sink = Arc::new(CaptureSink {
        packets: bad.clone(),
    });
    registry.register("ok_sink", move |_| Ok(ok_sink.clone()));
    registry.register("bad_sink", move |_| Ok(bad_sink.clone()));

    let consume_yaml = format!(
        r#"
id: kafka-poison
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-kafka-poison
    security_protocol: PLAINTEXT
    message_timeout_ms: 15000
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: integration
      topic: {topic}
      group_id: {group}
      auto_offset_reset: earliest
      max_poll_messages: 20
      max_poll_ms: 1000
      max_idle_ms: 12000
      decode: json
  - id: ok
    type: ok_sink
  - id: bad
    type: bad_sink
connections:
  - from: consume
    relationship: success
    to: ok
  - from: consume
    relationship: failure
    to: bad
"#
    );
    let summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("poison consume");
    assert_eq!(summary.failed, 0);

    {
        let bad = bad.lock().expect("bad");
        assert_eq!(bad.len(), 1, "poison should go to failure");
        assert_eq!(
            bad[0].attributes.get("error.kind").map(String::as_str),
            Some("poison")
        );
        let ok = ok.lock().expect("ok");
        assert_eq!(ok.len(), 1, "valid message should go to success");
        assert_eq!(ok[0].records().unwrap()[0]["batch_id"], "ok");
    }

    // Segundo ciclo: el offset del poison ya está confirmado.
    let ok2 = Arc::new(Mutex::new(Vec::new()));
    let bad2 = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let ok_sink = Arc::new(CaptureSink {
        packets: ok2.clone(),
    });
    let bad_sink = Arc::new(CaptureSink {
        packets: bad2.clone(),
    });
    registry.register("ok_sink", move |_| Ok(ok_sink.clone()));
    registry.register("bad_sink", move |_| Ok(bad_sink.clone()));
    let summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("second consume");
    assert_eq!(summary.failed, 0);
    assert!(ok2.lock().unwrap().is_empty());
    assert!(bad2.lock().unwrap().is_empty());

    delete_topic(&brokers, &topic).await;
}

/// Smoke de rendimiento: 100 mensajes publish→consume en < 30s.
#[tokio::test]
async fn kafka_throughput_smoke() {
    let Some(brokers) = brokers() else {
        return;
    };
    let topic = unique("jaiva.kafka.thru");
    let group = unique("jaiva-kafka-thru");
    ensure_topic(&brokers, &topic).await;

    let records = (0..100)
        .map(|index| format!("        - event_id: e-{index}\n          n: {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let publish_yaml = format!(
        r#"
id: kafka-thru-publish
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-kafka-thru-pub
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
processors:
  - id: records
    type: generate_records
    config:
      records:
{records}
  - id: publish
    type: publish_kafka
    config:
      connection: integration
      topic: {topic}
      key_field: event_id
      queue_timeout_ms: 10000
connections:
  - from: records
    relationship: success
    to: publish
"#
    );

    let started = Instant::now();
    FlowEngine::new(serde_yaml::from_str(&publish_yaml).unwrap())
        .unwrap()
        .run()
        .await
        .expect("throughput publish");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let sink = Arc::new(CaptureSink {
        packets: captured.clone(),
    });
    registry.register("capture_sink", move |_| Ok(sink.clone()));
    let consume_yaml = format!(
        r#"
id: kafka-thru-consume
kafka_connections:
  integration:
    brokers_env: JAIBA_TEST_KAFKA_BROKERS
    client_id: jaiva-kafka-thru-cons
    security_protocol: PLAINTEXT
    message_timeout_ms: 30000
processors:
  - id: consume
    type: consume_kafka
    config:
      connection: integration
      topic: {topic}
      group_id: {group}
      auto_offset_reset: earliest
      max_poll_messages: 100
      max_poll_ms: 1000
      max_idle_ms: 20000
      decode: json
  - id: capture
    type: capture_sink
connections:
  - from: consume
    relationship: success
    to: capture
"#
    );
    let summary = FlowEngine::new(serde_yaml::from_str(&consume_yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("throughput consume");
    let elapsed = started.elapsed();
    assert_eq!(summary.failed, 0);
    assert_eq!(captured.lock().expect("capture").len(), 100);
    assert!(
        elapsed < Duration::from_secs(30),
        "throughput smoke exceeded 30s: {elapsed:?}"
    );
    delete_topic(&brokers, &topic).await;
}

/// Broker inalcanzable: `publish_kafka` falla de forma controlada (sin panic).
#[tokio::test]
async fn kafka_fail_broker_is_controlled() {
    if std::env::var("JAIBA_TEST_KAFKA_FAIL_BROKER").is_err() {
        if std::env::var("JAIBA_TEST_KAFKA_REQUIRED").is_ok() {
            panic!(
                "JAIBA_TEST_KAFKA_FAIL_BROKER is required when JAIBA_TEST_KAFKA_REQUIRED is set (use 127.0.0.1:1)"
            );
        }
        eprintln!("skipping real Kafka test: JAIBA_TEST_KAFKA_FAIL_BROKER is not set");
        return;
    }
    let yaml = r#"
id: kafka-fail-broker
kafka_connections:
  down:
    brokers_env: JAIBA_TEST_KAFKA_FAIL_BROKER
    client_id: jaiva-kafka-fail
    security_protocol: PLAINTEXT
    message_timeout_ms: 2000
processors:
  - id: records
    type: generate_records
    config:
      records:
        - id: 1
  - id: publish
    type: publish_kafka
    config:
      connection: down
      topic: jaiva.kafka.unreachable
      queue_timeout_ms: 2000
    retry:
      maximum_attempts: 0
      initial_delay_ms: 1
      maximum_delay_ms: 1
connections:
  - from: records
    relationship: success
    to: publish
"#;
    match FlowEngine::new(serde_yaml::from_str(yaml).unwrap())
        .unwrap()
        .run()
        .await
    {
        Ok(summary) => assert!(
            summary.failed >= 1 || summary.kafka_publish_errors >= 1,
            "expected controlled Kafka failure, got {summary:?}"
        ),
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains("Kafka")
                    || text.contains("broker")
                    || text.contains("Message")
                    || text.contains("transport")
                    || text.contains("connect")
                    || text.contains("timeout")
                    || text.contains("Timeout")
                    || text.contains("Local:"),
                "unexpected fail-broker error: {text}"
            );
        }
    }
}

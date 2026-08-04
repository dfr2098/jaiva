//! Pruebas del toolkit AI Prep vía [`FlowEngine`] (sin DBs externas).
//!
//! Cubren la cadena MVP de limpieza, features+split, lookup+manifest y el
//! registro de tipos en [`crate::processors::default_registry`].

use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    engine::{
        DataPacket, FlowEngine, OutputSender, Processor, ProcessorContext, ProcessorRegistry,
    },
    error::FlowError,
    processors::default_registry,
};

type CapturedPackets = Arc<Mutex<Vec<(String, Vec<Value>)>>>;

struct CaptureSink {
    packets: CapturedPackets,
}

#[async_trait]
impl Processor for CaptureSink {
    async fn execute(
        &self,
        packet: DataPacket,
        _context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet
            .records()
            .map(|rows| rows.to_vec())
            .unwrap_or_default();
        let split = packet
            .attributes
            .get("ai.split")
            .cloned()
            .unwrap_or_else(|| "success".to_owned());
        self.packets
            .lock()
            .expect("capture lock")
            .push((split, records));
        output.success(packet).await
    }
}

fn temp_path(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "jaiba-ai-prep-{}-{}",
        name,
        uuid::Uuid::new_v4().simple()
    ));
    path
}

#[tokio::test]
async fn mvp_clean_pipeline_produces_rows() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let sink = Arc::new(CaptureSink {
        packets: captured.clone(),
    });
    registry.register("capture_sink", move |_| Ok(sink.clone()));

    let yaml = r#"
id: ai-prep-mvp
engine:
  max_concurrency: 16
  repository:
    enabled: false
processors:
  - id: source
    type: generate_records
    config:
      records:
        - sensor_id: A
          temperature: "20"
          status: OK
          extra: 1
        - sensor_id: null
          temperature: 10
          status: OK
        - sensor_id: A
          temperature: null
          status: WARN
        - sensor_id: B
          temperature: 250
          status: OK
        - sensor_id: C
          temperature: 40
          status: OK
  - id: select
    type: ai_select_fields
    config:
      keep: [sensor_id, temperature, status]
  - id: drop_nulls
    type: ai_drop_nulls
    config:
      fields: [sensor_id]
  - id: cast
    type: ai_cast_types
    config:
      fields:
        temperature: number
      on_error: drop
  - id: fill
    type: ai_fill_missing
    config:
      fields: [temperature]
      strategy: mean
  - id: dedupe
    type: ai_remove_duplicates
    config:
      key_fields: [sensor_id]
  - id: filter
    type: ai_filter_range
    config:
      field: temperature
      mode: min_max
      min: 0
      max: 100
  - id: capture
    type: capture_sink
connections:
  - { from: source, relationship: success, to: select }
  - { from: select, relationship: success, to: drop_nulls }
  - { from: drop_nulls, relationship: success, to: cast }
  - { from: cast, relationship: success, to: fill }
  - { from: fill, relationship: success, to: dedupe }
  - { from: dedupe, relationship: success, to: filter }
  - { from: filter, relationship: success, to: capture }
"#;
    let summary = FlowEngine::new(serde_yaml::from_str(yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("mvp flow");
    assert_eq!(summary.failed, 0);
    let packets = captured.lock().unwrap();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].1.len(), 2);
    assert!(!packets[0].1[0].as_object().unwrap().contains_key("extra"));
}

#[tokio::test]
async fn features_normalize_encode_compute_and_split() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let sink = Arc::new(CaptureSink {
        packets: captured.clone(),
    });
    registry.register("capture_sink", move |_| Ok(sink.clone()));

    let yaml = r#"
id: ai-prep-features
engine:
  max_concurrency: 16
  repository:
    enabled: false
processors:
  - id: source
    type: generate_records
    config:
      records:
        - temperature: 10
          vibration: 1
          status: OK
        - temperature: 20
          vibration: 2
          status: WARN
        - temperature: 30
          vibration: 3
          status: OK
        - temperature: 40
          vibration: 4
          status: WARN
  - id: normalize
    type: ai_normalize
    config:
      fields: [temperature]
      method: min_max
  - id: encode
    type: ai_encode_categories
    config:
      fields:
        status:
          OK: 0
          WARN: 1
  - id: features
    type: ai_compute_fields
    config:
      fields:
        score: temperature * 10 + status
  - id: split
    type: ai_split_dataset
    config:
      train: 0.5
      validation: 0.25
      test: 0.25
  - id: capture
    type: capture_sink
connections:
  - { from: source, relationship: success, to: normalize }
  - { from: normalize, relationship: success, to: encode }
  - { from: encode, relationship: success, to: features }
  - { from: features, relationship: success, to: split }
  - { from: split, relationship: train, to: capture }
  - { from: split, relationship: validation, to: capture }
  - { from: split, relationship: test, to: capture }
"#;
    let summary = FlowEngine::new(serde_yaml::from_str(yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("features flow");
    assert_eq!(summary.failed, 0);
    let packets = captured.lock().unwrap();
    assert!(!packets.is_empty());
    let total: usize = packets.iter().map(|(_, rows)| rows.len()).sum();
    assert_eq!(total, 4);
    assert!(packets.iter().any(|(split, _)| split == "train"));
}

#[tokio::test]
async fn lookup_join_and_export_manifest() {
    let manifest_path = temp_path("manifest.json");
    let yaml = format!(
        r#"
id: ai-prep-join
engine:
  repository:
    enabled: false
processors:
  - id: source
    type: generate_records
    config:
      records:
        - line_id: L1
          temperature: 1
        - line_id: L2
          temperature: 2
  - id: join
    type: ai_lookup_join
    config:
      key: line_id
      lookup_records:
        - line_id: L1
          plant: norte
        - line_id: L2
          plant: sur
      copy_fields: [plant]
  - id: manifest
    type: ai_export_manifest
    config:
      path: {}
      dataset_name: conveyor
      train_path: output/train.csv
      validation_path: output/validation.csv
      test_path: output/test.csv
  - id: sink
    type: log_records
connections:
  - {{ from: source, relationship: success, to: join }}
  - {{ from: join, relationship: success, to: manifest }}
  - {{ from: manifest, relationship: success, to: sink }}
"#,
        manifest_path.display()
    );
    let summary = FlowEngine::new(serde_yaml::from_str(&yaml).unwrap())
        .unwrap()
        .with_registry(default_registry())
        .run()
        .await
        .expect("join flow");
    assert_eq!(summary.failed, 0);
    let body = fs::read_to_string(&manifest_path).expect("manifest written");
    assert!(body.contains("checksum_sha256"));
    assert!(body.contains("\"row_count\": 2"));
    assert!(body.contains("norte") || body.contains("plant"));
    assert!(body.contains("train_path"));
    assert!(body.contains("output/train.csv"));
    let _ = fs::remove_file(&manifest_path);
}

#[tokio::test]
async fn split_manifest_waits_for_and_writes_every_split() {
    let manifest_path = temp_path("collected-manifest.json");
    let train_path = temp_path("train.csv");
    let validation_path = temp_path("validation.csv");
    let test_path = temp_path("test.csv");
    let captured = Arc::new(Mutex::new(Vec::new()));
    let mut registry = default_registry();
    let sink = Arc::new(CaptureSink {
        packets: captured.clone(),
    });
    registry.register("capture_sink", move |_| Ok(sink.clone()));

    let yaml = format!(
        r#"
id: ai-prep-collected-manifest
engine:
  repository:
    enabled: false
processors:
  - id: source
    type: generate_records
    config:
      records:
        - {{ id: 1, value: 10 }}
        - {{ id: 2, value: 20 }}
        - {{ id: 3, value: 30 }}
        - {{ id: 4, value: 40 }}
  - id: split
    type: ai_split_dataset
    config: {{ train: 0.5, validation: 0.25, test: 0.25 }}
  - id: manifest
    type: ai_export_manifest
    config:
      path: {}
      dataset_name: conveyor
      collect_splits: true
      train_path: {}
      validation_path: {}
      test_path: {}
  - id: sink
    type: capture_sink
connections:
  - {{ from: source, relationship: success, to: split }}
  - {{ from: split, relationship: train, to: manifest }}
  - {{ from: split, relationship: validation, to: manifest }}
  - {{ from: split, relationship: test, to: manifest }}
  - {{ from: manifest, relationship: success, to: sink }}
"#,
        manifest_path.display(),
        train_path.display(),
        validation_path.display(),
        test_path.display()
    );
    let summary = FlowEngine::new(serde_yaml::from_str(&yaml).unwrap())
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .expect("collected manifest flow");

    assert_eq!(summary.failed, 0);
    for path in [&train_path, &validation_path, &test_path, &manifest_path] {
        assert!(path.is_file(), "missing output {}", path.display());
    }
    let manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["row_count"], 4);
    assert_eq!(manifest["split_counts"]["train"], 2);
    assert_eq!(manifest["split_counts"]["validation"], 1);
    assert_eq!(manifest["split_counts"]["test"], 1);
    let packets = captured.lock().unwrap();
    assert_eq!(
        packets.as_slice(),
        &[(
            "all".to_owned(),
            vec![
                serde_json::json!({"id": 1, "value": 10}),
                serde_json::json!({"id": 2, "value": 20}),
                serde_json::json!({"id": 3, "value": 30}),
                serde_json::json!({"id": 4, "value": 40}),
            ]
        )]
    );
    drop(packets);

    for path in [manifest_path, train_path, validation_path, test_path] {
        let _ = fs::remove_file(path);
    }
}

#[test]
fn registry_exposes_ai_prep_processors() {
    let registry: ProcessorRegistry = default_registry();
    for (name, config) in [
        ("ai_select_fields", serde_json::json!({"keep": ["a"]})),
        ("ai_drop_nulls", serde_json::json!({"fields": ["a"]})),
        (
            "ai_fill_missing",
            serde_json::json!({"fields": ["a"], "strategy": "previous"}),
        ),
        (
            "ai_remove_duplicates",
            serde_json::json!({"key_fields": ["a"]}),
        ),
        (
            "ai_filter_range",
            serde_json::json!({"field": "a", "min": 0, "max": 1}),
        ),
        (
            "ai_cast_types",
            serde_json::json!({"fields": {"a": "number"}}),
        ),
        ("ai_normalize", serde_json::json!({"fields": ["a"]})),
        (
            "ai_encode_categories",
            serde_json::json!({"fields": {"a": {"x": 1}}}),
        ),
        (
            "ai_compute_fields",
            serde_json::json!({"fields": {"b": "a + 1"}}),
        ),
        ("ai_split_dataset", serde_json::json!({})),
        (
            "ai_lookup_join",
            serde_json::json!({
                "key": "a",
                "lookup_records": [{"a": 1}]
            }),
        ),
        (
            "ai_export_manifest",
            serde_json::json!({"path": "/tmp/jaiba-manifest-test.json"}),
        ),
        (
            "ai_trigger_webhook",
            serde_json::json!({"url": "http://127.0.0.1:9/hook"}),
        ),
    ] {
        registry
            .build(name, &config)
            .unwrap_or_else(|error| panic!("build {name}: {error}"));
    }
}

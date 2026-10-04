//! Pruebas del motor: enrutamiento, reintentos y DLQ, pausa y drenado,
//! concurrencia, orden y particiones, memoria de paquetes, muchas fuentes y
//! validación del YAML.

use super::*;
use super::{partition::packet_partition_key, retry::execute_with_retry};
use crate::{
    config::{ExecutionMode, RetryConfig},
    engine::{OutputSender, ProcessorContext},
};

#[tokio::test]
async fn branches_reserve_independent_copies_before_routing() {
    let config: FlowConfig = serde_yaml::from_str("id: branch-test\nprocessors: []\nconnections:\n  - {from: source, to: a, relationship: success}\n  - {from: source, to: b, relationship: success}\n").unwrap();
    for budget in [65536, 4 * 65536] {
        let metrics = FlowMetrics::default();
        let memory = MemoryLimiter::from_budget(budget, metrics.clone());
        let packet = DataPacket::with_records(vec![serde_json::json!({"payload": "value"})]);
        let size = packet.estimated_size() as u64;
        let emission = ProcessorEmission {
            processor_id: "source".into(),
            relationship: "success".into(),
            _reservation: memory.reserve(size as usize).await.unwrap(),
            packet,
        };
        let mut pending = VecDeque::new();
        let result = route_emission(
            &emission,
            &mut pending,
            &config.connections,
            10,
            &metrics,
            &HashMap::new(),
            &HashMap::new(),
            None,
            "branch-test",
            &memory,
        )
        .await;
        if budget == 65536 {
            assert!(matches!(
                result,
                Ok(RouteOutcome::MemoryFull(FlowError::MemoryCapacity { .. }))
            ));
            assert!(pending.is_empty());
            assert_eq!(metrics.summary().memory_used_bytes, size);
        } else {
            assert!(matches!(result, Ok(RouteOutcome::Routed)));
            assert_eq!(pending.len(), 2);
            assert_eq!(metrics.summary().memory_used_bytes, size * 3);
        }
        drop(emission);
        pending.clear();
        assert_eq!(metrics.summary().memory_used_bytes, 0);
    }
}
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};

struct SlowSource;
struct Sink;
struct ForwardingCpuSink;
struct BurstSource;
struct AlwaysFail;
struct ConcurrencyProbe {
    active: AtomicUsize,
    maximum: AtomicUsize,
}

#[async_trait]
impl Processor for SlowSource {
    async fn execute(
        &self,
        packet: DataPacket,
        _: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        output.success(packet).await
    }
}

#[async_trait]
impl Processor for Sink {
    async fn execute(
        &self,
        _: DataPacket,
        _: &ProcessorContext,
        _: &OutputSender,
    ) -> Result<(), FlowError> {
        Ok(())
    }
}

#[async_trait]
impl Processor for ForwardingCpuSink {
    fn execution_mode(&self) -> ExecutionMode {
        ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        packet: DataPacket,
        _: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        output.success(packet).await
    }
}

/// Stub de prueba: siempre falla para ejercitar retry → dead-letter.
#[async_trait]
impl Processor for AlwaysFail {
    async fn execute(
        &self,
        _: DataPacket,
        context: &ProcessorContext,
        _: &OutputSender,
    ) -> Result<(), FlowError> {
        Err(FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message: "phase8 intentional failure".to_owned(),
        })
    }
}

#[async_trait]
impl Processor for BurstSource {
    async fn execute(
        &self,
        _: DataPacket,
        _: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        for index in 0..6 {
            output
                .success(DataPacket::with_records(vec![serde_json::json!({
                    "index": index
                })]))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Processor for ConcurrencyProbe {
    async fn execute(
        &self,
        _: DataPacket,
        _: &ProcessorContext,
        _: &OutputSender,
    ) -> Result<(), FlowError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(40)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn bundled_jme_flows_validate() {
    let flows = [
        include_str!("../../../../../examples/stable-runtime-stress.yaml")
            .replace("__ROWS__", "1000"),
        include_str!("../../../../../examples/jme-runtime-flow.yaml").to_owned(),
    ];
    for yaml in flows {
        let config = parse(&yaml);
        let domain_memory = config.engine.domain_memory.clone();
        assert!(domain_memory.enabled, "{}", config.id);
        jaiba_memory::MemoryPolicy::from_memory_value(
            domain_memory.policy.expect("inline JME policy"),
        )
        .unwrap();
        FlowEngine::new(config).unwrap();
    }
}

#[tokio::test]
async fn exhausted_packet_memory_fails_the_packet_not_the_flow() {
    let metrics = FlowMetrics::default();
    let memory = MemoryLimiter::from_budget(65536, metrics.clone());
    let input = memory.reserve(1).await.unwrap();
    let (sender, _receiver) = mpsc::channel(4);
    let output = OutputSender::new(sender, "transform", memory, metrics.clone())
        .with_input_reservation(true);
    let state_path =
        std::env::temp_dir().join(format!("jaiba-exec-state-{}.json", uuid::Uuid::new_v4()));
    let context = ProcessorContext {
        flow_id: "memory-test".into(),
        processor_id: "transform".into(),
        parameters: Arc::new(HashMap::new()),
        connections: ConnectionManager::default(),
        metrics: metrics.clone(),
        state: StateStore::load(&state_path).unwrap(),
        circuits: CircuitBreakers::new(crate::config::CircuitBreakerConfig {
            enabled: false,
            ..Default::default()
        })
        .unwrap(),
        domain_memory: None,
    };
    let result = execute_with_retry(
        Arc::new(ForwardingCpuSink),
        DataPacket::empty(),
        context,
        RetryConfig::default(),
        None,
        output,
        None,
        None,
    )
    .await;
    let (message, _) = result.unwrap().expect("packet routed to failure");
    assert!(message.contains("memory capacity exhausted"));
    drop(input);
    assert_eq!(metrics.summary().memory_used_bytes, 0);
}

fn lifecycle_registry() -> ProcessorRegistry {
    let mut registry = ProcessorRegistry::default();
    registry.register("slow_source", |_| Ok(Arc::new(SlowSource)));
    registry.register("test_sink", |_| Ok(Arc::new(Sink)));
    registry
}

fn parse(yaml: &str) -> FlowConfig {
    serde_yaml::from_str(yaml).unwrap()
}

#[test]
fn rejects_unknown_destination() {
    let config = parse(
        r#"
id: test
processors:
  - id: source
    type: generate_records
connections:
  - from: source
    relationship: success
    to: missing
"#,
    );
    assert!(FlowEngine::new(config).is_err());
}

#[test]
fn real_runtime_rejects_simulation_modes() {
    let config = parse(
        r#"
id: mock-flow
processors:
  - id: source
    type: generate_records
    simulation:
      mode: mock
"#,
    );
    let error = FlowEngine::new(config).err().unwrap();
    assert!(error.to_string().contains("jaiba-simulator"));
}

#[test]
fn resolves_parameters() {
    let config = parse(
        r#"
id: test
parameters:
  table: customers
processors:
  - id: source
    type: generate_records
    config:
      records:
        - table: "${table}"
"#,
    );
    let engine = FlowEngine::new(config).unwrap();
    assert_eq!(
        engine.config.processors[0].config["records"][0]["table"],
        "customers"
    );
}

#[tokio::test]
async fn runs_a_complete_flow() {
    let config = parse(
        r#"
id: test
processors:
  - id: source
    type: generate_records
    config:
      records:
        - old_name: Ada
  - id: rename
    type: rename_fields
    config:
      fields:
        old_name: name
  - id: sink
    type: log_records
connections:
  - from: source
    relationship: success
    to: rename
  - from: rename
    relationship: success
    to: sink
"#,
    );

    let summary = FlowEngine::new(config).unwrap().run().await.unwrap();
    assert_eq!(summary.processed, 3);
    assert_eq!(summary.failed, 0);
    assert_eq!(summary.emitted, 2);
}

#[tokio::test]
async fn routes_processor_errors_to_failure() {
    let config = parse(
        r#"
id: test
processors:
  - id: source
    type: generate_records
    config:
      records:
        - "not an object"
  - id: rename
    type: rename_fields
    config:
      fields:
        old_name: name
  - id: errors
    type: log_records
connections:
  - from: source
    relationship: success
    to: rename
  - from: rename
    relationship: failure
    to: errors
"#,
    );

    let summary = FlowEngine::new(config).unwrap().run().await.unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.processed, 2);
}

/// Fase 8: reintentos agotados → fila en DLQ → `requeue_dead_letter`.
#[tokio::test]
async fn flow_retry_then_dead_letter() {
    let root = std::env::temp_dir().join(format!("jaiva-phase8-dlq-{}", uuid::Uuid::new_v4()));
    let database_path = root.join("repository.db");
    let content_path = root.join("content");
    let config = parse(&format!(
        r#"
id: phase8-dlq
engine:
  repository:
    enabled: true
    database_path: {}
    content_path: {}
    completed_retention_hours: 1
    provenance_retention_hours: 1
processors:
  - id: source
    type: generate_records
    config:
      records:
        - id: 1
  - id: boom
    type: always_fail
    retry:
      maximum_attempts: 2
      initial_delay_ms: 1
      maximum_delay_ms: 1
connections:
  - from: source
    relationship: success
    to: boom
"#,
        database_path.display(),
        content_path.display()
    ));

    let mut registry = default_registry();
    registry.register("always_fail", |_| Ok(Arc::new(AlwaysFail)));
    let summary = FlowEngine::new(config)
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .unwrap();
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.retried, 2);
    assert_eq!(summary.repository_dead_letter, 1);

    let repository = LocalPacketRepository::open(&crate::config::RepositoryConfig {
        enabled: true,
        database_path: database_path.clone(),
        content_path: content_path.clone(),
        abandoned_after_seconds: 60,
        completed_retention_hours: 1,
        provenance_retention_hours: 1,
    })
    .await
    .unwrap();
    let letters = repository.dead_letters("phase8-dlq", 10).await.unwrap();
    assert_eq!(letters.len(), 1);
    assert!(
        letters[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("phase8 intentional failure")
    );
    assert!(
        repository
            .requeue_dead_letter(&letters[0].queue_id)
            .await
            .unwrap()
    );
    assert_eq!(repository.pending("phase8-dlq").await.unwrap().len(), 1);
    drop(repository);
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn pause_stops_downstream_scheduling_until_resume() {
    let config = parse(
        r#"
id: lifecycle
processors:
  - { id: source, type: slow_source }
  - { id: sink, type: test_sink }
connections:
  - { from: source, relationship: success, to: sink }
"#,
    );
    let control = FlowControl::default();
    let engine = FlowEngine::new(config)
        .unwrap()
        .with_registry(lifecycle_registry())
        .with_control(control.clone());
    let task = tokio::spawn(async move { engine.run().await });
    while control.state() != FlowLifecycle::Running {
        tokio::task::yield_now().await;
    }
    assert!(control.pause());
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!task.is_finished());
    assert!(control.resume());
    let summary = task.await.unwrap().unwrap();
    assert_eq!(summary.processed, 2);
}

#[tokio::test]
async fn drain_finishes_active_work_without_scheduling_pending_work() {
    let config = parse(
        r#"
id: lifecycle
processors:
  - { id: source, type: slow_source }
  - { id: sink, type: test_sink }
connections:
  - { from: source, relationship: success, to: sink }
"#,
    );
    let control = FlowControl::default();
    let engine = FlowEngine::new(config)
        .unwrap()
        .with_registry(lifecycle_registry())
        .with_control(control.clone());
    let task = tokio::spawn(async move { engine.run().await });
    while control.state() != FlowLifecycle::Running {
        tokio::task::yield_now().await;
    }
    assert!(control.drain());
    let summary = task.await.unwrap().unwrap();
    assert_eq!(summary.processed, 1);
    assert_eq!(control.state(), FlowLifecycle::Stopped);
}

#[tokio::test]
async fn global_concurrency_is_a_strict_upper_bound() {
    let config = parse(
        r#"
id: bounded
engine:
  max_concurrency: 2
processors:
  - { id: p1, type: concurrency_probe }
  - { id: p2, type: concurrency_probe }
  - { id: p3, type: concurrency_probe }
  - { id: p4, type: concurrency_probe }
  - { id: p5, type: concurrency_probe }
  - { id: p6, type: concurrency_probe }
"#,
    );
    let probe = Arc::new(ConcurrencyProbe {
        active: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let mut registry = ProcessorRegistry::default();
    let registered = probe.clone();
    registry.register("concurrency_probe", move |_| Ok(registered.clone()));

    let summary = FlowEngine::new(config)
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .unwrap();

    assert_eq!(summary.processed, 6);
    assert_eq!(probe.maximum.load(Ordering::SeqCst), 2);
}

#[test]
fn partition_key_requires_one_value_per_packet() {
    let packet = DataPacket::with_records(vec![
        serde_json::json!({"customer_id": 10}),
        serde_json::json!({"customer_id": 10}),
    ]);
    assert_eq!(
        packet_partition_key(&packet, "customer_id", "write").unwrap(),
        "10"
    );

    let mixed = DataPacket::with_records(vec![
        serde_json::json!({"customer_id": 10}),
        serde_json::json!({"customer_id": 11}),
    ]);
    assert!(packet_partition_key(&mixed, "customer_id", "write").is_err());
}

#[tokio::test]
async fn preserve_order_forces_one_active_task_for_the_processor() {
    let config = parse(
        r#"
id: ordered
engine:
  max_concurrency: 8
processors:
  - { id: source, type: burst_source }
  - id: ordered_sink
    type: concurrency_probe
    scheduling:
      concurrent_tasks: 6
      ordering: preserve
connections:
  - { from: source, relationship: success, to: ordered_sink }
"#,
    );
    let probe = Arc::new(ConcurrencyProbe {
        active: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let mut registry = ProcessorRegistry::default();
    registry.register("burst_source", |_| Ok(Arc::new(BurstSource)));
    let registered = probe.clone();
    registry.register("concurrency_probe", move |_| Ok(registered.clone()));

    FlowEngine::new(config)
        .unwrap()
        .with_registry(registry)
        .run()
        .await
        .unwrap();
    assert_eq!(probe.maximum.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn strict_limit_keeps_a_streaming_pipeline_moving() {
    let config = parse(
        r#"
id: streaming
engine:
  max_concurrency: 2
  queue_capacity: 1
processors:
  - { id: source, type: burst_source }
  - { id: sink, type: test_sink }
connections:
  - from: source
    relationship: success
    to: sink
    queue: { capacity: 1 }
"#,
    );
    let mut registry = lifecycle_registry();
    registry.register("burst_source", |_| Ok(Arc::new(BurstSource)));
    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        FlowEngine::new(config)
            .unwrap()
            .with_registry(registry)
            .run(),
    )
    .await
    .expect("streaming pipeline must not deadlock")
    .unwrap();
    assert_eq!(summary.processed, 7);
}

#[tokio::test]
async fn terminal_cpu_output_cannot_deadlock_a_full_upstream_queue() {
    let config = parse(
        r#"
id: terminal-output
engine:
  max_concurrency: 2
  queue_capacity: 2
processors:
  - { id: source, type: burst_source }
  - { id: terminal, type: forwarding_cpu_sink }
connections:
  - from: source
    relationship: success
    to: terminal
    queue: { capacity: 2 }
"#,
    );
    let mut registry = ProcessorRegistry::default();
    registry.register("burst_source", |_| Ok(Arc::new(BurstSource)));
    registry.register("forwarding_cpu_sink", |_| Ok(Arc::new(ForwardingCpuSink)));

    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        FlowEngine::new(config)
            .unwrap()
            .with_registry(registry)
            .run(),
    )
    .await
    .expect("terminal output must bypass the full routing channel")
    .unwrap();
    assert_eq!(summary.processed, 7);
    assert_eq!(summary.failed, 0);
}

#[tokio::test]
async fn more_sources_than_queue_capacity_do_not_deadlock() {
    let sources = 40;
    let mut yaml = String::from(
        "id: many-sources\nengine:\n  max_concurrency: 3\n  queue_capacity: 4\n  repository: { enabled: false }\nprocessors:\n",
    );
    for index in 0..sources {
        yaml.push_str(&format!(
            "  - {{ id: source{index}, type: generate_records, config: {{ records: [{{ id: {index} }}] }} }}\n"
        ));
    }
    yaml.push_str("  - { id: middle, type: log_records, config: {} }\n");
    yaml.push_str("  - { id: sink, type: log_records, config: {} }\nconnections:\n");
    for index in 0..sources {
        yaml.push_str(&format!(
            "  - {{ from: source{index}, relationship: success, to: middle }}\n"
        ));
    }
    yaml.push_str("  - { from: middle, relationship: success, to: sink }\n");
    let summary = tokio::time::timeout(
        Duration::from_secs(10),
        FlowEngine::new(parse(&yaml)).unwrap().run(),
    )
    .await
    .expect("source seeds must not fill the routing queue")
    .unwrap();
    assert_eq!(summary.processed, sources * 3);
    assert_eq!(summary.failed, 0);
}

#[test]
fn rejects_a_global_limit_smaller_than_the_streaming_path() {
    let config = parse(
        r#"
id: too-small
engine:
  max_concurrency: 1
processors:
  - { id: source, type: generate_records }
  - { id: sink, type: log_records }
connections:
  - { from: source, relationship: success, to: sink }
"#,
    );
    assert!(FlowEngine::new(config).is_err());
}

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use jaiba_memory::MemorySnapshot;
use serde::Serialize;

#[derive(Clone, Default, Debug)]
pub struct FlowMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default, Debug)]
struct MetricsInner {
    flow_id: Mutex<String>,
    flow_status: AtomicU64,
    flow_last_success_timestamp: AtomicU64,
    processed: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    emitted: AtomicU64,
    queue_depth: AtomicU64,
    active_tasks: AtomicU64,
    memory_used_bytes: AtomicU64,
    memory_budget_bytes: AtomicU64,
    backpressure_total: AtomicU64,
    repository_pending: AtomicU64,
    repository_running: AtomicU64,
    repository_dead_letter: AtomicU64,
    repository_content_bytes: AtomicU64,
    recovered_packets: AtomicU64,
    database_rows_written: AtomicU64,
    database_batches_written: AtomicU64,
    database_write_errors: AtomicU64,
    database_rollbacks: AtomicU64,
    database_write_duration_ms: AtomicU64,
    kafka_messages_published: AtomicU64,
    kafka_bytes_published: AtomicU64,
    kafka_publish_errors: AtomicU64,
    kafka_messages_consumed: AtomicU64,
    kafka_bytes_consumed: AtomicU64,
    kafka_consume_errors: AtomicU64,
    circuit_rejections: AtomicU64,
    circuits_open: AtomicU64,
    available_parallelism: AtomicU64,
    cpu_worker_limit: AtomicU64,
    blocking_worker_limit: AtomicU64,
    processors: Mutex<HashMap<String, ProcessorMetrics>>,
    connection_queues: Mutex<HashMap<String, ConnectionQueueMetrics>>,
    domain_memory: Mutex<Option<MemorySnapshot>>,
}

#[derive(Debug, Clone, Default)]
struct ProcessorMetrics {
    active_tasks: u64,
    queue_depth: u64,
    concurrency_limit: u64,
    completed: u64,
    records: u64,
    failed: u64,
    execution_duration_ns: u64,
}

#[derive(Debug, Clone, Default)]
struct ConnectionQueueMetrics {
    packets: u64,
    bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessorSummary {
    pub active_tasks: u64,
    pub queue_depth: u64,
    pub concurrency_limit: u64,
    pub completed: u64,
    pub records: u64,
    pub failed: u64,
    pub execution_duration_ms: u64,
    pub execution_duration_seconds: f64,
    pub saturation_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlowSummary {
    pub flow_id: String,
    pub flow_status: u64,
    pub flow_last_success_timestamp: u64,
    pub processed: u64,
    pub failed: u64,
    pub retried: u64,
    pub emitted: u64,
    pub queue_depth: u64,
    pub active_tasks: u64,
    pub memory_used_bytes: u64,
    pub memory_budget_bytes: u64,
    pub backpressure_total: u64,
    pub repository_pending: u64,
    pub repository_running: u64,
    pub repository_dead_letter: u64,
    pub repository_content_bytes: u64,
    pub recovered_packets: u64,
    pub database_rows_written: u64,
    pub database_batches_written: u64,
    pub database_write_errors: u64,
    pub database_rollbacks: u64,
    pub database_write_duration_ms: u64,
    pub kafka_messages_published: u64,
    pub kafka_bytes_published: u64,
    pub kafka_publish_errors: u64,
    pub kafka_messages_consumed: u64,
    pub kafka_bytes_consumed: u64,
    pub kafka_consume_errors: u64,
    pub circuit_rejections: u64,
    pub circuits_open: u64,
    pub available_parallelism: u64,
    pub cpu_worker_limit: u64,
    pub blocking_worker_limit: u64,
    pub processors: HashMap<String, ProcessorSummary>,
    pub connection_queues: HashMap<String, ConnectionQueueSummary>,
    pub domain_memory: Option<MemorySnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionQueueSummary {
    pub packets: u64,
    pub bytes: u64,
}

impl FlowMetrics {
    pub fn set_flow_id(&self, flow_id: &str) {
        *self.inner.flow_id.lock().expect("flow id metrics poisoned") = flow_id.to_owned();
    }

    /// 0 stopped, 1 starting, 2 running, 3 paused, 4 draining, 5 failed.
    pub fn set_flow_status(&self, status: u64) {
        self.inner.flow_status.store(status, Ordering::Relaxed);
    }

    pub fn set_domain_memory(&self, snapshot: MemorySnapshot) {
        *self
            .inner
            .domain_memory
            .lock()
            .expect("domain memory metrics poisoned") = Some(snapshot);
    }

    pub fn flow_succeeded(&self) {
        self.inner.flow_last_success_timestamp.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            Ordering::Relaxed,
        );
    }

    pub fn set_connection_queues(&self, queues: HashMap<String, (u64, u64)>) {
        *self
            .inner
            .connection_queues
            .lock()
            .expect("connection queue metrics poisoned") = queues
            .into_iter()
            .map(|(id, (packets, bytes))| (id, ConnectionQueueMetrics { packets, bytes }))
            .collect();
    }

    pub fn processed(&self) {
        self.inner.processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn failed(&self) {
        self.inner.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn retried(&self) {
        self.inner.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub fn emitted(&self, count: usize) {
        self.inner
            .emitted
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn set_queue_depth(&self, count: usize) {
        self.inner
            .queue_depth
            .store(count as u64, Ordering::Relaxed);
    }

    pub fn set_active_tasks(&self, count: usize) {
        self.inner
            .active_tasks
            .store(count as u64, Ordering::Relaxed);
    }

    pub fn set_memory_budget(&self, bytes: u64) {
        self.inner
            .memory_budget_bytes
            .store(bytes, Ordering::Relaxed);
    }

    pub fn reserve_memory(&self, bytes: u64) {
        self.inner
            .memory_used_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn release_memory(&self, bytes: u64) {
        self.inner
            .memory_used_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn backpressure(&self) {
        self.inner
            .backpressure_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_repository(&self, pending: u64, running: u64, dead_letter: u64, content_bytes: u64) {
        self.inner
            .repository_pending
            .store(pending, Ordering::Relaxed);
        self.inner
            .repository_running
            .store(running, Ordering::Relaxed);
        self.inner
            .repository_dead_letter
            .store(dead_letter, Ordering::Relaxed);
        self.inner
            .repository_content_bytes
            .store(content_bytes, Ordering::Relaxed);
    }

    pub fn recovered(&self, count: u64) {
        self.inner
            .recovered_packets
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Records a committed database write.
    pub fn database_write(&self, rows: u64, batches: u64, duration_ms: u64) {
        self.inner
            .database_rows_written
            .fetch_add(rows, Ordering::Relaxed);
        self.inner
            .database_batches_written
            .fetch_add(batches, Ordering::Relaxed);
        self.inner
            .database_write_duration_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Records a database writer error.
    pub fn database_write_error(&self) {
        self.inner
            .database_write_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a rolled-back database write attempt.
    pub fn database_rollback(&self) {
        self.inner
            .database_rollbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn kafka_published(&self, messages: u64, bytes: u64) {
        self.inner
            .kafka_messages_published
            .fetch_add(messages, Ordering::Relaxed);
        self.inner
            .kafka_bytes_published
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn kafka_publish_error(&self) {
        self.inner
            .kafka_publish_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn kafka_consumed(&self, messages: u64, bytes: u64) {
        self.inner
            .kafka_messages_consumed
            .fetch_add(messages, Ordering::Relaxed);
        self.inner
            .kafka_bytes_consumed
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn kafka_consume_error(&self) {
        self.inner
            .kafka_consume_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn circuit_rejected(&self) {
        self.inner
            .circuit_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_circuits_open(&self, count: usize) {
        self.inner
            .circuits_open
            .store(count as u64, Ordering::Relaxed);
    }

    pub fn set_worker_limits(&self, available: usize, cpu: usize, blocking: usize) {
        self.inner
            .available_parallelism
            .store(available as u64, Ordering::Relaxed);
        self.inner
            .cpu_worker_limit
            .store(cpu as u64, Ordering::Relaxed);
        self.inner
            .blocking_worker_limit
            .store(blocking as u64, Ordering::Relaxed);
    }

    pub fn set_processor_load(
        &self,
        processor_id: &str,
        queue_depth: usize,
        active_tasks: usize,
        concurrency_limit: usize,
    ) {
        let mut processors = self
            .inner
            .processors
            .lock()
            .expect("processor metrics poisoned");
        let processor = processors.entry(processor_id.to_owned()).or_default();
        processor.queue_depth = queue_depth as u64;
        processor.active_tasks = active_tasks as u64;
        processor.concurrency_limit = concurrency_limit as u64;
    }

    pub fn processor_finished(&self, processor_id: &str, duration: Duration, success: bool) {
        let mut processors = self
            .inner
            .processors
            .lock()
            .expect("processor metrics poisoned");
        let processor = processors.entry(processor_id.to_owned()).or_default();
        if success {
            processor.completed = processor.completed.saturating_add(1);
        } else {
            processor.failed = processor.failed.saturating_add(1);
        }
        processor.execution_duration_ns = processor
            .execution_duration_ns
            .saturating_add(duration.as_nanos().min(u64::MAX as u128) as u64);
    }

    pub fn processor_records(&self, processor_id: &str, count: u64) {
        let mut processors = self
            .inner
            .processors
            .lock()
            .expect("processor metrics poisoned");
        let processor = processors.entry(processor_id.to_owned()).or_default();
        processor.records = processor.records.saturating_add(count);
    }

    pub fn summary(&self) -> FlowSummary {
        FlowSummary {
            flow_id: self
                .inner
                .flow_id
                .lock()
                .expect("flow id metrics poisoned")
                .clone(),
            flow_status: self.inner.flow_status.load(Ordering::Relaxed),
            flow_last_success_timestamp: self
                .inner
                .flow_last_success_timestamp
                .load(Ordering::Relaxed),
            processed: self.inner.processed.load(Ordering::Relaxed),
            failed: self.inner.failed.load(Ordering::Relaxed),
            retried: self.inner.retried.load(Ordering::Relaxed),
            emitted: self.inner.emitted.load(Ordering::Relaxed),
            queue_depth: self.inner.queue_depth.load(Ordering::Relaxed),
            active_tasks: self.inner.active_tasks.load(Ordering::Relaxed),
            memory_used_bytes: self.inner.memory_used_bytes.load(Ordering::Relaxed),
            memory_budget_bytes: self.inner.memory_budget_bytes.load(Ordering::Relaxed),
            backpressure_total: self.inner.backpressure_total.load(Ordering::Relaxed),
            repository_pending: self.inner.repository_pending.load(Ordering::Relaxed),
            repository_running: self.inner.repository_running.load(Ordering::Relaxed),
            repository_dead_letter: self.inner.repository_dead_letter.load(Ordering::Relaxed),
            repository_content_bytes: self.inner.repository_content_bytes.load(Ordering::Relaxed),
            recovered_packets: self.inner.recovered_packets.load(Ordering::Relaxed),
            database_rows_written: self.inner.database_rows_written.load(Ordering::Relaxed),
            database_batches_written: self.inner.database_batches_written.load(Ordering::Relaxed),
            database_write_errors: self.inner.database_write_errors.load(Ordering::Relaxed),
            database_rollbacks: self.inner.database_rollbacks.load(Ordering::Relaxed),
            database_write_duration_ms: self
                .inner
                .database_write_duration_ms
                .load(Ordering::Relaxed),
            kafka_messages_published: self.inner.kafka_messages_published.load(Ordering::Relaxed),
            kafka_bytes_published: self.inner.kafka_bytes_published.load(Ordering::Relaxed),
            kafka_publish_errors: self.inner.kafka_publish_errors.load(Ordering::Relaxed),
            kafka_messages_consumed: self.inner.kafka_messages_consumed.load(Ordering::Relaxed),
            kafka_bytes_consumed: self.inner.kafka_bytes_consumed.load(Ordering::Relaxed),
            kafka_consume_errors: self.inner.kafka_consume_errors.load(Ordering::Relaxed),
            circuit_rejections: self.inner.circuit_rejections.load(Ordering::Relaxed),
            circuits_open: self.inner.circuits_open.load(Ordering::Relaxed),
            available_parallelism: self.inner.available_parallelism.load(Ordering::Relaxed),
            cpu_worker_limit: self.inner.cpu_worker_limit.load(Ordering::Relaxed),
            blocking_worker_limit: self.inner.blocking_worker_limit.load(Ordering::Relaxed),
            processors: self
                .inner
                .processors
                .lock()
                .expect("processor metrics poisoned")
                .iter()
                .map(|(id, metrics)| {
                    let limit = metrics.concurrency_limit.max(1);
                    (
                        id.clone(),
                        ProcessorSummary {
                            active_tasks: metrics.active_tasks,
                            queue_depth: metrics.queue_depth,
                            concurrency_limit: metrics.concurrency_limit,
                            completed: metrics.completed,
                            records: metrics.records,
                            failed: metrics.failed,
                            execution_duration_ms: metrics.execution_duration_ns / 1_000_000,
                            execution_duration_seconds: metrics.execution_duration_ns as f64
                                / 1_000_000_000.0,
                            saturation_ratio: metrics.active_tasks as f64 / limit as f64,
                        },
                    )
                })
                .collect(),
            connection_queues: self
                .inner
                .connection_queues
                .lock()
                .expect("connection queue metrics poisoned")
                .iter()
                .map(|(id, metrics)| {
                    (
                        id.clone(),
                        ConnectionQueueSummary {
                            packets: metrics.packets,
                            bytes: metrics.bytes,
                        },
                    )
                })
                .collect(),
            domain_memory: self
                .inner
                .domain_memory
                .lock()
                .expect("domain memory metrics poisoned")
                .clone(),
        }
    }

    pub fn prometheus(&self) -> String {
        let snapshot = self.summary();
        let flow = escape_label(&snapshot.flow_id);
        let mut output = format!(
            "# HELP jaiva_packets_processed_total Packets processed successfully.\n\
             # TYPE jaiva_packets_processed_total counter\n\
             jaiva_packets_processed_total {}\n\
             # HELP jaiva_packets_failed_total Packets that exhausted retries.\n\
             # TYPE jaiva_packets_failed_total counter\n\
             jaiva_packets_failed_total {}\n\
             # HELP jaiva_packet_retries_total Processor retry attempts.\n\
             # TYPE jaiva_packet_retries_total counter\n\
             jaiva_packet_retries_total {}\n\
             # HELP jaiva_packets_emitted_total Packets routed to connections.\n\
             # TYPE jaiva_packets_emitted_total counter\n\
             jaiva_packets_emitted_total {}\n\
             # HELP jaiva_queue_depth Current in-memory queue depth.\n\
             # TYPE jaiva_queue_depth gauge\n\
             jaiva_queue_depth {}\n\
             # HELP jaiva_active_tasks Current processor tasks.\n\
             # TYPE jaiva_active_tasks gauge\n\
             jaiva_active_tasks {}\n\
             # HELP jaiva_memory_used_bytes Estimated bytes reserved by packets.\n\
             # TYPE jaiva_memory_used_bytes gauge\n\
             jaiva_memory_used_bytes {}\n\
             # HELP jaiva_memory_budget_bytes Maximum packet-memory budget.\n\
             # TYPE jaiva_memory_budget_bytes gauge\n\
             jaiva_memory_budget_bytes {}\n\
             # HELP jaiva_backpressure_total Times producers waited for memory.\n\
             # TYPE jaiva_backpressure_total counter\n\
             jaiva_backpressure_total {}\n\
             # HELP jaiva_repository_pending_packets Persistent packets waiting for processing.\n\
             # TYPE jaiva_repository_pending_packets gauge\n\
             jaiva_repository_pending_packets {}\n\
             # HELP jaiva_repository_running_packets Persistent packets being processed.\n\
             # TYPE jaiva_repository_running_packets gauge\n\
             jaiva_repository_running_packets {}\n\
             # HELP jaiva_repository_dead_letter_packets Persistent failed packets.\n\
             # TYPE jaiva_repository_dead_letter_packets gauge\n\
             jaiva_repository_dead_letter_packets {}\n\
             # HELP jaiva_repository_content_bytes Referenced persistent content bytes.\n\
             # TYPE jaiva_repository_content_bytes gauge\n\
             jaiva_repository_content_bytes {}\n\
             # HELP jaiva_recovered_packets_total Packets recovered after interruption.\n\
             # TYPE jaiva_recovered_packets_total counter\n\
             jaiva_recovered_packets_total {}\n\
             # HELP jaiva_database_rows_written_total Rows committed by database writers.\n\
             # TYPE jaiva_database_rows_written_total counter\n\
             jaiva_database_rows_written_total {}\n\
             # HELP jaiva_database_batches_written_total Batches committed by database writers.\n\
             # TYPE jaiva_database_batches_written_total counter\n\
             jaiva_database_batches_written_total {}\n\
             # HELP jaiva_database_write_errors_total Database write attempts that failed.\n\
             # TYPE jaiva_database_write_errors_total counter\n\
             jaiva_database_write_errors_total {}\n\
             # HELP jaiva_database_transaction_rollbacks_total Database write rollbacks.\n\
             # TYPE jaiva_database_transaction_rollbacks_total counter\n\
             jaiva_database_transaction_rollbacks_total {}\n\
             # HELP jaiva_database_write_duration_milliseconds_total Total committed write time.\n\
             # TYPE jaiva_database_write_duration_milliseconds_total counter\n\
             jaiva_database_write_duration_milliseconds_total {}\n\
             # HELP jaiva_kafka_messages_published_total Kafka messages acknowledged by brokers.\n\
             # TYPE jaiva_kafka_messages_published_total counter\n\
             jaiva_kafka_messages_published_total {}\n\
             # HELP jaiva_kafka_bytes_published_total Kafka payload bytes acknowledged by brokers.\n\
             # TYPE jaiva_kafka_bytes_published_total counter\n\
             jaiva_kafka_bytes_published_total {}\n\
             # HELP jaiva_kafka_publish_errors_total Kafka publish attempts that failed.\n\
             # TYPE jaiva_kafka_publish_errors_total counter\n\
             jaiva_kafka_publish_errors_total {}\n\
             # HELP jaiva_kafka_messages_consumed_total Kafka messages committed by consumers.\n\
             # TYPE jaiva_kafka_messages_consumed_total counter\n\
             jaiva_kafka_messages_consumed_total {}\n\
             # HELP jaiva_kafka_bytes_consumed_total Kafka payload bytes consumed.\n\
             # TYPE jaiva_kafka_bytes_consumed_total counter\n\
             jaiva_kafka_bytes_consumed_total {}\n\
             # HELP jaiva_kafka_consume_errors_total Kafka consume attempts that failed.\n\
             # TYPE jaiva_kafka_consume_errors_total counter\n\
             jaiva_kafka_consume_errors_total {}\n\
             # HELP jaiva_circuit_breaker_rejections_total Operations rejected by open circuits.\n\
             # TYPE jaiva_circuit_breaker_rejections_total counter\n\
             jaiva_circuit_breaker_rejections_total {}\n\
             # HELP jaiva_circuit_breakers_open Current open connection circuits.\n\
             # TYPE jaiva_circuit_breakers_open gauge\n\
             jaiva_circuit_breakers_open {}\n\
             # HELP jaiva_available_parallelism Logical CPUs visible to Jaiva.\n\
             # TYPE jaiva_available_parallelism gauge\n\
             jaiva_available_parallelism {}\n\
             # HELP jaiva_cpu_worker_limit Maximum concurrent CPU jobs.\n\
             # TYPE jaiva_cpu_worker_limit gauge\n\
             jaiva_cpu_worker_limit {}\n\
             # HELP jaiva_blocking_worker_limit Maximum concurrent blocking jobs.\n\
             # TYPE jaiva_blocking_worker_limit gauge\n\
             jaiva_blocking_worker_limit {}\n",
            snapshot.processed,
            snapshot.failed,
            snapshot.retried,
            snapshot.emitted,
            snapshot.queue_depth,
            snapshot.active_tasks,
            snapshot.memory_used_bytes,
            snapshot.memory_budget_bytes,
            snapshot.backpressure_total,
            snapshot.repository_pending,
            snapshot.repository_running,
            snapshot.repository_dead_letter,
            snapshot.repository_content_bytes,
            snapshot.recovered_packets,
            snapshot.database_rows_written,
            snapshot.database_batches_written,
            snapshot.database_write_errors,
            snapshot.database_rollbacks,
            snapshot.database_write_duration_ms,
            snapshot.kafka_messages_published,
            snapshot.kafka_bytes_published,
            snapshot.kafka_publish_errors,
            snapshot.kafka_messages_consumed,
            snapshot.kafka_bytes_consumed,
            snapshot.kafka_consume_errors,
            snapshot.circuit_rejections,
            snapshot.circuits_open,
            snapshot.available_parallelism,
            snapshot.cpu_worker_limit,
            snapshot.blocking_worker_limit,
        );
        output.push_str(
            "# HELP jaiva_processor_active_tasks Active tasks by processor.\n\
             # TYPE jaiva_processor_active_tasks gauge\n\
             # HELP jaiva_processor_queue_depth Queued packets by processor.\n\
             # TYPE jaiva_processor_queue_depth gauge\n\
             # HELP jaiva_processor_completed_total Successful packets by processor.\n\
             # TYPE jaiva_processor_completed_total counter\n\
             # HELP jaiva_processor_failed_total Failed packets by processor.\n\
             # TYPE jaiva_processor_failed_total counter\n\
             # HELP jaiva_processor_execution_milliseconds_total Execution time by processor.\n\
             # TYPE jaiva_processor_execution_milliseconds_total counter\n\
             # HELP jaiva_processor_saturation_ratio Active tasks divided by configured concurrency.\n\
             # TYPE jaiva_processor_saturation_ratio gauge\n",
        );
        if let Some(jme) = &snapshot.domain_memory {
            output.push_str(&format!(
                "# HELP jaiba_memory_hot_objects JME objects in Hot.\n\
                 # TYPE jaiba_memory_hot_objects gauge\n\
                 jaiba_memory_hot_objects {}\n\
                 # HELP jaiba_memory_hot_bytes Estimated JME bytes in Hot.\n\
                 # TYPE jaiba_memory_hot_bytes gauge\n\
                 jaiba_memory_hot_bytes {}\n\
                 # HELP jaiba_memory_warm_objects JME objects in Warm.\n\
                 # TYPE jaiba_memory_warm_objects gauge\n\
                 jaiba_memory_warm_objects {}\n\
                 # HELP jaiba_memory_cold_objects JME objects in segmented Cold.\n\
                 # TYPE jaiba_memory_cold_objects gauge\n\
                 jaiba_memory_cold_objects {}\n\
                 # HELP jaiba_memory_cold_bytes JME bytes used by Cold segments.\n\
                 # TYPE jaiba_memory_cold_bytes gauge\n\
                 jaiba_memory_cold_bytes {}\n\
                 # HELP jaiba_memory_cold_max_disk_bytes Configured JME Cold disk quota; zero means unlimited.\n\
                 # TYPE jaiba_memory_cold_max_disk_bytes gauge\n\
                 jaiba_memory_cold_max_disk_bytes {}\n\
                 # HELP jaiba_memory_cold_quota_rejections_total JME Cold writes rejected by disk quota.\n\
                 # TYPE jaiba_memory_cold_quota_rejections_total counter\n\
                 jaiba_memory_cold_quota_rejections_total {}\n\
                 # HELP jaiba_memory_cold_hits_total JME Cold read hits.\n\
                 # TYPE jaiba_memory_cold_hits_total counter\n\
                 jaiba_memory_cold_hits_total {}\n\
                 # HELP jaiba_memory_cold_misses_total JME Cold read misses.\n\
                 # TYPE jaiba_memory_cold_misses_total counter\n\
                 jaiba_memory_cold_misses_total {}\n\
                 # HELP jaiba_memory_frozen_objects JME objects in Frozen.\n\
                 # TYPE jaiba_memory_frozen_objects gauge\n\
                 jaiba_memory_frozen_objects {}\n\
                 # HELP jaiba_memory_evictions_total JME Hot evictions.\n\
                 # TYPE jaiba_memory_evictions_total counter\n\
                 jaiba_memory_evictions_total {}\n\
                 # HELP jaiba_memory_persist_queue Deferred records pending persistence.\n\
                 # TYPE jaiba_memory_persist_queue gauge\n\
                 jaiba_memory_persist_queue {}\n\
                 # HELP jaiba_memory_promotions_total JME promotions to Hot.\n\
                 # TYPE jaiba_memory_promotions_total counter\n\
                 jaiba_memory_promotions_total {}\n\
                 # HELP jaiba_memory_demotions_total JME demotions from Hot.\n\
                 # TYPE jaiba_memory_demotions_total counter\n\
                 jaiba_memory_demotions_total {}\n\
                 # HELP jaiba_memory_immediate_failures_total Immediate persistence failures.\n\
                 # TYPE jaiba_memory_immediate_failures_total counter\n\
                 jaiba_memory_immediate_failures_total {}\n\
                 # HELP jaiba_memory_deferred_failures_total Deferred persistence failures.\n\
                 # TYPE jaiba_memory_deferred_failures_total counter\n\
                 jaiba_memory_deferred_failures_total {}\n",
                jme.hot_objects,
                jme.hot_bytes,
                jme.warm_objects,
                jme.cold_objects,
                jme.cold_bytes,
                jme.cold_max_disk_bytes,
                jme.cold_quota_rejections,
                jme.cold_hits,
                jme.cold_misses,
                jme.frozen_objects,
                jme.evictions,
                jme.persist_queue,
                jme.promotions,
                jme.demotions,
                jme.immediate_failures,
                jme.deferred_failures,
            ));
        }
        output.push_str(
            "# HELP jaiva_processor_records_total Records processed successfully.\n\
             # TYPE jaiva_processor_records_total counter\n\
             # HELP jaiva_processor_duration_seconds Cumulative processor execution time in seconds.\n\
             # TYPE jaiva_processor_duration_seconds counter\n\
             # HELP jaiva_processor_errors_total Failed processor executions.\n\
             # TYPE jaiva_processor_errors_total counter\n",
        );
        let mut processors: Vec<_> = snapshot.processors.iter().collect();
        processors.sort_by_key(|(id, _)| *id);
        for (id, processor) in processors {
            let id = escape_label(id);
            output.push_str(&format!(
                "jaiva_processor_active_tasks{{processor=\"{id}\"}} {}\n\
                 jaiva_processor_queue_depth{{processor=\"{id}\"}} {}\n\
                 jaiva_processor_completed_total{{processor=\"{id}\"}} {}\n\
                 jaiva_processor_failed_total{{processor=\"{id}\"}} {}\n\
                 jaiva_processor_execution_milliseconds_total{{processor=\"{id}\"}} {}\n\
                 jaiva_processor_saturation_ratio{{processor=\"{id}\"}} {}\n",
                processor.active_tasks,
                processor.queue_depth,
                processor.completed,
                processor.failed,
                processor.execution_duration_ms,
                processor.saturation_ratio,
            ));
            output.push_str(&format!(
                "jaiva_processor_records_total{{flow=\"{flow}\",processor=\"{id}\"}} {}\n\
                 jaiva_processor_duration_seconds{{flow=\"{flow}\",processor=\"{id}\"}} {:.9}\n\
                 jaiva_processor_errors_total{{flow=\"{flow}\",processor=\"{id}\"}} {}\n",
                processor.records, processor.execution_duration_seconds, processor.failed,
            ));
        }
        output.push_str(
            "# HELP jaiva_queue_packets Current queued packets by flow connection.\n\
             # TYPE jaiva_queue_packets gauge\n\
             # HELP jaiva_queue_bytes Estimated queued bytes by flow connection.\n\
             # TYPE jaiva_queue_bytes gauge\n",
        );
        let mut queues: Vec<_> = snapshot.connection_queues.iter().collect();
        queues.sort_by_key(|(id, _)| *id);
        for (id, queue) in queues {
            let id = escape_label(id);
            output.push_str(&format!(
                "jaiva_queue_packets{{flow=\"{flow}\",connection=\"{id}\"}} {}\n\
                 jaiva_queue_bytes{{flow=\"{flow}\",connection=\"{id}\"}} {}\n",
                queue.packets, queue.bytes,
            ));
        }
        output.push_str(&format!(
            "# HELP jaiva_flow_status Flow lifecycle: 0 stopped, 1 starting, 2 running, 3 paused, 4 draining, 5 failed.\n\
             # TYPE jaiva_flow_status gauge\n\
             jaiva_flow_status{{flow=\"{flow}\"}} {}\n\
             # HELP jaiva_flow_last_success_timestamp Unix timestamp of the last successful flow execution.\n\
             # TYPE jaiva_flow_last_success_timestamp gauge\n\
             jaiva_flow_last_success_timestamp{{flow=\"{flow}\"}} {}\n",
            snapshot.flow_status, snapshot.flow_last_success_timestamp,
        ));
        output
    }
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_prometheus_counters_and_gauges() {
        let metrics = FlowMetrics::default();
        metrics.set_flow_id("orders");
        metrics.set_flow_status(2);
        metrics.processed();
        metrics.failed();
        metrics.retried();
        metrics.emitted(2);
        metrics.set_queue_depth(4);
        metrics.set_active_tasks(3);
        metrics.set_memory_budget(1_000);
        metrics.reserve_memory(400);
        metrics.backpressure();
        metrics.set_domain_memory(MemorySnapshot {
            hot_objects: 7,
            persist_queue: 2,
            ..MemorySnapshot::default()
        });
        metrics.set_repository(5, 2, 1, 50_000);
        metrics.recovered(3);
        metrics.database_write(100, 2, 45);
        metrics.database_write_error();
        metrics.database_rollback();
        metrics.kafka_published(7, 512);
        metrics.kafka_publish_error();
        metrics.kafka_consumed(3, 128);
        metrics.kafka_consume_error();
        metrics.circuit_rejected();
        metrics.set_circuits_open(2);
        metrics.set_worker_limits(16, 8, 4);
        metrics.set_processor_load("write", 5, 2, 4);
        metrics.processor_finished("write", Duration::from_millis(25), true);
        metrics.processor_records("write", 12);
        metrics.set_connection_queues(HashMap::from([(
            "source.success.write".to_owned(),
            (2, 512),
        )]));
        metrics.flow_succeeded();

        let output = metrics.prometheus();
        assert!(output.contains("jaiva_packets_processed_total 1"));
        assert!(output.contains("jaiva_packets_failed_total 1"));
        assert!(output.contains("jaiva_packet_retries_total 1"));
        assert!(output.contains("jaiva_packets_emitted_total 2"));
        assert!(output.contains("jaiva_queue_depth 4"));
        assert!(output.contains("jaiva_active_tasks 3"));
        assert!(output.contains("jaiva_memory_used_bytes 400"));
        assert!(output.contains("jaiba_memory_hot_objects 7"));
        assert!(output.contains("jaiba_memory_persist_queue 2"));
        assert!(output.contains("jaiva_memory_budget_bytes 1000"));
        assert!(output.contains("jaiva_backpressure_total 1"));
        assert!(output.contains("jaiva_repository_pending_packets 5"));
        assert!(output.contains("jaiva_repository_running_packets 2"));
        assert!(output.contains("jaiva_repository_dead_letter_packets 1"));
        assert!(output.contains("jaiva_repository_content_bytes 50000"));
        assert!(output.contains("jaiva_recovered_packets_total 3"));
        assert!(output.contains("jaiva_database_rows_written_total 100"));
        assert!(output.contains("jaiva_database_batches_written_total 2"));
        assert!(output.contains("jaiva_database_write_errors_total 1"));
        assert!(output.contains("jaiva_database_transaction_rollbacks_total 1"));
        assert!(output.contains("jaiva_kafka_messages_published_total 7"));
        assert!(output.contains("jaiva_kafka_bytes_published_total 512"));
        assert!(output.contains("jaiva_kafka_publish_errors_total 1"));
        assert!(output.contains("jaiva_kafka_messages_consumed_total 3"));
        assert!(output.contains("jaiva_kafka_consume_errors_total 1"));
        assert!(output.contains("jaiva_circuit_breaker_rejections_total 1"));
        assert!(output.contains("jaiva_circuit_breakers_open 2"));
        assert!(output.contains("jaiva_available_parallelism 16"));
        assert!(output.contains("jaiva_cpu_worker_limit 8"));
        assert!(output.contains("jaiva_blocking_worker_limit 4"));
        assert!(output.contains("jaiva_processor_active_tasks{processor=\"write\"} 2"));
        assert!(output.contains("jaiva_processor_queue_depth{processor=\"write\"} 5"));
        assert!(output.contains("jaiva_processor_completed_total{processor=\"write\"} 1"));
        assert!(output.contains("jaiva_processor_saturation_ratio{processor=\"write\"} 0.5"));
        assert!(
            output
                .contains("jaiva_processor_records_total{flow=\"orders\",processor=\"write\"} 12")
        );
        assert!(output.contains(
            "jaiva_processor_duration_seconds{flow=\"orders\",processor=\"write\"} 0.025000000"
        ));
        assert!(
            output.contains("jaiva_processor_errors_total{flow=\"orders\",processor=\"write\"} 0")
        );
        assert!(output.contains(
            "jaiva_queue_packets{flow=\"orders\",connection=\"source.success.write\"} 2"
        ));
        assert!(output.contains(
            "jaiva_queue_bytes{flow=\"orders\",connection=\"source.success.write\"} 512"
        ));
        assert!(output.contains("jaiva_flow_status{flow=\"orders\"} 2"));
        assert!(output.contains("jaiva_flow_last_success_timestamp{flow=\"orders\"} "));
        assert!(!output.contains("packet_id"));
    }
}

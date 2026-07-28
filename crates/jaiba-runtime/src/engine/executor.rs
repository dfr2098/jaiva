use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{sync::mpsc, task::JoinSet, time::sleep};
use tracing::{info, warn};

use crate::{
    config::{
        ConnectionConfig, ExecutionMode, FlowConfig, OrderingMode, ProcessorConfig, RetryConfig,
    },
    error::FlowError,
    processors::default_registry,
};

use super::{
    CircuitBreakers, ConnectionManager, ConnectionResolver, DataPacket, FlowControl, FlowLifecycle,
    FlowMetrics, FlowSummary, LocalPacketRepository, MemoryLimiter, MemoryReservation, OutputSender,
    PacketRepository, Processor, ProcessorContext, ProcessorEmission, ProcessorRegistry,
    ProvenanceEvent, StateStore, WorkerPools, referenced_db_aliases,
};

struct WorkItem {
    processor_id: String,
    packet: DataPacket,
    /// Stable graph edge identifier. It never contains packet data.
    connection: Option<String>,
    reservation: Option<MemoryReservation>,
    queue_id: Option<String>,
}

struct TaskCompletion {
    processor_id: String,
    partition_key: Option<String>,
    queue_id: Option<String>,
    failure: Option<(String, u32)>,
}

/// Validated executable flow.
pub struct FlowEngine {
    config: FlowConfig,
    registry: ProcessorRegistry,
    metrics: FlowMetrics,
    control: FlowControl,
    resolver: Option<Arc<dyn ConnectionResolver>>,
}

impl FlowEngine {
    /// Resolves parameters, validates the graph and creates an engine.
    pub fn new(mut config: FlowConfig) -> Result<Self, FlowError> {
        jaiba_core::FlowGraph::build(&config)
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        resolve_processor_parameters(&mut config)?;
        validate(&config)?;
        Ok(Self {
            config,
            registry: default_registry(),
            metrics: FlowMetrics::default(),
            control: FlowControl::default(),
            resolver: None,
        })
    }

    /// Replaces the built-in processor registry.
    pub fn with_registry(mut self, registry: ProcessorRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Inyecta un resolvedor para conexiones referenciadas por alias.
    pub fn with_connection_resolver(
        mut self,
        resolver: Option<Arc<dyn ConnectionResolver>>,
    ) -> Self {
        self.resolver = resolver;
        self
    }

    /// Uses a shared metrics instance, typically exposed by observability.
    pub fn with_metrics(mut self, metrics: FlowMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn with_control(mut self, control: FlowControl) -> Self {
        self.control = control;
        self
    }

    /// Executes current work until every packet reaches a terminal route.
    ///
    /// When persistence is enabled, abandoned and pending work is recovered
    /// before new source processors are scheduled.
    pub async fn run(&self) -> Result<FlowSummary, FlowError> {
        self.metrics.set_flow_id(&self.config.id);
        self.metrics.set_flow_status(1);
        self.control.starting();
        let result = self.run_inner().await;
        match &result {
            Ok(_) => {
                self.metrics.flow_succeeded();
                self.metrics.set_flow_status(0);
                self.control.stopped();
            }
            Err(error) => {
                self.metrics.set_flow_status(5);
                self.control.failed(error.to_string());
            }
        }
        result
    }

    async fn run_inner(&self) -> Result<FlowSummary, FlowError> {
        let aliases = referenced_db_aliases(&self.config);
        let connections = ConnectionManager::build(
            &self.config.database_connections,
            &self.config.kafka_connections,
            &aliases,
            self.resolver.as_ref(),
        )
        .await?;
        let circuits = CircuitBreakers::new(self.config.engine.circuit_breaker.clone())?;
        let metrics = self.metrics.clone();
        let worker_pools = WorkerPools::new(&self.config.engine.workers)?;
        let resolved_workers = worker_pools.resolved();
        metrics.set_worker_limits(
            resolved_workers.available_parallelism,
            resolved_workers.cpu_threads,
            resolved_workers.blocking_threads,
        );
        info!(
            available_parallelism = resolved_workers.available_parallelism,
            cpu_threads = resolved_workers.cpu_threads,
            blocking_threads = resolved_workers.blocking_threads,
            "worker limits resolved"
        );
        let memory = MemoryLimiter::detect(&self.config.engine.memory, metrics.clone())?;
        let state = StateStore::load(&self.config.engine.state_file)?;
        let repository = if self.config.engine.repository.enabled {
            Some(Arc::new(
                LocalPacketRepository::open(&self.config.engine.repository).await?,
            ))
        } else {
            None
        };
        let parameters = Arc::new(self.config.parameters.clone());
        let definitions: HashMap<String, ProcessorConfig> = self
            .config
            .processors
            .iter()
            .map(|definition| (definition.id.clone(), definition.clone()))
            .collect();
        let processors: HashMap<String, Arc<dyn Processor>> = definitions
            .values()
            .map(|definition| {
                self.registry
                    .build(&definition.processor_type, &definition.config)
                    .map(|processor| (definition.id.clone(), processor))
            })
            .collect::<Result<_, _>>()?;
        let downstream_depths = processor_downstream_depths(&self.config);

        let destinations: HashSet<&str> = self
            .config
            .connections
            .iter()
            .map(|connection| connection.to.as_str())
            .collect();
        let mut pending = VecDeque::new();
        if let Some(repository) = &repository {
            let recovered = repository
                .recover_abandoned(
                    &self.config.id,
                    self.config.engine.repository.abandoned_after_seconds,
                )
                .await?;
            metrics.recovered(recovered);
            sync_repository_metrics(repository, &metrics).await?;
            for stored in repository.pending(&self.config.id).await? {
                let reservation = memory.reserve(stored.packet.estimated_size()).await?;
                pending.push_back(WorkItem {
                    processor_id: stored.processor_id,
                    packet: stored.packet,
                    connection: None,
                    reservation: Some(reservation),
                    queue_id: Some(stored.queue_id),
                });
            }
        }
        if pending.is_empty() {
            pending.extend(
                self.config
                    .processors
                    .iter()
                    .filter(|processor| !destinations.contains(processor.id.as_str()))
                    .map(|processor| WorkItem {
                        processor_id: processor.id.clone(),
                        packet: DataPacket::empty(),
                        connection: None,
                        reservation: None,
                        queue_id: None,
                    }),
            );
        }
        let (emission_sender, mut emission_receiver) =
            mpsc::channel(self.config.engine.queue_capacity);
        let mut running: JoinSet<TaskCompletion> = JoinSet::new();
        let mut active_per_processor: HashMap<String, usize> = HashMap::new();
        let mut active_partitions: HashMap<String, HashSet<String>> = HashMap::new();
        let mut deferred: Option<ProcessorEmission> = None;
        let concurrency_limit = self.config.engine.max_concurrency;
        metrics.set_connection_queues(empty_connection_queues(&self.config.connections));
        metrics.set_flow_status(2);
        self.control.running();

        loop {
            match self.control.state() {
                FlowLifecycle::Draining | FlowLifecycle::Stopped if running.is_empty() => break,
                FlowLifecycle::Paused if running.is_empty() => {
                    self.control.changed().await;
                    continue;
                }
                _ => {}
            }
            if let Some(emission) = deferred.take()
                && !route_emission(
                    &emission,
                    &mut pending,
                    &self.config.connections,
                    self.config.engine.queue_capacity,
                    &metrics,
                    &definitions,
                    &active_per_processor,
                    repository.as_deref(),
                    &self.config.id,
                )
                .await?
            {
                deferred = Some(emission);
            }

            schedule_available(
                &mut pending,
                &mut running,
                &mut active_per_processor,
                &mut active_partitions,
                &definitions,
                &downstream_depths,
                &processors,
                &self.config,
                parameters.clone(),
                connections.clone(),
                circuits.clone(),
                metrics.clone(),
                state.clone(),
                emission_sender.clone(),
                concurrency_limit,
                memory.clone(),
                self.control.clone(),
                worker_pools.clone(),
                repository.as_deref(),
            )
            .await?;
            metrics.set_queue_depth(pending.len());
            metrics.set_active_tasks(running.len());
            sync_processor_metrics(&metrics, &definitions, &pending, &active_per_processor);
            sync_connection_metrics(&metrics, &self.config.connections, &pending);

            if pending.is_empty() && running.is_empty() && deferred.is_none() {
                match emission_receiver.try_recv() {
                    Ok(emission) => {
                        deferred = Some(emission);
                        continue;
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            tokio::select! {
                joined = running.join_next(), if !running.is_empty() => {
                    match joined {
                        Some(Ok(completion)) => {
                            if let Some(active) =
                                active_per_processor.get_mut(&completion.processor_id)
                            {
                                *active = active.saturating_sub(1);
                            }
                            if let Some(partition_key) = completion.partition_key.as_deref()
                                && let Some(partitions) =
                                    active_partitions.get_mut(&completion.processor_id)
                            {
                                partitions.remove(partition_key);
                            }
                            if let (Some(repository), Some(queue_id)) =
                                (&repository, completion.queue_id.as_deref())
                            {
                                if let Some((error, attempt)) = &completion.failure {
                                    repository.fail(queue_id, error, *attempt).await?;
                                } else {
                                    repository.complete(queue_id, ProvenanceEvent::Completed).await?;
                                }
                                sync_repository_metrics(repository, &metrics).await?;
                            }
                        }
                        Some(Err(error)) => {
                            return Err(FlowError::Server(format!(
                                "processor task failed: {error}"
                            )));
                        }
                        None => {}
                    }
                }
                emission = emission_receiver.recv(), if deferred.is_none() => {
                    if let Some(emission) = emission
                        && !route_emission(
                            &emission,
                            &mut pending,
                            &self.config.connections,
                            self.config.engine.queue_capacity,
                            &metrics,
                            &definitions,
                            &active_per_processor,
                            repository.as_deref(),
                            &self.config.id,
                        )
                        .await?
                    {
                        deferred = Some(emission);
                    }
                }
            }
        }

        metrics.set_queue_depth(0);
        metrics.set_active_tasks(0);
        metrics.set_connection_queues(empty_connection_queues(&self.config.connections));
        if let Some(repository) = &repository {
            repository
                .cleanup_completed(self.config.engine.repository.completed_retention_hours)
                .await?;
            repository
                .cleanup_provenance(self.config.engine.repository.provenance_retention_hours)
                .await?;
            sync_repository_metrics(repository, &metrics).await?;
        }
        Ok(metrics.summary())
    }
}

#[allow(clippy::too_many_arguments)]
async fn schedule_available(
    pending: &mut VecDeque<WorkItem>,
    running: &mut JoinSet<TaskCompletion>,
    active_per_processor: &mut HashMap<String, usize>,
    active_partitions: &mut HashMap<String, HashSet<String>>,
    definitions: &HashMap<String, ProcessorConfig>,
    downstream_depths: &HashMap<String, usize>,
    processors: &HashMap<String, Arc<dyn Processor>>,
    config: &FlowConfig,
    parameters: Arc<HashMap<String, String>>,
    connections: ConnectionManager,
    circuits: CircuitBreakers,
    metrics: FlowMetrics,
    state: StateStore,
    emission_sender: mpsc::Sender<ProcessorEmission>,
    concurrency_limit: usize,
    memory: MemoryLimiter,
    control: FlowControl,
    worker_pools: WorkerPools,
    repository: Option<&LocalPacketRepository>,
) -> Result<(), FlowError> {
    if control.state() != FlowLifecycle::Running {
        return Ok(());
    }
    let mut inspected = 0;
    while running.len() < concurrency_limit && inspected < pending.len() {
        let item = pending.pop_front().expect("pending item");
        let definition = definitions.get(&item.processor_id).expect("validated");
        let active = active_per_processor
            .get(&item.processor_id)
            .copied()
            .unwrap_or_default();
        let reserved_downstream_slots = downstream_depths
            .get(&item.processor_id)
            .copied()
            .unwrap_or_default();
        if running
            .len()
            .saturating_add(1)
            .saturating_add(reserved_downstream_slots)
            > concurrency_limit
        {
            pending.push_back(item);
            inspected += 1;
            continue;
        }
        let processor_limit = match definition.scheduling.ordering {
            OrderingMode::Preserve => 1,
            OrderingMode::Unordered | OrderingMode::Partitioned => {
                definition.scheduling.concurrent_tasks
            }
        };

        if active >= processor_limit {
            pending.push_back(item);
            inspected += 1;
            continue;
        }
        let partition_key = match definition.scheduling.ordering {
            OrderingMode::Partitioned => Some(packet_partition_key(
                &item.packet,
                definition
                    .scheduling
                    .partition_by
                    .as_deref()
                    .expect("validated partition selector"),
                &item.processor_id,
            )?),
            OrderingMode::Unordered | OrderingMode::Preserve => None,
        };
        if let Some(key) = partition_key.as_deref()
            && active_partitions
                .get(&item.processor_id)
                .is_some_and(|active| active.contains(key))
        {
            pending.push_back(item);
            inspected += 1;
            continue;
        }

        if let (Some(repository), Some(queue_id)) = (repository, item.queue_id.as_deref())
            && !repository.claim(queue_id).await?
        {
            continue;
        }
        if let Some(repository) = repository {
            sync_repository_metrics(repository, &metrics).await?;
        }

        inspected = 0;
        *active_per_processor
            .entry(item.processor_id.clone())
            .or_default() += 1;
        if let Some(key) = partition_key.as_ref() {
            active_partitions
                .entry(item.processor_id.clone())
                .or_default()
                .insert(key.clone());
        }
        let processor = processors.get(&item.processor_id).expect("built").clone();
        let processor_id = item.processor_id.clone();
        let retry = definition.retry.clone();
        let timeout_ms = definition.scheduling.timeout_ms;
        let context = ProcessorContext {
            flow_id: config.id.clone(),
            processor_id: processor_id.clone(),
            parameters: parameters.clone(),
            connections: connections.clone(),
            metrics: metrics.clone(),
            state: state.clone(),
            circuits: circuits.clone(),
        };
        let output = OutputSender::new(
            emission_sender.clone(),
            processor_id.clone(),
            memory.clone(),
            metrics.clone(),
        );
        let queue_id = item.queue_id.clone();
        let provenance_repository = repository.cloned();
        let execution_mode = match definition.scheduling.execution_mode {
            ExecutionMode::Auto => processor.execution_mode(),
            configured => configured,
        };
        let task_workers = worker_pools.clone();
        running.spawn(async move {
            let _input_reservation = item.reservation;
            let worker_permit = task_workers.acquire(execution_mode).await;
            let execution = execute_with_retry(
                processor,
                item.packet,
                context,
                retry,
                timeout_ms,
                output,
                provenance_repository,
                queue_id.clone(),
            );
            let failure = match worker_permit {
                Err(error) => Some((error.to_string(), 0)),
                Ok(None) => execution.await,
                Ok(Some(permit)) => {
                    let runtime = tokio::runtime::Handle::current();
                    match tokio::task::spawn_blocking(move || {
                        let _permit = permit;
                        runtime.block_on(execution)
                    })
                    .await
                    {
                        Ok(failure) => failure,
                        Err(error) => Some((format!("worker task failed: {error}"), 0)),
                    }
                }
            };
            TaskCompletion {
                processor_id,
                partition_key,
                queue_id,
                failure,
            }
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_with_retry(
    processor: Arc<dyn Processor>,
    mut packet: DataPacket,
    context: ProcessorContext,
    retry: RetryConfig,
    timeout_ms: Option<u64>,
    output: OutputSender,
    repository: Option<LocalPacketRepository>,
    queue_id: Option<String>,
) -> Option<(String, u32)> {
    loop {
        let started = Instant::now();
        let input_records = packet
            .records()
            .map(|records| records.len() as u64)
            .unwrap_or(1);
        if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
            let _ = repository
                .record_event(
                    queue_id,
                    ProvenanceEvent::ProcessingStarted,
                    serde_json::json!({
                        "attempt": packet.attempt,
                        "input_bytes": packet.estimated_size()
                    }),
                )
                .await;
        }
        info!(
            flow_id = %context.flow_id,
            processor_id = %context.processor_id,
            packet_id = %packet.id,
            attempt = packet.attempt,
            "executing processor"
        );

        let execution = processor.execute(packet.clone(), &context, &output);
        let outcome = match timeout_ms {
            Some(milliseconds) => {
                tokio::time::timeout(Duration::from_millis(milliseconds), execution)
                    .await
                    .map_err(|_| FlowError::Processor {
                        processor_id: context.processor_id.clone(),
                        message: format!("execution exceeded timeout of {milliseconds} ms"),
                    })
                    .and_then(|result| result)
            }
            None => execution.await,
        };

        match outcome {
            Ok(()) => {
                if output.emitted_records() == 0 {
                    context
                        .metrics
                        .processor_records(&context.processor_id, input_records);
                }
                context.metrics.processed();
                context
                    .metrics
                    .processor_finished(&context.processor_id, started.elapsed(), true);
                if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
                    let _ = repository
                        .record_event(
                            queue_id,
                            ProvenanceEvent::Processed,
                            serde_json::json!({
                                "attempt": packet.attempt,
                                "duration_ms": started.elapsed().as_millis()
                            }),
                        )
                        .await;
                }
                return None;
            }
            Err(error) if packet.attempt < retry.maximum_attempts => {
                packet.attempt += 1;
                context.metrics.retried();
                let multiplier = 2_u64.saturating_pow(packet.attempt.saturating_sub(1));
                let delay = retry
                    .initial_delay_ms
                    .saturating_mul(multiplier)
                    .min(retry.maximum_delay_ms);
                warn!(
                    processor_id = %context.processor_id,
                    packet_id = %packet.id,
                    attempt = packet.attempt,
                    delay_ms = delay,
                    error = %error,
                    "retrying processor"
                );
                if let (Some(repository), Some(queue_id)) = (&repository, queue_id.as_deref()) {
                    let _ = repository
                        .record_event(
                            queue_id,
                            ProvenanceEvent::Retried,
                            serde_json::json!({
                                "attempt": packet.attempt,
                                "duration_ms": started.elapsed().as_millis(),
                                "delay_ms": delay,
                                "error": error.to_string()
                            }),
                        )
                        .await;
                }
                sleep(Duration::from_millis(delay)).await;
            }
            Err(error) => {
                context.metrics.failed();
                context
                    .metrics
                    .processor_finished(&context.processor_id, started.elapsed(), false);
                let error_message = error.to_string();
                packet
                    .attributes
                    .insert("error.processor".to_owned(), context.processor_id.clone());
                packet
                    .attributes
                    .insert("error.message".to_owned(), error_message.clone());
                let attempt = packet.attempt;
                if let Err(send_error) = output.emit("failure", packet).await {
                    warn!(
                        processor_id = %context.processor_id,
                        error = %send_error,
                        "could not route failed packet"
                    );
                }
                return Some((error_message, attempt));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn route_emission(
    emission: &ProcessorEmission,
    pending: &mut VecDeque<WorkItem>,
    connections: &[ConnectionConfig],
    global_capacity: usize,
    metrics: &FlowMetrics,
    definitions: &HashMap<String, ProcessorConfig>,
    active_per_processor: &HashMap<String, usize>,
    repository: Option<&LocalPacketRepository>,
    flow_id: &str,
) -> Result<bool, FlowError> {
    let next = outgoing(connections, &emission.processor_id, &emission.relationship);
    if next.is_empty() {
        warn!(
            processor_id = emission.processor_id,
            relationship = emission.relationship,
            "packet reached the end of the flow"
        );
        return Ok(true);
    }

    if pending.len().saturating_add(next.len()) > global_capacity {
        return Ok(false);
    }
    for connection in &next {
        let edge_size = pending
            .iter()
            .filter(|item| item.connection.as_deref() == Some(connection_id(connection).as_str()))
            .count();
        if edge_size >= connection.queue.capacity {
            return Ok(false);
        }
        if let Some(maximum) = definitions
            .get(&connection.to)
            .and_then(|definition| definition.scheduling.maximum_in_flight)
        {
            let queued = pending
                .iter()
                .filter(|item| item.processor_id == connection.to)
                .count();
            let active = active_per_processor
                .get(&connection.to)
                .copied()
                .unwrap_or_default();
            let incoming = next
                .iter()
                .filter(|candidate| candidate.to == connection.to)
                .count();
            if queued.saturating_add(active).saturating_add(incoming) > maximum {
                return Ok(false);
            }
        }
    }

    for connection in next {
        let queue_id = if let Some(repository) = repository {
            let queue_id = repository
                .enqueue(
                    flow_id,
                    &connection.to,
                    &emission.relationship,
                    &emission.packet,
                )
                .await?;
            repository
                .record_event(
                    &queue_id,
                    ProvenanceEvent::Routed,
                    serde_json::json!({
                        "source_processor": connection.from,
                        "destination_processor": connection.to,
                        "relationship": emission.relationship,
                        "packet_bytes": emission.packet.estimated_size()
                    }),
                )
                .await?;
            Some(queue_id)
        } else {
            None
        };
        pending.push_back(WorkItem {
            processor_id: connection.to.clone(),
            packet: emission.packet.clone(),
            connection: Some(connection_id(connection)),
            reservation: Some(emission.reservation.clone()),
            queue_id,
        });
        metrics.emitted(1);
    }
    if let Some(repository) = repository {
        sync_repository_metrics(repository, metrics).await?;
    }
    Ok(true)
}

async fn sync_repository_metrics(
    repository: &LocalPacketRepository,
    metrics: &FlowMetrics,
) -> Result<(), FlowError> {
    let stats = repository.stats().await?;
    metrics.set_repository(
        stats.pending,
        stats.running,
        stats.dead_letter,
        stats.content_bytes,
    );
    Ok(())
}

fn sync_processor_metrics(
    metrics: &FlowMetrics,
    definitions: &HashMap<String, ProcessorConfig>,
    pending: &VecDeque<WorkItem>,
    active: &HashMap<String, usize>,
) {
    for (id, definition) in definitions {
        let queue_depth = pending
            .iter()
            .filter(|item| item.processor_id == *id)
            .count();
        let concurrency_limit = match definition.scheduling.ordering {
            OrderingMode::Preserve => 1,
            OrderingMode::Unordered | OrderingMode::Partitioned => {
                definition.scheduling.concurrent_tasks
            }
        };
        metrics.set_processor_load(
            id,
            queue_depth,
            active.get(id).copied().unwrap_or_default(),
            concurrency_limit,
        );
    }
}

fn empty_connection_queues(connections: &[ConnectionConfig]) -> HashMap<String, (u64, u64)> {
    connections
        .iter()
        .map(|connection| (connection_id(connection), (0, 0)))
        .collect()
}

fn sync_connection_metrics(
    metrics: &FlowMetrics,
    connections: &[ConnectionConfig],
    pending: &VecDeque<WorkItem>,
) {
    let mut queues = empty_connection_queues(connections);
    for item in pending {
        let Some(connection) = item.connection.as_ref() else {
            continue;
        };
        let queue = queues.entry(connection.clone()).or_default();
        queue.0 = queue.0.saturating_add(1);
        queue.1 = queue.1.saturating_add(item.packet.estimated_size() as u64);
    }
    metrics.set_connection_queues(queues);
}

fn connection_id(connection: &ConnectionConfig) -> String {
    format!(
        "{}.{}.{}",
        connection.from, connection.relationship, connection.to
    )
}

fn packet_partition_key(
    packet: &DataPacket,
    selector: &str,
    processor_id: &str,
) -> Result<String, FlowError> {
    let attribute = selector.strip_prefix("attribute.").unwrap_or(selector);
    if let Some(value) = packet.attributes.get(attribute) {
        return Ok(value.clone());
    }
    let records = packet.records().map_err(|message| FlowError::Processor {
        processor_id: processor_id.to_owned(),
        message: format!("cannot resolve partition '{selector}': {message}"),
    })?;
    let mut values = records.iter().map(|record| {
        record
            .get(selector)
            .map(canonical_partition_value)
            .ok_or_else(|| FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!("partition field '{selector}' is missing"),
            })
    });
    let first = values
        .next()
        .transpose()?
        .ok_or_else(|| FlowError::Processor {
            processor_id: processor_id.to_owned(),
            message: format!("cannot partition an empty packet by '{selector}'"),
        })?;
    for value in values {
        if value? != first {
            return Err(FlowError::Processor {
                processor_id: processor_id.to_owned(),
                message: format!(
                    "packet contains multiple values for partition field '{selector}'; split it before this processor"
                ),
            });
        }
    }
    Ok(first)
}

fn canonical_partition_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn processor_downstream_depths(config: &FlowConfig) -> HashMap<String, usize> {
    fn visit(
        processor: &str,
        connections: &[ConnectionConfig],
        memo: &mut HashMap<String, usize>,
        visiting: &mut HashSet<String>,
    ) -> usize {
        if let Some(depth) = memo.get(processor) {
            return *depth;
        }
        if !visiting.insert(processor.to_owned()) {
            return 0;
        }
        let depth = connections
            .iter()
            .filter(|connection| connection.from == processor)
            .map(|connection| {
                1_usize.saturating_add(visit(&connection.to, connections, memo, visiting))
            })
            .max()
            .unwrap_or_default();
        visiting.remove(processor);
        memo.insert(processor.to_owned(), depth);
        depth
    }

    let mut depths = HashMap::new();
    for processor in &config.processors {
        visit(
            &processor.id,
            &config.connections,
            &mut depths,
            &mut HashSet::new(),
        );
    }
    depths
}

fn outgoing<'a>(
    connections: &'a [ConnectionConfig],
    processor_id: &str,
    relationship: &str,
) -> Vec<&'a ConnectionConfig> {
    connections
        .iter()
        .filter(|connection| {
            connection.from == processor_id && connection.relationship == relationship
        })
        .collect()
}

fn resolve_processor_parameters(config: &mut FlowConfig) -> Result<(), FlowError> {
    for processor in &mut config.processors {
        interpolate_value(&mut processor.config, &config.parameters)?;
    }
    Ok(())
}

fn interpolate_value(
    value: &mut Value,
    parameters: &HashMap<String, String>,
) -> Result<(), FlowError> {
    match value {
        Value::String(text) => {
            for (name, replacement) in parameters {
                *text = text.replace(&format!("${{{name}}}"), replacement);
            }
            resolve_environment_placeholders(text)?;
            if text.contains("${") {
                return Err(FlowError::Configuration(format!(
                    "unresolved parameter in '{text}'"
                )));
            }
        }
        Value::Array(items) => {
            for item in items {
                interpolate_value(item, parameters)?;
            }
        }
        Value::Object(object) => {
            for item in object.values_mut() {
                interpolate_value(item, parameters)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_environment_placeholders(text: &mut String) -> Result<(), FlowError> {
    while let Some(start) = text.find("${env:") {
        let name_start = start + "${env:".len();
        let relative_end = text[name_start..].find('}').ok_or_else(|| {
            FlowError::Configuration(format!("invalid environment placeholder in '{text}'"))
        })?;
        let end = name_start + relative_end;
        let name = &text[name_start..end];
        let value = std::env::var(name).map_err(|_| {
            FlowError::Configuration(format!(
                "environment variable '{name}' required by processor configuration is missing"
            ))
        })?;
        text.replace_range(start..=end, &value);
    }
    Ok(())
}

fn validate(config: &FlowConfig) -> Result<(), FlowError> {
    if config.processors.is_empty() {
        return Err(FlowError::Configuration(
            "the flow must contain at least one processor".to_owned(),
        ));
    }
    if config.engine.queue_capacity == 0 || config.engine.max_concurrency == 0 {
        return Err(FlowError::Configuration(
            "engine capacities must be greater than zero".to_owned(),
        ));
    }
    if config.engine.shutdown.drain_timeout_seconds == 0 {
        return Err(FlowError::Configuration(
            "shutdown drain_timeout_seconds must be greater than zero".to_owned(),
        ));
    }
    if config.engine.admin.max_request_body_bytes == 0 {
        return Err(FlowError::Configuration(
            "admin max_request_body_bytes must be greater than zero".to_owned(),
        ));
    }
    if config.engine.admin.enabled
        && config.engine.admin.authentication == crate::config::AdminAuthentication::Bearer
        && config.engine.admin.token_env.trim().is_empty()
    {
        return Err(FlowError::Configuration(
            "admin token_env cannot be empty when the administrative API is enabled".to_owned(),
        ));
    }
    if config.engine.workers.cpu_threads > 1024 || config.engine.workers.blocking_threads > 1024 {
        return Err(FlowError::Configuration(
            "worker thread limits cannot exceed 1024".to_owned(),
        ));
    }

    let mut ids = HashSet::new();
    for processor in &config.processors {
        if processor.simulation.mode != crate::config::DataExecutionMode::Real {
            return Err(FlowError::Configuration(format!(
                "processor '{}' uses {:?} data mode; execute it through jaiba-simulator",
                processor.id, processor.simulation.mode
            )));
        }
        if processor.scheduling.concurrent_tasks == 0 {
            return Err(FlowError::Configuration(format!(
                "processor '{}' must allow at least one concurrent task",
                processor.id
            )));
        }
        if processor.scheduling.maximum_in_flight == Some(0) {
            return Err(FlowError::Configuration(format!(
                "processor '{}' maximum_in_flight must be greater than zero",
                processor.id
            )));
        }
        if processor
            .scheduling
            .maximum_in_flight
            .is_some_and(|maximum| maximum < processor.scheduling.concurrent_tasks)
        {
            return Err(FlowError::Configuration(format!(
                "processor '{}' maximum_in_flight cannot be lower than concurrent_tasks",
                processor.id
            )));
        }
        if processor.scheduling.ordering == OrderingMode::Partitioned
            && processor
                .scheduling
                .partition_by
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err(FlowError::Configuration(format!(
                "processor '{}' uses partitioned ordering but has no partition_by selector",
                processor.id
            )));
        }
        if !ids.insert(processor.id.as_str()) {
            return Err(FlowError::Configuration(format!(
                "duplicate processor id '{}'",
                processor.id
            )));
        }
    }

    for connection in &config.connections {
        if connection.queue.capacity == 0 {
            return Err(FlowError::Configuration(
                "connection queue capacity must be greater than zero".to_owned(),
            ));
        }
        if !ids.contains(connection.from.as_str()) || !ids.contains(connection.to.as_str()) {
            return Err(FlowError::Configuration(format!(
                "connection '{} -> {}' references an unknown processor",
                connection.from, connection.to
            )));
        }
    }

    let incoming: HashSet<&str> = config
        .connections
        .iter()
        .map(|connection| connection.to.as_str())
        .collect();
    if config
        .processors
        .iter()
        .all(|processor| incoming.contains(processor.id.as_str()))
    {
        return Err(FlowError::Configuration(
            "the flow has no starting processor; check for cycles".to_owned(),
        ));
    }
    let required_pipeline_slots = processor_downstream_depths(config)
        .values()
        .copied()
        .max()
        .unwrap_or_default()
        .saturating_add(1);
    if config.engine.max_concurrency < required_pipeline_slots {
        return Err(FlowError::Configuration(format!(
            "engine max_concurrency must be at least {required_pipeline_slots} for the longest streaming path"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SlowSource;
    struct Sink;
    struct BurstSource;
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
}

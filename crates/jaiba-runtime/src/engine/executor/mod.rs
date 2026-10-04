use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::{sync::mpsc, task::JoinSet};
use tracing::info;

use crate::{
    config::{FlowConfig, ProcessorConfig},
    error::FlowError,
    processors::default_registry,
};

use super::{
    CircuitBreakers, ConnectionManager, ConnectionResolver, DataPacket, FlowControl, FlowLifecycle,
    FlowMetrics, FlowSummary, LocalPacketRepository, MemoryLimiter, MemoryReservation,
    PacketRepository, Processor, ProcessorEmission, ProcessorRegistry, ProvenanceEvent, StateStore,
    WorkerPools, open_domain_memory, referenced_db_aliases,
};

mod metrics_sync;
mod partition;
mod retry;
mod routing;
mod scheduler;
#[cfg(test)]
mod tests;
mod validation;

use metrics_sync::{
    empty_connection_queues, sync_connection_metrics, sync_processor_metrics,
    sync_repository_metrics,
};
use routing::route_emission;
use scheduler::schedule_available;
use validation::{processor_downstream_depths, resolve_processor_parameters, validate};

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
    fatal: Option<FlowError>,
}

/// Maximum time a deferred emission may wait for packet memory while no task completes.
const MEMORY_STALL_TIMEOUT: Duration = Duration::from_secs(30);

enum RouteOutcome {
    Routed,
    QueueFull,
    MemoryFull(FlowError),
}

struct MemoryStall {
    error: FlowError,
    deadline: tokio::time::Instant,
}

fn hold_unrouted(
    outcome: RouteOutcome,
    emission: ProcessorEmission,
    deferred: &mut Option<ProcessorEmission>,
    memory_stall: &mut Option<MemoryStall>,
) {
    match outcome {
        RouteOutcome::Routed => *memory_stall = None,
        RouteOutcome::QueueFull => {
            *memory_stall = None;
            *deferred = Some(emission);
        }
        RouteOutcome::MemoryFull(error) => {
            memory_stall.get_or_insert_with(|| MemoryStall {
                error,
                deadline: tokio::time::Instant::now() + MEMORY_STALL_TIMEOUT,
            });
            *deferred = Some(emission);
        }
    }
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
        self.run_started(None).await
    }

    pub(crate) async fn run_started(
        &self,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<FlowSummary, FlowError> {
        self.metrics.set_flow_id(&self.config.id);
        self.metrics.set_flow_status(1);
        self.control.starting();
        let result = self.run_inner(started).await;
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

    async fn run_inner(
        &self,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<FlowSummary, FlowError> {
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
        let domain_memory = open_domain_memory(
            &self.config.engine.domain_memory,
            &self.config.id,
            metrics.clone(),
        )?;
        let mut memory = MemoryLimiter::detect(&self.config.engine.memory, metrics.clone())?;
        if let Some(handle) = domain_memory.clone() {
            memory = memory.with_domain_memory(handle);
        }
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
                let reservation = memory.try_reserve(stored.packet.estimated_size())?;
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
        let mut memory_stall: Option<MemoryStall> = None;
        let concurrency_limit = self.config.engine.max_concurrency;
        metrics.set_connection_queues(empty_connection_queues(&self.config.connections));
        metrics.set_flow_status(2);
        self.control.running();
        if let Some(started) = started {
            let _ = started.send(());
        }
        let mut domain_memory_tick = tokio::time::interval(std::time::Duration::from_millis(250));
        domain_memory_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            match self.control.state() {
                FlowLifecycle::Draining | FlowLifecycle::Stopped if running.is_empty() => break,
                FlowLifecycle::Paused if running.is_empty() => {
                    self.control.changed().await;
                    continue;
                }
                _ => {}
            }
            if let Some(emission) = deferred.take() {
                let outcome = route_emission(
                    &emission,
                    &mut pending,
                    &self.config.connections,
                    self.config.engine.queue_capacity,
                    &metrics,
                    &definitions,
                    &active_per_processor,
                    repository.as_deref(),
                    &self.config.id,
                    &memory,
                )
                .await?;
                hold_unrouted(outcome, emission, &mut deferred, &mut memory_stall);
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
                domain_memory.clone(),
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
            if running.is_empty()
                && let Some(stall) = memory_stall.take()
            {
                return Err(stall.error);
            }

            tokio::select! {
                _ = domain_memory_tick.tick(), if domain_memory.is_some() => {
                    if let Some(jme) = &domain_memory {
                        jme.maintain()?;
                    }
                }
                joined = running.join_next(), if !running.is_empty() => {
                    match joined {
                        Some(Ok(completion)) => {
                            if let Some(error) = completion.fatal { return Err(error); }
                            memory_stall = None;
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
                    if let Some(emission) = emission {
                        let outcome = route_emission(
                            &emission,
                            &mut pending,
                            &self.config.connections,
                            self.config.engine.queue_capacity,
                            &metrics,
                            &definitions,
                            &active_per_processor,
                            repository.as_deref(),
                            &self.config.id,
                            &memory,
                        )
                        .await?;
                        hold_unrouted(outcome, emission, &mut deferred, &mut memory_stall);
                    }
                }
                _ = tokio::time::sleep_until(
                    memory_stall
                        .as_ref()
                        .map_or_else(tokio::time::Instant::now, |stall| stall.deadline),
                ), if memory_stall.is_some() => {
                    if let Some(stall) = memory_stall.take() {
                        return Err(stall.error);
                    }
                }
                else => {
                    return Err(FlowError::Server(
                        "flow deadlocked: deferred emission exceeds engine.queue_capacity"
                            .to_owned(),
                    ));
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

fn is_source_seed(item: &WorkItem) -> bool {
    item.connection.is_none() && item.queue_id.is_none()
}

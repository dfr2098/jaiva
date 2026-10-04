//! Decide qué trabajo pendiente arranca en cada vuelta del bucle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use tokio::{sync::mpsc, task::JoinSet};

use crate::{
    config::{ExecutionMode, FlowConfig, OrderingMode, ProcessorConfig},
    engine::{
        CircuitBreakers, ConnectionManager, DomainMemoryHandle, FlowControl, FlowLifecycle,
        FlowMetrics, LocalPacketRepository, MemoryLimiter, OutputSender, PacketRepository,
        Processor, ProcessorContext, ProcessorEmission, StateStore, WorkerPools,
    },
    error::FlowError,
};

use super::{
    TaskCompletion, WorkItem, is_source_seed, metrics_sync::sync_repository_metrics,
    partition::packet_partition_key, retry::execute_with_retry,
};

/// Lanza tareas mientras haya cupo, respetando, en este orden:
///
/// - semillas de fuente solo si su salida cabe en `engine.queue_capacity`;
/// - `max_concurrency` menos los huecos reservados para las etapas siguientes;
/// - `concurrent_tasks` por processor (`ordering: preserve` = 1);
/// - una tarea a la vez por clave de partición.
///
/// Lo que no cabe vuelve al final de `pending`. Con repositorio, cada paquete
/// se reclama antes de ejecutarse.
#[allow(clippy::too_many_arguments)]
pub(super) async fn schedule_available(
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
    domain_memory: Option<DomainMemoryHandle>,
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
    // Source seeds only start while their output fits: routed work, unread
    // emissions and running tasks must stay below the queue capacity, or the
    // downstream stages cannot emit and the router deadlocks.
    let routed_backlog = pending
        .iter()
        .filter(|item| !is_source_seed(item))
        .count()
        .saturating_add(emission_sender.max_capacity() - emission_sender.capacity());
    let mut inspected = 0;
    while running.len() < concurrency_limit && inspected < pending.len() {
        let item = pending.pop_front().expect("pending item");
        if is_source_seed(&item)
            && routed_backlog.saturating_add(running.len()) >= config.engine.queue_capacity
        {
            pending.push_back(item);
            inspected += 1;
            continue;
        }
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
            domain_memory: domain_memory.clone(),
        };
        let routed_relationships: HashSet<String> = config
            .connections
            .iter()
            .filter(|connection| connection.from == processor_id)
            .map(|connection| connection.relationship.clone())
            .collect();
        let output = OutputSender::new(
            emission_sender.clone(),
            processor_id.clone(),
            memory.clone(),
            metrics.clone(),
        )
        .with_routed_relationships(routed_relationships)
        .with_input_reservation(item.reservation.is_some());
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
            let outcome = match worker_permit {
                Err(error) => Err(error),
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
                        Err(error) => {
                            Err(FlowError::Server(format!("worker task failed: {error}")))
                        }
                    }
                }
            };
            let (failure, fatal) = match outcome {
                Ok(failure) => (failure, None),
                Err(error) => (None, Some(error)),
            };
            TaskCompletion {
                processor_id,
                partition_key,
                queue_id,
                failure,
                fatal,
            }
        });
    }
    Ok(())
}

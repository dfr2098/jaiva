use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    config::{ExecutionMode, ResolvedWorkerConfig, WorkerConfig},
    error::FlowError,
};

/// Bounded permits for CPU-heavy and blocking work.
///
/// Tokio continues to run network and timer futures. Only work explicitly
/// classified as `cpu` or `blocking_io` enters the blocking executor, guarded
/// by these independent limits.
#[derive(Debug, Clone)]
pub struct WorkerPools {
    resolved: ResolvedWorkerConfig,
    cpu: Arc<Semaphore>,
    blocking: Arc<Semaphore>,
}

impl WorkerPools {
    pub fn new(config: &WorkerConfig) -> Result<Self, FlowError> {
        let resolved = config.resolved();
        if resolved.cpu_threads == 0 || resolved.blocking_threads == 0 {
            return Err(FlowError::Configuration(
                "resolved worker limits must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            resolved,
            cpu: Arc::new(Semaphore::new(resolved.cpu_threads)),
            blocking: Arc::new(Semaphore::new(resolved.blocking_threads)),
        })
    }

    pub fn resolved(&self) -> ResolvedWorkerConfig {
        self.resolved
    }

    pub async fn acquire(
        &self,
        mode: ExecutionMode,
    ) -> Result<Option<OwnedSemaphorePermit>, FlowError> {
        let semaphore = match mode {
            ExecutionMode::Cpu => &self.cpu,
            ExecutionMode::BlockingIo => &self.blocking,
            ExecutionMode::Auto | ExecutionMode::AsyncIo => return Ok(None),
        };
        semaphore
            .clone()
            .acquire_owned()
            .await
            .map(Some)
            .map_err(|_| FlowError::Server("worker pool was closed".to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cpu_pool_enforces_its_limit() {
        let pools = WorkerPools::new(&WorkerConfig {
            cpu_threads: 1,
            blocking_threads: 1,
        })
        .unwrap();
        let first = pools.acquire(ExecutionMode::Cpu).await.unwrap();
        let waiting_pools = pools.clone();
        let waiting =
            tokio::spawn(async move { waiting_pools.acquire(ExecutionMode::Cpu).await.unwrap() });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        assert!(waiting.await.unwrap().is_some());
    }
}

use std::{fs, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{config::MemoryConfig, error::FlowError};

use super::FlowMetrics;

const UNIT_BYTES: u64 = 64 * 1024;

/// Shared byte budget for packets in streaming execution.
///
/// Detection respects physical RAM and cgroup limits.
#[derive(Clone, Debug)]
pub struct MemoryLimiter {
    semaphore: Arc<Semaphore>,
    total_units: u32,
    budget_bytes: u64,
    metrics: FlowMetrics,
}

/// RAII reservation released when its final clone is dropped.
#[derive(Clone, Debug)]
pub struct MemoryReservation {
    _inner: Arc<ReservationInner>,
}

#[derive(Debug)]
struct ReservationInner {
    _permit: OwnedSemaphorePermit,
    reserved_bytes: u64,
    metrics: FlowMetrics,
}

impl Drop for ReservationInner {
    fn drop(&mut self) {
        self.metrics.release_memory(self.reserved_bytes);
    }
}

impl MemoryLimiter {
    /// Detects available memory and applies the configured percentage.
    pub fn detect(config: &MemoryConfig, metrics: FlowMetrics) -> Result<Self, FlowError> {
        if !(1..=90).contains(&config.maximum_percent) {
            return Err(FlowError::Configuration(
                "memory maximum_percent must be between 1 and 90".to_owned(),
            ));
        }
        let available = detected_memory_limit()?;
        let budget_bytes = available.saturating_mul(config.maximum_percent as u64) / 100;
        Ok(Self::from_budget(budget_bytes, metrics))
    }

    fn from_budget(budget_bytes: u64, metrics: FlowMetrics) -> Self {
        let total_units_u64 = budget_bytes.div_ceil(UNIT_BYTES).max(1);
        let total_units = total_units_u64.min(u32::MAX as u64) as u32;
        metrics.set_memory_budget(budget_bytes);

        Self {
            semaphore: Arc::new(Semaphore::new(total_units as usize)),
            total_units,
            budget_bytes,
            metrics,
        }
    }

    /// Waits until the requested estimated bytes are available.
    pub async fn reserve(&self, bytes: usize) -> Result<MemoryReservation, FlowError> {
        let reserved_bytes = (bytes as u64).max(1);
        if reserved_bytes > self.budget_bytes {
            return Err(FlowError::PacketTooLarge {
                packet_bytes: reserved_bytes,
                budget_bytes: self.budget_bytes,
            });
        }
        let units = reserved_bytes.div_ceil(UNIT_BYTES) as u32;
        let permit = match self.semaphore.clone().try_acquire_many_owned(units) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.metrics.backpressure();
                self.semaphore
                    .clone()
                    .acquire_many_owned(units)
                    .await
                    .map_err(|_| FlowError::ChannelClosed)?
            }
            Err(tokio::sync::TryAcquireError::Closed) => return Err(FlowError::ChannelClosed),
        };
        self.metrics.reserve_memory(reserved_bytes);
        Ok(MemoryReservation {
            _inner: Arc::new(ReservationInner {
                _permit: permit,
                reserved_bytes,
                metrics: self.metrics.clone(),
            }),
        })
    }

    /// Returns the configured budget in bytes.
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Returns the internal count of 64-KiB semaphore units.
    pub fn total_units(&self) -> u32 {
        self.total_units
    }
}

fn detected_memory_limit() -> Result<u64, FlowError> {
    let physical = read_mem_total()?;
    let cgroup = read_cgroup_limit();
    Ok(cgroup.map_or(physical, |limit| limit.min(physical)))
}

fn read_mem_total() -> Result<u64, FlowError> {
    let contents = fs::read_to_string("/proc/meminfo")?;
    let kilobytes = contents
        .lines()
        .find_map(|line| {
            line.strip_prefix("MemTotal:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .ok_or_else(|| FlowError::Configuration("cannot detect system memory".to_owned()))?;
    Ok(kilobytes.saturating_mul(1024))
}

fn read_cgroup_limit() -> Option<u64> {
    let value = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = value.trim();
    if trimmed == "max" {
        None
    } else {
        trimmed.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn waits_until_reserved_memory_is_released() {
        let metrics = FlowMetrics::default();
        let limiter = MemoryLimiter::from_budget(UNIT_BYTES, metrics.clone());
        let reservation = limiter.reserve(UNIT_BYTES as usize).await.unwrap();

        let waiting_limiter = limiter.clone();
        let waiting =
            tokio::spawn(async move { waiting_limiter.reserve(UNIT_BYTES as usize).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiting.is_finished());
        assert_eq!(metrics.summary().backpressure_total, 1);

        drop(reservation);
        let second = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("reservation should resume")
            .expect("task should complete")
            .expect("reservation should succeed");
        drop(second);
        assert_eq!(metrics.summary().memory_used_bytes, 0);
    }

    #[tokio::test]
    async fn rejects_a_packet_larger_than_the_budget() {
        let limiter = MemoryLimiter::from_budget(100, FlowMetrics::default());
        let error = limiter.reserve(101).await.unwrap_err();
        assert!(matches!(error, FlowError::PacketTooLarge { .. }));
    }
}

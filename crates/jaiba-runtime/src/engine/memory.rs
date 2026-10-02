use std::sync::{Arc, Mutex};

static PROCESS_BUDGET: Mutex<Option<(u64, Arc<Semaphore>)>> = Mutex::new(None);

#[cfg(target_os = "linux")]
use std::fs;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{config::MemoryConfig, error::FlowError};

use super::{DomainMemoryHandle, FlowMetrics};

const UNIT_BYTES: u64 = 64 * 1024;
const DOMAIN_MEMORY_PRESSURE_DEMOTES: usize = 8;
#[cfg(not(target_os = "linux"))]
const PORTABLE_MEMORY_FALLBACK_BYTES: u64 = 512 * 1024 * 1024;

/// Shared byte budget for packets in streaming execution.
///
/// Detection respects physical RAM and cgroup limits.
#[derive(Clone, Debug)]
pub struct MemoryLimiter {
    semaphore: Arc<Semaphore>,
    shared: Option<Arc<Semaphore>>,
    total_units: u32,
    budget_bytes: u64,
    metrics: FlowMetrics,
    domain_memory: Option<DomainMemoryHandle>,
}

/// RAII reservation released when its final clone is dropped.
#[derive(Clone, Debug)]
pub struct MemoryReservation {
    _inner: Arc<ReservationInner>,
}

#[derive(Debug)]
struct ReservationInner {
    _permit: OwnedSemaphorePermit,
    _shared_permit: Option<OwnedSemaphorePermit>,
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
        let mut shared = PROCESS_BUDGET
            .lock()
            .map_err(|_| FlowError::Configuration("process memory lock poisoned".into()))?;
        if shared.is_none() {
            let bytes = match std::env::var("JAIBA_MEMORY_MAX_BYTES") {
                Ok(value) => value.parse::<u64>().map_err(|_| {
                    FlowError::Configuration(
                        "JAIBA_MEMORY_MAX_BYTES must be a positive integer".into(),
                    )
                })?,
                Err(std::env::VarError::NotPresent) => {
                    available.saturating_mul(MemoryConfig::default().maximum_percent as u64) / 100
                }
                Err(_) => {
                    return Err(FlowError::Configuration(
                        "invalid JAIBA_MEMORY_MAX_BYTES".into(),
                    ));
                }
            };
            if bytes < UNIT_BYTES || bytes / UNIT_BYTES > u32::MAX as u64 {
                return Err(FlowError::Configuration(
                    "process memory budget must fit 1..=u32::MAX units of 64 KiB".into(),
                ));
            }
            *shared = Some((
                bytes,
                Arc::new(Semaphore::new((bytes / UNIT_BYTES) as usize)),
            ));
        }
        let (global_bytes, semaphore) = shared.as_ref().expect("initialized process budget");
        let mut limiter = Self::from_budget(budget_bytes.min(*global_bytes), metrics);
        limiter.shared = Some(semaphore.clone());
        Ok(limiter)
    }

    pub(crate) fn from_budget(budget_bytes: u64, metrics: FlowMetrics) -> Self {
        let total_units_u64 = (budget_bytes / UNIT_BYTES).max(1);
        let total_units = total_units_u64.min(u32::MAX as u64) as u32;
        metrics.set_memory_budget(budget_bytes);

        Self {
            semaphore: Arc::new(Semaphore::new(total_units as usize)),
            shared: None,
            total_units,
            budget_bytes,
            metrics,
            domain_memory: None,
        }
    }

    /// Enlaza el JME para demote bajo backpressure del limiter de paquetes.
    pub fn with_domain_memory(mut self, handle: DomainMemoryHandle) -> Self {
        self.domain_memory = Some(handle);
        self
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
        let requested_units = reserved_bytes.div_ceil(UNIT_BYTES);
        if requested_units > self.total_units as u64 {
            return Err(FlowError::PacketTooLarge {
                packet_bytes: reserved_bytes,
                budget_bytes: self.total_units as u64 * UNIT_BYTES,
            });
        }
        let units = requested_units as u32;
        let permit = match self.semaphore.clone().try_acquire_many_owned(units) {
            Ok(permit) => permit,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                self.metrics.backpressure();
                if let Some(jme) = &self.domain_memory {
                    jme.notify_pressure_budget(DOMAIN_MEMORY_PRESSURE_DEMOTES)?;
                }
                self.semaphore
                    .clone()
                    .acquire_many_owned(units)
                    .await
                    .map_err(|_| FlowError::ChannelClosed)?
            }
            Err(tokio::sync::TryAcquireError::Closed) => return Err(FlowError::ChannelClosed),
        };
        let shared_permit = if let Some(shared) = &self.shared {
            let permit = match shared.clone().try_acquire_many_owned(units) {
                Ok(permit) => permit,
                Err(tokio::sync::TryAcquireError::NoPermits) => {
                    self.metrics.backpressure();
                    shared
                        .clone()
                        .acquire_many_owned(units)
                        .await
                        .map_err(|_| FlowError::ChannelClosed)?
                }
                Err(tokio::sync::TryAcquireError::Closed) => return Err(FlowError::ChannelClosed),
            };
            Some(permit)
        } else {
            None
        };
        self.metrics.reserve_memory(reserved_bytes);
        Ok(MemoryReservation {
            _inner: Arc::new(ReservationInner {
                _permit: permit,
                _shared_permit: shared_permit,
                reserved_bytes,
                metrics: self.metrics.clone(),
            }),
        })
    }

    /// Never wait while holding input memory or while routing on the scheduler.
    pub fn try_reserve(&self, bytes: usize) -> Result<MemoryReservation, FlowError> {
        let reserved_bytes = (bytes as u64).max(1);
        let units = reserved_bytes.div_ceil(UNIT_BYTES);
        if reserved_bytes > self.budget_bytes || units > self.total_units as u64 {
            return Err(FlowError::PacketTooLarge {
                packet_bytes: reserved_bytes,
                budget_bytes: self.budget_bytes,
            });
        }
        let exhausted = |_| {
            self.metrics.backpressure();
            FlowError::MemoryCapacity {
                packet_bytes: reserved_bytes,
            }
        };
        let permit = self
            .semaphore
            .clone()
            .try_acquire_many_owned(units as u32)
            .map_err(exhausted)?;
        let shared_permit = self
            .shared
            .as_ref()
            .map(|shared| shared.clone().try_acquire_many_owned(units as u32))
            .transpose()
            .map_err(exhausted)?;
        self.metrics.reserve_memory(reserved_bytes);
        Ok(MemoryReservation {
            _inner: Arc::new(ReservationInner {
                _permit: permit,
                _shared_permit: shared_permit,
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

#[cfg(target_os = "linux")]
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

/// `/proc/meminfo` no existe en Windows ni macOS. Hasta incorporar una sonda
/// nativa por plataforma usamos un límite deliberadamente conservador; el
/// porcentaje configurado sigue aplicándose sobre este valor.
#[cfg(not(target_os = "linux"))]
fn read_mem_total() -> Result<u64, FlowError> {
    Ok(PORTABLE_MEMORY_FALLBACK_BYTES)
}

#[cfg(target_os = "linux")]
fn read_cgroup_limit() -> Option<u64> {
    let value = fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let trimmed = value.trim();
    if trimmed == "max" {
        None
    } else {
        trimmed.parse().ok()
    }
}

#[cfg(not(target_os = "linux"))]
fn read_cgroup_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn detected_limiters_use_the_same_process_pool() {
        let first =
            MemoryLimiter::detect(&MemoryConfig::default(), FlowMetrics::default()).unwrap();
        let second = MemoryLimiter::detect(
            &MemoryConfig {
                maximum_percent: 20,
            },
            FlowMetrics::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            first.shared.as_ref().unwrap(),
            second.shared.as_ref().unwrap()
        ));
        assert!(second.budget_bytes() <= first.budget_bytes());
    }

    #[tokio::test]
    async fn independent_flows_share_capacity_and_cancel_safely() {
        let shared = Arc::new(Semaphore::new(1));
        let mut first = MemoryLimiter::from_budget(UNIT_BYTES, FlowMetrics::default());
        let mut second = MemoryLimiter::from_budget(UNIT_BYTES, FlowMetrics::default());
        first.shared = Some(shared.clone());
        second.shared = Some(shared.clone());
        let held = first.reserve(UNIT_BYTES as usize).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), second.reserve(1))
                .await
                .is_err()
        );
        assert_eq!(second.semaphore.available_permits(), 1);
        let clone = held.clone();
        drop(held);
        assert_eq!(shared.available_permits(), 0);
        drop(clone);
        let reservation = second.reserve(UNIT_BYTES as usize).await.unwrap();
        assert_eq!(shared.available_permits(), 0);
        drop(reservation);
        assert_eq!(shared.available_permits(), 1);
    }

    #[tokio::test]
    async fn fractional_units_do_not_overcommit_or_wait_forever() {
        let limiter = MemoryLimiter::from_budget(UNIT_BYTES + 1, FlowMetrics::default());
        assert!(matches!(
            limiter.reserve(UNIT_BYTES as usize + 1).await,
            Err(FlowError::PacketTooLarge { .. })
        ));
    }

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

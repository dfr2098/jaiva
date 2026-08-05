//! Jaiba Memory Engine (JME) — lifecycle Hot/Warm/Cold/Frozen.
//!
//! Ciclo de vida de **estado de dominio** (no paquetes del DAG).
//! Ver `docs/priority-jme-memory-manager.md`.
//!
//! Cold local: segmentos LZ4 con lectura bajo demanda (`mmap` opcional).
//! Redis opcional: compilar con `--features redis` y `warm.backend: redis`.

mod cold;
mod deferred;
mod duration;
mod error;
mod frozen;
mod hot;
mod manager;
mod policy;
mod rebuild;
mod sink;
mod warm;

#[cfg(feature = "redis")]
mod redis_warm;

pub use cold::{ColdEntry, ColdStore, NoopColdStore, RecordingColdStore, SegmentedColdStore};
pub use error::MemoryError;
pub use frozen::{
    FileFrozenStore, FrozenEntry, FrozenStore, NoopFrozenStore, RecordingFrozenStore,
};
pub use hot::{HotEntry, HotMetrics, HotStore};
pub use manager::{MemoryManager, MemorySnapshot};
pub use policy::{
    ClassPolicy, ColdBackend, FrozenBackend, MemoryPolicy, Policy, Priority, Temperature,
    WarmBackend,
};
pub use rebuild::{MapRebuildHook, RebuildHook};
pub use sink::{ImmediateSink, JsonlFileSink, PersistRecord, RecordingSink};
pub use warm::{NoopWarmStore, RecordingWarmStore, WarmEntry, WarmStore};

#[cfg(feature = "redis")]
pub use redis_warm::RedisWarmStore;

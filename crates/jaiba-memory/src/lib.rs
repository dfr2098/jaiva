//! Jaiba Memory Engine (JME) — Paso 6: Frozen + Redis opcional.
//!
//! Ciclo de vida de **estado de dominio** (no paquetes del DAG).
//! Ver `docs/priority-jme-memory-manager.md`.
//!
//! Redis: compilar con `--features redis` y `warm.backend: redis`.

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

pub use error::MemoryError;
pub use frozen::{
    FileFrozenStore, FrozenEntry, FrozenStore, NoopFrozenStore, RecordingFrozenStore,
};
pub use hot::{HotEntry, HotMetrics, HotStore};
pub use manager::{MemoryManager, MemorySnapshot};
pub use policy::{
    ClassPolicy, FrozenBackend, MemoryPolicy, Policy, Priority, Temperature, WarmBackend,
};
pub use rebuild::{MapRebuildHook, RebuildHook};
pub use sink::{ImmediateSink, JsonlFileSink, PersistRecord, RecordingSink};
pub use warm::{NoopWarmStore, RecordingWarmStore, WarmEntry, WarmStore};

#[cfg(feature = "redis")]
pub use redis_warm::RedisWarmStore;

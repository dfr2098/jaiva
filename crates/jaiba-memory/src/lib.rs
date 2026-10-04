//! Jaiba Memory Engine (JME) — lifecycle Hot/Warm/Cold/Frozen.
//!
//! Ciclo de vida de **estado de dominio** (no paquetes del DAG). Estado:
//! **Beta**; contrato de política `memory.version: 1` (`POLICY_VERSION`).
//!
//! Punto de entrada: `MemoryManager`, construido desde una `MemoryPolicy`.
//! Guía de uso: `docs/configuration.md` (sección JME) y
//! `docs/jme-cold-memory.md`; historia de diseño en
//! `docs/history/priority-jme-memory-manager.md`.
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
    ClassPolicy, ColdBackend, FrozenBackend, MemoryPolicy, POLICY_VERSION, Policy, Priority,
    Temperature, WarmBackend,
};
pub use rebuild::{MapRebuildHook, RebuildHook};
pub use sink::{ImmediateSink, JsonlFileSink, PersistRecord, RecordingSink};
pub use warm::{NoopWarmStore, RecordingWarmStore, WarmEntry, WarmStore};

#[cfg(feature = "redis")]
pub use redis_warm::RedisWarmStore;

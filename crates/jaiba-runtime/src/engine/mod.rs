//! Core runtime services.
//!
//! [`crate::engine::FlowEngine`] executes a processor graph,
//! [`crate::engine::OutputSender`] provides bounded streaming, and
//! [`crate::engine::PacketRepository`] persists work for recovery.

mod circuit;
mod connections;
mod context;
mod control;
mod executor;
mod memory;
mod metrics;
mod packet;
mod processor;
mod registry;
mod repository;
mod schema;
mod state;
mod supervisor;
mod workers;

pub use circuit::CircuitBreakers;
pub use connections::ConnectionManager;
pub use context::ProcessorContext;
pub use control::{FlowControl, FlowControlSnapshot, FlowLifecycle};
pub use executor::FlowEngine;
pub use memory::{MemoryLimiter, MemoryReservation};
pub use metrics::{FlowMetrics, FlowSummary};
pub use packet::{DataPacket, PacketContent};
pub use processor::{OutputSender, Processor, ProcessorEmission};
pub use registry::{ProcessorFactory, ProcessorRegistry};
pub use repository::{
    ContentReference, ContentRepository, DeadLetterEntry, LocalContentRepository,
    LocalPacketRepository, PacketRepository, ProvenanceEvent, ProvenanceRecord, RepositoryStats,
    StoredWork,
};
pub use schema::{DataType, FieldSchema, RecordSchema};
pub use state::StateStore;
pub use supervisor::{FlowSupervisor, SupervisedFlowSnapshot};
pub use workers::WorkerPools;

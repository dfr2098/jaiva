//! API administrativa, salud, métricas y eventos en tiempo real.

mod connection_api;
mod observability;

pub use observability::ObservabilityServer;

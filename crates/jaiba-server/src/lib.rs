//! API administrativa, salud, métricas y eventos en tiempo real.

mod auth;
mod connection_api;
mod flow_registry;
mod observability;
mod schedule_store;
mod scheduler;
mod sql_builder;

pub use observability::{ObservabilityServer, rotate_connection_master_key};

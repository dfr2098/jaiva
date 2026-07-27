//! Tipos estables que describen un flujo Jaiba.
//!
//! Este crate no abre conexiones, no ejecuta procesadores y no contiene
//! servidores. El YAML se deserializa a estos modelos antes de que el runtime
//! construya su grafo ejecutable.

pub mod config;
pub mod graph;

pub use config::FlowConfig;
pub use graph::FlowGraph;

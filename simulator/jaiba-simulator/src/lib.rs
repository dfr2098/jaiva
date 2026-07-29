//! Proveedores Real, Mock y Replay sin alterar el DAG.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
pub use jaiba_core::config::DataExecutionMode as ExecutionMode;
use jaiba_plugin_sdk::PluginPacket;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationRequest {
    pub flow_id: String,
    pub processor_id: String,
    pub mode: ExecutionMode,
    #[serde(default)]
    pub options: Value,
}

#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("mode {0:?} is not configured")]
    MissingProvider(ExecutionMode),
    #[error("simulation source failed: {0}")]
    Source(String),
    #[error("invalid simulation options: {0}")]
    InvalidOptions(String),
}

#[async_trait]
pub trait SimulationSource: Send + Sync {
    fn mode(&self) -> ExecutionMode;

    async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError>;
}

/// A replay reference points at provenance; packet content remains in the
/// runtime repository and is never embedded in the flow YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReference {
    pub source_flow_id: String,
    pub packet_ids: Vec<String>,
}

/// Selects a provider without changing the flow graph.
#[derive(Default)]
pub struct ProviderRegistry {
    providers: RwLock<BTreeMap<&'static str, Arc<dyn SimulationSource>>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, provider: Arc<dyn SimulationSource>) {
        self.providers
            .write()
            .await
            .insert(mode_key(provider.mode()), provider);
    }

    pub async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError> {
        let provider = self
            .providers
            .read()
            .await
            .get(mode_key(request.mode))
            .cloned()
            .ok_or(SimulationError::MissingProvider(request.mode))?;
        provider.packets(request).await
    }
}

fn mode_key(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Real => "real",
        ExecutionMode::Mock => "mock",
        ExecutionMode::Replay => "replay",
    }
}

/// Adapter for normal execution. The host supplies the runtime callback.
pub struct RealProvider<F> {
    execute: F,
}

impl<F> RealProvider<F> {
    pub fn new(execute: F) -> Self {
        Self { execute }
    }
}

#[async_trait]
impl<F, Fut> SimulationSource for RealProvider<F>
where
    F: Send + Sync + Fn(SimulationRequest) -> Fut,
    Fut: Send + std::future::Future<Output = Result<Vec<PluginPacket>, SimulationError>>,
{
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Real
    }

    async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError> {
        (self.execute)(request.clone()).await
    }
}

/// Deterministic provider. `options.packets` entries are record arrays or
/// `{ "bytes": [...], "media_type": "..." }` objects.
pub struct MockProvider;

#[async_trait]
impl SimulationSource for MockProvider {
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Mock
    }

    async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError> {
        request
            .options
            .get("packets")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                SimulationError::InvalidOptions("mock requires options.packets".to_owned())
            })?
            .iter()
            .map(mock_packet)
            .collect()
    }
}

fn mock_packet(value: &Value) -> Result<PluginPacket, SimulationError> {
    if let Some(records) = value.as_array() {
        return Ok(PluginPacket::Records(records.clone()));
    }
    let object = value.as_object().ok_or_else(|| {
        SimulationError::InvalidOptions("each mock packet must be an array or object".to_owned())
    })?;
    let bytes = object
        .get("bytes")
        .and_then(Value::as_array)
        .ok_or_else(|| SimulationError::InvalidOptions("byte packet requires bytes".to_owned()))?
        .iter()
        .map(|byte| {
            byte.as_u64()
                .filter(|byte| *byte <= u8::MAX as u64)
                .map(|byte| byte as u8)
                .ok_or_else(|| {
                    SimulationError::InvalidOptions(
                        "byte values must be integers from 0 to 255".to_owned(),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PluginPacket::Bytes {
        content: bytes,
        media_type: object
            .get("media_type")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

#[async_trait]
pub trait ReplayStore: Send + Sync {
    async fn load(
        &self,
        source_flow_id: &str,
        packet_id: &str,
    ) -> Result<PluginPacket, SimulationError>;
}

pub struct ReplayProvider<S> {
    store: S,
}

impl<S> ReplayProvider<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S: ReplayStore> SimulationSource for ReplayProvider<S> {
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Replay
    }

    async fn packets(
        &self,
        request: &SimulationRequest,
    ) -> Result<Vec<PluginPacket>, SimulationError> {
        let reference: ReplayReference =
            serde_json::from_value(request.options.clone()).map_err(|error| {
                SimulationError::InvalidOptions(format!("invalid replay reference: {error}"))
            })?;
        let mut packets = Vec::with_capacity(reference.packet_ids.len());
        for packet_id in &reference.packet_ids {
            packets.push(
                self.store
                    .load(&reference.source_flow_id, packet_id)
                    .await?,
            );
        }
        Ok(packets)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct MemoryReplayStore(BTreeMap<String, Value>);

    #[async_trait]
    impl ReplayStore for MemoryReplayStore {
        async fn load(
            &self,
            source_flow_id: &str,
            packet_id: &str,
        ) -> Result<PluginPacket, SimulationError> {
            self.0
                .get(&format!("{source_flow_id}/{packet_id}"))
                .cloned()
                .map(|value| PluginPacket::Records(vec![value]))
                .ok_or_else(|| SimulationError::Source(format!("packet '{packet_id}' not found")))
        }
    }

    fn request(mode: ExecutionMode, options: Value) -> SimulationRequest {
        SimulationRequest {
            flow_id: "flow".to_owned(),
            processor_id: "source".to_owned(),
            mode,
            options,
        }
    }

    #[tokio::test]
    async fn registry_dispatches_all_three_providers() {
        let registry = ProviderRegistry::new();
        registry
            .register(Arc::new(RealProvider::new(|_| async {
                Ok(vec![PluginPacket::Records(vec![json!({"mode": "real"})])])
            })))
            .await;
        registry.register(Arc::new(MockProvider)).await;
        registry
            .register(Arc::new(ReplayProvider::new(MemoryReplayStore(
                [("original/p1".to_owned(), json!({"mode": "replay"}))]
                    .into_iter()
                    .collect(),
            ))))
            .await;

        assert_eq!(
            registry
                .packets(&request(ExecutionMode::Real, json!({})))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            registry
                .packets(&request(
                    ExecutionMode::Mock,
                    json!({"packets": [[{"mode": "mock"}], {"bytes": [1, 2, 3]}]})
                ))
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            registry
                .packets(&request(
                    ExecutionMode::Replay,
                    json!({"source_flow_id": "original", "packet_ids": ["p1"]})
                ))
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn missing_provider_and_invalid_mock_are_errors() {
        let registry = ProviderRegistry::new();
        assert!(matches!(
            registry
                .packets(&request(ExecutionMode::Mock, json!({})))
                .await,
            Err(SimulationError::MissingProvider(ExecutionMode::Mock))
        ));
        assert!(matches!(
            MockProvider
                .packets(&request(
                    ExecutionMode::Mock,
                    json!({"packets": [{"bytes": [999]}]})
                ))
                .await,
            Err(SimulationError::InvalidOptions(_))
        ));
    }
}

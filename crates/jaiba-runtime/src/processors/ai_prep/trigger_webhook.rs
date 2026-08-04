//! `ai_trigger_webhook`: notifica a un job ML externo por HTTP(S).
//!
//! Solo dispara la petición (POST/PUT/GET). El entrenamiento / scoring ocurre
//! en Azure ML, Fabric, SageMaker, etc. Por defecto el body incluye atributos
//! y conteo; con `include_records: true` envía también los records (cuidado
//! con el tamaño del payload).

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

/// Hand-off HTTP hacia la plataforma ML (sin código Python en el worker).
pub struct AiTriggerWebhook {
    url: String,
    method: String,
    headers: Vec<(String, String)>,
    include_records: bool,
    timeout_ms: u64,
    /// Si true, un fallo HTTP no aborta el flujo (útil en demos / mock ausente).
    optional: bool,
}

#[derive(Deserialize)]
struct WebhookConfig {
    url: String,
    #[serde(default = "default_post")]
    method: String,
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    include_records: bool,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default)]
    optional: bool,
}

fn default_post() -> String {
    "POST".to_owned()
}

fn default_timeout() -> u64 {
    10_000
}

impl AiTriggerWebhook {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: WebhookConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.url.trim().is_empty() {
            return Err(FlowError::Configuration(
                "ai_trigger_webhook requires url".to_owned(),
            ));
        }
        Ok(Self {
            url: config.url,
            method: config.method.to_ascii_uppercase(),
            headers: config.headers.into_iter().collect(),
            include_records: config.include_records,
            // Evita timeouts absurdamente bajos que fallan por scheduling.
            timeout_ms: config.timeout_ms.max(250),
            optional: config.optional,
        })
    }
}

#[async_trait]
impl Processor for AiTriggerWebhook {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::AsyncIo
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let body = if self.include_records {
            json!({
                "flow_processor": context.processor_id,
                "attributes": packet.attributes,
                "records": packet.records().unwrap_or(&[]),
            })
        } else {
            json!({
                "flow_processor": context.processor_id,
                "attributes": packet.attributes,
                "record_count": packet.records().map(|r| r.len()).unwrap_or(0),
                "event": "jaiba.ai_prep.ready",
            })
        };

        let client = match reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(self.timeout_ms))
            .build()
        {
            Ok(client) => client,
            Err(error) if self.optional => {
                tracing::warn!(
                    processor_id = %context.processor_id,
                    %error,
                    "optional webhook client build failed; continuing"
                );
                return output.success(packet).await;
            }
            Err(error) => {
                return Err(FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: error.to_string(),
                });
            }
        };

        let mut request = match self.method.as_str() {
            "GET" => client.get(&self.url),
            "PUT" => client.put(&self.url).json(&body),
            _ => client.post(&self.url).json(&body),
        };
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => output.success(packet).await,
            Ok(response) if self.optional => {
                tracing::warn!(
                    processor_id = %context.processor_id,
                    status = %response.status(),
                    "optional webhook returned non-success; continuing"
                );
                output.success(packet).await
            }
            Ok(response) => Err(FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: format!("webhook returned HTTP {}", response.status()),
            }),
            Err(error) if self.optional => {
                tracing::warn!(
                    processor_id = %context.processor_id,
                    %error,
                    "optional webhook request failed; continuing"
                );
                output.success(packet).await
            }
            Err(error) => Err(FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: format!("webhook request failed: {error}"),
            }),
        }
    }
}

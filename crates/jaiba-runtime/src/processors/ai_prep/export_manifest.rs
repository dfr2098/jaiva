//! `ai_export_manifest`: escribe metadatos del dataset para hand-off ML.
//!
//! Genera un `manifest.json` con columnas, dtypes (inferidos del primer valor
//! no nulo visto), `row_count`, atributo `ai.split` si existe, paths de split
//! opcionales y checksum SHA-256 del JSON de records. El entrenamiento ocurre
//! **fuera** de Jaiba.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::support::require_objects;

/// Destino de metadatos del paquete preparado (suele colgarse del split train).
pub struct AiExportManifest {
    path: PathBuf,
    dataset_name: String,
    train_path: Option<String>,
    validation_path: Option<String>,
    test_path: Option<String>,
}

#[derive(Deserialize)]
struct ManifestConfig {
    path: PathBuf,
    #[serde(default = "default_dataset_name")]
    dataset_name: String,
    #[serde(default)]
    train_path: Option<String>,
    #[serde(default)]
    validation_path: Option<String>,
    #[serde(default)]
    test_path: Option<String>,
}

fn default_dataset_name() -> String {
    "dataset".to_owned()
}

impl AiExportManifest {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: ManifestConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        Ok(Self {
            path: config.path,
            dataset_name: config.dataset_name,
            train_path: config.train_path.filter(|v| !v.trim().is_empty()),
            validation_path: config.validation_path.filter(|v| !v.trim().is_empty()),
            test_path: config.test_path.filter(|v| !v.trim().is_empty()),
        })
    }
}

#[async_trait]
impl Processor for AiExportManifest {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::BlockingIo
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        require_objects(records, &context.processor_id)?;

        let mut columns: BTreeSet<String> = BTreeSet::new();
        let mut dtypes: BTreeMap<String, String> = BTreeMap::new();
        for record in records {
            let Some(object) = record.as_object() else {
                continue;
            };
            for (key, value) in object {
                columns.insert(key.clone());
                dtypes
                    .entry(key.clone())
                    .or_insert_with(|| dtype_name(value));
            }
        }

        let payload = serde_json::to_vec(records).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let checksum = format!("{:x}", hasher.finalize());

        let mut splits = serde_json::Map::new();
        if let Some(path) = &self.train_path {
            splits.insert("train_path".to_owned(), json!(path));
        }
        if let Some(path) = &self.validation_path {
            splits.insert("validation_path".to_owned(), json!(path));
        }
        if let Some(path) = &self.test_path {
            splits.insert("test_path".to_owned(), json!(path));
        }

        let manifest = json!({
            "dataset": self.dataset_name,
            "row_count": records.len(),
            "columns": columns.into_iter().collect::<Vec<_>>(),
            "dtypes": dtypes,
            "split": packet.attributes.get("ai.split"),
            "splits": if splits.is_empty() { Value::Null } else { Value::Object(splits) },
            "checksum_sha256": checksum,
            "engine": "jaiba",
            "toolkit": "ai_prep",
        });

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(
            &self.path,
            serde_json::to_vec_pretty(&manifest).map_err(|error| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: error.to_string(),
            })?,
        )?;

        output.success(packet).await
    }
}

/// Nombre de tipo lógico para el manifest (no es un schema Arrow completo).
fn dtype_name(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "bool".to_owned(),
        Value::Number(number) if number.is_i64() || number.is_u64() => "int".to_owned(),
        Value::Number(_) => "float".to_owned(),
        Value::String(_) => "string".to_owned(),
        Value::Array(_) => "array".to_owned(),
        Value::Object(_) => "object".to_owned(),
    }
}

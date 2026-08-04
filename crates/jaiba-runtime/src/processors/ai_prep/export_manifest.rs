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
    sync::Mutex,
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
    collect_splits: bool,
    pending: Mutex<BTreeMap<String, BTreeMap<String, Vec<Value>>>>,
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
    #[serde(default)]
    collect_splits: bool,
}

fn default_dataset_name() -> String {
    "dataset".to_owned()
}

impl AiExportManifest {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: ManifestConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.collect_splits
            && [
                config.train_path.as_ref(),
                config.validation_path.as_ref(),
                config.test_path.as_ref(),
            ]
            .iter()
            .any(|path| path.is_none_or(|path| path.trim().is_empty()))
        {
            return Err(FlowError::Configuration(
                "collect_splits requires train_path, validation_path and test_path".to_owned(),
            ));
        }
        Ok(Self {
            path: config.path,
            dataset_name: config.dataset_name,
            train_path: config.train_path.filter(|v| !v.trim().is_empty()),
            validation_path: config.validation_path.filter(|v| !v.trim().is_empty()),
            test_path: config.test_path.filter(|v| !v.trim().is_empty()),
            collect_splits: config.collect_splits,
            pending: Mutex::new(BTreeMap::new()),
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

        if self.collect_splits {
            let split = packet
                .attributes
                .get("ai.split")
                .filter(|split| matches!(split.as_str(), "train" | "validation" | "test"))
                .ok_or_else(|| FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: "collect_splits requires packet attribute ai.split".to_owned(),
                })?
                .clone();
            let group = packet
                .attributes
                .get("ai.split_group")
                .cloned()
                .unwrap_or_else(|| packet.id.to_string());
            let completed = {
                let mut pending = self.pending.lock().map_err(|_| FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: "manifest split collector lock poisoned".to_owned(),
                })?;
                let group_splits = pending.entry(group.clone()).or_default();
                group_splits.insert(split, records.to_vec());
                if ["train", "validation", "test"]
                    .iter()
                    .all(|split| group_splits.contains_key(*split))
                {
                    pending.remove(&group)
                } else {
                    None
                }
            };
            let Some(splits) = completed else {
                return Ok(());
            };
            return self.write_collected_splits(splits, context, output).await;
        }

        self.write_manifest(records, packet.attributes.get("ai.split"), None, context)?;
        output.success(packet).await
    }
}

impl AiExportManifest {
    async fn write_collected_splits(
        &self,
        splits: BTreeMap<String, Vec<Value>>,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let paths = [
            ("train", self.train_path.as_deref()),
            ("validation", self.validation_path.as_deref()),
            ("test", self.test_path.as_deref()),
        ];
        for (split, path) in paths {
            let path = path.ok_or_else(|| {
                FlowError::Configuration(format!("collect_splits requires {split}_path"))
            })?;
            let records = splits.get(split).expect("all required splits collected");
            let bytes = crate::processors::encode::encode_csv(records, true, b',', context)?;
            let path = PathBuf::from(path);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, bytes)?;
        }

        let mut combined = Vec::new();
        let mut counts = BTreeMap::new();
        for split in ["train", "validation", "test"] {
            let records = splits.get(split).expect("all required splits collected");
            counts.insert(split.to_owned(), records.len());
            combined.extend(records.iter().cloned());
        }
        self.write_manifest(&combined, None, Some(&counts), context)?;
        let mut packet = DataPacket::with_records(combined);
        packet
            .attributes
            .insert("ai.split".to_owned(), "all".to_owned());
        output.success(packet).await
    }

    fn write_manifest(
        &self,
        records: &[Value],
        split: Option<&String>,
        split_counts: Option<&BTreeMap<String, usize>>,
        context: &ProcessorContext,
    ) -> Result<(), FlowError> {
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
            "split": split,
            "split_counts": split_counts,
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

        Ok(())
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

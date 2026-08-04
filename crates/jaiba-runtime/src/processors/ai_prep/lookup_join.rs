//! `ai_lookup_join`: enriquecimiento por clave con lado derecho en memoria.
//!
//! Cubren casos PLC + catálogo Oracle/WMS sin un join shuffle distribuido:
//! el lookup se carga una vez (`lookup_records` YAML o `lookup_path` JSON) y
//! se indexa por `key`. Filas sin match se dejan igual (left join laxo).

use std::{collections::HashMap, fs, sync::Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::support::{as_object_mut, require_objects};

/// Join de lookup cacheado: stream principal ← tabla derecha estática.
pub struct AiLookupJoin {
    key: String,
    lookup: Vec<Value>,
    /// Vacío = copiar todos los campos del lookup excepto la clave.
    copy_fields: Vec<String>,
    /// Índice lazy clave → fila (clonado por execute para no retener Mutex).
    index: Mutex<Option<HashMap<String, Value>>>,
}

#[derive(Deserialize)]
struct LookupConfig {
    key: String,
    #[serde(default)]
    lookup_records: Vec<Value>,
    #[serde(default)]
    lookup_path: Option<String>,
    #[serde(default)]
    copy_fields: Vec<String>,
}

impl AiLookupJoin {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: LookupConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.key.trim().is_empty() {
            return Err(FlowError::Configuration(
                "ai_lookup_join requires key".to_owned(),
            ));
        }
        let mut lookup = config.lookup_records;
        if let Some(path) = config.lookup_path {
            let bytes = fs::read(&path).map_err(|error| {
                FlowError::Configuration(format!("ai_lookup_join lookup_path: {error}"))
            })?;
            let loaded: Value = serde_json::from_slice(&bytes).map_err(|error| {
                FlowError::Configuration(format!("ai_lookup_join lookup JSON: {error}"))
            })?;
            let array = loaded.as_array().ok_or_else(|| {
                FlowError::Configuration(
                    "ai_lookup_join lookup_path must be a JSON array".to_owned(),
                )
            })?;
            lookup.extend(array.iter().cloned());
        }
        if lookup.is_empty() {
            return Err(FlowError::Configuration(
                "ai_lookup_join requires lookup_records or lookup_path".to_owned(),
            ));
        }
        Ok(Self {
            key: config.key,
            lookup,
            copy_fields: config.copy_fields,
            index: Mutex::new(None),
        })
    }

    /// Construye el mapa clave → objeto; claves duplicadas: gana la última.
    fn build_index(&self) -> Result<HashMap<String, Value>, FlowError> {
        let mut map = HashMap::new();
        for record in &self.lookup {
            let object = record.as_object().ok_or_else(|| {
                FlowError::Configuration("lookup records must be objects".to_owned())
            })?;
            let key = object.get(&self.key).ok_or_else(|| {
                FlowError::Configuration(format!("lookup record missing key '{}'", self.key))
            })?;
            map.insert(key_to_string(key), record.clone());
        }
        Ok(map)
    }
}

fn key_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

#[async_trait]
impl Processor for AiLookupJoin {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        // Materializar el índice y soltar el Mutex antes de mutar el paquete.
        let index = {
            let mut guard = self.index.lock().map_err(|_| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: "ai_lookup_join state lock poisoned".to_owned(),
            })?;
            if guard.is_none() {
                let built = self.build_index().map_err(|error| match error {
                    FlowError::Configuration(message) => FlowError::Processor {
                        processor_id: context.processor_id.clone(),
                        message,
                    },
                    other => other,
                })?;
                *guard = Some(built);
            }
            guard.as_ref().expect("index initialized").clone()
        };

        let records = packet
            .records_mut()
            .map_err(|message| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message,
            })?;
        require_objects(records, &context.processor_id)?;

        for record in records.iter_mut() {
            let object = as_object_mut(record, &context.processor_id)?;
            let Some(key_value) = object.get(&self.key) else {
                continue;
            };
            let Some(lookup) = index.get(&key_to_string(key_value)) else {
                continue;
            };
            let Some(lookup_obj) = lookup.as_object() else {
                continue;
            };
            if self.copy_fields.is_empty() {
                for (field, value) in lookup_obj {
                    if field != &self.key {
                        object.insert(field.clone(), value.clone());
                    }
                }
            } else {
                for field in &self.copy_fields {
                    if let Some(value) = lookup_obj.get(field) {
                        object.insert(field.clone(), value.clone());
                    }
                }
            }
        }
        output.success(packet).await
    }
}

use serde_json::Value;

use crate::{
    error::MemoryError,
    policy::{Policy, Priority},
};

/// Registro enviado a persistencia inmediata (Cold).
#[derive(Debug, Clone)]
pub struct PersistRecord {
    pub key: String,
    pub class: String,
    pub value: Value,
    pub policy: Policy,
    pub priority: Priority,
}

/// Sink síncrono para `immediate` / `persistent`.
///
/// Paso 2: el motor falla ruidoso si `persist` retorna error. No hay reintento
/// aquí; eso queda al caller o a un Paso posterior.
pub trait ImmediateSink: Send {
    fn persist(&mut self, record: &PersistRecord) -> Result<(), MemoryError>;
}

/// Sink de prueba / inspección: guarda en memoria lo persistido.
#[derive(Debug, Default)]
pub struct RecordingSink {
    pub records: Vec<PersistRecord>,
    pub fail_next: bool,
}

impl ImmediateSink for RecordingSink {
    fn persist(&mut self, record: &PersistRecord) -> Result<(), MemoryError> {
        if self.fail_next {
            self.fail_next = false;
            return Err(MemoryError::Persistence(
                "recording sink forced failure".to_owned(),
            ));
        }
        self.records.push(record.clone());
        Ok(())
    }
}

/// Escribe una línea JSON por registro (durabilidad mínima para demos).
#[derive(Debug)]
pub struct JsonlFileSink {
    path: std::path::PathBuf,
}

impl JsonlFileSink {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl ImmediateSink for JsonlFileSink {
    fn persist(&mut self, record: &PersistRecord) -> Result<(), MemoryError> {
        use std::io::Write;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                MemoryError::Persistence(format!(
                    "no se pudo crear directorio {}: {error}",
                    parent.display()
                ))
            })?;
        }
        let line = serde_json::json!({
            "key": record.key,
            "class": record.class,
            "policy": record.policy.as_str(),
            "priority": record.priority.as_str(),
            "value": record.value,
        });
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| {
                MemoryError::Persistence(format!(
                    "no se pudo abrir {}: {error}",
                    self.path.display()
                ))
            })?;
        writeln!(file, "{line}").map_err(|error| {
            MemoryError::Persistence(format!(
                "no se pudo escribir {}: {error}",
                self.path.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            MemoryError::Persistence(format!("fsync falló en {}: {error}", self.path.display()))
        })?;
        Ok(())
    }
}

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::MemoryError;

/// Entrada en Warm (backend opcional: noop / recording / redis).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WarmEntry {
    pub class: String,
    pub value: Value,
}

/// Backend Warm enchufable (`none` | `redis` con feature).
pub trait WarmStore: Send {
    fn get(&self, key: &str) -> Result<Option<WarmEntry>, MemoryError>;
    fn put(&mut self, key: &str, entry: WarmEntry) -> Result<(), MemoryError>;
    fn remove(&mut self, key: &str) -> Result<bool, MemoryError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn name(&self) -> &'static str;
}

/// Backend `warm.backend: none` — no almacena; siempre miss.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopWarmStore;

impl WarmStore for NoopWarmStore {
    fn get(&self, _key: &str) -> Result<Option<WarmEntry>, MemoryError> {
        Ok(None)
    }

    fn put(&mut self, _key: &str, _entry: WarmEntry) -> Result<(), MemoryError> {
        Ok(())
    }

    fn remove(&mut self, _key: &str) -> Result<bool, MemoryError> {
        Ok(false)
    }

    fn len(&self) -> usize {
        0
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// Warm en memoria para tests (simula un backend que sí responde).
#[derive(Debug, Default)]
pub struct RecordingWarmStore {
    pub entries: HashMap<String, WarmEntry>,
    pub puts: u64,
    pub removes: u64,
}

impl WarmStore for RecordingWarmStore {
    fn get(&self, key: &str) -> Result<Option<WarmEntry>, MemoryError> {
        Ok(self.entries.get(key).cloned())
    }

    fn put(&mut self, key: &str, entry: WarmEntry) -> Result<(), MemoryError> {
        self.puts += 1;
        self.entries.insert(key.to_owned(), entry);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        self.removes += 1;
        Ok(self.entries.remove(key).is_some())
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

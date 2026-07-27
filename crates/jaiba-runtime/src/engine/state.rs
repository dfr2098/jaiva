use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::error::FlowError;

#[derive(Debug, Clone)]
pub struct StateStore {
    path: Arc<PathBuf>,
    values: Arc<Mutex<HashMap<String, String>>>,
}

impl StateStore {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, FlowError> {
        let path = path.into();
        let values = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)
                .map_err(|error| FlowError::Configuration(format!("invalid state file: {error}")))?
        } else {
            HashMap::new()
        };
        Ok(Self {
            path: Arc::new(path),
            values: Arc::new(Mutex::new(values)),
        })
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.values
            .lock()
            .expect("state lock poisoned")
            .get(key)
            .cloned()
    }

    pub fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<(), FlowError> {
        self.values
            .lock()
            .expect("state lock poisoned")
            .insert(key.into(), value.into());
        self.persist()
    }

    fn persist(&self) -> Result<(), FlowError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = temporary_path(&self.path);
        let bytes = serde_json::to_vec_pretty(&*self.values.lock().expect("state lock poisoned"))
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, self.path.as_ref())?;
        Ok(())
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    temporary.into()
}

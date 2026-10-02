use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use crate::error::FlowError;

type Values = Mutex<HashMap<String, String>>;
type Stores = Mutex<HashMap<PathBuf, Weak<Values>>>;
static STORES: OnceLock<Stores> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct StateStore {
    path: Arc<PathBuf>,
    values: Arc<Values>,
}

impl StateStore {
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, FlowError> {
        let path = path.into();
        let absolute = if path.is_absolute() {
            path
        } else {
            std::env::current_dir()?.join(path)
        };
        let parent = absolute
            .parent()
            .ok_or_else(|| FlowError::Configuration("state path requires a parent".into()))?;
        fs::create_dir_all(parent)?;
        let path = if absolute.exists() {
            fs::canonicalize(&absolute)?
        } else {
            fs::canonicalize(parent)?.join(
                absolute
                    .file_name()
                    .ok_or_else(|| FlowError::Configuration("invalid state filename".into()))?,
            )
        };
        let mut stores = STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("state registry lock poisoned");
        stores.retain(|_, store| store.strong_count() > 0);
        let values = if let Some(values) = stores.get(&path).and_then(Weak::upgrade) {
            values
        } else {
            let values = if path.exists() {
                serde_json::from_slice(&fs::read(&path)?).map_err(|error| {
                    FlowError::Configuration(format!("invalid state file: {error}"))
                })?
            } else {
                HashMap::new()
            };
            let values = Arc::new(Mutex::new(values));
            stores.insert(path.clone(), Arc::downgrade(&values));
            values
        };
        Ok(Self {
            path: Arc::new(path),
            values,
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
        let mut values = self.values.lock().expect("state lock poisoned");
        let mut next = values.clone();
        next.insert(key.into(), value.into());
        persist(&self.path, &next)?;
        *values = next;
        Ok(())
    }
}

fn persist(path: &Path, values: &HashMap<String, String>) -> Result<(), FlowError> {
    let bytes = serde_json::to_vec_pretty(values)
        .map_err(|error| FlowError::Configuration(error.to_string()))?;
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> std::io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(FlowError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separate_instances_serialize_concurrent_updates() {
        let path = std::env::temp_dir().join(format!("jaiba-state-{}.json", uuid::Uuid::new_v4()));
        let stores: Vec<_> = (0..8).map(|_| StateStore::load(&path).unwrap()).collect();
        let threads: Vec<_> = stores
            .into_iter()
            .enumerate()
            .map(|(index, store)| {
                std::thread::spawn(move || {
                    for value in 0..20 {
                        store.set(index.to_string(), value.to_string()).unwrap();
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        let reopened = StateStore::load(&path).unwrap();
        for index in 0..8 {
            assert_eq!(reopened.get(&index.to_string()).as_deref(), Some("19"));
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn failed_persistence_does_not_publish_new_value() {
        let dir = std::env::temp_dir().join(format!("jaiba-state-{}", uuid::Uuid::new_v4()));
        let path = dir.join("state.json");
        let state = StateStore::load(&path).unwrap();
        state.set("key", "before").unwrap();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(state.set("key", "after").is_err());
        assert_eq!(state.get("key").as_deref(), Some("before"));
        fs::remove_dir(&path).unwrap();
        fs::remove_dir(&dir).unwrap();
    }
}

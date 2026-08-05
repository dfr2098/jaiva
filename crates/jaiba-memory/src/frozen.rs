use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::MemoryError;

/// Entrada archivada en Frozen.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FrozenEntry {
    pub class: String,
    pub value: Value,
}

/// Archivo / objeto de auditoría (temperatura `frozen`).
pub trait FrozenStore: Send {
    fn get(&self, key: &str) -> Result<Option<FrozenEntry>, MemoryError>;
    fn put(&mut self, key: &str, entry: FrozenEntry) -> Result<(), MemoryError>;
    fn remove(&mut self, key: &str) -> Result<bool, MemoryError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn name(&self) -> &'static str;
}

/// Sin backend Frozen (`frozen.backend` ausente o `none`).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopFrozenStore;

impl FrozenStore for NoopFrozenStore {
    fn get(&self, _key: &str) -> Result<Option<FrozenEntry>, MemoryError> {
        Ok(None)
    }

    fn put(&mut self, _key: &str, _entry: FrozenEntry) -> Result<(), MemoryError> {
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

/// Frozen en disco: un JSON por clave bajo `path/`.
#[derive(Debug)]
pub struct FileFrozenStore {
    root: PathBuf,
    objects: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct FrozenFile {
    key: String,
    class: String,
    value: Value,
}

impl FileFrozenStore {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, MemoryError> {
        let root = path.into();
        fs::create_dir_all(&root).map_err(|error| {
            MemoryError::Frozen(format!(
                "no se pudo crear directorio {}: {error}",
                root.display()
            ))
        })?;
        let objects = count_json_files(&root)?;
        Ok(Self { root, objects })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn file_path(&self, key: &str) -> PathBuf {
        let digest = Sha256::digest(key.as_bytes());
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        self.root.join(format!("{hex}.json"))
    }
}

impl FrozenStore for FileFrozenStore {
    fn get(&self, key: &str) -> Result<Option<FrozenEntry>, MemoryError> {
        let path = self.file_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path)
            .map_err(|error| MemoryError::Frozen(format!("lectura {}: {error}", path.display())))?;
        let file: FrozenFile = serde_json::from_str(&text)
            .map_err(|error| MemoryError::Frozen(format!("json {}: {error}", path.display())))?;
        Ok(Some(FrozenEntry {
            class: file.class,
            value: file.value,
        }))
    }

    fn put(&mut self, key: &str, entry: FrozenEntry) -> Result<(), MemoryError> {
        let path = self.file_path(key);
        let existed = path.exists();
        let file = FrozenFile {
            key: key.to_owned(),
            class: entry.class,
            value: entry.value,
        };
        let text = serde_json::to_string(&file)
            .map_err(|error| MemoryError::Frozen(format!("serialize: {error}")))?;
        let temp = path.with_extension(format!("json.tmp-{}", std::process::id()));
        fs::write(&temp, text).map_err(|error| {
            MemoryError::Frozen(format!("escritura {}: {error}", path.display()))
        })?;
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&temp)
            .map_err(|error| {
                MemoryError::Frozen(format!("abrir temporal {}: {error}", temp.display()))
            })?;
        file.sync_all()
            .map_err(|error| MemoryError::Frozen(format!("fsync {}: {error}", temp.display())))?;
        fs::rename(&temp, &path).map_err(|error| {
            MemoryError::Frozen(format!("renombrar {}: {error}", path.display()))
        })?;
        if !existed {
            self.objects += 1;
        }
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        let path = self.file_path(key);
        if !path.exists() {
            return Ok(false);
        }
        fs::remove_file(&path)
            .map_err(|error| MemoryError::Frozen(format!("borrar {}: {error}", path.display())))?;
        self.objects = self.objects.saturating_sub(1);
        Ok(true)
    }

    fn len(&self) -> usize {
        self.objects as usize
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// Frozen en memoria para tests.
#[derive(Debug, Default)]
pub struct RecordingFrozenStore {
    pub entries: HashMap<String, FrozenEntry>,
    pub puts: u64,
}

impl FrozenStore for RecordingFrozenStore {
    fn get(&self, key: &str) -> Result<Option<FrozenEntry>, MemoryError> {
        Ok(self.entries.get(key).cloned())
    }

    fn put(&mut self, key: &str, entry: FrozenEntry) -> Result<(), MemoryError> {
        self.puts += 1;
        self.entries.insert(key.to_owned(), entry);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        Ok(self.entries.remove(key).is_some())
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

fn count_json_files(root: &Path) -> Result<u64, MemoryError> {
    let mut n = 0u64;
    for entry in fs::read_dir(root)
        .map_err(|error| MemoryError::Frozen(format!("listar {}: {error}", root.display())))?
    {
        let entry = entry.map_err(|error| MemoryError::Frozen(format!("dir entry: {error}")))?;
        if entry.path().extension().and_then(|e| e.to_str()) == Some("json") {
            n += 1;
        }
    }
    Ok(n)
}

//! Persistencia ligera del estado del calendario (último / próximo disparo).

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};

use jaiba_runtime::error::FlowError;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScheduleState {
    pub last_fire_at: Option<u64>,
    pub next_fire_at: Option<u64>,
    pub last_status: Option<String>,
    pub updated_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ScheduleFile {
    flows: HashMap<String, ScheduleState>,
}

/// Almacén JSON de estado de agendas (`schedules.json` bajo JAIBA_DATA_DIR).
pub struct ScheduleStore {
    path: Option<PathBuf>,
    inner: Mutex<HashMap<String, ScheduleState>>,
}

impl ScheduleStore {
    pub fn open(data_dir: Option<PathBuf>) -> Self {
        let path = data_dir.map(|dir| dir.join("schedules.json"));
        let inner = path
            .as_ref()
            .and_then(|path| load(path).ok())
            .unwrap_or_default();
        Self {
            path,
            inner: Mutex::new(inner),
        }
    }

    pub fn get(&self, flow_id: &str) -> ScheduleState {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(flow_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn put(&self, flow_id: &str, state: ScheduleState) -> Result<(), FlowError> {
        {
            let mut guard = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.insert(flow_id.to_owned(), state);
            if let Some(path) = &self.path {
                persist(path, &guard)?;
            }
        }
        Ok(())
    }

}

fn load(path: &Path) -> Result<HashMap<String, ScheduleState>, FlowError> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let bytes = fs::read(path).map_err(|error| {
        FlowError::Configuration(format!("no se pudo leer schedules.json: {error}"))
    })?;
    let file: ScheduleFile = serde_json::from_slice(&bytes).map_err(|error| {
        FlowError::Configuration(format!("schedules.json corrupto: {error}"))
    })?;
    Ok(file.flows)
}

fn persist(path: &Path, flows: &HashMap<String, ScheduleState>) -> Result<(), FlowError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            FlowError::Server(format!("no se pudo crear directorio de agendas: {error}"))
        })?;
    }
    let file = ScheduleFile {
        flows: flows.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&file)
        .map_err(|error| FlowError::Server(format!("no se pudo serializar agendas: {error}")))?;
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, &bytes).map_err(|error| {
        FlowError::Server(format!("no se pudo escribir agenda temporal: {error}"))
    })?;
    fs::rename(&temporary, path)
        .map_err(|error| FlowError::Server(format!("no se pudo persistir agendas: {error}")))
}

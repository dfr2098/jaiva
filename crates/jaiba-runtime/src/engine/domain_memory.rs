use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use jaiba_memory::{JsonlFileSink, MemoryManager, MemoryPolicy};

use super::FlowMetrics;
use crate::{config::DomainMemoryConfig, error::FlowError};

/// Handle compartido del Jaiba Memory Engine (un manager por flujo).
#[derive(Clone)]
pub struct DomainMemoryHandle {
    inner: Arc<Mutex<MemoryManager>>,
    metrics: FlowMetrics,
}

impl std::fmt::Debug for DomainMemoryHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DomainMemoryHandle(..)")
    }
}

impl DomainMemoryHandle {
    pub fn new(manager: MemoryManager, metrics: FlowMetrics) -> Self {
        metrics.set_domain_memory(manager.snapshot());
        Self {
            inner: Arc::new(Mutex::new(manager)),
            metrics,
        }
    }

    pub fn lock(&self) -> Result<std::sync::MutexGuard<'_, MemoryManager>, FlowError> {
        self.inner
            .lock()
            .map_err(|_| FlowError::Server("domain memory lock poisoned".to_owned()))
    }

    /// Demotes bajo presión (cap para no bloquear el hot path del limiter).
    pub fn notify_pressure_budget(&self, max_demotes: usize) -> Result<(), FlowError> {
        let mut manager = self.lock()?;
        for _ in 0..max_demotes.max(1) {
            if !manager
                .notify_pressure()
                .map_err(|error| FlowError::Server(format!("domain memory pressure: {error}")))?
            {
                break;
            }
        }
        self.metrics.set_domain_memory(manager.snapshot());
        Ok(())
    }

    /// Ejecuta el reloj de deferred y publica el snapshot de observabilidad.
    pub fn maintain(&self) -> Result<(), FlowError> {
        let mut manager = self.lock()?;
        manager
            .poll()
            .map_err(|error| FlowError::Server(format!("domain memory maintenance: {error}")))?;
        self.metrics.set_domain_memory(manager.snapshot());
        Ok(())
    }
}

/// Abre JME según `engine.domain_memory`. `None` si está deshabilitado.
pub fn open_domain_memory(
    config: &DomainMemoryConfig,
    flow_id: &str,
    metrics: FlowMetrics,
) -> Result<Option<DomainMemoryHandle>, FlowError> {
    if !config.enabled {
        return Ok(None);
    }
    let yaml = std::fs::read_to_string(&config.policy_file).map_err(|error| {
        FlowError::Configuration(format!(
            "domain_memory.policy_file '{}': {error}",
            config.policy_file.display()
        ))
    })?;
    let policy = MemoryPolicy::from_yaml(&yaml)
        .map_err(|error| FlowError::Configuration(format!("domain_memory policy: {error}")))?;
    let manager = if policy.requires_persist_sink() {
        let path = persist_sink_path(flow_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        MemoryManager::open_with_sink(policy, JsonlFileSink::new(path)).map_err(|error| {
            FlowError::Configuration(format!("domain_memory open_with_sink: {error}"))
        })?
    } else {
        MemoryManager::open(policy)
            .map_err(|error| FlowError::Configuration(format!("domain_memory open: {error}")))?
    };
    Ok(Some(DomainMemoryHandle::new(manager, metrics)))
}

fn persist_sink_path(flow_id: &str) -> PathBuf {
    let data_dir =
        PathBuf::from(std::env::var("JAIBA_DATA_DIR").unwrap_or_else(|_| "data".to_owned()));
    let safe_flow_id: String = flow_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    data_dir
        .join("jme")
        .join(safe_flow_id)
        .join("persist.jsonl")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn disabled_returns_none() {
        let handle = open_domain_memory(
            &DomainMemoryConfig::default(),
            "test",
            FlowMetrics::default(),
        )
        .unwrap();
        assert!(handle.is_none());
    }

    #[test]
    fn enabled_loads_hot_policy() {
        let policy_file =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/jme-hot-policy.yaml");
        let config = DomainMemoryConfig {
            enabled: true,
            policy_file,
        };
        let handle = open_domain_memory(&config, "test", FlowMetrics::default())
            .unwrap()
            .expect("handle");
        let mut mm = handle.lock().unwrap();
        mm.upsert_keyed("telegram", "t1", serde_json::json!({"raw": "PONG"}))
            .unwrap();
        assert!(mm.get_keyed("telegram", "t1").is_some());
    }

    #[test]
    fn notify_pressure_budget_reclaims() {
        let mm = MemoryManager::from_yaml(
            r#"
memory:
  max_entries: 10
  classes:
    telegram:
      policy: volatile
      ttl: 5m
      priority: low
"#,
        )
        .unwrap();
        let handle = DomainMemoryHandle::new(mm, FlowMetrics::default());
        {
            let mut guard = handle.lock().unwrap();
            guard
                .upsert_keyed("telegram", "a", serde_json::json!(1))
                .unwrap();
        }
        handle.notify_pressure_budget(4).unwrap();
        assert_eq!(handle.lock().unwrap().snapshot().hot_objects, 0);
    }
}

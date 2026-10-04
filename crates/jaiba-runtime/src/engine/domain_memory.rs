//! Integración de JME con el runtime: un `MemoryManager` por flujo.
//!
//! Resuelve la política (`policy` embebida o `policy_file`, excluyentes), fija
//! las rutas por flujo bajo `JAIBA_DATA_DIR` (o `data/`) y publica el snapshot
//! en `FlowMetrics`. El executor llama a `maintain` cada 250 ms.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use jaiba_memory::{ColdBackend, FrozenBackend, JsonlFileSink, MemoryManager, MemoryPolicy};

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
    /// Envuelve el manager y publica su primer snapshot.
    pub fn new(manager: MemoryManager, metrics: FlowMetrics) -> Self {
        metrics.set_domain_memory(manager.snapshot());
        Self {
            inner: Arc::new(Mutex::new(manager)),
            metrics,
        }
    }

    /// Acceso exclusivo al manager; los processors `memory_*` lo toman por paquete.
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

    /// Flush de `deferred`, degradación por inactividad y snapshot de métricas.
    /// Solo falla si falla el flush; la degradación es best-effort.
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
    let mut policy = load_policy(config)?;
    let data_root = data_root();
    scope_store_paths(&mut policy, &data_root, flow_id);
    let manager = if policy.requires_persist_sink() {
        let path = data_root
            .join("jme")
            .join(safe_flow_id(flow_id))
            .join("persist.jsonl");
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

fn load_policy(config: &DomainMemoryConfig) -> Result<MemoryPolicy, FlowError> {
    let policy = match (&config.policy, &config.policy_file) {
        (Some(_), Some(_)) => {
            return Err(FlowError::Configuration(
                "domain_memory: use policy (embebida) o policy_file, no ambas".to_owned(),
            ));
        }
        (None, None) => {
            return Err(FlowError::Configuration(
                "domain_memory.enabled requiere policy (embebida) o policy_file".to_owned(),
            ));
        }
        (Some(inline), None) => MemoryPolicy::from_memory_value(inline.clone()),
        (None, Some(path)) => {
            let yaml = std::fs::read_to_string(path).map_err(|error| {
                FlowError::Configuration(format!(
                    "domain_memory.policy_file '{}': {error}",
                    path.display()
                ))
            })?;
            MemoryPolicy::from_yaml(&yaml)
        }
    };
    policy.map_err(|error| FlowError::Configuration(format!("domain_memory policy: {error}")))
}

fn data_root() -> PathBuf {
    std::env::var("JAIBA_DATA_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map_or_else(|| PathBuf::from("data"), PathBuf::from)
}

/// Cold y Frozen sin `path` viven bajo `<data_root>/jme/{cold,frozen}`. Cada
/// flujo recibe un subdirectorio propio para evitar writers concurrentes.
fn scope_store_paths(policy: &mut MemoryPolicy, data_root: &Path, flow_id: &str) {
    let flow = safe_flow_id(flow_id);
    if matches!(policy.cold_backend, ColdBackend::Segmented) {
        let base = policy
            .cold_path
            .take()
            .unwrap_or_else(|| data_root.join("jme").join("cold"));
        policy.cold_path = Some(base.join(&flow));
    }
    if matches!(policy.frozen_backend, FrozenBackend::File) && policy.frozen_path.is_none() {
        policy.frozen_path = Some(data_root.join("jme").join("frozen").join(&flow));
    }
}

fn safe_flow_id(flow_id: &str) -> String {
    flow_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
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
            policy: None,
            policy_file: Some(policy_file),
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

    #[test]
    fn cold_path_is_scoped_and_flow_id_is_sanitized() {
        let mut policy = MemoryPolicy::from_yaml(
            r#"
memory:
  cold:
    backend: segmented
    path: data/jme/cold
  classes:
    carrier:
      policy: cache
      temperature: cold
      ttl: 1h
"#,
        )
        .unwrap();
        scope_store_paths(&mut policy, Path::new("/srv/jaiba"), "plant/a 1");
        assert_eq!(
            policy.cold_path,
            Some(PathBuf::from("data/jme/cold/plant_a_1"))
        );
    }

    #[test]
    fn omitted_store_paths_live_under_data_root() {
        let mut policy = MemoryPolicy::from_memory_value(serde_json::json!({
            "cold": {"backend": "segmented"},
            "frozen": {"backend": "file"},
            "classes": {"carrier": {"policy": "cache", "temperature": "cold", "ttl": "1h"}}
        }))
        .unwrap();
        scope_store_paths(&mut policy, Path::new("/srv/jaiba"), "plant-a");
        assert_eq!(
            policy.cold_path,
            Some(PathBuf::from("/srv/jaiba/jme/cold/plant-a"))
        );
        assert_eq!(
            policy.frozen_path,
            Some(PathBuf::from("/srv/jaiba/jme/frozen/plant-a"))
        );
    }

    #[test]
    fn inline_policy_opens_without_files() {
        let config = DomainMemoryConfig {
            enabled: true,
            policy: Some(serde_json::json!({
                "version": 1,
                "classes": {"telegram": {"policy": "volatile", "ttl": "5m"}}
            })),
            policy_file: None,
        };
        let handle = open_domain_memory(&config, "inline", FlowMetrics::default())
            .unwrap()
            .expect("handle");
        let mut mm = handle.lock().unwrap();
        mm.upsert_keyed("telegram", "t1", serde_json::json!(1))
            .unwrap();
        assert_eq!(mm.get_keyed("telegram", "t1"), Some(serde_json::json!(1)));
    }

    #[test]
    fn policy_source_must_be_exactly_one() {
        let neither = DomainMemoryConfig {
            enabled: true,
            ..DomainMemoryConfig::default()
        };
        let both = DomainMemoryConfig {
            enabled: true,
            policy: Some(serde_json::json!({})),
            policy_file: Some(PathBuf::from("policy.yaml")),
        };
        for config in [neither, both] {
            assert!(matches!(
                open_domain_memory(&config, "test", FlowMetrics::default()),
                Err(FlowError::Configuration(_))
            ));
        }
    }
}

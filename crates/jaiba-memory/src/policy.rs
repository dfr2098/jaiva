use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use serde::Deserialize;

use crate::{duration::parse_duration, error::MemoryError};

/// Política de lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    Volatile,
    Cache,
    Deferred,
    Immediate,
    Persistent,
}

impl Policy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "volatile",
            Self::Cache => "cache",
            Self::Deferred => "deferred",
            Self::Immediate => "immediate",
            Self::Persistent => "persistent",
        }
    }

    /// Paso 6: políticas de lifecycle (Warm/Frozen/rebuild son enchufes).
    fn supported_now(self) -> bool {
        matches!(
            self,
            Self::Volatile | Self::Cache | Self::Immediate | Self::Persistent | Self::Deferred
        )
    }

    pub fn requires_persist_sink(self) -> bool {
        matches!(self, Self::Immediate | Self::Persistent | Self::Deferred)
    }

    /// Alias histórico Paso 2.
    pub fn requires_immediate_sink(self) -> bool {
        self.requires_persist_sink()
    }
}

/// Temperatura objetivo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Temperature {
    Hot,
    Warm,
    Cold,
    Frozen,
}

/// Backend Warm declarado en YAML (`memory.warm.backend`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WarmBackend {
    #[default]
    None,
    /// Requiere feature `redis` + URL en env al abrir el manager.
    Redis,
}

/// Backend Frozen declarado en YAML (`memory.frozen.backend`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrozenBackend {
    #[default]
    None,
    File,
}

/// Prioridad bajo presión de memoria.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClassPolicy {
    pub name: String,
    pub policy: Policy,
    pub temperature: Temperature,
    pub ttl: Option<Duration>,
    pub flush: Option<Duration>,
    pub priority: Priority,
    /// Token opcional para [`crate::RebuildHook`] (solo `cache`).
    pub rebuild: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MemoryPolicy {
    pub max_entries: usize,
    pub max_pending_deferred: usize,
    pub default_priority: Priority,
    pub warm_backend: WarmBackend,
    /// Nombre de variable de entorno con URL Redis (default `REDIS_URL`).
    pub warm_url_env: String,
    pub warm_key_prefix: String,
    pub frozen_backend: FrozenBackend,
    pub frozen_path: Option<PathBuf>,
    pub classes: BTreeMap<String, ClassPolicy>,
}

#[derive(Debug, Deserialize)]
struct FileRoot {
    #[serde(default)]
    memory: FileMemory,
}

#[derive(Debug, Default, Deserialize)]
struct FileMemory {
    #[serde(default)]
    max_entries: Option<usize>,
    #[serde(default)]
    max_pending_deferred: Option<usize>,
    #[serde(default)]
    defaults: FileDefaults,
    #[serde(default)]
    classes: BTreeMap<String, FileClass>,
    #[serde(default)]
    warm: Option<FileWarm>,
    #[serde(default)]
    frozen: Option<FileFrozen>,
}

#[derive(Debug, Default, Deserialize)]
struct FileDefaults {
    #[serde(default)]
    priority: Option<Priority>,
}

#[derive(Debug, Deserialize)]
struct FileWarm {
    #[serde(default = "default_warm_backend")]
    backend: String,
    #[serde(default)]
    url_env: Option<String>,
    #[serde(default)]
    key_prefix: Option<String>,
}

fn default_warm_backend() -> String {
    "none".to_owned()
}

#[derive(Debug, Deserialize)]
struct FileFrozen {
    #[serde(default = "default_frozen_backend")]
    backend: String,
    #[serde(default)]
    path: Option<String>,
}

fn default_frozen_backend() -> String {
    "none".to_owned()
}

#[derive(Debug, Deserialize)]
struct FileClass {
    policy: Policy,
    #[serde(default)]
    temperature: Option<Temperature>,
    #[serde(default)]
    ttl: Option<String>,
    #[serde(default)]
    priority: Option<Priority>,
    #[serde(default)]
    flush: Option<String>,
    #[serde(default)]
    rebuild: Option<String>,
}

impl MemoryPolicy {
    pub fn from_yaml(text: &str) -> Result<Self, MemoryError> {
        let root: FileRoot = serde_yaml::from_str(text)
            .map_err(|error| MemoryError::Configuration(error.to_string()))?;
        Self::from_file(root.memory)
    }

    fn from_file(file: FileMemory) -> Result<Self, MemoryError> {
        let (warm_backend, warm_url_env, warm_key_prefix) = match file.warm.as_ref() {
            None => (
                WarmBackend::None,
                "REDIS_URL".to_owned(),
                "jaiba:jme:".to_owned(),
            ),
            Some(warm) => {
                let backend = warm.backend.trim().to_ascii_lowercase();
                let warm_backend = match backend.as_str() {
                    "" | "none" => WarmBackend::None,
                    "redis" => WarmBackend::Redis,
                    other => {
                        return Err(MemoryError::Configuration(format!(
                            "warm.backend '{other}' no soportado (none|redis)"
                        )));
                    }
                };
                (
                    warm_backend,
                    warm.url_env
                        .as_deref()
                        .unwrap_or("REDIS_URL")
                        .trim()
                        .to_owned(),
                    warm.key_prefix
                        .as_deref()
                        .unwrap_or("jaiba:jme:")
                        .to_owned(),
                )
            }
        };

        let (frozen_backend, frozen_path) = match file.frozen.as_ref() {
            None => (FrozenBackend::None, None),
            Some(frozen) => {
                let backend = frozen.backend.trim().to_ascii_lowercase();
                match backend.as_str() {
                    "" | "none" => (FrozenBackend::None, None),
                    "file" => {
                        let path = frozen
                            .path
                            .as_deref()
                            .map(str::trim)
                            .filter(|p| !p.is_empty())
                            .ok_or_else(|| {
                                MemoryError::Configuration(
                                    "frozen.backend 'file' requiere path".to_owned(),
                                )
                            })?;
                        (FrozenBackend::File, Some(PathBuf::from(path)))
                    }
                    other => {
                        return Err(MemoryError::Configuration(format!(
                            "frozen.backend '{other}' no soportado (none|file)"
                        )));
                    }
                }
            }
        };

        let default_priority = file.defaults.priority.unwrap_or(Priority::Normal);
        let max_entries = file.max_entries.unwrap_or(10_000).max(1);
        let max_pending_deferred = file.max_pending_deferred.unwrap_or(1_024).max(1);
        let mut classes = BTreeMap::new();

        for (name, class) in file.classes {
            if !class.policy.supported_now() {
                return Err(MemoryError::UnsupportedPolicy(
                    class.policy.as_str().to_owned(),
                ));
            }
            let flush = match class.flush.as_deref() {
                Some(raw) => Some(parse_duration(raw)?),
                None => None,
            };
            match class.policy {
                Policy::Deferred => {
                    if flush.is_none() {
                        return Err(MemoryError::Configuration(format!(
                            "clase '{name}': deferred requiere flush (p. ej. 2s)"
                        )));
                    }
                }
                _ if flush.is_some() => {
                    return Err(MemoryError::Configuration(format!(
                        "clase '{name}': flush solo aplica a deferred"
                    )));
                }
                _ => {}
            }
            let ttl = match class.ttl.as_deref() {
                Some(raw) => Some(parse_duration(raw)?),
                None => None,
            };
            if matches!(class.policy, Policy::Volatile | Policy::Cache) && ttl.is_none() {
                return Err(MemoryError::Configuration(format!(
                    "clase '{name}': policy {} requiere ttl (p. ej. 5m)",
                    class.policy.as_str()
                )));
            }
            let temperature = class.temperature.unwrap_or(match class.policy {
                Policy::Immediate | Policy::Persistent | Policy::Deferred => Temperature::Cold,
                _ => Temperature::Hot,
            });
            if matches!(temperature, Temperature::Frozen)
                && matches!(frozen_backend, FrozenBackend::None)
            {
                return Err(MemoryError::Configuration(format!(
                    "clase '{name}': temperature frozen requiere memory.frozen.backend: file"
                )));
            }
            let priority = class.priority.unwrap_or(match class.policy {
                Policy::Immediate | Policy::Persistent => Priority::Critical,
                Policy::Deferred => Priority::High,
                _ => default_priority,
            });
            let rebuild = match class.rebuild {
                Some(ref token) => {
                    let token = token.trim();
                    if token.is_empty() {
                        return Err(MemoryError::Configuration(format!(
                            "clase '{name}': rebuild no puede estar vacío"
                        )));
                    }
                    if !matches!(class.policy, Policy::Cache) {
                        return Err(MemoryError::Configuration(format!(
                            "clase '{name}': rebuild solo aplica a policy cache"
                        )));
                    }
                    Some(token.to_owned())
                }
                None => None,
            };
            classes.insert(
                name.clone(),
                ClassPolicy {
                    name,
                    policy: class.policy,
                    temperature,
                    ttl,
                    flush,
                    priority,
                    rebuild,
                },
            );
        }

        if classes.is_empty() {
            return Err(MemoryError::Configuration(
                "memory.classes no puede estar vacío".to_owned(),
            ));
        }

        Ok(Self {
            max_entries,
            max_pending_deferred,
            default_priority,
            warm_backend,
            warm_url_env,
            warm_key_prefix,
            frozen_backend,
            frozen_path,
            classes,
        })
    }

    pub fn class(&self, name: &str) -> Result<&ClassPolicy, MemoryError> {
        self.classes
            .get(name)
            .ok_or_else(|| MemoryError::UnknownClass(name.to_owned()))
    }

    pub fn requires_persist_sink(&self) -> bool {
        self.classes
            .values()
            .any(|class| class.policy.requires_persist_sink())
    }

    pub fn requires_immediate_sink(&self) -> bool {
        self.requires_persist_sink()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_step1_policy_file() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  max_entries: 100
  defaults:
    priority: normal
  warm:
    backend: none
  classes:
    telegram:
      policy: volatile
      ttl: 5m
      priority: low
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      priority: high
"#,
        )
        .unwrap();
        assert_eq!(policy.max_entries, 100);
        assert_eq!(policy.warm_backend, WarmBackend::None);
        assert_eq!(policy.class("telegram").unwrap().priority, Priority::Low);
        assert_eq!(policy.class("carrier").unwrap().policy, Policy::Cache);
        assert_eq!(
            policy.class("carrier").unwrap().temperature,
            Temperature::Warm
        );
    }

    #[test]
    fn loads_redis_warm_backend() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  warm:
    backend: redis
    url_env: MY_REDIS
    key_prefix: "app:jme:"
  classes:
    carrier:
      policy: cache
      ttl: 30m
"#,
        )
        .unwrap();
        assert_eq!(policy.warm_backend, WarmBackend::Redis);
        assert_eq!(policy.warm_url_env, "MY_REDIS");
        assert_eq!(policy.warm_key_prefix, "app:jme:");
    }

    #[test]
    fn loads_frozen_file_backend() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  frozen:
    backend: file
    path: /tmp/jme-frozen
  classes:
    audit:
      policy: cache
      temperature: frozen
      ttl: 1h
"#,
        )
        .unwrap();
        assert_eq!(policy.frozen_backend, FrozenBackend::File);
        assert_eq!(
            policy.frozen_path.as_deref(),
            Some(std::path::Path::new("/tmp/jme-frozen"))
        );
    }

    #[test]
    fn frozen_temperature_requires_backend() {
        let error = MemoryPolicy::from_yaml(
            r#"
memory:
  classes:
    audit:
      policy: cache
      temperature: frozen
      ttl: 1h
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("frozen"));
    }

    #[test]
    fn loads_immediate_and_defaults_critical() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  classes:
    alarm:
      policy: immediate
    configuration:
      policy: persistent
      temperature: cold
"#,
        )
        .unwrap();
        let alarm = policy.class("alarm").unwrap();
        assert_eq!(alarm.policy, Policy::Immediate);
        assert_eq!(alarm.priority, Priority::Critical);
        assert_eq!(alarm.temperature, Temperature::Cold);
        assert!(policy.requires_persist_sink());
    }

    #[test]
    fn loads_deferred_with_flush() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  max_pending_deferred: 64
  classes:
    inventory:
      policy: deferred
      flush: 2s
      priority: high
"#,
        )
        .unwrap();
        let inventory = policy.class("inventory").unwrap();
        assert_eq!(inventory.policy, Policy::Deferred);
        assert_eq!(inventory.flush, Some(Duration::from_secs(2)));
        assert_eq!(policy.max_pending_deferred, 64);
    }

    #[test]
    fn deferred_requires_flush() {
        let error = MemoryPolicy::from_yaml(
            r#"
memory:
  classes:
    inventory:
      policy: deferred
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("flush"));
    }

    #[test]
    fn loads_rebuild_on_cache() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  classes:
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      rebuild: query:carrier_by_id
"#,
        )
        .unwrap();
        assert_eq!(
            policy.class("carrier").unwrap().rebuild.as_deref(),
            Some("query:carrier_by_id")
        );
    }

    #[test]
    fn rebuild_rejects_non_cache() {
        let error = MemoryPolicy::from_yaml(
            r#"
memory:
  classes:
    telegram:
      policy: volatile
      ttl: 5m
      rebuild: query:x
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("rebuild"));
    }
}

use std::time::Instant;

use serde_json::Value;

use crate::{
    deferred::DeferredQueue,
    error::MemoryError,
    frozen::{FileFrozenStore, FrozenEntry, FrozenStore, NoopFrozenStore},
    hot::{HotEntry, HotMetrics, HotStore},
    policy::{ClassPolicy, FrozenBackend, MemoryPolicy, Policy, Temperature, WarmBackend},
    rebuild::RebuildHook,
    sink::{ImmediateSink, PersistRecord},
    warm::{NoopWarmStore, WarmEntry, WarmStore},
};

/// Motor JME Paso 6: Hot/Warm/Frozen + promote/demote/rebuild + Redis opcional.
pub struct MemoryManager {
    policy: MemoryPolicy,
    hot: HotStore,
    warm: Box<dyn WarmStore>,
    frozen: Box<dyn FrozenStore>,
    persist: Option<Box<dyn ImmediateSink>>,
    rebuild: Option<Box<dyn RebuildHook>>,
    deferred: DeferredQueue,
    immediate_failures: u64,
    immediate_writes: u64,
    deferred_writes: u64,
    deferred_failures: u64,
    warm_puts: u64,
    warm_hits: u64,
    warm_misses: u64,
    frozen_puts: u64,
    frozen_hits: u64,
    frozen_misses: u64,
    promotions: u64,
    demotions: u64,
    demotion_failures: u64,
    freezes: u64,
    rebuilds: u64,
    rebuild_misses: u64,
    rebuild_failures: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct MemorySnapshot {
    pub hot_objects: u64,
    pub warm_objects: u64,
    pub frozen_objects: u64,
    pub warm_backend: &'static str,
    pub frozen_backend: &'static str,
    pub warm_puts: u64,
    pub warm_hits: u64,
    pub warm_misses: u64,
    pub frozen_puts: u64,
    pub frozen_hits: u64,
    pub frozen_misses: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub demotion_failures: u64,
    pub freezes: u64,
    pub rebuilds: u64,
    pub rebuild_misses: u64,
    pub rebuild_failures: u64,
    pub evictions: u64,
    pub expired_removals: u64,
    pub max_entries: u64,
    pub immediate_writes: u64,
    pub immediate_failures: u64,
    pub persist_queue: u64,
    pub deferred_writes: u64,
    pub deferred_failures: u64,
    pub deferred_flushes: u64,
    pub deferred_enqueued: u64,
}

impl MemoryManager {
    /// Construye el manager. Si hay `immediate`/`persistent`/`deferred`,
    /// usa [`Self::with_immediate_sink`].
    pub fn new(policy: MemoryPolicy) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(NoopWarmStore),
            None,
            Box::new(NoopFrozenStore),
        ))
    }

    /// Abre backends según YAML (`warm.backend`, `frozen.backend`).
    pub fn open(policy: MemoryPolicy) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        let warm = Self::build_warm_backend(&policy)?;
        let frozen = Self::build_frozen_backend(&policy)?;
        Ok(Self::build(policy, None, warm, None, frozen))
    }

    pub fn open_with_sink(
        policy: MemoryPolicy,
        sink: impl ImmediateSink + 'static,
    ) -> Result<Self, MemoryError> {
        let warm = Self::build_warm_backend(&policy)?;
        let frozen = Self::build_frozen_backend(&policy)?;
        Ok(Self::build(
            policy,
            Some(Box::new(sink)),
            warm,
            None,
            frozen,
        ))
    }

    pub fn with_immediate_sink(policy: MemoryPolicy, sink: impl ImmediateSink + 'static) -> Self {
        Self::build(
            policy,
            Some(Box::new(sink)),
            Box::new(NoopWarmStore),
            None,
            Box::new(NoopFrozenStore),
        )
    }

    /// Igual que [`Self::new`] pero con WarmStore custom (tests / futuros backends).
    pub fn with_warm_store(
        policy: MemoryPolicy,
        warm: impl WarmStore + 'static,
    ) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(warm),
            None,
            Box::new(NoopFrozenStore),
        ))
    }

    pub fn with_frozen_store(
        policy: MemoryPolicy,
        frozen: impl FrozenStore + 'static,
    ) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(NoopWarmStore),
            None,
            Box::new(frozen),
        ))
    }

    pub fn with_warm_and_frozen(
        policy: MemoryPolicy,
        warm: impl WarmStore + 'static,
        frozen: impl FrozenStore + 'static,
    ) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(warm),
            None,
            Box::new(frozen),
        ))
    }

    pub fn with_sink_and_warm(
        policy: MemoryPolicy,
        sink: impl ImmediateSink + 'static,
        warm: impl WarmStore + 'static,
    ) -> Self {
        Self::build(
            policy,
            Some(Box::new(sink)),
            Box::new(warm),
            None,
            Box::new(NoopFrozenStore),
        )
    }

    pub fn with_rebuild_hook(
        policy: MemoryPolicy,
        rebuild: impl RebuildHook + 'static,
    ) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(NoopWarmStore),
            Some(Box::new(rebuild)),
            Box::new(NoopFrozenStore),
        ))
    }

    pub fn with_warm_and_rebuild(
        policy: MemoryPolicy,
        warm: impl WarmStore + 'static,
        rebuild: impl RebuildHook + 'static,
    ) -> Result<Self, MemoryError> {
        if policy.requires_persist_sink() {
            return Err(MemoryError::MissingImmediateSink);
        }
        Ok(Self::build(
            policy,
            None,
            Box::new(warm),
            Some(Box::new(rebuild)),
            Box::new(NoopFrozenStore),
        ))
    }

    fn build(
        policy: MemoryPolicy,
        persist: Option<Box<dyn ImmediateSink>>,
        warm: Box<dyn WarmStore>,
        rebuild: Option<Box<dyn RebuildHook>>,
        frozen: Box<dyn FrozenStore>,
    ) -> Self {
        let max_entries = policy.max_entries;
        let max_pending = policy.max_pending_deferred;
        Self {
            policy,
            hot: HotStore::new(max_entries),
            warm,
            frozen,
            persist,
            rebuild,
            deferred: DeferredQueue::new(max_pending),
            immediate_failures: 0,
            immediate_writes: 0,
            deferred_writes: 0,
            deferred_failures: 0,
            warm_puts: 0,
            warm_hits: 0,
            warm_misses: 0,
            frozen_puts: 0,
            frozen_hits: 0,
            frozen_misses: 0,
            promotions: 0,
            demotions: 0,
            demotion_failures: 0,
            freezes: 0,
            rebuilds: 0,
            rebuild_misses: 0,
            rebuild_failures: 0,
        }
    }

    fn build_warm_backend(policy: &MemoryPolicy) -> Result<Box<dyn WarmStore>, MemoryError> {
        match policy.warm_backend {
            WarmBackend::None => Ok(Box::new(NoopWarmStore)),
            WarmBackend::Redis => {
                #[cfg(feature = "redis")]
                {
                    Ok(Box::new(crate::redis_warm::RedisWarmStore::from_env(
                        &policy.warm_url_env,
                        policy.warm_key_prefix.clone(),
                    )?))
                }
                #[cfg(not(feature = "redis"))]
                {
                    let _ = policy;
                    Err(MemoryError::RedisFeatureDisabled)
                }
            }
        }
    }

    fn build_frozen_backend(policy: &MemoryPolicy) -> Result<Box<dyn FrozenStore>, MemoryError> {
        match policy.frozen_backend {
            FrozenBackend::None => Ok(Box::new(NoopFrozenStore)),
            FrozenBackend::File => {
                let path = policy.frozen_path.as_ref().ok_or_else(|| {
                    MemoryError::Configuration(
                        "frozen.backend file sin path (bug de validación)".to_owned(),
                    )
                })?;
                Ok(Box::new(FileFrozenStore::new(path)?))
            }
        }
    }

    pub fn from_yaml(text: &str) -> Result<Self, MemoryError> {
        Self::open(MemoryPolicy::from_yaml(text)?)
    }

    pub fn from_yaml_with_sink(
        text: &str,
        sink: impl ImmediateSink + 'static,
    ) -> Result<Self, MemoryError> {
        Self::open_with_sink(MemoryPolicy::from_yaml(text)?, sink)
    }

    pub fn from_yaml_with_warm(
        text: &str,
        warm: impl WarmStore + 'static,
    ) -> Result<Self, MemoryError> {
        Self::with_warm_store(MemoryPolicy::from_yaml(text)?, warm)
    }

    pub fn from_yaml_with_rebuild(
        text: &str,
        rebuild: impl RebuildHook + 'static,
    ) -> Result<Self, MemoryError> {
        Self::with_rebuild_hook(MemoryPolicy::from_yaml(text)?, rebuild)
    }

    pub fn from_yaml_with_warm_and_rebuild(
        text: &str,
        warm: impl WarmStore + 'static,
        rebuild: impl RebuildHook + 'static,
    ) -> Result<Self, MemoryError> {
        Self::with_warm_and_rebuild(MemoryPolicy::from_yaml(text)?, warm, rebuild)
    }

    pub fn policy(&self) -> &MemoryPolicy {
        &self.policy
    }

    /// Inserta o actualiza. `class` debe existir en la política.
    ///
    /// - `immediate`/`persistent`: persiste en el sink **antes** de Hot.
    /// - `deferred`: Hot primero, luego cola; flush por intervalo o tope.
    /// - `cache` con temperatura `warm`: Hot + mirror en WarmStore.
    /// - temperatura `frozen`: archiva en FrozenStore (además de Hot).
    /// - Bajo presión: warm → WarmStore; frozen → FrozenStore.
    pub fn upsert(
        &mut self,
        key: impl Into<String>,
        value: Value,
        class: &str,
    ) -> Result<(), MemoryError> {
        self.upsert_at(key, value, class, Instant::now())
    }

    pub fn upsert_at(
        &mut self,
        key: impl Into<String>,
        value: Value,
        class: &str,
        now: Instant,
    ) -> Result<(), MemoryError> {
        let key = key.into();
        let class_policy = self.policy.class(class)?.clone();
        let archive_frozen = matches!(class_policy.temperature, Temperature::Frozen)
            .then(|| (key.clone(), value.clone(), class_policy.name.clone()));

        match class_policy.policy {
            Policy::Immediate | Policy::Persistent => {
                self.persist_now(PersistRecord {
                    key: key.clone(),
                    class: class_policy.name.clone(),
                    value: value.clone(),
                    policy: class_policy.policy,
                    priority: class_policy.priority,
                })?;
                self.hot_upsert(key, value, &class_policy, now)?;
            }
            Policy::Deferred => {
                // Working set visible de inmediato; Cold en batch.
                self.hot_upsert(key.clone(), value.clone(), &class_policy, now)?;
                let flush_after = class_policy.flush.expect("validated flush");
                let force = self.deferred.enqueue(
                    PersistRecord {
                        key,
                        class: class_policy.name.clone(),
                        value,
                        policy: class_policy.policy,
                        priority: class_policy.priority,
                    },
                    flush_after,
                    now,
                );
                self.flush_deferred_at(now, force)?;
            }
            Policy::Volatile => {
                self.hot_upsert(key, value, &class_policy, now)?;
                self.flush_deferred_at(now, false)?;
            }
            Policy::Cache => {
                self.hot_upsert(key.clone(), value.clone(), &class_policy, now)?;
                if self.should_mirror_warm(&class_policy) {
                    self.warm_put(
                        &key,
                        WarmEntry {
                            class: class_policy.name.clone(),
                            value,
                        },
                    )?;
                }
                self.flush_deferred_at(now, false)?;
            }
        }

        if let Some((fkey, fvalue, fclass)) = archive_frozen {
            self.frozen_put(
                &fkey,
                FrozenEntry {
                    class: fclass,
                    value: fvalue,
                },
            )?;
        }
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Option<Value> {
        self.get_at(key, Instant::now())
    }

    pub fn get_at(&mut self, key: &str, now: Instant) -> Option<Value> {
        let _ = self.flush_deferred_at(now, false);
        if let Some(value) = self.hot.get(key, now) {
            return Some(value);
        }
        if let Some(value) = self.promote_from_warm(key, now) {
            return Some(value);
        }
        if let Some(value) = self.promote_from_frozen(key, now) {
            return Some(value);
        }
        self.rebuild_from_hook(key, now)
    }

    pub fn remove(&mut self, key: &str) -> bool {
        let hot = self.hot.remove(key);
        let warm = self.warm.remove(key).unwrap_or(false);
        let frozen = self.frozen.remove(key).unwrap_or(false);
        hot || warm || frozen
    }

    pub fn upsert_keyed(&mut self, class: &str, id: &str, value: Value) -> Result<(), MemoryError> {
        self.upsert(format!("{class}:{id}"), value, class)
    }

    pub fn get_keyed(&mut self, class: &str, id: &str) -> Option<Value> {
        self.get(&format!("{class}:{id}"))
    }

    /// Señal de presión: flush deferred y demote/evict una entrada Hot.
    pub fn notify_pressure(&mut self) -> Result<bool, MemoryError> {
        let now = Instant::now();
        self.flush_deferred_at(now, true)?;
        if let Some((key, entry)) = self.hot.reclaim_one(now) {
            if let Err(error) = self.demote_or_drop(&key, entry.clone()) {
                self.hot.restore(key, entry);
                return Err(error);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Fuerza el flush de toda la cola deferred.
    pub fn flush(&mut self) -> Result<usize, MemoryError> {
        self.flush_deferred_at(Instant::now(), true)
    }

    /// Flush de registros cuyo intervalo ya venció.
    pub fn poll(&mut self) -> Result<usize, MemoryError> {
        self.flush_deferred_at(Instant::now(), false)
    }

    pub fn snapshot(&self) -> MemorySnapshot {
        let HotMetrics {
            objects,
            evictions,
            expired_removals,
        } = self.hot.metrics();
        MemorySnapshot {
            hot_objects: objects,
            warm_objects: self.warm.len() as u64,
            frozen_objects: self.frozen.len() as u64,
            warm_backend: self.warm.name(),
            frozen_backend: self.frozen.name(),
            warm_puts: self.warm_puts,
            warm_hits: self.warm_hits,
            warm_misses: self.warm_misses,
            frozen_puts: self.frozen_puts,
            frozen_hits: self.frozen_hits,
            frozen_misses: self.frozen_misses,
            promotions: self.promotions,
            demotions: self.demotions,
            demotion_failures: self.demotion_failures,
            freezes: self.freezes,
            rebuilds: self.rebuilds,
            rebuild_misses: self.rebuild_misses,
            rebuild_failures: self.rebuild_failures,
            evictions,
            expired_removals,
            max_entries: self.policy.max_entries as u64,
            immediate_writes: self.immediate_writes,
            immediate_failures: self.immediate_failures,
            persist_queue: self.deferred.len() as u64,
            deferred_writes: self.deferred_writes,
            deferred_failures: self.deferred_failures,
            deferred_flushes: self.deferred.flushes(),
            deferred_enqueued: self.deferred.enqueued_total(),
        }
    }

    fn should_mirror_warm(&self, class: &ClassPolicy) -> bool {
        // Solo clases cache con temperatura warm espejan / demotean a WarmStore.
        matches!(class.policy, Policy::Cache) && matches!(class.temperature, Temperature::Warm)
    }

    fn should_archive_frozen(&self, class: &ClassPolicy) -> bool {
        matches!(class.temperature, Temperature::Frozen)
    }

    fn hot_upsert(
        &mut self,
        key: String,
        value: Value,
        class: &ClassPolicy,
        now: Instant,
    ) -> Result<(), MemoryError> {
        let victims = self.hot.upsert(key, value, class, now)?;
        self.apply_demotions(victims)
    }

    fn apply_demotions(&mut self, victims: Vec<(String, HotEntry)>) -> Result<(), MemoryError> {
        for (key, entry) in victims {
            if let Err(error) = self.demote_or_drop(&key, entry.clone()) {
                self.hot.restore(key, entry);
                return Err(error);
            }
        }
        Ok(())
    }

    fn demote_or_drop(&mut self, key: &str, entry: HotEntry) -> Result<(), MemoryError> {
        let class = match self.policy.class(&entry.class) {
            Ok(class) => class.clone(),
            Err(_) => return Ok(()),
        };
        if self.should_archive_frozen(&class) {
            return match self.frozen.put(
                key,
                FrozenEntry {
                    class: entry.class,
                    value: entry.value,
                },
            ) {
                Ok(()) => {
                    self.frozen_puts += 1;
                    self.freezes += 1;
                    self.demotions += 1;
                    Ok(())
                }
                Err(error) => {
                    self.demotion_failures += 1;
                    Err(error)
                }
            };
        }
        if !self.should_mirror_warm(&class) {
            return Ok(());
        }
        match self.warm.put(
            key,
            WarmEntry {
                class: entry.class,
                value: entry.value,
            },
        ) {
            Ok(()) => {
                self.warm_puts += 1;
                self.demotions += 1;
                Ok(())
            }
            Err(error) => {
                self.demotion_failures += 1;
                Err(error)
            }
        }
    }

    fn warm_put(&mut self, key: &str, entry: WarmEntry) -> Result<(), MemoryError> {
        self.warm.put(key, entry)?;
        self.warm_puts += 1;
        Ok(())
    }

    fn frozen_put(&mut self, key: &str, entry: FrozenEntry) -> Result<(), MemoryError> {
        self.frozen.put(key, entry)?;
        self.frozen_puts += 1;
        Ok(())
    }

    /// Hot miss → Warm hit → rehidrata Hot (promote).
    fn promote_from_warm(&mut self, key: &str, now: Instant) -> Option<Value> {
        let entry = match self.warm.get(key) {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                self.warm_misses += 1;
                return None;
            }
            Err(_) => {
                self.warm_misses += 1;
                return None;
            }
        };
        self.warm_hits += 1;
        if let Ok(class_policy) = self.policy.class(&entry.class).cloned()
            && self
                .hot_upsert(key.to_owned(), entry.value.clone(), &class_policy, now)
                .is_ok()
        {
            self.promotions += 1;
        }
        Some(entry.value)
    }

    /// Hot+Warm miss → Frozen hit → rehidrata Hot.
    fn promote_from_frozen(&mut self, key: &str, now: Instant) -> Option<Value> {
        let entry = match self.frozen.get(key) {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                self.frozen_misses += 1;
                return None;
            }
            Err(_) => {
                self.frozen_misses += 1;
                return None;
            }
        };
        self.frozen_hits += 1;
        if let Ok(class_policy) = self.policy.class(&entry.class).cloned()
            && self
                .hot_upsert(key.to_owned(), entry.value.clone(), &class_policy, now)
                .is_ok()
        {
            self.promotions += 1;
        }
        Some(entry.value)
    }

    /// Hot+Warm miss → rebuild hook (solo si la clase declara `rebuild`).
    fn rebuild_from_hook(&mut self, key: &str, now: Instant) -> Option<Value> {
        let class_name = key.split_once(':').map(|(class, _)| class)?;
        let class_policy = self.policy.class(class_name).ok()?.clone();
        let rebuild_ref = class_policy.rebuild?;
        self.rebuild.as_ref()?;
        let result = self
            .rebuild
            .as_mut()
            .expect("checked")
            .rebuild(class_name, key, &rebuild_ref);
        match result {
            Ok(Some(value)) => {
                self.rebuilds += 1;
                let _ = self.upsert_at(key, value.clone(), class_name, now);
                Some(value)
            }
            Ok(None) => {
                self.rebuild_misses += 1;
                None
            }
            Err(_) => {
                self.rebuild_failures += 1;
                None
            }
        }
    }

    fn persist_now(&mut self, record: PersistRecord) -> Result<(), MemoryError> {
        let sink = self
            .persist
            .as_mut()
            .ok_or(MemoryError::MissingImmediateSink)?;
        if let Err(error) = sink.persist(&record) {
            self.immediate_failures += 1;
            return Err(error);
        }
        self.immediate_writes += 1;
        Ok(())
    }

    fn flush_deferred_at(&mut self, now: Instant, force: bool) -> Result<usize, MemoryError> {
        let batch = self.deferred.take_ready(now, force);
        if batch.is_empty() {
            return Ok(0);
        }
        let sink = self
            .persist
            .as_mut()
            .ok_or(MemoryError::MissingImmediateSink)?;
        let mut written = 0usize;
        let mut records = batch.into_iter();
        while let Some(record) = records.next() {
            if let Err(error) = sink.persist(&record) {
                self.deferred_failures += 1;
                self.deferred
                    .requeue_failed(std::iter::once(record).chain(records), now);
                return Err(error);
            }
            self.deferred_writes += 1;
            written += 1;
        }
        Ok(written)
    }
}

impl Drop for MemoryManager {
    fn drop(&mut self) {
        let _ = self.flush_deferred_at(Instant::now(), true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sink::RecordingSink,
        warm::{RecordingWarmStore, WarmEntry, WarmStore},
    };
    use serde_json::json;
    use std::time::Duration;

    fn hot_only(max: usize) -> MemoryManager {
        MemoryManager::from_yaml(&format!(
            r#"
memory:
  max_entries: {max}
  classes:
    telegram:
      policy: volatile
      ttl: 5m
      priority: low
    carrier:
      policy: cache
      ttl: 30m
      priority: high
    safety:
      policy: cache
      ttl: 1h
      priority: critical
"#
        ))
        .unwrap()
    }

    #[test]
    fn upsert_get_and_unknown_class() {
        let mut mm = hot_only(10);
        mm.upsert_keyed("telegram", "chute-1", json!({"raw": "PING"}))
            .unwrap();
        assert_eq!(
            mm.get_keyed("telegram", "chute-1"),
            Some(json!({"raw": "PING"}))
        );
        assert!(mm.upsert("x", json!(1), "missing").is_err());
    }

    #[test]
    fn expires_by_ttl() {
        let mut mm = hot_only(10);
        let t0 = Instant::now();
        mm.upsert_at("telegram:1", json!(1), "telegram", t0)
            .unwrap();
        assert!(
            mm.get_at("telegram:1", t0 + Duration::from_secs(10))
                .is_some()
        );
        assert!(
            mm.get_at("telegram:1", t0 + Duration::from_secs(5 * 60 + 1))
                .is_none()
        );
        assert_eq!(mm.snapshot().expired_removals, 1);
    }

    #[test]
    fn evicts_low_priority_before_high_under_pressure() {
        let mut mm = hot_only(2);
        let t0 = Instant::now();
        mm.upsert_at("telegram:a", json!("a"), "telegram", t0)
            .unwrap();
        mm.upsert_at(
            "carrier:1",
            json!("c"),
            "carrier",
            t0 + Duration::from_millis(1),
        )
        .unwrap();
        mm.upsert_at(
            "telegram:b",
            json!("b"),
            "telegram",
            t0 + Duration::from_millis(2),
        )
        .unwrap();
        assert!(
            mm.get_at("telegram:a", t0 + Duration::from_secs(1))
                .is_none()
        );
        assert!(
            mm.get_at("carrier:1", t0 + Duration::from_secs(1))
                .is_some()
        );
        assert!(mm.snapshot().evictions >= 1);
    }

    #[test]
    fn never_evicts_critical() {
        let mut mm = hot_only(1);
        let t0 = Instant::now();
        mm.upsert_at("safety:1", json!("keep"), "safety", t0)
            .unwrap();
        let result = mm.upsert_at(
            "telegram:1",
            json!("temp"),
            "telegram",
            t0 + Duration::from_millis(1),
        );
        assert!(matches!(result, Err(MemoryError::CriticalCapacity { .. })));
        assert!(mm.get_at("safety:1", t0 + Duration::from_secs(1)).is_some());
    }

    #[test]
    fn rejects_growth_when_capacity_contains_only_critical_entries() {
        let mut mm = hot_only(1);
        mm.upsert_keyed("safety", "1", json!("keep")).unwrap();
        let error = mm.upsert_keyed("safety", "2", json!("reject")).unwrap_err();
        assert!(matches!(
            error,
            MemoryError::CriticalCapacity { max_entries: 1 }
        ));
        assert_eq!(mm.snapshot().hot_objects, 1);
        assert!(mm.get_keyed("safety", "1").is_some());
    }

    #[test]
    fn deferred_failure_requeues_the_entire_unwritten_tail() {
        #[derive(Default)]
        struct FailFirstSink(bool);
        impl ImmediateSink for FailFirstSink {
            fn persist(&mut self, _record: &PersistRecord) -> Result<(), MemoryError> {
                if !self.0 {
                    self.0 = true;
                    return Err(MemoryError::Persistence("first write fails".into()));
                }
                Ok(())
            }
        }
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  max_pending_deferred: 10
  classes:
    inventory:
      policy: deferred
      flush: 1h
"#,
        )
        .unwrap();
        let mut mm = MemoryManager::open_with_sink(policy, FailFirstSink::default()).unwrap();
        mm.upsert_keyed("inventory", "a", json!(1)).unwrap();
        mm.upsert_keyed("inventory", "b", json!(2)).unwrap();
        assert!(mm.flush().is_err());
        assert_eq!(mm.snapshot().persist_queue, 2);
        assert_eq!(mm.flush().unwrap(), 2);
        assert_eq!(mm.snapshot().persist_queue, 0);
    }

    #[test]
    fn loads_example_policy_file() {
        let yaml = include_str!("../../../examples/jme-hot-policy.yaml");
        let mm = MemoryManager::from_yaml(yaml).unwrap();
        assert_eq!(mm.policy().max_entries, 1000);
        assert!(mm.policy().class("carrier").is_ok());
    }

    #[test]
    fn immediate_requires_sink_at_construction() {
        let result = MemoryManager::from_yaml(
            r#"
memory:
  classes:
    alarm:
      policy: immediate
"#,
        );
        assert!(matches!(result, Err(MemoryError::MissingImmediateSink)));
    }

    #[test]
    fn immediate_persists_before_hot() {
        struct SharedSink {
            inner: std::sync::Arc<std::sync::Mutex<RecordingSink>>,
        }
        impl ImmediateSink for SharedSink {
            fn persist(&mut self, record: &PersistRecord) -> Result<(), MemoryError> {
                self.inner.lock().expect("lock").persist(record)
            }
        }
        let inner = std::sync::Arc::new(std::sync::Mutex::new(RecordingSink::default()));
        let mut mm = MemoryManager::from_yaml_with_sink(
            r#"
memory:
  classes:
    alarm:
      policy: immediate
"#,
            SharedSink {
                inner: inner.clone(),
            },
        )
        .unwrap();

        mm.upsert_keyed("alarm", "STOP_LINE", json!({"code": "E-STOP"}))
            .unwrap();
        assert_eq!(
            mm.get_keyed("alarm", "STOP_LINE"),
            Some(json!({"code": "E-STOP"}))
        );
        let records = inner.lock().unwrap();
        assert_eq!(records.records.len(), 1);
        assert_eq!(records.records[0].key, "alarm:STOP_LINE");
        assert_eq!(mm.snapshot().immediate_writes, 1);
        assert_eq!(mm.snapshot().immediate_failures, 0);
    }

    #[test]
    fn immediate_failure_does_not_update_hot() {
        struct FailSink;
        impl ImmediateSink for FailSink {
            fn persist(&mut self, _: &PersistRecord) -> Result<(), MemoryError> {
                Err(MemoryError::Persistence("disk full".to_owned()))
            }
        }
        let mut mm = MemoryManager::from_yaml_with_sink(
            r#"
memory:
  classes:
    alarm:
      policy: immediate
"#,
            FailSink,
        )
        .unwrap();
        let error = mm.upsert_keyed("alarm", "x", json!({"a": 1})).unwrap_err();
        assert!(error.to_string().contains("disk full"));
        assert!(mm.get_keyed("alarm", "x").is_none());
        assert_eq!(mm.snapshot().immediate_failures, 1);
        assert_eq!(mm.snapshot().immediate_writes, 0);
    }

    #[test]
    fn jsonl_sink_and_example_policy() {
        // Evitar /tmp: en algunos hosts la cuota de /tmp está llena.
        let dir = test_scratch_dir("immediate");
        let path = dir.join("alarms.jsonl");
        let yaml = include_str!("../../../examples/jme-immediate-policy.yaml");
        let mut mm =
            MemoryManager::from_yaml_with_sink(yaml, crate::sink::JsonlFileSink::new(&path))
                .unwrap();
        mm.upsert_keyed("alarm", "E1", json!({"level": "critical"}))
            .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("alarm:E1"));
        assert!(text.contains("critical"));
        let _ = std::fs::remove_dir_all(dir);
    }

    fn test_scratch_dir(label: &str) -> std::path::PathBuf {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/jme-test")
            .join(format!(
                "{label}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        std::fs::create_dir_all(&dir).expect("create jme test scratch dir");
        dir
    }

    fn shared_sink() -> (
        MemoryManager,
        std::sync::Arc<std::sync::Mutex<RecordingSink>>,
    ) {
        struct SharedSink {
            inner: std::sync::Arc<std::sync::Mutex<RecordingSink>>,
        }
        impl ImmediateSink for SharedSink {
            fn persist(&mut self, record: &PersistRecord) -> Result<(), MemoryError> {
                self.inner.lock().expect("lock").persist(record)
            }
        }
        let inner = std::sync::Arc::new(std::sync::Mutex::new(RecordingSink::default()));
        let mm = MemoryManager::from_yaml_with_sink(
            r#"
memory:
  max_pending_deferred: 3
  classes:
    inventory:
      policy: deferred
      flush: 2s
      priority: high
    telegram:
      policy: volatile
      ttl: 1m
      priority: low
"#,
            SharedSink {
                inner: inner.clone(),
            },
        )
        .unwrap();
        (mm, inner)
    }

    #[test]
    fn deferred_buffers_until_flush_interval() {
        let (mut mm, sink) = shared_sink();
        let t0 = Instant::now();
        mm.upsert_at("inventory:1", json!({"qty": 1}), "inventory", t0)
            .unwrap();
        assert_eq!(mm.get_at("inventory:1", t0), Some(json!({"qty": 1})));
        assert!(sink.lock().unwrap().records.is_empty());
        assert_eq!(mm.snapshot().persist_queue, 1);

        // Aún no venció el flush de 2s.
        mm.flush_deferred_at(t0 + Duration::from_secs(1), false)
            .unwrap();
        assert!(sink.lock().unwrap().records.is_empty());

        let written = mm
            .flush_deferred_at(t0 + Duration::from_secs(2), false)
            .unwrap();
        assert_eq!(written, 1);
        assert_eq!(sink.lock().unwrap().records.len(), 1);
        assert_eq!(mm.snapshot().persist_queue, 0);
        assert_eq!(mm.snapshot().deferred_writes, 1);
    }

    #[test]
    fn deferred_coalesces_same_key() {
        let (mut mm, sink) = shared_sink();
        let t0 = Instant::now();
        mm.upsert_at("inventory:1", json!({"qty": 1}), "inventory", t0)
            .unwrap();
        mm.upsert_at(
            "inventory:1",
            json!({"qty": 9}),
            "inventory",
            t0 + Duration::from_millis(10),
        )
        .unwrap();
        assert_eq!(mm.snapshot().persist_queue, 1);
        mm.flush_deferred_at(t0 + Duration::from_secs(3), false)
            .unwrap();
        let records = sink.lock().unwrap();
        assert_eq!(records.records.len(), 1);
        assert_eq!(records.records[0].value, json!({"qty": 9}));
    }

    #[test]
    fn deferred_max_pending_forces_flush() {
        let (mut mm, sink) = shared_sink();
        let t0 = Instant::now();
        mm.upsert_at("inventory:a", json!(1), "inventory", t0)
            .unwrap();
        mm.upsert_at("inventory:b", json!(2), "inventory", t0)
            .unwrap();
        assert_eq!(sink.lock().unwrap().records.len(), 0);
        // max_pending_deferred = 3 → al tercer distinto fuerza flush.
        mm.upsert_at("inventory:c", json!(3), "inventory", t0)
            .unwrap();
        assert_eq!(sink.lock().unwrap().records.len(), 3);
        assert_eq!(mm.snapshot().persist_queue, 0);
    }

    #[test]
    fn deferred_example_policy_loads() {
        let yaml = include_str!("../../../examples/jme-deferred-policy.yaml");
        let mm = MemoryManager::from_yaml_with_sink(yaml, RecordingSink::default()).unwrap();
        assert!(mm.policy().class("inventory").is_ok());
        assert_eq!(
            mm.policy().class("inventory").unwrap().policy,
            Policy::Deferred
        );
    }

    #[test]
    fn warm_noop_backend_compiles_and_snapshots() {
        let yaml = include_str!("../../../examples/jme-warm-policy.yaml");
        let mut mm = MemoryManager::from_yaml(yaml).unwrap();
        assert_eq!(mm.policy().warm_backend, crate::policy::WarmBackend::None);
        mm.upsert_keyed("carrier", "A12", json!({"lane": 3}))
            .unwrap();
        let snap = mm.snapshot();
        assert_eq!(snap.warm_backend, "none");
        assert_eq!(snap.warm_objects, 0);
        assert_eq!(snap.warm_puts, 1); // intentado; noop no retiene
        assert_eq!(mm.get_keyed("carrier", "A12"), Some(json!({"lane": 3})));
    }

    #[test]
    fn warm_recording_promotes_on_hot_miss() {
        use std::sync::{Arc, Mutex};

        struct SharedWarm {
            inner: Arc<Mutex<RecordingWarmStore>>,
        }
        impl WarmStore for SharedWarm {
            fn get(&self, key: &str) -> Result<Option<WarmEntry>, MemoryError> {
                self.inner.lock().expect("lock").get(key)
            }
            fn put(&mut self, key: &str, entry: WarmEntry) -> Result<(), MemoryError> {
                self.inner.lock().expect("lock").put(key, entry)
            }
            fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
                self.inner.lock().expect("lock").remove(key)
            }
            fn len(&self) -> usize {
                self.inner.lock().expect("lock").len()
            }
            fn name(&self) -> &'static str {
                "recording"
            }
        }

        let store = Arc::new(Mutex::new(RecordingWarmStore::default()));
        let mut mm = MemoryManager::from_yaml_with_warm(
            r#"
memory:
  warm:
    backend: none
  max_entries: 10
  classes:
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      priority: high
"#,
            SharedWarm {
                inner: store.clone(),
            },
        )
        .unwrap();

        mm.upsert_keyed("carrier", "A12", json!({"lane": 3}))
            .unwrap();
        assert_eq!(store.lock().unwrap().len(), 1);
        assert_eq!(mm.snapshot().warm_puts, 1);

        // Simula eviction de Hot; Warm sigue teniendo el valor.
        assert!(mm.remove("carrier:A12"));
        // remove limpia warm también — re-put solo en warm para forzar promote.
        store
            .lock()
            .unwrap()
            .put(
                "carrier:A12",
                WarmEntry {
                    class: "carrier".into(),
                    value: json!({"lane": 3}),
                },
            )
            .unwrap();
        assert!(mm.get_keyed("carrier", "A12").is_some());
        assert_eq!(mm.snapshot().warm_hits, 1);
        assert_eq!(mm.snapshot().promotions, 1);
        // Segunda lectura ya viene de Hot.
        assert!(mm.get_keyed("carrier", "A12").is_some());
        assert_eq!(mm.snapshot().promotions, 1);
    }

    #[test]
    fn volatile_does_not_mirror_warm() {
        let warm = RecordingWarmStore::default();
        let mut mm = MemoryManager::from_yaml_with_warm(
            r#"
memory:
  classes:
    telegram:
      policy: volatile
      ttl: 5m
      priority: low
"#,
            warm,
        )
        .unwrap();
        mm.upsert_keyed("telegram", "1", json!(1)).unwrap();
        assert_eq!(mm.snapshot().warm_puts, 0);
    }

    #[test]
    fn demotes_cache_warm_on_capacity_eviction() {
        let mut mm = MemoryManager::from_yaml_with_warm(
            r#"
memory:
  max_entries: 1
  warm:
    backend: none
  classes:
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      priority: high
"#,
            RecordingWarmStore::default(),
        )
        .unwrap();
        let t0 = Instant::now();
        mm.upsert_at("carrier:A", json!({"id": "A"}), "carrier", t0)
            .unwrap();
        mm.upsert_at(
            "carrier:B",
            json!({"id": "B"}),
            "carrier",
            t0 + Duration::from_millis(1),
        )
        .unwrap();
        assert!(mm.snapshot().demotions >= 1);
        assert!(mm.snapshot().evictions >= 1);
        // A quedó en Warm → promote al leer.
        assert_eq!(
            mm.get_at("carrier:A", t0 + Duration::from_secs(1)),
            Some(json!({"id": "A"}))
        );
        assert_eq!(mm.snapshot().promotions, 1);
    }

    #[test]
    fn notify_pressure_demotes_warm_class() {
        let mut mm = MemoryManager::from_yaml_with_warm(
            r#"
memory:
  max_entries: 10
  classes:
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      priority: normal
"#,
            RecordingWarmStore::default(),
        )
        .unwrap();
        mm.upsert_keyed("carrier", "X", json!(1)).unwrap();
        assert!(mm.notify_pressure().unwrap());
        assert_eq!(mm.snapshot().demotions, 1);
        assert_eq!(mm.snapshot().hot_objects, 0);
        assert_eq!(mm.get_keyed("carrier", "X"), Some(json!(1)));
        assert_eq!(mm.snapshot().promotions, 1);
    }

    #[test]
    fn rebuild_on_hot_and_warm_miss() {
        use crate::rebuild::MapRebuildHook;
        let mut hook = MapRebuildHook::default();
        hook.values.insert("carrier:A12".into(), json!({"lane": 9}));
        let mut mm = MemoryManager::from_yaml_with_rebuild(
            r#"
memory:
  classes:
    carrier:
      policy: cache
      temperature: warm
      ttl: 30m
      rebuild: query:carrier_by_id
"#,
            hook,
        )
        .unwrap();
        // Miss total (noop warm) → rebuild.
        assert_eq!(mm.get_keyed("carrier", "A12"), Some(json!({"lane": 9})));
        assert_eq!(mm.snapshot().rebuilds, 1);
        assert_eq!(mm.snapshot().hot_objects, 1);
    }

    #[test]
    fn lifecycle_example_policy_loads() {
        let yaml = include_str!("../../../examples/jme-lifecycle-policy.yaml");
        let mm = MemoryManager::from_yaml(yaml).unwrap();
        assert_eq!(
            mm.policy().class("carrier").unwrap().rebuild.as_deref(),
            Some("query:carrier_by_id")
        );
    }

    #[test]
    fn frozen_recording_promotes_after_pressure() {
        use crate::frozen::RecordingFrozenStore;

        let mut mm = MemoryManager::with_frozen_store(
            MemoryPolicy::from_yaml(
                r#"
memory:
  frozen:
    backend: file
    path: /tmp/unused
  classes:
    audit_event:
      policy: cache
      temperature: frozen
      ttl: 1h
      priority: low
"#,
            )
            .unwrap(),
            RecordingFrozenStore::default(),
        )
        .unwrap();

        mm.upsert_keyed("audit_event", "E1", json!({"n": 1}))
            .unwrap();
        assert_eq!(mm.snapshot().frozen_puts, 1);
        assert!(mm.notify_pressure().unwrap()); // Hot → Frozen (ya archivado)
        assert_eq!(mm.snapshot().hot_objects, 0);
        assert!(mm.snapshot().freezes >= 1);
        assert_eq!(mm.get_keyed("audit_event", "E1"), Some(json!({"n": 1})));
        assert_eq!(mm.snapshot().frozen_hits, 1);
        assert_eq!(mm.snapshot().promotions, 1);
    }

    #[test]
    fn open_redis_without_feature_fails() {
        let policy = MemoryPolicy::from_yaml(
            r#"
memory:
  warm:
    backend: redis
  classes:
    carrier:
      policy: cache
      ttl: 30m
"#,
        )
        .unwrap();
        let result = MemoryManager::open(policy);
        #[cfg(not(feature = "redis"))]
        assert!(matches!(result, Err(MemoryError::RedisFeatureDisabled)));
        #[cfg(feature = "redis")]
        {
            // Sin REDIS_URL debe fallar por configuración.
            assert!(result.is_err());
        }
    }

    #[test]
    fn frozen_example_policy_opens_file_store() {
        let dir = test_scratch_dir("frozen");
        let yaml = format!(
            r#"
memory:
  frozen:
    backend: file
    path: {}
  classes:
    audit_event:
      policy: cache
      temperature: frozen
      ttl: 1h
"#,
            dir.display()
        );
        let mut mm = MemoryManager::from_yaml(&yaml).unwrap();
        assert_eq!(mm.snapshot().frozen_backend, "file");
        mm.upsert_keyed("audit_event", "Z", json!({"z": 1}))
            .unwrap();
        assert!(mm.snapshot().frozen_objects >= 1);
        let _ = std::fs::remove_dir_all(dir);
    }
}

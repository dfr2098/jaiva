use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use serde_json::Value;

use crate::{
    error::MemoryError,
    policy::{ClassPolicy, Priority},
};

#[derive(Debug, Clone)]
pub struct HotEntry {
    pub class: String,
    pub value: Value,
    pub priority: Priority,
    pub expires_at: Option<Instant>,
    pub last_access: Instant,
    pub access_count: u64,
    pub demote_after: Option<Duration>,
    pub size_bytes: usize,
}

#[derive(Debug, Default, Clone)]
pub struct HotMetrics {
    pub objects: u64,
    pub bytes: u64,
    pub evictions: u64,
    pub expired_removals: u64,
}

/// Almacén Hot en proceso: TTL + eviction por prioridad y LRU.
#[derive(Debug)]
pub struct HotStore {
    entries: HashMap<String, HotEntry>,
    max_entries: usize,
    max_bytes: u64,
    evictions: u64,
    expired_removals: u64,
}

/// Admission prepared without changing Hot; commit only after external writes succeed.
pub(crate) struct PreparedUpsert {
    key: String,
    entry: HotEntry,
    now: Instant,
    pub(crate) victims: Vec<(String, HotEntry)>,
}

impl HotStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.max(1),
            max_bytes: 64 * 1024 * 1024,
            evictions: 0,
            expired_removals: 0,
        }
    }

    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn metrics(&self) -> HotMetrics {
        HotMetrics {
            objects: self.entries.len() as u64,
            bytes: self
                .entries
                .iter()
                .map(|(key, entry)| key.len().saturating_add(entry.size_bytes) as u64)
                .sum(),
            evictions: self.evictions,
            expired_removals: self.expired_removals,
        }
    }

    /// Inserta o actualiza. Devuelve víctimas de eviction (para demote en el manager).
    pub fn upsert(
        &mut self,
        key: String,
        value: Value,
        class: &ClassPolicy,
        now: Instant,
    ) -> Result<Vec<(String, HotEntry)>, MemoryError> {
        let prepared = self.prepare_upsert(key, value, class, now)?;
        Ok(self.commit_upsert(prepared))
    }

    pub(crate) fn prepare_upsert(
        &self,
        key: String,
        value: Value,
        class: &ClassPolicy,
        now: Instant,
    ) -> Result<PreparedUpsert, MemoryError> {
        let size_bytes = serde_json::to_vec(&value)
            .map_err(|error| MemoryError::Configuration(error.to_string()))?
            .len();
        let live = |entry: &HotEntry| !entry.expires_at.is_some_and(|deadline| now >= deadline);
        let entry_bytes =
            |key: &str, entry: &HotEntry| key.len().saturating_add(entry.size_bytes) as u64;
        let incoming_bytes = key.len().saturating_add(size_bytes) as u64;
        if incoming_bytes > self.max_bytes {
            return Err(MemoryError::HotByteCapacity {
                requested_bytes: incoming_bytes,
                max_bytes: self.max_bytes,
            });
        }
        let replaces_live = self.entries.get(&key).is_some_and(live);
        let needed = if replaces_live {
            0
        } else {
            (self.entries.values().filter(|entry| live(entry)).count() + 1)
                .saturating_sub(self.max_entries)
        };
        let mut resident_bytes = self
            .entries
            .iter()
            .filter(|(existing, entry)| existing.as_str() != key && live(entry))
            .map(|(existing, entry)| entry_bytes(existing, entry))
            .sum::<u64>();
        let mut victims: Vec<(String, HotEntry)> = Vec::new();
        while victims.len() < needed
            || resident_bytes.saturating_add(incoming_bytes) > self.max_bytes
        {
            let Some((victim_key, victim)) = self
                .entries
                .iter()
                .filter(|(existing, entry)| {
                    existing.as_str() != key
                        && live(entry)
                        && entry.priority < Priority::Critical
                        && !victims.iter().any(|(selected, _)| selected == *existing)
                })
                .min_by(|(_, left), (_, right)| {
                    left.priority
                        .cmp(&right.priority)
                        .then_with(|| left.access_count.cmp(&right.access_count))
                        .then_with(|| right.size_bytes.cmp(&left.size_bytes))
                        .then_with(|| left.last_access.cmp(&right.last_access))
                })
            else {
                return Err(if victims.len() < needed {
                    MemoryError::CriticalCapacity {
                        max_entries: self.max_entries,
                    }
                } else {
                    MemoryError::HotByteCapacity {
                        requested_bytes: resident_bytes.saturating_add(incoming_bytes),
                        max_bytes: self.max_bytes,
                    }
                });
            };
            resident_bytes = resident_bytes.saturating_sub(entry_bytes(victim_key, victim));
            victims.push((victim_key.clone(), victim.clone()));
        }
        let expires_at = class.ttl.map(|ttl| now + ttl);
        Ok(PreparedUpsert {
            key,
            entry: HotEntry {
                class: class.name.clone(),
                value,
                priority: class.priority,
                expires_at,
                last_access: now,
                access_count: 1,
                demote_after: class.demote_after,
                size_bytes,
            },
            now,
            victims,
        })
    }

    /// The store must not be mutated between prepare and commit.
    pub(crate) fn commit_upsert(&mut self, prepared: PreparedUpsert) -> Vec<(String, HotEntry)> {
        self.purge_expired(prepared.now);
        for (key, _) in &prepared.victims {
            self.entries.remove(key);
            self.evictions += 1;
        }
        self.entries.insert(prepared.key, prepared.entry);
        prepared.victims
    }

    pub fn get(&mut self, key: &str, now: Instant) -> Option<Value> {
        self.purge_expired(now);
        let entry = self.entries.get_mut(key)?;
        if entry.expires_at.is_some_and(|deadline| now >= deadline) {
            self.entries.remove(key);
            self.expired_removals += 1;
            return None;
        }
        entry.last_access = now;
        entry.access_count = entry.access_count.saturating_add(1);
        Some(entry.value.clone())
    }

    pub fn remove(&mut self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }

    pub(crate) fn restore(&mut self, key: String, entry: HotEntry) {
        self.entries.insert(key, entry);
    }

    /// Fuerza presión: libera una entrada no critical y la devuelve (demote).
    pub fn reclaim_one(&mut self, now: Instant) -> Option<(String, HotEntry)> {
        self.purge_expired(now);
        self.evict_one()
    }

    /// Extrae hasta `limit` entradas cuya ventana de inactividad venció.
    pub fn reclaim_idle(&mut self, now: Instant, limit: usize) -> Vec<(String, HotEntry)> {
        self.purge_expired(now);
        let mut candidates = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.priority < Priority::Critical
                    && entry.demote_after.is_some_and(|idle| {
                        now.checked_duration_since(entry.last_access)
                            .is_some_and(|elapsed| elapsed >= idle)
                    })
            })
            .map(|(key, entry)| {
                (
                    key.clone(),
                    entry.priority,
                    entry.access_count,
                    entry.size_bytes,
                    entry.last_access,
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| left.2.cmp(&right.2))
                .then_with(|| right.3.cmp(&left.3))
                .then_with(|| left.4.cmp(&right.4))
        });
        candidates
            .into_iter()
            .take(limit)
            .filter_map(|(key, _, _, _, _)| {
                self.entries.remove(&key).map(|entry| {
                    self.evictions += 1;
                    (key, entry)
                })
            })
            .collect()
    }

    fn purge_expired(&mut self, now: Instant) {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| !entry.expires_at.is_some_and(|deadline| now >= deadline));
        self.expired_removals += (before - self.entries.len()) as u64;
    }

    fn evict_one(&mut self) -> Option<(String, HotEntry)> {
        let victim_key = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.priority < Priority::Critical)
            .min_by(|(_, left), (_, right)| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| left.access_count.cmp(&right.access_count))
                    .then_with(|| right.size_bytes.cmp(&left.size_bytes))
                    .then_with(|| left.last_access.cmp(&right.last_access))
            })
            .map(|(key, _)| key.clone())?;
        let entry = self.entries.remove(&victim_key)?;
        self.evictions += 1;
        Some((victim_key, entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::MemoryPolicy;

    #[test]
    fn byte_limit_evicts_non_critical_entries() {
        let policy = MemoryPolicy::from_yaml(
            "memory:\n  classes:\n    cache:\n      policy: volatile\n      ttl: 5m\n",
        )
        .unwrap();
        let class = policy.class("cache").unwrap();
        let now = Instant::now();
        let mut hot = HotStore::new(10).with_max_bytes(10);
        hot.upsert("a".into(), serde_json::json!("1234567"), class, now)
            .unwrap();
        assert_eq!(hot.metrics().bytes, 10);
        let victims = hot.upsert("b".into(), Value::Null, class, now).unwrap();
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].0, "a");
        assert_eq!(hot.get("a", now), None);
        assert_eq!(hot.get("b", now), Some(Value::Null));
        assert_eq!(hot.metrics().evictions, 1);

        hot.upsert("c".into(), Value::Null, class, now).unwrap();
        assert_eq!(hot.metrics().bytes, 10);
        let victims = hot
            .upsert("b".into(), serde_json::json!("12345"), class, now)
            .unwrap();
        assert_eq!(victims.len(), 1);
        assert_eq!(victims[0].0, "c");
        assert_eq!(hot.metrics().bytes, 8);
    }

    #[test]
    fn byte_limit_rejects_oversized_values_and_never_evicts_critical() {
        let policy = MemoryPolicy::from_yaml(
            "memory:\n  classes:\n    cache:\n      policy: volatile\n      ttl: 5m\n    safety:\n      policy: cache\n      ttl: 5m\n      priority: critical\n",
        )
        .unwrap();
        let cache = policy.class("cache").unwrap();
        let safety = policy.class("safety").unwrap();
        let now = Instant::now();
        let mut hot = HotStore::new(10).with_max_bytes(10);
        assert!(matches!(
            hot.upsert("a".into(), serde_json::json!("123456789"), cache, now),
            Err(MemoryError::HotByteCapacity { .. })
        ));
        hot.upsert("s".into(), serde_json::json!("1234567"), safety, now)
            .unwrap();
        assert!(matches!(
            hot.upsert("b".into(), Value::Null, cache, now),
            Err(MemoryError::HotByteCapacity { .. })
        ));
        assert_eq!(hot.get("s", now), Some(serde_json::json!("1234567")));
        assert_eq!(hot.metrics().evictions, 0);
    }

    #[test]
    fn policy_validates_byte_budget() {
        assert!(MemoryPolicy::from_yaml("memory:\n  max_hot_bytes: 0").is_err());
        assert_eq!(
            MemoryPolicy::from_yaml("memory:\n  max_hot_bytes: 1234\n  classes:\n    v:\n      policy: volatile\n      ttl: 5m")
                .unwrap()
                .max_hot_bytes,
            1234
        );
        assert_eq!(
            MemoryPolicy::from_yaml(
                "memory:\n  classes:\n    v:\n      policy: volatile\n      ttl: 5m"
            )
            .unwrap()
            .max_hot_bytes,
            64 * 1024 * 1024
        );
    }
}

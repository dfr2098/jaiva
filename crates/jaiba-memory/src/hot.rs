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
    evictions: u64,
    expired_removals: u64,
}

impl HotStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.max(1),
            evictions: 0,
            expired_removals: 0,
        }
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
        self.purge_expired(now);
        let mut evicted = Vec::new();
        if !self.entries.contains_key(&key) {
            while self.entries.len() >= self.max_entries {
                match self.evict_one() {
                    Some(victim) => evicted.push(victim),
                    None => {
                        return Err(MemoryError::CriticalCapacity {
                            max_entries: self.max_entries,
                        });
                    }
                }
            }
        }
        let expires_at = class.ttl.map(|ttl| now + ttl);
        let size_bytes = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
        self.entries.insert(
            key,
            HotEntry {
                class: class.name.clone(),
                value,
                priority: class.priority,
                expires_at,
                last_access: now,
                access_count: 1,
                demote_after: class.demote_after,
                size_bytes,
            },
        );
        Ok(evicted)
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

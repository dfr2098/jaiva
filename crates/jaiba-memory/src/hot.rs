use std::{collections::HashMap, time::Instant};

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
}

#[derive(Debug, Default, Clone)]
pub struct HotMetrics {
    pub objects: u64,
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
        self.entries.insert(
            key,
            HotEntry {
                class: class.name.clone(),
                value,
                priority: class.priority,
                expires_at,
                last_access: now,
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
                    .then_with(|| left.last_access.cmp(&right.last_access))
            })
            .map(|(key, _)| key.clone())?;
        let entry = self.entries.remove(&victim_key)?;
        self.evictions += 1;
        Some((victim_key, entry))
    }
}

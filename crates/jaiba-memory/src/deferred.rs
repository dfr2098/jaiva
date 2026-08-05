use std::{collections::BTreeMap, time::Instant};

use crate::sink::PersistRecord;

#[derive(Debug, Clone)]
struct DeferredItem {
    record: PersistRecord,
    enqueued_at: Instant,
    flush_after: std::time::Duration,
}

/// Cola acotada de persistencia diferida (coalesce por clave).
#[derive(Debug, Default)]
pub struct DeferredQueue {
    items: BTreeMap<String, DeferredItem>,
    max_pending: usize,
    flushes: u64,
    enqueued: u64,
}

impl DeferredQueue {
    pub fn new(max_pending: usize) -> Self {
        Self {
            items: BTreeMap::new(),
            max_pending: max_pending.max(1),
            flushes: 0,
            enqueued: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn flushes(&self) -> u64 {
        self.flushes
    }

    pub fn enqueued_total(&self) -> u64 {
        self.enqueued
    }

    /// Encola (o reemplaza) un registro. Devuelve `true` si hay que forzar flush
    /// por tope de cola.
    pub fn enqueue(
        &mut self,
        record: PersistRecord,
        flush_after: std::time::Duration,
        now: Instant,
    ) -> bool {
        self.enqueued += 1;
        self.items.insert(
            record.key.clone(),
            DeferredItem {
                record,
                enqueued_at: now,
                flush_after,
            },
        );
        self.items.len() >= self.max_pending
    }

    /// Extrae registros vencidos por TTL de flush (o todos si `force`).
    pub fn take_ready(&mut self, now: Instant, force: bool) -> Vec<PersistRecord> {
        if force {
            return self.drain_all();
        }
        let keys: Vec<String> = self
            .items
            .iter()
            .filter(|(_, item)| now.duration_since(item.enqueued_at) >= item.flush_after)
            .map(|(key, _)| key.clone())
            .collect();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(item) = self.items.remove(&key) {
                out.push(item.record);
            }
        }
        if !out.is_empty() {
            self.flushes += 1;
        }
        out
    }

    /// Restaura registros que no pudieron persistirse. Quedan listos para el
    /// siguiente intento sin perder el coalesce por clave.
    pub fn requeue_failed(
        &mut self,
        records: impl IntoIterator<Item = PersistRecord>,
        now: Instant,
    ) {
        for record in records {
            self.items.insert(
                record.key.clone(),
                DeferredItem {
                    record,
                    enqueued_at: now,
                    flush_after: std::time::Duration::ZERO,
                },
            );
        }
    }

    fn drain_all(&mut self) -> Vec<PersistRecord> {
        if self.items.is_empty() {
            return Vec::new();
        }
        self.flushes += 1;
        std::mem::take(&mut self.items)
            .into_values()
            .map(|item| item.record)
            .collect()
    }
}

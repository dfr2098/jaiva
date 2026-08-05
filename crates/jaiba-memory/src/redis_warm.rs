//! WarmStore Redis — solo con feature `redis`.

use std::sync::Mutex;

use redis::{Client, Commands, Connection};

use crate::{
    error::MemoryError,
    warm::{WarmEntry, WarmStore},
};

/// Warm respaldado por Redis (`warm.backend: redis`).
pub struct RedisWarmStore {
    conn: Mutex<Connection>,
    key_prefix: String,
}

impl std::fmt::Debug for RedisWarmStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisWarmStore")
            .field("key_prefix", &self.key_prefix)
            .finish_non_exhaustive()
    }
}

impl RedisWarmStore {
    pub fn connect(url: &str, key_prefix: impl Into<String>) -> Result<Self, MemoryError> {
        let client = Client::open(url)
            .map_err(|error| MemoryError::Warm(format!("redis url inválida: {error}")))?;
        let conn = client
            .get_connection()
            .map_err(|error| MemoryError::Warm(format!("redis connect: {error}")))?;
        Ok(Self {
            conn: Mutex::new(conn),
            key_prefix: key_prefix.into(),
        })
    }

    pub fn from_env(url_env: &str, key_prefix: impl Into<String>) -> Result<Self, MemoryError> {
        let url = std::env::var(url_env).map_err(|_| {
            MemoryError::Configuration(format!(
                "warm.backend redis requiere variable de entorno {url_env}"
            ))
        })?;
        Self::connect(&url, key_prefix)
    }

    fn redis_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix)
    }
}

impl WarmStore for RedisWarmStore {
    fn get(&self, key: &str) -> Result<Option<WarmEntry>, MemoryError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| MemoryError::Warm("redis lock envenenado".to_owned()))?;
        let raw: Option<String> = conn
            .get(self.redis_key(key))
            .map_err(|error| MemoryError::Warm(format!("redis get: {error}")))?;
        match raw {
            None => Ok(None),
            Some(text) => Ok(Some(decode_warm_entry(&text)?)),
        }
    }

    fn put(&mut self, key: &str, entry: WarmEntry) -> Result<(), MemoryError> {
        let payload = encode_warm_entry(&entry)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| MemoryError::Warm("redis lock envenenado".to_owned()))?;
        let _: () = conn
            .set(self.redis_key(key), payload)
            .map_err(|error| MemoryError::Warm(format!("redis set: {error}")))?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<bool, MemoryError> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| MemoryError::Warm("redis lock envenenado".to_owned()))?;
        let deleted: i32 = conn
            .del(self.redis_key(key))
            .map_err(|error| MemoryError::Warm(format!("redis del: {error}")))?;
        Ok(deleted > 0)
    }

    fn len(&self) -> usize {
        // Redis no expone tamaño del prefijo sin SCAN; reportamos 0.
        0
    }

    fn name(&self) -> &'static str {
        "redis"
    }
}

/// Serialización usada por Redis (también útil en tests sin servidor).
pub fn encode_warm_entry(entry: &WarmEntry) -> Result<String, MemoryError> {
    serde_json::to_string(entry).map_err(|error| MemoryError::Warm(format!("encode: {error}")))
}

pub fn decode_warm_entry(text: &str) -> Result<WarmEntry, MemoryError> {
    serde_json::from_str(text).map_err(|error| MemoryError::Warm(format!("decode: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_payload() {
        let entry = WarmEntry {
            class: "carrier".into(),
            value: json!({"lane": 1}),
        };
        let text = encode_warm_entry(&entry).unwrap();
        let back = decode_warm_entry(&text).unwrap();
        assert_eq!(back, entry);
    }

    #[test]
    fn connect_rejects_bad_url() {
        match RedisWarmStore::connect("not-a-redis-url", "jaiba:") {
            Ok(_) => panic!("expected connection failure"),
            Err(err) => assert!(err.to_string().contains("redis")),
        }
    }
}

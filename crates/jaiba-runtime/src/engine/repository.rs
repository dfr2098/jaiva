//! Durable packet metadata, content and provenance repositories.
//!
//! The local implementation uses SQLite WAL and SHA-256-addressed files. These
//! contracts are independent from source and destination database connectors.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use uuid::Uuid;

use crate::{config::RepositoryConfig, error::FlowError};

use super::{DataPacket, PacketContent, RecordSchema};

/// Stable reference to content stored outside packet metadata.
#[derive(Debug, Clone)]
pub struct ContentReference {
    pub key: String,
    pub size: u64,
}

/// Persistent work item reconstructed after startup.
#[derive(Debug, Clone)]
pub struct StoredWork {
    pub queue_id: String,
    pub flow_id: String,
    pub processor_id: String,
    pub relationship: String,
    pub packet: DataPacket,
}

/// Current persistent queue and content gauges.
#[derive(Debug, Clone, Copy, Default)]
pub struct RepositoryStats {
    pub pending: u64,
    pub running: u64,
    pub dead_letter: u64,
    pub content_bytes: u64,
}

/// Metadata for a packet retained after exhausting processor retries.
#[derive(Debug, Clone, Serialize)]
pub struct DeadLetterEntry {
    pub queue_id: String,
    pub packet_id: String,
    pub flow_id: String,
    pub processor_id: String,
    pub relationship: String,
    pub attempt: u32,
    pub error: Option<String>,
    pub content_size: u64,
    pub created_at: i64,
    pub failed_at: i64,
}

/// One ordered, queryable event in a packet's execution history.
#[derive(Debug, Clone, Serialize)]
pub struct ProvenanceRecord {
    pub id: i64,
    pub queue_id: String,
    pub packet_id: String,
    pub flow_id: String,
    pub processor_id: String,
    pub event_type: String,
    pub details: Value,
    pub created_at: i64,
}

/// Auditable lifecycle event for a queue item.
#[derive(Debug, Clone, Copy)]
pub enum ProvenanceEvent {
    Enqueued,
    Claimed,
    Completed,
    Failed,
    Recovered,
    Requeued,
    ProcessingStarted,
    Retried,
    Processed,
    Routed,
}

impl ProvenanceEvent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Enqueued => "ENQUEUED",
            Self::Claimed => "CLAIMED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Recovered => "RECOVERED",
            Self::Requeued => "REQUEUED",
            Self::ProcessingStarted => "PROCESSING_STARTED",
            Self::Retried => "RETRIED",
            Self::Processed => "PROCESSED",
            Self::Routed => "ROUTED",
        }
    }
}

/// Storage contract for packet content.
#[async_trait]
pub trait ContentRepository: Send + Sync {
    async fn put(&self, bytes: &[u8]) -> Result<ContentReference, FlowError>;
    async fn get(&self, reference: &ContentReference) -> Result<Vec<u8>, FlowError>;
    async fn delete(&self, reference: &ContentReference) -> Result<(), FlowError>;
}

/// Storage contract for queue state and provenance.
#[async_trait]
pub trait PacketRepository: Send + Sync {
    async fn enqueue(
        &self,
        flow_id: &str,
        processor_id: &str,
        relationship: &str,
        packet: &DataPacket,
    ) -> Result<String, FlowError>;
    async fn claim(&self, queue_id: &str) -> Result<bool, FlowError>;
    async fn complete(&self, queue_id: &str, event: ProvenanceEvent) -> Result<(), FlowError>;
    async fn fail(&self, queue_id: &str, error: &str, attempt: u32) -> Result<(), FlowError>;
    async fn dead_letters(
        &self,
        flow_id: &str,
        limit: u32,
    ) -> Result<Vec<DeadLetterEntry>, FlowError>;
    async fn requeue_dead_letter(&self, queue_id: &str) -> Result<bool, FlowError>;
    async fn record_event(
        &self,
        queue_id: &str,
        event: ProvenanceEvent,
        details: Value,
    ) -> Result<(), FlowError>;
    async fn provenance_for_packet(
        &self,
        flow_id: &str,
        packet_id: &str,
        limit: u32,
    ) -> Result<Vec<ProvenanceRecord>, FlowError>;
    async fn recent_provenance(
        &self,
        flow_id: &str,
        limit: u32,
    ) -> Result<Vec<ProvenanceRecord>, FlowError>;
    async fn pending(&self, flow_id: &str) -> Result<Vec<StoredWork>, FlowError>;
    async fn recover_abandoned(
        &self,
        flow_id: &str,
        abandoned_after_seconds: u64,
    ) -> Result<u64, FlowError>;
    async fn cleanup_completed(&self, retention_hours: u64) -> Result<u64, FlowError>;
    async fn cleanup_provenance(&self, retention_hours: u64) -> Result<u64, FlowError>;
    async fn stats(&self) -> Result<RepositoryStats, FlowError>;
}

/// SHA-256-addressed local content repository.
#[derive(Debug, Clone)]
pub struct LocalContentRepository {
    root: Arc<PathBuf>,
}

impl LocalContentRepository {
    /// Creates the content root when necessary.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, FlowError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(&key[..2]).join(format!("{key}.bin"))
    }
}

#[async_trait]
impl ContentRepository for LocalContentRepository {
    async fn put(&self, bytes: &[u8]) -> Result<ContentReference, FlowError> {
        let key = hex_digest(bytes);
        let path = self.path_for(&key);
        if !path.exists() {
            let parent = path.parent().expect("content path has parent");
            fs::create_dir_all(parent)?;
            let temporary = parent.join(format!("{key}.{}.tmp", Uuid::new_v4()));
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            match fs::rename(&temporary, &path) {
                Ok(()) => {}
                Err(error) if path.exists() => {
                    let _ = fs::remove_file(temporary);
                    drop(error);
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(ContentReference {
            key,
            size: bytes.len() as u64,
        })
    }

    async fn get(&self, reference: &ContentReference) -> Result<Vec<u8>, FlowError> {
        let bytes = fs::read(self.path_for(&reference.key))?;
        if hex_digest(&bytes) != reference.key {
            return Err(FlowError::Repository(format!(
                "content checksum mismatch for '{}'",
                reference.key
            )));
        }
        Ok(bytes)
    }

    async fn delete(&self, reference: &ContentReference) -> Result<(), FlowError> {
        let path = self.path_for(&reference.key);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

/// SQLite WAL implementation of packet state and provenance.
#[derive(Clone)]
pub struct LocalPacketRepository {
    pool: SqlitePool,
    content: LocalContentRepository,
}

impl LocalPacketRepository {
    /// Opens the repository and applies internal schema migrations.
    pub async fn open(config: &RepositoryConfig) -> Result<Self, FlowError> {
        if let Some(parent) = config.database_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(&config.database_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let repository = Self {
            pool,
            content: LocalContentRepository::new(&config.content_path)?,
        };
        repository.migrate().await?;
        Ok(repository)
    }

    async fn migrate(&self) -> Result<(), FlowError> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jaiva_packet_queue (
                queue_id TEXT PRIMARY KEY,
                packet_id TEXT NOT NULL,
                flow_id TEXT NOT NULL,
                processor_id TEXT NOT NULL,
                relationship TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                attributes_json TEXT NOT NULL,
                schema_json TEXT,
                content_key TEXT NOT NULL,
                content_kind TEXT NOT NULL,
                media_type TEXT,
                content_size INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                claimed_at INTEGER,
                completed_at INTEGER,
                error TEXT
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS ix_jaiva_packet_pending
            ON jaiva_packet_queue (flow_id, status, created_at)
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS jaiva_provenance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_id TEXT NOT NULL,
                packet_id TEXT NOT NULL,
                flow_id TEXT NOT NULL,
                processor_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS ix_jaiva_provenance_packet ON jaiva_provenance (flow_id, packet_id, created_at, id)",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn provenance_details(
        &self,
        queue_id: &str,
        event: ProvenanceEvent,
        details: Value,
    ) -> Result<(), FlowError> {
        let details = serde_json::to_string(&details)
            .map_err(|error| FlowError::Repository(error.to_string()))?;
        sqlx::query(
            r#"
            INSERT INTO jaiva_provenance
                (queue_id, packet_id, flow_id, processor_id, event_type, details_json, created_at)
            SELECT queue_id, packet_id, flow_id, processor_id, ?, ?, ?
            FROM jaiva_packet_queue
            WHERE queue_id = ?
            "#,
        )
        .bind(event.as_str())
        .bind(details)
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

#[async_trait]
impl PacketRepository for LocalPacketRepository {
    async fn enqueue(
        &self,
        flow_id: &str,
        processor_id: &str,
        relationship: &str,
        packet: &DataPacket,
    ) -> Result<String, FlowError> {
        let (content_kind, media_type, bytes) = serialize_content(&packet.content)?;
        let reference = self.content.put(&bytes).await?;
        let queue_id = Uuid::new_v4().to_string();
        let attributes = serde_json::to_string(&packet.attributes)
            .map_err(|error| FlowError::Repository(error.to_string()))?;
        let schema = packet
            .schema
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| FlowError::Repository(error.to_string()))?;
        let operational_attributes: BTreeMap<&str, &str> = packet
            .attributes
            .iter()
            .filter(|(key, _)| {
                key.starts_with("kafka.") || key.starts_with("write.") || key.starts_with("error.")
            })
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();

        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO jaiva_packet_queue (
                queue_id, packet_id, flow_id, processor_id, relationship,
                status, attempt, attributes_json, schema_json, content_key,
                content_kind, media_type, content_size, created_at
            ) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&queue_id)
        .bind(packet.id.to_string())
        .bind(flow_id)
        .bind(processor_id)
        .bind(relationship)
        .bind(packet.attempt as i64)
        .bind(attributes)
        .bind(schema)
        .bind(&reference.key)
        .bind(content_kind)
        .bind(media_type)
        .bind(reference.size as i64)
        .bind(now_epoch())
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO jaiva_provenance (
                queue_id, packet_id, flow_id, processor_id, event_type,
                details_json, created_at
            ) VALUES (?, ?, ?, ?, 'ENQUEUED', ?, ?)
            "#,
        )
        .bind(&queue_id)
        .bind(packet.id.to_string())
        .bind(flow_id)
        .bind(processor_id)
        .bind(
            serde_json::json!({
                "relationship": relationship,
                "attempt": packet.attempt,
                "content_bytes": reference.size,
                "content_kind": content_kind,
                "media_type": media_type,
                "attribute_count": packet.attributes.len(),
                "has_schema": packet.schema.is_some(),
                "operational_attributes": operational_attributes
            })
            .to_string(),
        )
        .bind(now_epoch())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(queue_id)
    }

    async fn claim(&self, queue_id: &str) -> Result<bool, FlowError> {
        let result = sqlx::query(
            r#"
            UPDATE jaiva_packet_queue
            SET status = 'RUNNING', claimed_at = ?
            WHERE queue_id = ? AND status = 'PENDING'
            "#,
        )
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            self.provenance_details(
                queue_id,
                ProvenanceEvent::Claimed,
                serde_json::json!({"status": "RUNNING"}),
            )
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn complete(&self, queue_id: &str, event: ProvenanceEvent) -> Result<(), FlowError> {
        let status = match event {
            ProvenanceEvent::Failed => "DEAD_LETTER",
            _ => "COMPLETED",
        };
        sqlx::query(
            r#"
            UPDATE jaiva_packet_queue
            SET status = ?, completed_at = ?
            WHERE queue_id = ?
            "#,
        )
        .bind(status)
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&self.pool)
        .await?;
        self.provenance_details(queue_id, event, serde_json::json!({"status": status}))
            .await
    }

    async fn fail(&self, queue_id: &str, error: &str, attempt: u32) -> Result<(), FlowError> {
        let result = sqlx::query(
            "UPDATE jaiva_packet_queue SET status = 'DEAD_LETTER', attempt = ?, error = ?, completed_at = ? WHERE queue_id = ? AND status = 'RUNNING'",
        )
        .bind(attempt as i64)
        .bind(error)
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            self.provenance_details(
                queue_id,
                ProvenanceEvent::Failed,
                serde_json::json!({
                    "status": "DEAD_LETTER",
                    "attempt": attempt,
                    "error": error
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn dead_letters(
        &self,
        flow_id: &str,
        limit: u32,
    ) -> Result<Vec<DeadLetterEntry>, FlowError> {
        let rows = sqlx::query(
            r#"SELECT queue_id, packet_id, flow_id, processor_id, relationship,
                      attempt, error, content_size, created_at, completed_at
               FROM jaiva_packet_queue
               WHERE flow_id = ? AND status = 'DEAD_LETTER'
               ORDER BY completed_at DESC, queue_id
               LIMIT ?"#,
        )
        .bind(flow_id)
        .bind(i64::from(limit.clamp(1, 1000)))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok(DeadLetterEntry {
                    queue_id: row.try_get("queue_id")?,
                    packet_id: row.try_get("packet_id")?,
                    flow_id: row.try_get("flow_id")?,
                    processor_id: row.try_get("processor_id")?,
                    relationship: row.try_get("relationship")?,
                    attempt: row.try_get::<i64, _>("attempt")? as u32,
                    error: row.try_get("error")?,
                    content_size: row.try_get::<i64, _>("content_size")? as u64,
                    created_at: row.try_get("created_at")?,
                    failed_at: row.try_get("completed_at")?,
                })
            })
            .collect()
    }

    async fn requeue_dead_letter(&self, queue_id: &str) -> Result<bool, FlowError> {
        let result = sqlx::query(
            r#"UPDATE jaiva_packet_queue
               SET status = 'PENDING', attempt = 0, error = NULL,
                   claimed_at = NULL, completed_at = NULL
               WHERE queue_id = ? AND status = 'DEAD_LETTER'"#,
        )
        .bind(queue_id)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            self.provenance_details(
                queue_id,
                ProvenanceEvent::Requeued,
                serde_json::json!({"status": "PENDING", "attempt": 0}),
            )
            .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn record_event(
        &self,
        queue_id: &str,
        event: ProvenanceEvent,
        details: Value,
    ) -> Result<(), FlowError> {
        self.provenance_details(queue_id, event, details).await
    }

    async fn provenance_for_packet(
        &self,
        flow_id: &str,
        packet_id: &str,
        limit: u32,
    ) -> Result<Vec<ProvenanceRecord>, FlowError> {
        provenance_query(
            &self.pool,
            "WHERE flow_id = ? AND packet_id = ? ORDER BY created_at, id LIMIT ?",
            flow_id,
            Some(packet_id),
            limit,
        )
        .await
    }

    async fn recent_provenance(
        &self,
        flow_id: &str,
        limit: u32,
    ) -> Result<Vec<ProvenanceRecord>, FlowError> {
        provenance_query(
            &self.pool,
            "WHERE flow_id = ? ORDER BY created_at DESC, id DESC LIMIT ?",
            flow_id,
            None,
            limit,
        )
        .await
    }

    async fn pending(&self, flow_id: &str) -> Result<Vec<StoredWork>, FlowError> {
        let rows = sqlx::query(
            r#"
            SELECT queue_id, packet_id, flow_id, processor_id, relationship,
                   attempt, attributes_json, schema_json, content_key,
                   content_kind, media_type, content_size
            FROM jaiva_packet_queue
            WHERE flow_id = ? AND status = 'PENDING'
            ORDER BY created_at, queue_id
            "#,
        )
        .bind(flow_id)
        .fetch_all(&self.pool)
        .await?;

        let mut work = Vec::with_capacity(rows.len());
        for row in rows {
            let reference = ContentReference {
                key: row.try_get("content_key")?,
                size: row.try_get::<i64, _>("content_size")? as u64,
            };
            let bytes = self.content.get(&reference).await?;
            let content = deserialize_content(
                row.try_get("content_kind")?,
                row.try_get("media_type")?,
                bytes,
            )?;
            let attributes: HashMap<String, String> =
                serde_json::from_str(row.try_get("attributes_json")?)
                    .map_err(|error| FlowError::Repository(error.to_string()))?;
            let schema: Option<RecordSchema> = row
                .try_get::<Option<String>, _>("schema_json")?
                .map(|json| serde_json::from_str(&json))
                .transpose()
                .map_err(|error| FlowError::Repository(error.to_string()))?;
            work.push(StoredWork {
                queue_id: row.try_get("queue_id")?,
                flow_id: row.try_get("flow_id")?,
                processor_id: row.try_get("processor_id")?,
                relationship: row.try_get("relationship")?,
                packet: DataPacket {
                    id: Uuid::parse_str(row.try_get("packet_id")?)
                        .map_err(|error| FlowError::Repository(error.to_string()))?,
                    attributes,
                    content,
                    schema,
                    attempt: row.try_get::<i64, _>("attempt")? as u32,
                },
            });
        }
        Ok(work)
    }

    async fn recover_abandoned(
        &self,
        flow_id: &str,
        abandoned_after_seconds: u64,
    ) -> Result<u64, FlowError> {
        let cutoff = now_epoch().saturating_sub(abandoned_after_seconds as i64);
        let ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT queue_id
            FROM jaiva_packet_queue
            WHERE flow_id = ? AND status = 'RUNNING' AND claimed_at <= ?
            "#,
        )
        .bind(flow_id)
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        for queue_id in &ids {
            sqlx::query(
                r#"
                UPDATE jaiva_packet_queue
                SET status = 'PENDING', claimed_at = NULL
                WHERE queue_id = ?
                "#,
            )
            .bind(queue_id)
            .execute(&self.pool)
            .await?;
            self.provenance_details(
                queue_id,
                ProvenanceEvent::Recovered,
                serde_json::json!({"status": "PENDING", "reason": "abandoned"}),
            )
            .await?;
        }
        Ok(ids.len() as u64)
    }

    async fn cleanup_completed(&self, retention_hours: u64) -> Result<u64, FlowError> {
        let cutoff = now_epoch().saturating_sub((retention_hours.saturating_mul(3600)) as i64);
        let references: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT content_key
            FROM jaiva_packet_queue
            WHERE status = 'COMPLETED' AND completed_at <= ?
            "#,
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await?;
        let result = sqlx::query(
            r#"
            DELETE FROM jaiva_packet_queue
            WHERE status = 'COMPLETED' AND completed_at <= ?
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        for key in references {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM jaiva_packet_queue WHERE content_key = ?")
                    .bind(&key)
                    .fetch_one(&self.pool)
                    .await?;
            if count == 0 {
                self.content
                    .delete(&ContentReference { key, size: 0 })
                    .await?;
            }
        }
        Ok(result.rows_affected())
    }

    async fn cleanup_provenance(&self, retention_hours: u64) -> Result<u64, FlowError> {
        let cutoff = now_epoch().saturating_sub((retention_hours.saturating_mul(3600)) as i64);
        Ok(
            sqlx::query("DELETE FROM jaiva_provenance WHERE created_at <= ?")
                .bind(cutoff)
                .execute(&self.pool)
                .await?
                .rows_affected(),
        )
    }

    async fn stats(&self) -> Result<RepositoryStats, FlowError> {
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END), 0) AS pending,
                COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running,
                COALESCE(SUM(CASE WHEN status = 'DEAD_LETTER' THEN 1 ELSE 0 END), 0) AS dead_letter,
                COALESCE(SUM(content_size), 0) AS content_bytes
            FROM jaiva_packet_queue
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(RepositoryStats {
            pending: row.try_get::<i64, _>("pending")?.max(0) as u64,
            running: row.try_get::<i64, _>("running")?.max(0) as u64,
            dead_letter: row.try_get::<i64, _>("dead_letter")?.max(0) as u64,
            content_bytes: row.try_get::<i64, _>("content_bytes")?.max(0) as u64,
        })
    }
}

async fn provenance_query(
    pool: &SqlitePool,
    clause: &str,
    flow_id: &str,
    packet_id: Option<&str>,
    limit: u32,
) -> Result<Vec<ProvenanceRecord>, FlowError> {
    let sql = format!(
        "SELECT id, queue_id, packet_id, flow_id, processor_id, event_type, details_json, created_at FROM jaiva_provenance {clause}"
    );
    let mut query = sqlx::query(&sql).bind(flow_id);
    if let Some(packet_id) = packet_id {
        query = query.bind(packet_id);
    }
    let rows = query
        .bind(i64::from(limit.clamp(1, 5000)))
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|row| {
            let details_json: String = row.try_get("details_json")?;
            Ok(ProvenanceRecord {
                id: row.try_get("id")?,
                queue_id: row.try_get("queue_id")?,
                packet_id: row.try_get("packet_id")?,
                flow_id: row.try_get("flow_id")?,
                processor_id: row.try_get("processor_id")?,
                event_type: row.try_get("event_type")?,
                details: serde_json::from_str(&details_json)
                    .map_err(|error| FlowError::Repository(error.to_string()))?,
                created_at: row.try_get("created_at")?,
            })
        })
        .collect()
}

fn serialize_content(
    content: &PacketContent,
) -> Result<(&'static str, Option<&str>, Vec<u8>), FlowError> {
    match content {
        PacketContent::Records(records) => Ok((
            "records",
            Some("application/json"),
            serde_json::to_vec(records)
                .map_err(|error| FlowError::Repository(error.to_string()))?,
        )),
        PacketContent::Encoded { media_type, bytes } => {
            Ok(("encoded", Some(media_type.as_str()), bytes.clone()))
        }
    }
}

fn deserialize_content(
    kind: &str,
    media_type: Option<String>,
    bytes: Vec<u8>,
) -> Result<PacketContent, FlowError> {
    match kind {
        "records" => Ok(PacketContent::Records(
            serde_json::from_slice::<Vec<Value>>(&bytes)
                .map_err(|error| FlowError::Repository(error.to_string()))?,
        )),
        "encoded" => Ok(PacketContent::Encoded {
            media_type: media_type.unwrap_or_else(|| "application/octet-stream".to_owned()),
            bytes,
        }),
        other => Err(FlowError::Repository(format!(
            "unknown content kind '{other}'"
        ))),
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RepositoryConfig {
        let root = std::env::temp_dir().join(format!("jaiva-repository-{}", Uuid::new_v4()));
        RepositoryConfig {
            enabled: true,
            database_path: root.join("repository.db"),
            content_path: root.join("content"),
            abandoned_after_seconds: 0,
            completed_retention_hours: 24,
            provenance_retention_hours: 24 * 90,
        }
    }

    #[tokio::test]
    async fn stores_claims_recovers_and_completes_a_packet() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({
            "id": 1,
            "name": "Ada"
        })]);

        let queue_id = repository
            .enqueue("flow", "destination", "success", &packet)
            .await
            .unwrap();
        let pending = repository.pending("flow").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].packet.records().unwrap()[0]["name"], "Ada");

        assert!(repository.claim(&queue_id).await.unwrap());
        assert_eq!(repository.recover_abandoned("flow", 0).await.unwrap(), 1);
        assert_eq!(repository.pending("flow").await.unwrap().len(), 1);
        assert!(repository.claim(&queue_id).await.unwrap());
        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();
        assert!(repository.pending("flow").await.unwrap().is_empty());

        let event_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM jaiva_provenance WHERE queue_id = ?")
                .bind(&queue_id)
                .fetch_one(&repository.pool)
                .await
                .unwrap();
        assert_eq!(event_count, 5);
        let timeline = repository
            .provenance_for_packet("flow", &packet.id.to_string(), 100)
            .await
            .unwrap();
        assert_eq!(timeline.len(), 5);
        assert_eq!(timeline[0].event_type, "ENQUEUED");
        assert_eq!(timeline[0].details["content_kind"], "records");
        assert_eq!(timeline.last().unwrap().event_type, "COMPLETED");

        let failed_queue = repository
            .enqueue("flow", "destination", "failure", &packet)
            .await
            .unwrap();
        assert!(repository.claim(&failed_queue).await.unwrap());
        repository
            .fail(&failed_queue, "destination unavailable", 3)
            .await
            .unwrap();
        let dead_letters = repository.dead_letters("flow", 10).await.unwrap();
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].attempt, 3);
        assert_eq!(
            dead_letters[0].error.as_deref(),
            Some("destination unavailable")
        );
        let stats = repository.stats().await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 0);
        assert_eq!(stats.dead_letter, 1);
        assert!(repository.requeue_dead_letter(&failed_queue).await.unwrap());
        assert_eq!(repository.pending("flow").await.unwrap().len(), 1);
        let recent = repository.recent_provenance("flow", 2).await.unwrap();
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].event_type, "REQUEUED");

        // SQLite mantiene el archivo abierto hasta que el pool termina de
        // cerrarse; esperarlo evita un bloqueo al limpiar la prueba en Windows.
        repository.pool.close().await;
        let root = config
            .database_path
            .parent()
            .expect("test database has parent");
        drop(repository);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn content_is_addressed_and_verified_by_hash() {
        let config = test_config();
        let content = LocalContentRepository::new(&config.content_path).unwrap();
        let first = content.put(b"same content").await.unwrap();
        let second = content.put(b"same content").await.unwrap();
        assert_eq!(first.key, second.key);
        assert_eq!(content.get(&first).await.unwrap(), b"same content");

        let root = config
            .database_path
            .parent()
            .expect("test database has parent");
        drop(content);
        fs::remove_dir_all(root).unwrap();
    }
}

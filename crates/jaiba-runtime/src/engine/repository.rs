//! Durable packet metadata, content and provenance repositories.
//!
//! The local implementation uses SQLite WAL and SHA-256-addressed files. These
//! contracts are independent from source and destination database connectors.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
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

/// Resultado de barrer blobs/temporales sin fila de cola (Fase 2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub orphan_blobs_removed: u64,
    pub stale_tmp_removed: u64,
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
        // C1 / .tmp viejos: limpia basura de crashes anteriores al aceptar trabajo.
        repository.reconcile_orphans().await?;
        Ok(repository)
    }

    /// Elimina blobs `.bin` sin fila en cola y ficheros `.tmp` más viejos que
    /// una hora. Idempotente; seguro tras un kill a mitad de `enqueue`.
    pub async fn reconcile_orphans(&self) -> Result<ReconcileReport, FlowError> {
        self.reconcile_orphans_with_tmp_max_age(Duration::from_secs(3600))
            .await
    }

    /// Como [`Self::reconcile_orphans`], con edad máxima configurable para `.tmp`
    /// (las pruebas usan `Duration::ZERO` para borrar todos).
    pub async fn reconcile_orphans_with_tmp_max_age(
        &self,
        tmp_max_age: Duration,
    ) -> Result<ReconcileReport, FlowError> {
        let referenced: HashSet<String> =
            sqlx::query_scalar("SELECT DISTINCT content_key FROM jaiva_packet_queue")
                .fetch_all(&self.pool)
                .await?
                .into_iter()
                .collect();
        let mut report = ReconcileReport::default();
        let now = SystemTime::now();
        for path in list_content_files(self.content.root.as_path())? {
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if name.ends_with(".tmp") {
                let age_ok = match path.metadata().and_then(|meta| meta.modified()) {
                    Ok(modified) => {
                        now.duration_since(modified).unwrap_or(Duration::ZERO) >= tmp_max_age
                    }
                    Err(_) => true,
                };
                if age_ok {
                    let _ = fs::remove_file(&path);
                    report.stale_tmp_removed += 1;
                }
                continue;
            }
            if !name.ends_with(".bin") {
                continue;
            }
            let Some(key) = name.strip_suffix(".bin") else {
                continue;
            };
            if referenced.contains(key) {
                continue;
            }
            let _ = fs::remove_file(&path);
            report.orphan_blobs_removed += 1;
            if let Some(parent) = path.parent() {
                let _ = fs::remove_dir(parent);
            }
        }
        Ok(report)
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
        insert_provenance(&self.pool, queue_id, event, details).await
    }
}

/// Inserta provenance leyendo identidad de la fila de cola (misma conexión/TX).
async fn insert_provenance<'e, E>(
    executor: E,
    queue_id: &str,
    event: ProvenanceEvent,
    details: Value,
) -> Result<(), FlowError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
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
    .execute(executor)
    .await?;
    Ok(())
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
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            UPDATE jaiva_packet_queue
            SET status = 'RUNNING', claimed_at = ?
            WHERE queue_id = ? AND status = 'PENDING'
            "#,
        )
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Ok(false);
        }
        insert_provenance(
            &mut *transaction,
            queue_id,
            ProvenanceEvent::Claimed,
            serde_json::json!({"status": "RUNNING"}),
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
    }

    async fn complete(&self, queue_id: &str, event: ProvenanceEvent) -> Result<(), FlowError> {
        let status = match event {
            ProvenanceEvent::Failed => "DEAD_LETTER",
            _ => "COMPLETED",
        };
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            UPDATE jaiva_packet_queue
            SET status = ?, completed_at = ?
            WHERE queue_id = ? AND status = 'RUNNING'
            "#,
        )
        .bind(status)
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Ok(());
        }
        insert_provenance(
            &mut *transaction,
            queue_id,
            event,
            serde_json::json!({"status": status}),
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn fail(&self, queue_id: &str, error: &str, attempt: u32) -> Result<(), FlowError> {
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            "UPDATE jaiva_packet_queue SET status = 'DEAD_LETTER', attempt = ?, error = ?, completed_at = ? WHERE queue_id = ? AND status = 'RUNNING'",
        )
        .bind(attempt as i64)
        .bind(error)
        .bind(now_epoch())
        .bind(queue_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Ok(());
        }
        insert_provenance(
            &mut *transaction,
            queue_id,
            ProvenanceEvent::Failed,
            serde_json::json!({
                "status": "DEAD_LETTER",
                "attempt": attempt,
                "error": error
            }),
        )
        .await?;
        transaction.commit().await?;
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
        let mut transaction = self.pool.begin().await?;
        let result = sqlx::query(
            r#"UPDATE jaiva_packet_queue
               SET status = 'PENDING', attempt = 0, error = NULL,
                   claimed_at = NULL, completed_at = NULL
               WHERE queue_id = ? AND status = 'DEAD_LETTER'"#,
        )
        .bind(queue_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Ok(false);
        }
        insert_provenance(
            &mut *transaction,
            queue_id,
            ProvenanceEvent::Requeued,
            serde_json::json!({"status": "PENDING", "attempt": 0}),
        )
        .await?;
        transaction.commit().await?;
        Ok(true)
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
        let mut transaction = self.pool.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT queue_id
            FROM jaiva_packet_queue
            WHERE flow_id = ? AND status = 'RUNNING' AND claimed_at <= ?
            "#,
        )
        .bind(flow_id)
        .bind(cutoff)
        .fetch_all(&mut *transaction)
        .await?;
        let mut recovered = 0_u64;
        for queue_id in &ids {
            let result = sqlx::query(
                r#"
                UPDATE jaiva_packet_queue
                SET status = 'PENDING', claimed_at = NULL
                WHERE queue_id = ? AND status = 'RUNNING'
                "#,
            )
            .bind(queue_id)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                continue;
            }
            insert_provenance(
                &mut *transaction,
                queue_id,
                ProvenanceEvent::Recovered,
                serde_json::json!({"status": "PENDING", "reason": "abandoned"}),
            )
            .await?;
            recovered += 1;
        }
        transaction.commit().await?;
        Ok(recovered)
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

fn list_content_files(root: &Path) -> Result<Vec<PathBuf>, FlowError> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for shard in fs::read_dir(root)? {
        let shard = shard?;
        if !shard.file_type()?.is_dir() {
            continue;
        }
        for entry in fs::read_dir(shard.path())? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
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
        // La limpieza no forma parte de la aserciÃ³n; Windows puede conservar
        // brevemente un handle interno de SQLite aun despuÃ©s de cerrar el pool.
        let _ = fs::remove_dir_all(root);
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
        let _ = fs::remove_dir_all(root);
    }

    async fn queue_status(repository: &LocalPacketRepository, queue_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM jaiva_packet_queue WHERE queue_id = ?")
            .bind(queue_id)
            .fetch_one(&repository.pool)
            .await
            .unwrap()
    }

    async fn event_count(
        repository: &LocalPacketRepository,
        queue_id: &str,
        event_type: &str,
    ) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM jaiva_provenance WHERE queue_id = ? AND event_type = ?",
        )
        .bind(queue_id)
        .bind(event_type)
        .fetch_one(&repository.pool)
        .await
        .unwrap()
    }

    async fn close_repo(repository: LocalPacketRepository, config: RepositoryConfig) {
        repository.pool.close().await;
        let root = config
            .database_path
            .parent()
            .expect("test database has parent");
        drop(repository);
        let _ = fs::remove_dir_all(root);
    }

    /// T1: un solo claim gana; el ganador deja CLAIMED visible tras el commit.
    #[tokio::test]
    async fn claim_is_exclusive_and_records_claimed_in_same_commit() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 1})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();

        let left = repository.clone();
        let right = repository.clone();
        let left_id = queue_id.clone();
        let right_id = queue_id.clone();
        let (a, b) = tokio::join!(left.claim(&left_id), right.claim(&right_id));
        let won = [a.unwrap(), b.unwrap()]
            .into_iter()
            .filter(|ok| *ok)
            .count();
        assert_eq!(won, 1);
        assert_eq!(queue_status(&repository, &queue_id).await, "RUNNING");
        assert_eq!(event_count(&repository, &queue_id, "CLAIMED").await, 1);

        close_repo(repository, config).await;
    }

    /// T2: complete exige RUNNING; con éxito estado + provenance van juntos.
    #[tokio::test]
    async fn complete_requires_running_and_is_atomic_with_provenance() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 2})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();

        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();
        assert_eq!(queue_status(&repository, &queue_id).await, "PENDING");
        assert_eq!(event_count(&repository, &queue_id, "COMPLETED").await, 0);

        assert!(repository.claim(&queue_id).await.unwrap());
        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();
        assert_eq!(queue_status(&repository, &queue_id).await, "COMPLETED");
        assert_eq!(event_count(&repository, &queue_id, "COMPLETED").await, 1);

        close_repo(repository, config).await;
    }

    /// T3: fail exige RUNNING; DEAD_LETTER + FAILED atómicos.
    #[tokio::test]
    async fn fail_requires_running_and_is_atomic_with_provenance() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 3})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();

        repository.fail(&queue_id, "too early", 1).await.unwrap();
        assert_eq!(queue_status(&repository, &queue_id).await, "PENDING");
        assert_eq!(event_count(&repository, &queue_id, "FAILED").await, 0);

        assert!(repository.claim(&queue_id).await.unwrap());
        repository.fail(&queue_id, "boom", 2).await.unwrap();
        assert_eq!(queue_status(&repository, &queue_id).await, "DEAD_LETTER");
        assert_eq!(event_count(&repository, &queue_id, "FAILED").await, 1);

        close_repo(repository, config).await;
    }

    /// T4: recover no deja PENDING abandonado sin RECOVERED.
    #[tokio::test]
    async fn recover_abandoned_is_atomic_per_row() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 4})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        assert!(repository.claim(&queue_id).await.unwrap());

        assert_eq!(repository.recover_abandoned("flow", 0).await.unwrap(), 1);
        assert_eq!(queue_status(&repository, &queue_id).await, "PENDING");
        assert_eq!(event_count(&repository, &queue_id, "RECOVERED").await, 1);

        close_repo(repository, config).await;
    }

    /// T5: requeue solo desde DEAD_LETTER.
    #[tokio::test]
    async fn requeue_dead_letter_requires_dead_letter() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 5})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();

        assert!(!repository.requeue_dead_letter(&queue_id).await.unwrap());
        assert_eq!(event_count(&repository, &queue_id, "REQUEUED").await, 0);

        assert!(repository.claim(&queue_id).await.unwrap());
        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();
        assert!(!repository.requeue_dead_letter(&queue_id).await.unwrap());
        assert_eq!(event_count(&repository, &queue_id, "REQUEUED").await, 0);

        let dead = repository
            .enqueue("flow", "sink", "failure", &packet)
            .await
            .unwrap();
        assert!(repository.claim(&dead).await.unwrap());
        repository.fail(&dead, "gone", 1).await.unwrap();
        assert!(repository.requeue_dead_letter(&dead).await.unwrap());
        assert_eq!(queue_status(&repository, &dead).await, "PENDING");
        assert_eq!(event_count(&repository, &dead, "REQUEUED").await, 1);

        close_repo(repository, config).await;
    }

    /// T6: regresión — enqueue mantiene fila + ENQUEUED atómicos.
    #[tokio::test]
    async fn enqueue_keeps_queue_row_and_enqueued_atomic() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 6})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();

        assert_eq!(queue_status(&repository, &queue_id).await, "PENDING");
        assert_eq!(event_count(&repository, &queue_id, "ENQUEUED").await, 1);
        assert_eq!(repository.pending("flow").await.unwrap().len(), 1);

        close_repo(repository, config).await;
    }

    /// T7: blob sin fila de cola → reconcile lo elimina (C1).
    #[tokio::test]
    async fn reconcile_deletes_content_without_queue_row() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let orphan = repository.content.put(b"orphan-bytes").await.unwrap();
        assert!(repository.content.path_for(&orphan.key).exists());

        let report = repository.reconcile_orphans().await.unwrap();
        assert_eq!(report.orphan_blobs_removed, 1);
        assert!(!repository.content.path_for(&orphan.key).exists());

        close_repo(repository, config).await;
    }

    /// T8: blob referenciado por la cola no se toca.
    #[tokio::test]
    async fn reconcile_keeps_content_referenced_by_queue() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 8})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        let key: String =
            sqlx::query_scalar("SELECT content_key FROM jaiva_packet_queue WHERE queue_id = ?")
                .bind(&queue_id)
                .fetch_one(&repository.pool)
                .await
                .unwrap();
        let path = repository.content.path_for(&key);
        assert!(path.exists());

        let report = repository.reconcile_orphans().await.unwrap();
        assert_eq!(report.orphan_blobs_removed, 0);
        assert!(path.exists());

        close_repo(repository, config).await;
    }

    /// T9: `*.tmp` antiguos se limpian.
    #[tokio::test]
    async fn stale_content_tmp_files_are_removed() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let shard = config.content_path.join("aa");
        fs::create_dir_all(&shard).unwrap();
        let tmp = shard.join("deadbeef.tmp");
        fs::write(&tmp, b"partial").unwrap();
        assert!(tmp.exists());

        let report = repository
            .reconcile_orphans_with_tmp_max_age(Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(report.stale_tmp_removed, 1);
        assert!(!tmp.exists());

        close_repo(repository, config).await;
    }

    /// T10: contenido ausente o corrupto → error de repositorio (C10).
    #[tokio::test]
    async fn missing_or_corrupt_content_surfaces_repository_error() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 10})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        let key: String =
            sqlx::query_scalar("SELECT content_key FROM jaiva_packet_queue WHERE queue_id = ?")
                .bind(&queue_id)
                .fetch_one(&repository.pool)
                .await
                .unwrap();
        let path = repository.content.path_for(&key);

        fs::remove_file(&path).unwrap();
        let missing = repository.pending("flow").await;
        assert!(missing.is_err(), "missing content must fail pending()");

        // Restaurar bytes incorrectos con el mismo path (hash distinto).
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"tampered").unwrap();
        let corrupt = repository.pending("flow").await;
        assert!(
            matches!(corrupt, Err(FlowError::Repository(_))),
            "corrupt content must be FlowError::Repository, got {corrupt:?}"
        );

        close_repo(repository, config).await;
    }

    /// T11: reinicio + recover_abandoned recupera RUNNING abandonado.
    #[tokio::test]
    async fn restart_requeues_abandoned_running_work() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 11})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        assert!(repository.claim(&queue_id).await.unwrap());
        assert_eq!(queue_status(&repository, &queue_id).await, "RUNNING");
        repository.pool.close().await;
        drop(repository);

        let repository = LocalPacketRepository::open(&config).await.unwrap();
        assert_eq!(repository.recover_abandoned("flow", 0).await.unwrap(), 1);
        assert_eq!(queue_status(&repository, &queue_id).await, "PENDING");
        assert!(repository.claim(&queue_id).await.unwrap());
        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();
        assert_eq!(queue_status(&repository, &queue_id).await, "COMPLETED");

        close_repo(repository, config).await;
    }

    /// T12: tras COMMIT de enqueue, un reopen ve el trabajo (C2).
    #[tokio::test]
    async fn pending_after_crash_mid_enqueue_commit_is_visible() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 12})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        repository.pool.close().await;
        drop(repository);

        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let pending = repository.pending("flow").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].queue_id, queue_id);
        assert_eq!(pending[0].packet.records().unwrap()[0]["n"], 12);

        close_repo(repository, config).await;
    }

    /// T13: tras recover + complete + cleanup + reconcile, stats e invariantes OK.
    #[tokio::test]
    async fn stats_and_invariants_hold_after_recover_and_cleanup() {
        let config = test_config();
        let repository = LocalPacketRepository::open(&config).await.unwrap();
        let packet = DataPacket::with_records(vec![serde_json::json!({"n": 13})]);
        let queue_id = repository
            .enqueue("flow", "sink", "success", &packet)
            .await
            .unwrap();
        assert!(repository.claim(&queue_id).await.unwrap());
        assert_eq!(repository.recover_abandoned("flow", 0).await.unwrap(), 1);
        assert!(repository.claim(&queue_id).await.unwrap());
        repository
            .complete(&queue_id, ProvenanceEvent::Completed)
            .await
            .unwrap();

        let _ = repository.content.put(b"leftover-orphan").await.unwrap();
        let cleaned = repository.cleanup_completed(0).await.unwrap();
        assert_eq!(cleaned, 1);
        let report = repository.reconcile_orphans().await.unwrap();
        assert!(report.orphan_blobs_removed >= 1);

        let stats = repository.stats().await.unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 0);
        assert_eq!(stats.dead_letter, 0);
        assert_eq!(
            list_content_files(config.content_path.as_path())
                .unwrap()
                .len(),
            0
        );

        close_repo(repository, config).await;
    }
}

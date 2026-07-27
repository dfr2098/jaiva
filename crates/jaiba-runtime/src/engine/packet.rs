use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::RecordSchema;

/// Content transported between processors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PacketContent {
    /// Structured JSON-compatible records.
    Records(Vec<Value>),
    /// Serialized or binary content and its media type.
    Encoded { media_type: String, bytes: Vec<u8> },
}

/// Identifiable unit of data moving through a flow.
///
/// Packets may be replayed after recovery, so destination operations should be
/// idempotent where possible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPacket {
    pub id: Uuid,
    pub attributes: HashMap<String, String>,
    pub content: PacketContent,
    pub schema: Option<RecordSchema>,
    pub attempt: u32,
}

impl DataPacket {
    /// Creates an empty records packet with a new identifier.
    pub fn empty() -> Self {
        Self {
            id: Uuid::new_v4(),
            attributes: HashMap::new(),
            content: PacketContent::Records(Vec::new()),
            schema: None,
            attempt: 0,
        }
    }

    /// Borrows structured records or reports that content is encoded.
    pub fn records(&self) -> Result<&[Value], String> {
        match &self.content {
            PacketContent::Records(records) => Ok(records),
            PacketContent::Encoded { media_type, .. } => {
                Err(format!("expected records, found encoded {media_type}"))
            }
        }
    }

    /// Mutably borrows structured records.
    pub fn records_mut(&mut self) -> Result<&mut Vec<Value>, String> {
        match &mut self.content {
            PacketContent::Records(records) => Ok(records),
            PacketContent::Encoded { media_type, .. } => {
                Err(format!("expected records, found encoded {media_type}"))
            }
        }
    }

    /// Creates a new packet containing structured records.
    pub fn with_records(records: Vec<Value>) -> Self {
        let mut packet = Self::empty();
        packet.content = PacketContent::Records(records);
        packet
    }

    /// Estimates packet memory for backpressure accounting.
    ///
    /// This is deliberately an estimate, not an allocator-exact measurement.
    pub fn estimated_size(&self) -> usize {
        let attributes = self
            .attributes
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum::<usize>();
        let content = match &self.content {
            PacketContent::Records(records) => {
                serde_json::to_vec(records).map_or(0, |bytes| bytes.len())
            }
            PacketContent::Encoded { media_type, bytes } => {
                media_type.len().saturating_add(bytes.len())
            }
        };
        let schema = self
            .schema
            .as_ref()
            .and_then(|schema| serde_json::to_vec(schema).ok())
            .map_or(0, |bytes| bytes.len());
        attributes
            .saturating_add(content)
            .saturating_add(schema)
            .saturating_add(std::mem::size_of::<Self>())
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordSchema {
    pub name: Option<String>,
    pub fields: Vec<FieldSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub data_type: DataType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Boolean,
    Int64,
    Float64,
    Decimal { precision: u8, scale: u8 },
    String,
    Date,
    Timestamp,
    TimestampWithTimezone,
    Uuid,
    Binary,
    Json,
}

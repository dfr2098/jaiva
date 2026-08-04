mod ai_prep;
mod checkpoint;
#[cfg(feature = "kafka-driver")]
mod consume_kafka;
mod encode;
mod generate_records;
mod log_records;
#[cfg(feature = "mongodb-driver")]
mod mongodb_support;
#[cfg(feature = "kafka-driver")]
mod publish_kafka;
mod put_database;
#[cfg(feature = "mongodb-driver")]
mod put_mongodb;
#[cfg(feature = "mongodb-driver")]
mod query_mongodb;
#[cfg(feature = "oracle-driver")]
mod query_oracle;
mod query_postgres;
mod rename_fields;
mod write_file;

use std::sync::Arc;

use crate::engine::ProcessorRegistry;

use ai_prep::{
    AiCastTypes, AiComputeFields, AiDropNulls, AiEncodeCategories, AiExportManifest, AiFillMissing,
    AiFilterRange, AiLookupJoin, AiNormalize, AiRemoveDuplicates, AiSelectFields, AiSplitDataset,
    AiTriggerWebhook,
};
use checkpoint::{LoadCheckpoint, SaveCheckpoint};
#[cfg(feature = "kafka-driver")]
use consume_kafka::ConsumeKafka;
use encode::Encode;
use generate_records::GenerateRecords;
use log_records::LogRecords;
#[cfg(feature = "kafka-driver")]
use publish_kafka::PublishKafka;
use put_database::PutDatabase;
#[cfg(feature = "mongodb-driver")]
use put_mongodb::PutMongoDb;
#[cfg(feature = "mongodb-driver")]
use query_mongodb::QueryMongoDb;
#[cfg(feature = "oracle-driver")]
use query_oracle::QueryOracle;
use query_postgres::QueryPostgres;
use rename_fields::RenameFields;
use write_file::WriteFile;

pub fn default_registry() -> ProcessorRegistry {
    let mut registry = ProcessorRegistry::default();
    registry.register("generate_records", |config| {
        Ok(Arc::new(GenerateRecords::from_config(config)?))
    });
    registry.register("rename_fields", |config| {
        Ok(Arc::new(RenameFields::from_config(config)?))
    });
    // AI Data Prep (siempre disponible; ver `ai_prep` y docs/ai-data-prep.md).
    registry.register("ai_select_fields", |config| {
        Ok(Arc::new(AiSelectFields::from_config(config)?))
    });
    registry.register("ai_drop_nulls", |config| {
        Ok(Arc::new(AiDropNulls::from_config(config)?))
    });
    registry.register("ai_fill_missing", |config| {
        Ok(Arc::new(AiFillMissing::from_config(config)?))
    });
    registry.register("ai_remove_duplicates", |config| {
        Ok(Arc::new(AiRemoveDuplicates::from_config(config)?))
    });
    registry.register("ai_filter_range", |config| {
        Ok(Arc::new(AiFilterRange::from_config(config)?))
    });
    registry.register("ai_cast_types", |config| {
        Ok(Arc::new(AiCastTypes::from_config(config)?))
    });
    registry.register("ai_normalize", |config| {
        Ok(Arc::new(AiNormalize::from_config(config)?))
    });
    registry.register("ai_encode_categories", |config| {
        Ok(Arc::new(AiEncodeCategories::from_config(config)?))
    });
    registry.register("ai_compute_fields", |config| {
        Ok(Arc::new(AiComputeFields::from_config(config)?))
    });
    registry.register("ai_split_dataset", |config| {
        Ok(Arc::new(AiSplitDataset::from_config(config)?))
    });
    registry.register("ai_lookup_join", |config| {
        Ok(Arc::new(AiLookupJoin::from_config(config)?))
    });
    registry.register("ai_export_manifest", |config| {
        Ok(Arc::new(AiExportManifest::from_config(config)?))
    });
    registry.register("ai_trigger_webhook", |config| {
        Ok(Arc::new(AiTriggerWebhook::from_config(config)?))
    });
    registry.register("log_records", |_| Ok(Arc::new(LogRecords)));
    #[cfg(feature = "kafka-driver")]
    registry.register("publish_kafka", |config| {
        Ok(Arc::new(PublishKafka::from_config(config)?))
    });
    #[cfg(feature = "kafka-driver")]
    registry.register("consume_kafka", |config| {
        Ok(Arc::new(ConsumeKafka::from_config(config)?))
    });
    registry.register("query_postgres", |config| {
        Ok(Arc::new(QueryPostgres::from_config(config)?))
    });
    #[cfg(feature = "mongodb-driver")]
    registry.register("query_mongodb", |config| {
        Ok(Arc::new(QueryMongoDb::from_config(config)?))
    });
    #[cfg(feature = "oracle-driver")]
    registry.register("query_oracle", |config| {
        Ok(Arc::new(QueryOracle::from_config(config)?))
    });
    registry.register("put_database", |config| {
        Ok(Arc::new(PutDatabase::from_config(config)?))
    });
    #[cfg(feature = "mongodb-driver")]
    registry.register("put_mongodb", |config| {
        Ok(Arc::new(PutMongoDb::from_config(config)?))
    });
    registry.register("auto_destination", |config| {
        Ok(Arc::new(PutDatabase::from_auto_config(config)?))
    });
    registry.register("encode_json", |config| Ok(Arc::new(Encode::json(config)?)));
    registry.register("encode_yaml", |config| Ok(Arc::new(Encode::yaml(config)?)));
    registry.register("encode_csv", |config| Ok(Arc::new(Encode::csv(config)?)));
    registry.register("encode_xml", |config| Ok(Arc::new(Encode::xml(config)?)));
    registry.register("write_file", |config| {
        Ok(Arc::new(WriteFile::from_config(config)?))
    });
    registry.register("load_checkpoint", |config| {
        Ok(Arc::new(LoadCheckpoint::from_config(config)?))
    });
    registry.register("save_checkpoint", |config| {
        Ok(Arc::new(SaveCheckpoint::from_config(config)?))
    });
    registry
}

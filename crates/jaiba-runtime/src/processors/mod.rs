mod checkpoint;
mod encode;
mod generate_records;
mod log_records;
#[cfg(feature = "mongodb-driver")]
mod mongodb_support;
#[cfg(feature = "kafka-driver")]
mod consume_kafka;
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

use checkpoint::{LoadCheckpoint, SaveCheckpoint};
use encode::Encode;
use generate_records::GenerateRecords;
use log_records::LogRecords;
#[cfg(feature = "kafka-driver")]
use consume_kafka::ConsumeKafka;
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

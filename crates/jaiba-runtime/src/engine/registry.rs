use std::{collections::HashMap, sync::Arc};

use serde_json::Value;

use crate::error::FlowError;

use super::Processor;

/// Factory that constructs a processor from JSON/YAML configuration.
pub type ProcessorFactory =
    Arc<dyn Fn(&Value) -> Result<Arc<dyn Processor>, FlowError> + Send + Sync>;

/// Mapping between processor type names and factories.
#[derive(Clone, Default)]
pub struct ProcessorRegistry {
    factories: HashMap<String, ProcessorFactory>,
}

impl ProcessorRegistry {
    /// Registers or replaces a factory.
    pub fn register<F>(&mut self, processor_type: &str, factory: F)
    where
        F: Fn(&Value) -> Result<Arc<dyn Processor>, FlowError> + Send + Sync + 'static,
    {
        self.factories
            .insert(processor_type.to_owned(), Arc::new(factory));
    }

    /// Builds a processor or reports an unknown type.
    pub fn build(
        &self,
        processor_type: &str,
        config: &Value,
    ) -> Result<Arc<dyn Processor>, FlowError> {
        self.factories
            .get(processor_type)
            .ok_or_else(|| FlowError::Configuration(unknown_processor_hint(processor_type)))?(
            config,
        )
    }
}

fn unknown_processor_hint(processor_type: &str) -> String {
    let feature = match processor_type {
        name if name.contains("oracle") => Some("oracle-driver"),
        name if name.contains("sql_server") || name.contains("sqlserver") => {
            Some("sqlserver-driver")
        }
        name if name.contains("mongo") => Some("mongodb-driver"),
        name if name.contains("kafka") => Some("kafka-driver"),
        _ => None,
    };
    match feature {
        Some(flag) => format!(
            "procesador '{processor_type}' no disponible: activa --features {flag} \
             (ejemplo: cargo run --features {flag} -- serve examples/basic-flow.yaml)"
        ),
        None => format!(
            "procesador desconocido '{processor_type}' \
             (revisa el type en el YAML o la tabla quiero→feature en docs/guia-para-nuevos.md)"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::unknown_processor_hint;

    #[test]
    fn unknown_oracle_processor_hints_feature_flag() {
        let message = unknown_processor_hint("query_oracle");
        assert!(
            message.contains("activa --features oracle-driver"),
            "{message}"
        );
        assert!(message.contains("oracle-driver"), "{message}");
    }

    #[test]
    fn unknown_kafka_processor_hints_feature_flag() {
        let message = unknown_processor_hint("publish_kafka");
        assert!(
            message.contains("activa --features kafka-driver"),
            "{message}"
        );
    }
}

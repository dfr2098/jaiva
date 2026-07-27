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
        self.factories.get(processor_type).ok_or_else(|| {
            FlowError::Configuration(format!("unknown processor type '{processor_type}'"))
        })?(config)
    }
}

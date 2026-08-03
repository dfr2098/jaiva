//! Toolkit **AI Data Prep**: preparar datasets tabulares para ML externo.
//!
//! Jaiba limpia, tipa, normaliza, encodea, calcula features y parte train/val/test.
//! **No entrena ni despliega modelos** (sin Python, notebooks ni PyO3).
//!
//! # Unidad de trabajo
//!
//! Opera sobre [`crate::engine::PacketContent::Records`] (arrays de objetos JSON),
//! igual que `query_oracle` / `rename_fields`. Transforms usan
//! [`crate::config::ExecutionMode::Cpu`].
//!
//! # Submódulos
//!
//! - [`transforms`] — limpieza y tipado (`ai_select_fields` … `ai_cast_types`)
//! - [`features`] — normalize / encode / compute / split
//! - [`lookup_join`] — enriquecimiento por clave en memoria
//! - [`export_manifest`] / [`trigger_webhook`] — hand-off a Azure ML, Fabric, etc.
//! - [`support`] — helpers compartidos y evaluador aritmético seguro
//!
//! Documentación de producto: `docs/ai-data-prep.md`.
//! Ejemplo: `examples/ai-prep-conveyor.yaml`.

mod export_manifest;
mod features;
mod lookup_join;
mod support;
mod transforms;
mod trigger_webhook;

#[cfg(test)]
mod tests;

pub use export_manifest::AiExportManifest;
pub use features::{
    AiComputeFields, AiEncodeCategories, AiNormalize, AiSplitDataset,
};
pub use lookup_join::AiLookupJoin;
pub use transforms::{
    AiCastTypes, AiDropNulls, AiFillMissing, AiFilterRange, AiRemoveDuplicates, AiSelectFields,
};
pub use trigger_webhook::AiTriggerWebhook;

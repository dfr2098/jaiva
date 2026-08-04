//! Features y partición: normalize, encode, compute y split.
//!
//! Tras la limpieza, estos nodos dejan el dataset listo para export CSV/JSON.
//! `ai_split_dataset` emite relaciones `train` / `validation` / `test` (no
//! `success`); el YAML debe conectar cada relación al encode/write correspondiente.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::support::{as_f64, as_object_mut, eval_expr, json_number, require_objects};
use super::transforms::OnError;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NormalizeMethod {
    /// Escala a \[0, 1\] con min/max del lote (o acumulados).
    MinMax,
    /// (x − μ) / σ; σ=0 → 0.
    ZScore,
}

/// Momentos online por campo (min/max/sum/sum²) para normalize.
#[derive(Clone, Default)]
struct FieldStats {
    count: u64,
    sum: f64,
    sum_sq: f64,
    min: f64,
    max: f64,
}

impl FieldStats {
    fn observe(&mut self, value: f64) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
        self.count += 1;
        self.sum += value;
        self.sum_sq += value * value;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let mean = self.mean();
        let var = (self.sum_sq / self.count as f64) - (mean * mean);
        var.max(0.0).sqrt()
    }
}

/// `ai_normalize`: escala numérica min-max o z-score.
///
/// MVP: stats del paquete actual. Con `cumulative: true`, fusiona momentos
/// entre paquetes del mismo nodo (mejor para streams; no es pasada global offline).
pub struct AiNormalize {
    fields: Vec<String>,
    method: NormalizeMethod,
    cumulative: bool,
    state: Mutex<HashMap<String, FieldStats>>,
}

#[derive(Deserialize)]
struct NormalizeConfig {
    fields: Vec<String>,
    #[serde(default = "default_minmax_method")]
    method: NormalizeMethod,
    #[serde(default)]
    cumulative: bool,
}

fn default_minmax_method() -> NormalizeMethod {
    NormalizeMethod::MinMax
}

impl AiNormalize {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: NormalizeConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_normalize requires fields".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
            method: config.method,
            cumulative: config.cumulative,
            state: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl Processor for AiNormalize {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet
            .records_mut()
            .map_err(|message| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message,
            })?;
        require_objects(records, &context.processor_id)?;

        // Pasada 1: estadísticas del lote (y opcionalmente merge al estado).
        let mut batch_stats: HashMap<String, FieldStats> = HashMap::new();
        for field in &self.fields {
            for record in records.iter() {
                if let Some(number) = record.get(field).and_then(as_f64) {
                    batch_stats
                        .entry(field.clone())
                        .or_default()
                        .observe(number);
                }
            }
        }

        // Clonar stats fuera del Mutex para no retener el guard across await.
        let stats_view: HashMap<String, FieldStats> = if self.cumulative {
            let mut guard = self.state.lock().map_err(|_| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: "ai_normalize state lock poisoned".to_owned(),
            })?;
            for (field, batch) in &batch_stats {
                let entry = guard.entry(field.clone()).or_default();
                let was_empty = entry.count == 0;
                entry.count += batch.count;
                entry.sum += batch.sum;
                entry.sum_sq += batch.sum_sq;
                if batch.count > 0 {
                    if was_empty {
                        entry.min = batch.min;
                        entry.max = batch.max;
                    } else {
                        entry.min = entry.min.min(batch.min);
                        entry.max = entry.max.max(batch.max);
                    }
                }
            }
            guard.clone()
        } else {
            batch_stats
        };

        for record in records.iter_mut() {
            let object = as_object_mut(record, &context.processor_id)?;
            for field in &self.fields {
                let Some(number) = object.get(field).and_then(as_f64) else {
                    continue;
                };
                let Some(stats) = stats_view.get(field) else {
                    continue;
                };
                let scaled = match self.method {
                    NormalizeMethod::MinMax => {
                        let span = stats.max - stats.min;
                        if span == 0.0 {
                            0.0
                        } else {
                            (number - stats.min) / span
                        }
                    }
                    NormalizeMethod::ZScore => {
                        let std = stats.std_dev();
                        if std == 0.0 {
                            0.0
                        } else {
                            (number - stats.mean()) / std
                        }
                    }
                };
                object.insert(field.clone(), json_number(scaled));
            }
        }
        output.success(packet).await
    }
}

/// `ai_encode_categories`: label encoding con mapa fijo en YAML.
///
/// Ejemplo: `status: { OK: 0, WARN: 1 }`. Categorías desconocidas → drop/fail.
pub struct AiEncodeCategories {
    fields: HashMap<String, HashMap<String, i64>>,
    on_error: OnError,
}

#[derive(Deserialize)]
struct EncodeConfig {
    /// Campo → etiqueta → código entero.
    fields: HashMap<String, HashMap<String, i64>>,
    #[serde(default)]
    on_error: OnError,
}

impl AiEncodeCategories {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: EncodeConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_encode_categories requires fields".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
            on_error: config.on_error,
        })
    }
}

#[async_trait]
impl Processor for AiEncodeCategories {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet
            .records_mut()
            .map_err(|message| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message,
            })?;
        require_objects(records, &context.processor_id)?;
        let mut kept = Vec::with_capacity(records.len());
        for mut record in records.drain(..) {
            let object = as_object_mut(&mut record, &context.processor_id)?;
            let mut ok = true;
            for (field, mapping) in &self.fields {
                let Some(value) = object.get(field) else {
                    continue;
                };
                let label = match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                };
                match mapping.get(&label) {
                    Some(code) => {
                        object.insert(field.clone(), Value::from(*code));
                    }
                    None if matches!(self.on_error, OnError::Drop) => {
                        ok = false;
                        break;
                    }
                    None => {
                        return Err(FlowError::Processor {
                            processor_id: context.processor_id.clone(),
                            message: format!("unknown category '{label}' for field '{field}'"),
                        });
                    }
                }
            }
            if ok {
                kept.push(record);
            }
        }
        *packet.records_mut().unwrap() = kept;
        output.success(packet).await
    }
}

/// `ai_compute_fields`: crea columnas con expresiones `+ - * /` sobre números.
///
/// Ver [`super::support::eval_expr`]. Ideal para features industriales simples
/// (`temperature * vibration + plant`).
pub struct AiComputeFields {
    /// Campo destino → expresión.
    fields: HashMap<String, String>,
    on_error: OnError,
}

#[derive(Deserialize)]
struct ComputeConfig {
    fields: HashMap<String, String>,
    #[serde(default)]
    on_error: OnError,
}

impl AiComputeFields {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: ComputeConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_compute_fields requires fields".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
            on_error: config.on_error,
        })
    }
}

#[async_trait]
impl Processor for AiComputeFields {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet
            .records_mut()
            .map_err(|message| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message,
            })?;
        require_objects(records, &context.processor_id)?;
        let mut kept = Vec::with_capacity(records.len());
        for mut record in records.drain(..) {
            let object = as_object_mut(&mut record, &context.processor_id)?;
            let mut ok = true;
            for (target, expr) in &self.fields {
                match eval_expr(expr, object) {
                    Ok(value) => {
                        object.insert(target.clone(), json_number(value));
                    }
                    Err(_) if matches!(self.on_error, OnError::Drop) => {
                        ok = false;
                        break;
                    }
                    Err(message) => {
                        return Err(FlowError::Processor {
                            processor_id: context.processor_id.clone(),
                            message,
                        });
                    }
                }
            }
            if ok {
                kept.push(record);
            }
        }
        *packet.records_mut().unwrap() = kept;
        output.success(packet).await
    }
}

/// `ai_split_dataset`: parte el paquete en train / validation / test.
///
/// Ratios ≥ 0 y suma ≈ 1.0 (default 0.7 / 0.2 / 0.1). El remanente tras
/// redondeo de train+validation cae en `test`. Emite solo splits no vacíos
/// y marca el atributo `ai.split` en cada paquete saliente.
///
/// Con `shuffle: true` reordena las filas antes del corte (PRNG xorshift64;
/// `seed` opcional para reproducibilidad).
pub struct AiSplitDataset {
    train: f64,
    validation: f64,
    /// Guardado para validar la suma; el remanente del slice es `test`.
    #[allow(dead_code)]
    test: f64,
    shuffle: bool,
    seed: u64,
}

#[derive(Deserialize)]
struct SplitConfig {
    #[serde(default = "default_train")]
    train: f64,
    #[serde(default = "default_validation")]
    validation: f64,
    #[serde(default = "default_test")]
    test: f64,
    #[serde(default)]
    shuffle: bool,
    #[serde(default)]
    seed: Option<u64>,
}

fn default_train() -> f64 {
    0.7
}
fn default_validation() -> f64 {
    0.2
}
fn default_test() -> f64 {
    0.1
}

/// Semilla por defecto si `shuffle` sin `seed` explícito.
const DEFAULT_SPLIT_SEED: u64 = 0x4A41_4942_415F_5344; // JAIBA_SD

impl AiSplitDataset {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: SplitConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        let sum = config.train + config.validation + config.test;
        if (sum - 1.0).abs() > 0.001
            || config.train < 0.0
            || config.validation < 0.0
            || config.test < 0.0
        {
            return Err(FlowError::Configuration(
                "ai_split_dataset ratios must be >= 0 and sum to 1.0".to_owned(),
            ));
        }
        Ok(Self {
            train: config.train,
            validation: config.validation,
            test: config.test,
            shuffle: config.shuffle,
            seed: config.seed.unwrap_or(DEFAULT_SPLIT_SEED),
        })
    }
}

#[async_trait]
impl Processor for AiSplitDataset {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        require_objects(records, &context.processor_id)?;
        let mut records = records.to_vec();
        let total = records.len();
        if self.shuffle {
            fisher_yates_shuffle(&mut records, self.seed);
        }
        let train_end = ((total as f64) * self.train).round() as usize;
        let validation_end = train_end + ((total as f64) * self.validation).round() as usize;
        let train_end = train_end.min(total);
        let validation_end = validation_end.min(total);

        let train = records[..train_end].to_vec();
        let validation = records[train_end..validation_end].to_vec();
        let test = records[validation_end..].to_vec();

        emit_split(output, &packet, "train", train).await?;
        emit_split(output, &packet, "validation", validation).await?;
        emit_split(output, &packet, "test", test).await?;
        Ok(())
    }
}

/// Fisher–Yates con xorshift64 (sin dependencia `rand`).
fn fisher_yates_shuffle(records: &mut [Value], seed: u64) {
    let mut state = if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    };
    for i in (1..records.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let j = (state as usize) % (i + 1);
        records.swap(i, j);
    }
}

/// Emite un subconjunto por relación de routing (`train` | `validation` | `test`).
async fn emit_split(
    output: &OutputSender,
    template: &DataPacket,
    relationship: &str,
    records: Vec<Value>,
) -> Result<(), FlowError> {
    let mut packet = DataPacket::with_records(records);
    packet.attributes = template.attributes.clone();
    packet
        .attributes
        .insert("ai.split".to_owned(), relationship.to_owned());
    packet
        .attributes
        .insert("ai.split_group".to_owned(), template.id.to_string());
    output.emit(relationship, packet).await
}

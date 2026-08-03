//! Limpieza y tipado tabular (`ai_select_fields` … `ai_cast_types`).
//!
//! Cadena típica: select → drop_nulls → cast → fill → dedupe → filter_range.
//! Por defecto, filas inválidas se descartan (`on_error: drop`); `fail` aborta
//! el procesador ante el primer error.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, Processor, ProcessorContext},
    error::FlowError,
};

use super::support::{as_f64, as_object_mut, field_key, is_missing, json_number, require_objects};

/// Política ante fila inválida en cast / encode / compute.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OnError {
    /// Descarta la fila y sigue (default de prep).
    #[default]
    Drop,
    /// Propaga error y detiene el procesador.
    Fail,
}

/// `ai_select_fields`: conserva (`keep`) y/o elimina (`drop`) columnas.
pub struct AiSelectFields {
    keep: Option<Vec<String>>,
    drop: Vec<String>,
}

#[derive(Deserialize)]
struct SelectConfig {
    #[serde(default)]
    keep: Option<Vec<String>>,
    #[serde(default)]
    drop: Vec<String>,
}

impl AiSelectFields {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: SelectConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.drop.is_empty()
            && config
                .keep
                .as_ref()
                .map(|fields| fields.is_empty())
                .unwrap_or(true)
        {
            return Err(FlowError::Configuration(
                "ai_select_fields requires keep or drop".to_owned(),
            ));
        }
        Ok(Self {
            keep: config.keep,
            drop: config.drop,
        })
    }
}

#[async_trait]
impl Processor for AiSelectFields {
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
        let drop: HashSet<&str> = self.drop.iter().map(String::as_str).collect();
        for record in records.iter_mut() {
            let object = as_object_mut(record, &context.processor_id)?;
            if let Some(keep) = &self.keep {
                let keep: HashSet<&str> = keep.iter().map(String::as_str).collect();
                object.retain(|key, _| keep.contains(key.as_str()));
            }
            for key in &drop {
                object.remove(*key);
            }
        }
        output.success(packet).await
    }
}

/// `ai_drop_nulls`: elimina filas con null/vacío en cualquiera de `fields`.
pub struct AiDropNulls {
    fields: Vec<String>,
}

#[derive(Deserialize)]
struct DropNullsConfig {
    fields: Vec<String>,
}

impl AiDropNulls {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: DropNullsConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_drop_nulls requires fields".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
        })
    }
}

#[async_trait]
impl Processor for AiDropNulls {
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
        records.retain(|record| {
            let Some(object) = record.as_object() else {
                return false;
            };
            self.fields
                .iter()
                .all(|field| !is_missing(object.get(field)))
        });
        output.success(packet).await
    }
}

/// Estrategia de imputación para `ai_fill_missing`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FillStrategy {
    /// Último valor no missing visto en el recorrido del paquete (forward-fill).
    Previous,
    /// Valor fijo de configuración.
    Constant,
    /// Media de valores numéricos presentes (lote o acumulado).
    Mean,
    /// Mediana de valores numéricos presentes (lote o acumulado).
    Median,
}

/// Acumulador de momentos para mean/median con `cumulative: true`.
#[derive(Default)]
struct NumericAgg {
    count: u64,
    sum: f64,
    /// Solo se rellena en estrategia median (crece con el flujo).
    samples: Vec<f64>,
}

/// `ai_fill_missing`: imputa nulos (`previous` / `constant` / `mean` / `median`).
///
/// Con `cumulative: true`, mean/median se actualizan entre paquetes del mismo
/// procesador (útil en streams; consume memoria en median).
pub struct AiFillMissing {
    fields: Vec<String>,
    strategy: FillStrategy,
    constant: Option<Value>,
    cumulative: bool,
    state: Mutex<HashMap<String, NumericAgg>>,
}

#[derive(Deserialize)]
struct FillConfig {
    fields: Vec<String>,
    #[serde(default = "default_previous")]
    strategy: FillStrategy,
    #[serde(default)]
    constant: Option<Value>,
    #[serde(default)]
    cumulative: bool,
}

fn default_previous() -> FillStrategy {
    FillStrategy::Previous
}

impl AiFillMissing {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: FillConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_fill_missing requires fields".to_owned(),
            ));
        }
        if matches!(config.strategy, FillStrategy::Constant) && config.constant.is_none() {
            return Err(FlowError::Configuration(
                "ai_fill_missing strategy=constant requires constant".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
            strategy: config.strategy,
            constant: config.constant,
            cumulative: config.cumulative,
            state: Mutex::new(HashMap::new()),
        })
    }
}

#[async_trait]
impl Processor for AiFillMissing {
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

        let mut stats: HashMap<String, f64> = HashMap::new();
        if matches!(self.strategy, FillStrategy::Mean | FillStrategy::Median) {
            if self.cumulative {
                let mut guard = self.state.lock().map_err(|_| FlowError::Processor {
                    processor_id: context.processor_id.clone(),
                    message: "ai_fill_missing state lock poisoned".to_owned(),
                })?;
                for field in &self.fields {
                    let entry = guard.entry(field.clone()).or_default();
                    for record in records.iter() {
                        if let Some(number) = record.get(field).and_then(as_f64) {
                            entry.count += 1;
                            entry.sum += number;
                            if matches!(self.strategy, FillStrategy::Median) {
                                entry.samples.push(number);
                            }
                        }
                    }
                    if entry.count == 0 {
                        continue;
                    }
                    let aggregate = match self.strategy {
                        FillStrategy::Mean => entry.sum / entry.count as f64,
                        FillStrategy::Median => median_of(&mut entry.samples.clone()),
                        _ => unreachable!(),
                    };
                    stats.insert(field.clone(), aggregate);
                }
            } else {
                for field in &self.fields {
                    let mut values = Vec::new();
                    for record in records.iter() {
                        if let Some(number) = record.get(field).and_then(as_f64) {
                            values.push(number);
                        }
                    }
                    if values.is_empty() {
                        continue;
                    }
                    let aggregate = match self.strategy {
                        FillStrategy::Mean => values.iter().sum::<f64>() / values.len() as f64,
                        FillStrategy::Median => median_of(&mut values),
                        _ => unreachable!(),
                    };
                    stats.insert(field.clone(), aggregate);
                }
            }
        }

        let mut previous: HashMap<String, Value> = HashMap::new();
        for record in records.iter_mut() {
            let object = as_object_mut(record, &context.processor_id)?;
            for field in &self.fields {
                if !is_missing(object.get(field)) {
                    previous.insert(field.clone(), object.get(field).cloned().unwrap());
                    continue;
                }
                let fill = match self.strategy {
                    FillStrategy::Previous => previous.get(field).cloned(),
                    FillStrategy::Constant => self.constant.clone(),
                    FillStrategy::Mean | FillStrategy::Median => {
                        stats.get(field).copied().map(json_number)
                    }
                };
                if let Some(value) = fill {
                    object.insert(field.clone(), value);
                }
            }
        }
        output.success(packet).await
    }
}

/// `ai_remove_duplicates`: conserva la primera fila por `key_fields`.
///
/// Sin `window`: dedupe solo dentro del paquete actual.
/// Con `window: N`: ventana LRU de N claves entre paquetes (cuidado con RAM).
pub struct AiRemoveDuplicates {
    key_fields: Vec<String>,
    window: Option<usize>,
    window_state: Mutex<(HashSet<String>, VecDeque<String>)>,
}

#[derive(Deserialize)]
struct DedupeConfig {
    key_fields: Vec<String>,
    #[serde(default)]
    window: Option<usize>,
}

impl AiRemoveDuplicates {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: DedupeConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.key_fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_remove_duplicates requires key_fields".to_owned(),
            ));
        }
        if config.window == Some(0) {
            return Err(FlowError::Configuration(
                "ai_remove_duplicates window must be > 0 when set".to_owned(),
            ));
        }
        Ok(Self {
            key_fields: config.key_fields,
            window: config.window,
            window_state: Mutex::new((HashSet::new(), VecDeque::new())),
        })
    }
}

#[async_trait]
impl Processor for AiRemoveDuplicates {
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
        if let Some(window) = self.window {
            let mut guard = self.window_state.lock().map_err(|_| FlowError::Processor {
                processor_id: context.processor_id.clone(),
                message: "ai_remove_duplicates state lock poisoned".to_owned(),
            })?;
            let (seen, order) = &mut *guard;
            records.retain(|record| {
                let Some(object) = record.as_object() else {
                    return false;
                };
                let key = field_key(object, &self.key_fields);
                if seen.contains(&key) {
                    return false;
                }
                seen.insert(key.clone());
                order.push_back(key);
                while order.len() > window {
                    if let Some(expired) = order.pop_front() {
                        seen.remove(&expired);
                    }
                }
                true
            });
        } else {
            let mut seen = HashSet::new();
            records.retain(|record| {
                let Some(object) = record.as_object() else {
                    return false;
                };
                seen.insert(field_key(object, &self.key_fields))
            });
        }
        output.success(packet).await
    }
}

/// Mediana sobre slice ya materializado (ordena in-place).
fn median_of(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

/// Modo de filtrado de outliers.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RangeMode {
    /// Umbrales explícitos `min` / `max` (opcionales).
    MinMax,
    /// Tukey: fuera de `[Q1 - k·IQR, Q3 + k·IQR]` (stats del lote).
    Iqr,
}

/// `ai_filter_range`: descarta outliers numéricos en un campo.
pub struct AiFilterRange {
    field: String,
    mode: RangeMode,
    min: Option<f64>,
    max: Option<f64>,
    iqr_multiplier: f64,
}

#[derive(Deserialize)]
struct FilterRangeConfig {
    field: String,
    #[serde(default = "default_minmax")]
    mode: RangeMode,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default = "default_iqr_k")]
    iqr_multiplier: f64,
}

fn default_minmax() -> RangeMode {
    RangeMode::MinMax
}

fn default_iqr_k() -> f64 {
    1.5
}

impl AiFilterRange {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: FilterRangeConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.field.trim().is_empty() {
            return Err(FlowError::Configuration(
                "ai_filter_range requires field".to_owned(),
            ));
        }
        Ok(Self {
            field: config.field,
            mode: config.mode,
            min: config.min,
            max: config.max,
            iqr_multiplier: config.iqr_multiplier,
        })
    }
}

#[async_trait]
impl Processor for AiFilterRange {
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

        let (low, high) = match self.mode {
            RangeMode::MinMax => (self.min, self.max),
            RangeMode::Iqr => {
                let mut values: Vec<f64> = records
                    .iter()
                    .filter_map(|record| record.get(&self.field).and_then(as_f64))
                    .collect();
                if values.len() < 4 {
                    output.success(packet).await?;
                    return Ok(());
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let q1 = percentile(&values, 0.25);
                let q3 = percentile(&values, 0.75);
                let iqr = q3 - q1;
                (
                    Some(q1 - self.iqr_multiplier * iqr),
                    Some(q3 + self.iqr_multiplier * iqr),
                )
            }
        };

        records.retain(|record| {
            let Some(number) = record.get(&self.field).and_then(as_f64) else {
                return false;
            };
            if low.is_some_and(|min| number < min) {
                return false;
            }
            if high.is_some_and(|max| number > max) {
                return false;
            }
            true
        });
        output.success(packet).await
    }
}

/// Percentil lineal sobre slice ya ordenado (`p` en \[0, 1\]).
fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = p * (sorted.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        let weight = rank - lo as f64;
        sorted[lo] * (1.0 - weight) + sorted[hi] * weight
    }
}

/// Destino de coerción para `ai_cast_types`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CastKind {
    Number,
    String,
    Bool,
    /// Normaliza a string (ISO u epoch textual); no parsea calendario completo.
    Timestamp,
}

/// `ai_cast_types`: convierte campos a number/string/bool/timestamp.
pub struct AiCastTypes {
    fields: HashMap<String, CastKind>,
    on_error: OnError,
}

#[derive(Deserialize)]
struct CastConfig {
    fields: HashMap<String, CastKind>,
    #[serde(default)]
    on_error: OnError,
}

impl AiCastTypes {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let config: CastConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(error.to_string()))?;
        if config.fields.is_empty() {
            return Err(FlowError::Configuration(
                "ai_cast_types requires fields".to_owned(),
            ));
        }
        Ok(Self {
            fields: config.fields,
            on_error: config.on_error,
        })
    }
}

#[async_trait]
impl Processor for AiCastTypes {
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
        for record in records.drain(..) {
            match cast_record(record, &self.fields) {
                Ok(record) => kept.push(record),
                Err(_) if matches!(self.on_error, OnError::Drop) => continue,
                Err(message) => {
                    return Err(FlowError::Processor {
                        processor_id: context.processor_id.clone(),
                        message,
                    });
                }
            }
        }
        *packet.records_mut().unwrap() = kept;
        output.success(packet).await
    }
}

/// Aplica el mapa de casts a una fila; deja `null` sin tocar.
fn cast_record(
    mut record: Value,
    fields: &HashMap<String, CastKind>,
) -> Result<Value, String> {
    let object = record
        .as_object_mut()
        .ok_or_else(|| "expected object".to_owned())?;
    for (field, kind) in fields {
        let Some(value) = object.get(field).cloned() else {
            continue;
        };
        if matches!(value, Value::Null) {
            continue;
        }
        let casted = match kind {
            CastKind::Number => as_f64(&value)
                .map(json_number)
                .ok_or_else(|| format!("cannot cast '{field}' to number"))?,
            CastKind::String => Value::String(match value {
                Value::String(text) => text,
                other => other.to_string(),
            }),
            CastKind::Bool => Value::Bool(match value {
                Value::Bool(flag) => flag,
                Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
                Value::String(text) => matches!(
                    text.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "y" | "ok"
                ),
                _ => return Err(format!("cannot cast '{field}' to bool")),
            }),
            CastKind::Timestamp => match value {
                Value::String(text) => {
                    let trimmed = text.trim();
                    if trimmed.is_empty() {
                        return Err(format!("cannot cast '{field}' to timestamp"));
                    }
                    Value::String(trimmed.to_owned())
                }
                Value::Number(number) => Value::String(number.to_string()),
                _ => return Err(format!("cannot cast '{field}' to timestamp")),
            },
        };
        object.insert(field.clone(), casted);
    }
    Ok(record)
}

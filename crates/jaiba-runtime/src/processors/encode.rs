use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

pub struct Encode {
    format: Format,
}

enum Format {
    Json { pretty: bool },
    Yaml,
    Csv { headers: bool, delimiter: u8 },
    Xml { root: String, item: String },
}

#[derive(Deserialize)]
struct JsonConfig {
    #[serde(default)]
    pretty: bool,
}

#[derive(Deserialize)]
struct CsvConfig {
    #[serde(default = "default_true")]
    headers: bool,
    #[serde(default = "default_delimiter")]
    delimiter: char,
}

#[derive(Deserialize)]
struct XmlConfig {
    #[serde(default = "default_root")]
    root: String,
    #[serde(default = "default_item")]
    item: String,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> char {
    ','
}

fn default_root() -> String {
    "records".to_owned()
}

fn default_item() -> String {
    "record".to_owned()
}

impl Encode {
    pub fn json(value: &Value) -> Result<Self, FlowError> {
        let config: JsonConfig = parse_config(value)?;
        Ok(Self {
            format: Format::Json {
                pretty: config.pretty,
            },
        })
    }

    pub fn yaml(_: &Value) -> Result<Self, FlowError> {
        Ok(Self {
            format: Format::Yaml,
        })
    }

    pub fn csv(value: &Value) -> Result<Self, FlowError> {
        let config: CsvConfig = parse_config(value)?;
        if !config.delimiter.is_ascii() {
            return Err(FlowError::Configuration(
                "CSV delimiter must be one ASCII character".to_owned(),
            ));
        }
        Ok(Self {
            format: Format::Csv {
                headers: config.headers,
                delimiter: config.delimiter as u8,
            },
        })
    }

    pub fn xml(value: &Value) -> Result<Self, FlowError> {
        let config: XmlConfig = parse_config(value)?;
        validate_xml_name(&config.root)?;
        validate_xml_name(&config.item)?;
        Ok(Self {
            format: Format::Xml {
                root: config.root,
                item: config.item,
            },
        })
    }
}

fn parse_config<T>(value: &Value) -> Result<T, FlowError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value.clone())
        .map_err(|error| FlowError::Configuration(error.to_string()))
}

#[async_trait]
impl Processor for Encode {
    fn execution_mode(&self) -> crate::config::ExecutionMode {
        crate::config::ExecutionMode::Cpu
    }

    async fn execute(
        &self,
        mut packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let records = packet.records().map_err(|message| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        })?;
        let (media_type, bytes) = match &self.format {
            Format::Json { pretty } => {
                let bytes = if *pretty {
                    serde_json::to_vec_pretty(records)
                } else {
                    serde_json::to_vec(records)
                }
                .map_err(|error| processor_error(context, error))?;
                ("application/json", bytes)
            }
            Format::Yaml => (
                "application/yaml",
                serde_yaml::to_string(records)
                    .map_err(|error| processor_error(context, error))?
                    .into_bytes(),
            ),
            Format::Csv { headers, delimiter } => (
                "text/csv",
                encode_csv(records, *headers, *delimiter, context)?,
            ),
            Format::Xml { root, item } => {
                ("application/xml", encode_xml(records, root, item, context)?)
            }
        };

        packet.content = PacketContent::Encoded {
            media_type: media_type.to_owned(),
            bytes,
        };
        output.success(packet).await
    }
}

pub(crate) fn encode_csv(
    records: &[Value],
    headers: bool,
    delimiter: u8,
    context: &ProcessorContext,
) -> Result<Vec<u8>, FlowError> {
    let columns: BTreeSet<String> = records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|object| object.keys().cloned())
        .collect();
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .from_writer(Vec::new());

    if headers {
        writer
            .write_record(columns.iter())
            .map_err(|error| processor_error(context, error))?;
    }
    for record in records {
        let object = record.as_object().ok_or_else(|| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message: "encode_csv only accepts object records".to_owned(),
        })?;
        writer
            .write_record(columns.iter().map(|column| scalar_text(object.get(column))))
            .map_err(|error| processor_error(context, error))?;
    }
    writer
        .into_inner()
        .map_err(|error| processor_error(context, error))
}

fn scalar_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(value @ (Value::Bool(_) | Value::Number(_))) => value.to_string(),
        Some(value) => value.to_string(),
    }
}

fn encode_xml(
    records: &[Value],
    root: &str,
    item: &str,
    context: &ProcessorContext,
) -> Result<Vec<u8>, FlowError> {
    let mut output = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><{root}>");
    for record in records {
        let object = record.as_object().ok_or_else(|| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message: "encode_xml only accepts object records".to_owned(),
        })?;
        output.push_str(&format!("<{item}>"));
        for (name, value) in object {
            validate_xml_name(name)?;
            output.push_str(&format!(
                "<{name}>{}</{name}>",
                escape_xml(&scalar_text(Some(value)))
            ));
        }
        output.push_str(&format!("</{item}>"));
    }
    output.push_str(&format!("</{root}>"));
    Ok(output.into_bytes())
}

fn validate_xml_name(name: &str) -> Result<(), FlowError> {
    let valid = !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_-.".contains(character));
    if valid {
        Ok(())
    } else {
        Err(FlowError::Configuration(format!(
            "'{name}' is not a supported XML element name"
        )))
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn processor_error(context: &ProcessorContext, error: impl std::fmt::Display) -> FlowError {
    FlowError::Processor {
        processor_id: context.processor_id.clone(),
        message: error.to_string(),
    }
}

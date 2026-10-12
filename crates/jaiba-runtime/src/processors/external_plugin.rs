//! `external_plugin`: ejecuta un plugin de terceros como proceso aparte con el
//! protocolo JSON Lines v1 de `jaiba_plugin_sdk::isolated`.
//!
//! Los plugins solo se cargan desde `JAIBA_PLUGIN_DIR/<id>/plugin.yaml`: el flujo
//! nombra el plugin, nunca el comando, para que publicar un flujo no equivalga
//! a ejecutar programas arbitrarios en el servidor.

use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use jaiba_plugin_sdk::{
    PluginExecutionContext, PluginPacket,
    isolated::{
        MAX_FRAME_BYTES, PROTOCOL_VERSION, PluginRequest, PluginResponse, PluginResult,
        RequestOperation, validate_response,
    },
};
use serde::Deserialize;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

pub const PLUGIN_DIR_ENV: &str = "JAIBA_PLUGIN_DIR";
const MANIFEST_FILE: &str = "plugin.yaml";
const DEFAULT_MEDIA_TYPE: &str = "application/octet-stream";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalPluginConfig {
    plugin: String,
    #[serde(default = "empty_object")]
    config: Value,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn default_timeout_ms() -> u64 {
    30_000
}

/// `plugin.yaml` de un plugin instalado.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginManifest {
    pub id: String,
    pub version: String,
    #[serde(default)]
    pub description: String,
    /// Programa a ejecutar: un nombre del `PATH` (`python3`) o una ruta relativa
    /// a la carpeta del plugin (`./mi-plugin`).
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

struct PluginProcess {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

#[derive(Default)]
struct ProcessSlot {
    process: Option<PluginProcess>,
    /// Quedó en `true` si una petición se canceló a medias (timeout del motor):
    /// el proceso puede tener una respuesta pendiente y hay que reiniciarlo.
    in_flight: bool,
}

pub struct ExternalPlugin {
    manifest: PluginManifest,
    directory: PathBuf,
    configuration: Value,
    timeout: Duration,
    slot: Mutex<ProcessSlot>,
    next_request: AtomicU64,
}

impl ExternalPlugin {
    pub fn from_config(value: &Value) -> Result<Self, FlowError> {
        let plugin_dir = std::env::var_os(PLUGIN_DIR_ENV).map(PathBuf::from);
        Self::from_config_in(value, plugin_dir.as_deref())
    }

    fn from_config_in(value: &Value, plugin_dir: Option<&Path>) -> Result<Self, FlowError> {
        let config: ExternalPluginConfig = serde_json::from_value(value.clone())
            .map_err(|error| FlowError::Configuration(format!("external_plugin: {error}")))?;
        if !valid_plugin_id(&config.plugin) {
            return Err(FlowError::Configuration(format!(
                "external_plugin: id de plugin inválido '{}' (usa letras minúsculas, números, '-' o '_')",
                config.plugin
            )));
        }
        if config.timeout_ms == 0 {
            return Err(FlowError::Configuration(
                "external_plugin: timeout_ms debe ser mayor que 0".to_owned(),
            ));
        }
        let plugin_dir = plugin_dir.ok_or_else(|| {
            FlowError::Configuration(format!(
                "external_plugin: los plugins externos están desactivados; define {PLUGIN_DIR_ENV} con la carpeta de plugins instalados"
            ))
        })?;
        let directory = plugin_dir.join(&config.plugin);
        let manifest_path = directory.join(MANIFEST_FILE);
        let manifest_text = std::fs::read_to_string(&manifest_path).map_err(|error| {
            FlowError::Configuration(format!(
                "external_plugin: plugin '{}' no instalado ({}: {error})",
                config.plugin,
                manifest_path.display()
            ))
        })?;
        let manifest: PluginManifest = serde_yaml::from_str(&manifest_text).map_err(|error| {
            FlowError::Configuration(format!("{}: {error}", manifest_path.display()))
        })?;
        if manifest.id != config.plugin {
            return Err(FlowError::Configuration(format!(
                "{}: id '{}' no coincide con la carpeta '{}'",
                manifest_path.display(),
                manifest.id,
                config.plugin
            )));
        }
        Ok(Self {
            manifest,
            directory,
            configuration: config.config,
            timeout: Duration::from_millis(config.timeout_ms),
            slot: Mutex::new(ProcessSlot::default()),
            next_request: AtomicU64::new(1),
        })
    }

    fn request_id(&self) -> String {
        format!(
            "{}-{}",
            self.manifest.id,
            self.next_request.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn program(&self) -> PathBuf {
        let command = Path::new(&self.manifest.command);
        if command.components().count() > 1 {
            self.directory.join(command)
        } else {
            command.to_path_buf()
        }
    }

    async fn spawn(&self) -> Result<PluginProcess, String> {
        let program = self.program();
        let mut command = Command::new(&program);
        command
            .args(&self.manifest.args)
            .current_dir(&self.directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        let mut child = command.spawn().map_err(|error| {
            format!(
                "no se pudo arrancar el plugin '{}' ({}): {error}",
                self.manifest.id,
                program.display()
            )
        })?;
        if writable(&self.directory) {
            tracing::warn!(
                plugin = %self.manifest.id,
                directory = %self.directory.display(),
                "la carpeta del plugin es escribible por Jaiba; en producción móntala de solo lectura"
            );
        }
        let stdin = child.stdin.take().ok_or("el plugin no expuso stdin")?;
        let stdout = child.stdout.take().ok_or("el plugin no expuso stdout")?;
        let mut process = PluginProcess {
            _child: child,
            stdin,
            stdout: BufReader::new(stdout),
        };
        let describe = PluginRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: self.request_id(),
            operation: RequestOperation::Describe,
            configuration: Value::Null,
            packet: None,
            context: None,
        };
        let response = tokio::time::timeout(self.timeout, transact(&mut process, &describe))
            .await
            .map_err(|_| self.timeout_message("describe"))??;
        match response {
            PluginResponse {
                result: Some(PluginResult::Descriptor { descriptor }),
                ..
            } if descriptor.id == self.manifest.id => {
                tracing::info!(
                    plugin = %self.manifest.id,
                    version = %self.manifest.version,
                    description = %self.manifest.description,
                    "plugin externo arrancado"
                );
                Ok(process)
            }
            PluginResponse {
                result: Some(PluginResult::Descriptor { descriptor }),
                ..
            } => Err(format!(
                "el plugin respondió con id '{}' y se esperaba '{}'",
                descriptor.id, self.manifest.id
            )),
            PluginResponse {
                error: Some(error), ..
            } => Err(format!("describe falló: {}: {}", error.code, error.message)),
            _ => Err("describe debe responder un descriptor".to_owned()),
        }
    }

    fn timeout_message(&self, operation: &str) -> String {
        format!(
            "el plugin '{}' no respondió a {operation} en {} ms; se reiniciará",
            self.manifest.id,
            self.timeout.as_millis()
        )
    }

    async fn call(&self, request: &PluginRequest) -> Result<PluginResponse, String> {
        let mut slot = self.slot.lock().await;
        if slot.in_flight {
            slot.process = None;
            slot.in_flight = false;
        }
        if slot.process.is_none() {
            slot.process = Some(self.spawn().await?);
        }
        slot.in_flight = true;
        let process = slot.process.as_mut().expect("process spawned above");
        let outcome = match tokio::time::timeout(self.timeout, transact(process, request)).await {
            Ok(result) => result,
            Err(_) => Err(self.timeout_message("execute")),
        };
        slot.in_flight = false;
        if outcome.is_err() {
            slot.process = None;
        }
        outcome
    }
}

async fn transact(
    process: &mut PluginProcess,
    request: &PluginRequest,
) -> Result<PluginResponse, String> {
    let mut encoded = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    if encoded.len() > MAX_FRAME_BYTES {
        return Err(format!(
            "el paquete excede {MAX_FRAME_BYTES} bytes del protocolo de plugins"
        ));
    }
    encoded.push(b'\n');
    process
        .stdin
        .write_all(&encoded)
        .await
        .map_err(|error| format!("no se pudo escribir al plugin: {error}"))?;
    process
        .stdin
        .flush()
        .await
        .map_err(|error| format!("no se pudo escribir al plugin: {error}"))?;

    let mut line = Vec::new();
    (&mut process.stdout)
        .take(MAX_FRAME_BYTES as u64 + 1)
        .read_until(b'\n', &mut line)
        .await
        .map_err(|error| format!("no se pudo leer del plugin: {error}"))?;
    if line.last() != Some(&b'\n') {
        return Err(if line.len() > MAX_FRAME_BYTES {
            format!("la respuesta del plugin excede {MAX_FRAME_BYTES} bytes")
        } else {
            "el plugin terminó sin responder".to_owned()
        });
    }
    line.pop();
    let response: PluginResponse = serde_json::from_slice(&line)
        .map_err(|error| format!("el plugin respondió JSON inválido: {error}"))?;
    if response.protocol_version != PROTOCOL_VERSION {
        return Err(format!(
            "el plugin usa el protocolo {} y Jaiba el {PROTOCOL_VERSION}",
            response.protocol_version
        ));
    }
    validate_response(&response).map_err(|error| error.to_string())?;
    if response.request_id != request.request_id {
        return Err(format!(
            "el plugin respondió a '{}' y se esperaba '{}'",
            response.request_id, request.request_id
        ));
    }
    Ok(response)
}

fn writable(directory: &Path) -> bool {
    let probe = directory.join(format!(
        ".jaiba-write-probe-{}",
        uuid::Uuid::new_v4().simple()
    ));
    let created = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .is_ok();
    if created {
        let _ = std::fs::remove_file(&probe);
    }
    created
}

fn valid_plugin_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
}

fn to_plugin_packet(content: &PacketContent) -> PluginPacket {
    match content {
        PacketContent::Records(records) => PluginPacket::Records(records.clone()),
        PacketContent::Encoded { media_type, bytes } => PluginPacket::Bytes {
            content: bytes.clone(),
            media_type: Some(media_type.clone()),
        },
    }
}

fn from_plugin_packet(packet: PluginPacket) -> PacketContent {
    match packet {
        PluginPacket::Records(records) => PacketContent::Records(records),
        PluginPacket::Bytes {
            content,
            media_type,
        } => PacketContent::Encoded {
            media_type: media_type.unwrap_or_else(|| DEFAULT_MEDIA_TYPE.to_owned()),
            bytes: content,
        },
    }
}

#[async_trait]
impl Processor for ExternalPlugin {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        let failure = |message: String| FlowError::Processor {
            processor_id: context.processor_id.clone(),
            message,
        };
        let request = PluginRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: self.request_id(),
            operation: RequestOperation::Execute,
            configuration: self.configuration.clone(),
            packet: Some(to_plugin_packet(&packet.content)),
            context: Some(PluginExecutionContext {
                flow_id: context.flow_id.clone(),
                processor_id: context.processor_id.clone(),
                attributes: packet
                    .attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>(),
            }),
        };
        let response = self.call(&request).await.map_err(failure)?;
        let packets = match response {
            PluginResponse {
                result: Some(PluginResult::Packets { packets }),
                ..
            } => packets,
            PluginResponse {
                error: Some(error), ..
            } => return Err(failure(format!("{}: {}", error.code, error.message))),
            _ => return Err(failure("execute debe responder paquetes".to_owned())),
        };
        for result in packets {
            let mut emitted = DataPacket::empty();
            emitted.attributes = packet.attributes.clone();
            emitted
                .attributes
                .insert("plugin.id".to_owned(), self.manifest.id.clone());
            emitted
                .attributes
                .insert("plugin.version".to_owned(), self.manifest.version.clone());
            emitted.content = from_plugin_packet(result);
            output.success(emitted).await?;
        }
        Ok(())
    }
}

/// Rechaza destinos dentro de `JAIBA_PLUGIN_DIR` (exista el archivo o no) para
/// que un flujo publicado no pueda instalar ni modificar plugins.
pub(crate) fn reject_plugin_dir(path: &Path, processor: &str) -> Result<(), FlowError> {
    let inside = std::env::var_os(PLUGIN_DIR_ENV)
        .is_some_and(|plugin_dir| is_within(path, Path::new(&plugin_dir)));
    if inside {
        return Err(FlowError::Configuration(format!(
            "{processor}: no puede escribir '{}' dentro de {PLUGIN_DIR_ENV}",
            path.display()
        )));
    }
    Ok(())
}

fn is_within(path: &Path, directory: &Path) -> bool {
    let (Some(path), Some(directory)) = (resolve(path), resolve(directory)) else {
        return false;
    };
    path.starts_with(directory)
}

/// Ruta absoluta sin `.`/`..` y con los symlinks de la parte que ya existe
/// resueltos (la parte que no existe todavía se agrega tal cual).
fn resolve(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            other => normalized.push(other),
        }
    }
    let mut existing = normalized.as_path();
    let mut missing = Vec::new();
    loop {
        if let Ok(mut resolved) = existing.canonicalize() {
            resolved.extend(missing.iter().rev());
            return Some(resolved);
        }
        missing.push(existing.file_name()?.to_owned());
        existing = existing.parent()?;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write_plugin(root: &Path, id: &str, script: &str) {
        let directory = root.join(id);
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join(MANIFEST_FILE),
            format!("id: {id}\nversion: 0.1.0\ncommand: python3\nargs: [plugin.py]\n"),
        )
        .unwrap();
        fs::write(directory.join("plugin.py"), script).unwrap();
    }

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "jaiba-plugin-test-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn python_available() -> bool {
        std::process::Command::new("python3")
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    const UPPERCASE_PLUGIN: &str = r#"
import json, sys
for line in sys.stdin:
    request = json.loads(line)
    reply = {"protocol_version": 1, "request_id": request["request_id"], "result": None, "error": None}
    if request["operation"] == "describe":
        reply["result"] = {"kind": "descriptor", "descriptor": {"id": "upper", "version": "0.1.0",
            "display_name": "Upper", "category": "transform", "default_port": 0, "capabilities": []}}
    elif request["configuration"].get("fail"):
        reply["error"] = {"code": "bad_input", "message": "fallo pedido", "retryable": False}
    elif request["configuration"].get("hang"):
        continue
    else:
        records = request["packet"]["Records"]
        for record in records:
            record["name"] = record["name"].upper()
        reply["result"] = {"kind": "packets", "packets": [{"Records": records}]}
    print(json.dumps(reply), flush=True)
"#;

    fn plugin(root: &Path, config: Value) -> ExternalPlugin {
        ExternalPlugin::from_config_in(&config, Some(root)).unwrap()
    }

    async fn run(
        plugin: &ExternalPlugin,
        records: Vec<Value>,
    ) -> Result<Vec<DataPacket>, FlowError> {
        use crate::engine::{
            CircuitBreakers, ConnectionManager, FlowMetrics, MemoryLimiter, StateStore,
        };

        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let metrics = FlowMetrics::default();
        let memory = MemoryLimiter::from_budget(1 << 20, metrics.clone());
        let output = OutputSender::new(sender, "plugin", memory, metrics.clone());
        let state_path = std::env::temp_dir().join(format!(
            "jaiba-plugin-state-{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        let context = ProcessorContext {
            flow_id: "plugin-test".into(),
            processor_id: "plugin".into(),
            parameters: Default::default(),
            connections: ConnectionManager::default(),
            metrics,
            state: StateStore::load(&state_path).unwrap(),
            circuits: CircuitBreakers::new(crate::config::CircuitBreakerConfig {
                enabled: false,
                ..Default::default()
            })
            .unwrap(),
            domain_memory: None,
        };
        let result = plugin
            .execute(DataPacket::with_records(records), &context, &output)
            .await;
        let _ = fs::remove_file(&state_path);
        result?;
        drop(output);
        let mut packets = Vec::new();
        while let Some(emission) = receiver.recv().await {
            packets.push(emission.packet);
        }
        Ok(packets)
    }

    #[test]
    fn requires_plugin_dir_and_rejects_path_like_ids() {
        let disabled =
            ExternalPlugin::from_config_in(&serde_json::json!({"plugin": "upper"}), None)
                .err()
                .unwrap()
                .to_string();
        assert!(disabled.contains(PLUGIN_DIR_ENV), "{disabled}");

        let root = temp_root("ids");
        for id in ["../etc", "Upper", "a/b", ""] {
            let error =
                ExternalPlugin::from_config_in(&serde_json::json!({"plugin": id}), Some(&root))
                    .err()
                    .unwrap()
                    .to_string();
            assert!(error.contains("inválido"), "{id}: {error}");
        }
        let missing =
            ExternalPlugin::from_config_in(&serde_json::json!({"plugin": "nada"}), Some(&root))
                .err()
                .unwrap()
                .to_string();
        assert!(missing.contains("no instalado"), "{missing}");
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn runs_a_python_plugin_and_recovers_from_errors_and_hangs() {
        if !python_available() {
            eprintln!("skipping: python3 not available");
            return;
        }
        let root = temp_root("run");
        write_plugin(&root, "upper", UPPERCASE_PLUGIN);

        let ok = plugin(&root, serde_json::json!({"plugin": "upper"}));
        let packets = run(&ok, vec![serde_json::json!({"name": "ada"})])
            .await
            .unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].records().unwrap()[0]["name"], "ADA");
        assert_eq!(packets[0].attributes["plugin.id"], "upper");
        let again = run(&ok, vec![serde_json::json!({"name": "linus"})])
            .await
            .unwrap();
        assert_eq!(again[0].records().unwrap()[0]["name"], "LINUS");

        let failing = plugin(
            &root,
            serde_json::json!({"plugin": "upper", "config": {"fail": true}}),
        );
        let error = run(&failing, vec![]).await.unwrap_err().to_string();
        assert!(error.contains("bad_input: fallo pedido"), "{error}");

        let hanging = plugin(
            &root,
            serde_json::json!({"plugin": "upper", "config": {"hang": true}, "timeout_ms": 300}),
        );
        let error = run(&hanging, vec![]).await.unwrap_err().to_string();
        assert!(error.contains("no respondió"), "{error}");
        assert!(hanging.slot.lock().await.process.is_none());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn detects_paths_inside_the_plugin_dir() {
        let root = temp_root("within");
        let plugins = root.join("plugins");
        fs::create_dir_all(&plugins).unwrap();
        assert!(is_within(&plugins.join("x/plugin.yaml"), &plugins));
        assert!(is_within(&root.join("otro/../plugins/x.py"), &plugins));
        assert!(!is_within(&root.join("salida.csv"), &plugins));
        assert!(!is_within(&root.join("plugins-no/x"), &plugins));
        fs::remove_dir_all(root).unwrap();
    }
}

//! `external_plugin`: ejecuta un plugin de terceros como proceso aparte con el
//! protocolo JSON Lines v1 de `jaiba_plugin_sdk::isolated`.
//!
//! Los plugins solo se cargan desde `JAIBA_PLUGIN_DIR/<id>/plugin.yaml`: el flujo
//! nombra el plugin, nunca el comando, para que publicar un flujo no equivalga
//! a ejecutar programas arbitrarios en el servidor.
//!
//! El manifiesto elige cómo arrancarlo: `command` (proceso local) o `container`
//! (`docker run -i` / `podman run -i` sin red ni escritura por defecto).

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
pub const CONTAINER_RUNTIME_ENV: &str = "JAIBA_CONTAINER_RUNTIME";
const DEFAULT_CONTAINER_RUNTIME: &str = "docker";
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
    /// a la carpeta del plugin (`./mi-plugin`). Excluyente con `container`.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub container: Option<ContainerSpec>,
    /// Argumentos del programa o, en contenedor, del entrypoint de la imagen.
    #[serde(default)]
    pub args: Vec<String>,
}

/// Plugin empaquetado como imagen OCI.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContainerSpec {
    /// Imagen ya presente en el host: Jaiba nunca la descarga (`--pull=never`).
    pub image: String,
    #[serde(default)]
    pub network: ContainerNetwork,
    #[serde(default = "default_container_memory")]
    pub memory: String,
    #[serde(default = "default_container_cpus")]
    pub cpus: f64,
    #[serde(default = "default_container_pids")]
    pub pids: u32,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContainerNetwork {
    #[default]
    None,
    Bridge,
}

impl ContainerNetwork {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bridge => "bridge",
        }
    }
}

fn default_container_memory() -> String {
    "256m".to_owned()
}

fn default_container_cpus() -> f64 {
    1.0
}

fn default_container_pids() -> u32 {
    64
}

impl ContainerSpec {
    fn validate(&self) -> Result<(), String> {
        let image = &self.image;
        if image.is_empty()
            || image.len() > 255
            || image.starts_with('-')
            || image.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(format!("container.image inválida '{image}'"));
        }
        let digits = self
            .memory
            .strip_suffix(['b', 'k', 'm', 'g', 'B', 'K', 'M', 'G'])
            .unwrap_or(&self.memory);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!(
                "container.memory inválida '{}' (ejemplos: 256m, 1g)",
                self.memory
            ));
        }
        if !(self.cpus.is_finite() && self.cpus > 0.0 && self.cpus <= 256.0) {
            return Err(format!("container.cpus inválido {}", self.cpus));
        }
        if self.pids == 0 {
            return Err("container.pids debe ser mayor que 0".to_owned());
        }
        Ok(())
    }
}

/// Argumentos de `<runtime> run` (Docker y Podman aceptan los mismos).
fn container_run_args(
    spec: &ContainerSpec,
    plugin_id: &str,
    name: &str,
    args: &[String],
) -> Vec<String> {
    let mut run = vec![
        "run".to_owned(),
        "-i".to_owned(),
        "--rm".to_owned(),
        "--pull=never".to_owned(),
        format!("--name={name}"),
        format!("--label=jaiba.plugin={plugin_id}"),
        format!("--network={}", spec.network.as_str()),
        "--read-only".to_owned(),
        "--tmpfs=/tmp:rw,noexec,nosuid,size=64m".to_owned(),
        "--cap-drop=ALL".to_owned(),
        "--security-opt=no-new-privileges".to_owned(),
        format!("--memory={}", spec.memory),
        format!("--memory-swap={}", spec.memory),
        format!("--cpus={}", spec.cpus),
        format!("--pids-limit={}", spec.pids),
        spec.image.clone(),
    ];
    run.extend(args.iter().cloned());
    run
}

enum Launch {
    Process(PathBuf),
    Container {
        runtime: String,
        spec: ContainerSpec,
    },
}

struct PluginProcess {
    _child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    _container: Option<ContainerGuard>,
}

/// Matar el cliente `docker run` no detiene un contenedor colgado: al soltar el
/// proceso se fuerza `rm -f` del contenedor.
struct ContainerGuard {
    runtime: String,
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let mut command = std::process::Command::new(&self.runtime);
        command
            .args(["rm", "-f", &self.name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        match command.spawn() {
            Ok(mut child) => {
                std::thread::spawn(move || child.wait());
            }
            Err(error) => tracing::warn!(
                container = %self.name,
                %error,
                "no se pudo eliminar el contenedor del plugin"
            ),
        }
    }
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
    launch: Launch,
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
        let invalid = |message: String| {
            FlowError::Configuration(format!("{}: {message}", manifest_path.display()))
        };
        let mut manifest: PluginManifest =
            serde_yaml::from_str(&manifest_text).map_err(|error| invalid(error.to_string()))?;
        if manifest.id != config.plugin {
            return Err(invalid(format!(
                "id '{}' no coincide con la carpeta '{}'",
                manifest.id, config.plugin
            )));
        }
        let launch = match (manifest.command.as_deref(), manifest.container.take()) {
            (Some(command), None) => Launch::Process(program_path(&directory, command)),
            (None, Some(spec)) => {
                spec.validate().map_err(invalid)?;
                Launch::Container {
                    runtime: std::env::var(CONTAINER_RUNTIME_ENV)
                        .ok()
                        .filter(|runtime| !runtime.trim().is_empty())
                        .unwrap_or_else(|| DEFAULT_CONTAINER_RUNTIME.to_owned()),
                    spec,
                }
            }
            _ => {
                return Err(invalid(
                    "define exactamente uno de `command` o `container`".to_owned(),
                ));
            }
        };
        Ok(Self {
            manifest,
            launch,
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

    async fn spawn(&self) -> Result<PluginProcess, String> {
        let (program, args, container) = match &self.launch {
            Launch::Process(program) => (program.clone(), self.manifest.args.clone(), None),
            Launch::Container { runtime, spec } => {
                let name = format!(
                    "jaiba-plugin-{}-{}",
                    self.manifest.id,
                    &uuid::Uuid::new_v4().simple().to_string()[..12]
                );
                let args = container_run_args(spec, &self.manifest.id, &name, &self.manifest.args);
                (PathBuf::from(runtime), args, Some((runtime, spec, name)))
            }
        };
        let mut command = Command::new(&program);
        command
            .args(&args)
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
        let guard = container.as_ref().map(|(runtime, _, name)| ContainerGuard {
            runtime: (*runtime).clone(),
            name: name.clone(),
        });
        if container.is_none() && writable(&self.directory) {
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
            _container: guard,
        };
        self.handshake(&mut process).await.map_err(|error| {
            match &container {
                Some((runtime, spec, _)) => format!(
                    "{error} (contenedor '{}': revisa que exista localmente con `{runtime} image inspect {}` y la salida de error de {runtime} en el log)",
                    spec.image, spec.image
                ),
                None => error,
            }
        })?;
        tracing::info!(
            plugin = %self.manifest.id,
            version = %self.manifest.version,
            description = %self.manifest.description,
            container = container.as_ref().map(|(_, spec, _)| spec.image.as_str()),
            "plugin externo arrancado"
        );
        Ok(process)
    }

    async fn handshake(&self, process: &mut PluginProcess) -> Result<(), String> {
        let describe = PluginRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: self.request_id(),
            operation: RequestOperation::Describe,
            configuration: Value::Null,
            packet: None,
            context: None,
        };
        let response = tokio::time::timeout(self.timeout, transact(process, &describe))
            .await
            .map_err(|_| self.timeout_message("describe"))??;
        match response {
            PluginResponse {
                result: Some(PluginResult::Descriptor { descriptor }),
                ..
            } if descriptor.id == self.manifest.id => Ok(()),
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

fn program_path(directory: &Path, command: &str) -> PathBuf {
    let command = Path::new(command);
    if command.components().count() > 1 {
        directory.join(command)
    } else {
        command.to_path_buf()
    }
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

    fn write_manifest(root: &Path, id: &str, manifest: &str) {
        let directory = root.join(id);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join(MANIFEST_FILE), manifest).unwrap();
    }

    fn manifest_error(root: &Path, id: &str, manifest: &str) -> String {
        write_manifest(root, id, manifest);
        ExternalPlugin::from_config_in(&serde_json::json!({"plugin": id}), Some(root))
            .err()
            .unwrap()
            .to_string()
    }

    #[test]
    fn validates_container_manifests_and_builds_locked_down_run_args() {
        let root = temp_root("container");
        let cases = [
            (
                "id: x\nversion: '1'\ncommand: python3\ncontainer: {image: img}\n",
                "exactamente uno",
            ),
            ("id: x\nversion: '1'\n", "exactamente uno"),
            (
                "id: x\nversion: '1'\ncontainer: {image: '--privileged'}\n",
                "image inválida",
            ),
            (
                "id: x\nversion: '1'\ncontainer: {image: img, memory: mucha}\n",
                "memory inválida",
            ),
            (
                "id: x\nversion: '1'\ncontainer: {image: img, cpus: 0}\n",
                "cpus inválido",
            ),
            (
                "id: x\nversion: '1'\ncontainer: {image: img, network: host}\n",
                "unknown variant",
            ),
        ];
        for (manifest, expected) in cases {
            let error = manifest_error(&root, "x", manifest);
            assert!(error.contains(expected), "{manifest}: {error}");
        }

        write_manifest(
            &root,
            "x",
            "id: x\nversion: '1'\ncontainer: {image: 'ghcr.io/a/b:1', memory: 1g}\nargs: [--modo, rapido]\n",
        );
        let plugin = plugin(&root, serde_json::json!({"plugin": "x"}));
        let Launch::Container { spec, .. } = &plugin.launch else {
            panic!("se esperaba un plugin en contenedor");
        };
        let args = container_run_args(spec, "x", "jaiba-plugin-x-1", &plugin.manifest.args);
        for flag in [
            "-i",
            "--rm",
            "--pull=never",
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--memory=1g",
            "--memory-swap=1g",
            "--cpus=1",
            "--pids-limit=64",
            "--label=jaiba.plugin=x",
        ] {
            assert!(args.iter().any(|arg| arg == flag), "falta {flag}: {args:?}");
        }
        assert_eq!(
            &args[args.len() - 3..],
            ["ghcr.io/a/b:1", "--modo", "rapido"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn container_plugins_run_through_the_runtime_and_are_removed_on_hang() {
        use std::os::unix::fs::PermissionsExt;

        if !python_available() {
            eprintln!("skipping: python3 not available");
            return;
        }
        let root = temp_root("fake-runtime");
        write_plugin(&root, "upper", UPPERCASE_PLUGIN);
        write_manifest(
            &root,
            "upper",
            "id: upper\nversion: 0.1.0\ncontainer: {image: jaiba-upper:0.1.0}\n",
        );
        let removed = root.join("removed.log");
        let runtime = root.join("fake-runtime");
        fs::write(
            &runtime,
            format!(
                "#!/bin/sh\nif [ \"$1\" = run ]; then exec python3 plugin.py; fi\necho \"$@\" >> '{}'\n",
                removed.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&runtime, fs::Permissions::from_mode(0o755)).unwrap();
        let with_runtime = |config: Value| {
            let mut plugin = plugin(&root, config);
            let Launch::Container { runtime: slot, .. } = &mut plugin.launch else {
                panic!("se esperaba un plugin en contenedor");
            };
            *slot = runtime.display().to_string();
            plugin
        };

        let ok = with_runtime(serde_json::json!({"plugin": "upper"}));
        let packets = run(&ok, vec![serde_json::json!({"name": "ada"})])
            .await
            .unwrap();
        assert_eq!(packets[0].records().unwrap()[0]["name"], "ADA");

        let hanging = with_runtime(
            serde_json::json!({"plugin": "upper", "config": {"hang": true}, "timeout_ms": 300}),
        );
        let error = run(&hanging, vec![]).await.unwrap_err().to_string();
        assert!(error.contains("no respondió"), "{error}");
        let mut log = String::new();
        for _ in 0..50 {
            log = fs::read_to_string(&removed).unwrap_or_default();
            if !log.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(log.starts_with("rm -f jaiba-plugin-upper-"), "{log:?}");
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

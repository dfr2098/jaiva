//! Gestión del motor Jaiba como proceso hijo (sidecar) del shell desktop.
//!
//! Modo **local**: spawnea `jaiba serve <flow.yaml>` y apunta la UI a loopback.
//! Modo **remoto**: no spawnea; la UI usa `JAIBA_API_BASE` / override.

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use tauri::{AppHandle, Manager, State};

const DEFAULT_API_BASE: &str = "http://127.0.0.1:9090";
const MODE_FILE: &str = "engine-mode";
const HEALTH_WAIT: Duration = Duration::from_secs(45);
const HEALTH_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EngineMode {
    Local,
    Remote,
}

impl EngineMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "remote" => Some(Self::Remote),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EngineStatus {
    pub mode: EngineMode,
    pub running: bool,
    pub pid: Option<u32>,
    pub api_base: String,
    pub binary: Option<String>,
    pub flow: Option<String>,
    pub last_error: Option<String>,
}

pub struct EngineManager {
    inner: Mutex<EngineInner>,
}

struct EngineInner {
    mode: EngineMode,
    child: Option<Child>,
    api_base: String,
    binary: Option<PathBuf>,
    flow: Option<PathBuf>,
    last_error: Option<String>,
}

impl EngineManager {
    pub fn new(app: &AppHandle) -> Self {
        let api_base = resolve_api_base();
        let mode = load_mode(app).unwrap_or(EngineMode::Remote);
        Self {
            inner: Mutex::new(EngineInner {
                mode,
                child: None,
                api_base,
                binary: None,
                flow: None,
                last_error: None,
            }),
        }
    }

    pub fn status(&self) -> EngineStatus {
        let mut guard = self.inner.lock().expect("engine mutex");
        reap_if_exited(&mut guard);
        let mut status = status_from(&guard);
        if !status.running && status.mode == EngineMode::Local {
            if let Ok(addr) = socket_from_base(&status.api_base) {
                if engine_healthy(addr) {
                    status.running = true;
                }
            }
        }
        status
    }

    pub fn api_base(&self) -> String {
        self.inner.lock().expect("engine mutex").api_base.clone()
    }

    pub fn set_mode(&self, app: &AppHandle, mode: EngineMode) -> Result<EngineStatus, String> {
        save_mode(app, mode)?;
        if mode == EngineMode::Remote {
            let mut guard = self.inner.lock().expect("engine mutex");
            stop_child(&mut guard);
            guard.mode = EngineMode::Remote;
            guard.api_base = resolve_api_base();
            guard.last_error = None;
            return Ok(status_from(&guard));
        }
        {
            let mut guard = self.inner.lock().expect("engine mutex");
            guard.mode = EngineMode::Local;
            guard.api_base = DEFAULT_API_BASE.to_owned();
            guard.last_error = None;
        }
        self.start_local(app)
    }

    pub fn start_local(&self, app: &AppHandle) -> Result<EngineStatus, String> {
        let mut guard = self.inner.lock().expect("engine mutex");
        reap_if_exited(&mut guard);
        if guard.child.is_some() {
            return Ok(status_from(&guard));
        }

        let binary = resolve_jaiba_binary(app)?;
        let flow = resolve_flow_path(app)?;
        let api_base = DEFAULT_API_BASE.to_owned();
        let listen = socket_from_base(&api_base)?;

        if engine_healthy(listen) {
            // Ya hay un motor en ese puerto: reutilízalo sin segundo proceso.
            guard.mode = EngineMode::Local;
            guard.api_base = api_base;
            guard.binary = Some(binary);
            guard.flow = Some(flow);
            guard.last_error = None;
            let _ = save_mode(app, EngineMode::Local);
            return Ok(status_from(&guard));
        }

        let data_dir = app
            .path()
            .app_data_dir()
            .map_err(|error| format!("no se pudo resolver app_data_dir: {error}"))?;
        fs::create_dir_all(&data_dir)
            .map_err(|error| format!("no se pudo crear data dir: {error}"))?;

        let mut command = Command::new(&binary);
        command
            .arg("serve")
            .arg(&flow)
            .current_dir(&data_dir)
            .env("JAIBA_ADMIN_AUTH", "none")
            .env("JAIBA_SERVER_ADDR", listen.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = command.spawn().map_err(|error| {
            format!(
                "no se pudo arrancar sidecar '{}' serve '{}': {error}",
                binary.display(),
                flow.display()
            )
        })?;

        pipe_child_logs(&mut child);

        match wait_for_engine(listen, HEALTH_WAIT) {
            Ok(()) => {
                guard.mode = EngineMode::Local;
                guard.child = Some(child);
                guard.api_base = api_base;
                guard.binary = Some(binary);
                guard.flow = Some(flow);
                guard.last_error = None;
                let _ = save_mode(app, EngineMode::Local);
                Ok(status_from(&guard))
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                guard.last_error = Some(error.clone());
                Err(error)
            }
        }
    }

    pub fn stop_local(&self) -> Result<EngineStatus, String> {
        let mut guard = self.inner.lock().expect("engine mutex");
        stop_child(&mut guard);
        guard.last_error = None;
        Ok(status_from(&guard))
    }

    pub fn shutdown(&self) {
        let mut guard = self.inner.lock().expect("engine mutex");
        stop_child(&mut guard);
    }
}

fn status_from(guard: &EngineInner) -> EngineStatus {
    EngineStatus {
        mode: guard.mode,
        running: guard.child.is_some(),
        pid: guard.child.as_ref().map(|child| child.id()),
        api_base: guard.api_base.clone(),
        binary: guard.binary.as_ref().map(|p| p.display().to_string()),
        flow: guard.flow.as_ref().map(|p| p.display().to_string()),
        last_error: guard.last_error.clone(),
    }
}

fn resolve_api_base() -> String {
    std::env::var("JAIBA_API_BASE")
        .or_else(|_| std::env::var("JAIVA_API_BASE"))
        .unwrap_or_else(|_| DEFAULT_API_BASE.to_owned())
}

fn mode_path(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|error| format!("no se pudo resolver app_config_dir: {error}"))?;
    fs::create_dir_all(&dir).map_err(|error| format!("no se pudo crear config dir: {error}"))?;
    Ok(dir.join(MODE_FILE))
}

fn load_mode(app: &AppHandle) -> Option<EngineMode> {
    if let Ok(value) = std::env::var("JAIBA_ENGINE_MODE") {
        if let Some(mode) = EngineMode::parse(&value) {
            return Some(mode);
        }
    }
    let path = mode_path(app).ok()?;
    let raw = fs::read_to_string(path).ok()?;
    EngineMode::parse(&raw)
}

fn save_mode(app: &AppHandle, mode: EngineMode) -> Result<(), String> {
    let path = mode_path(app)?;
    fs::write(&path, mode.as_str())
        .map_err(|error| format!("no se pudo guardar modo en {}: {error}", path.display()))
}

fn resolve_jaiba_binary(app: &AppHandle) -> Result<PathBuf, String> {
    if let Ok(explicit) = std::env::var("JAIBA_BIN") {
        let path = PathBuf::from(explicit);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "JAIBA_BIN apunta a un binario inexistente: {}",
            path.display()
        ));
    }

    let triple = std::env::var("JAIBA_TARGET_TRIPLE")
        .ok()
        .or_else(|| option_env!("TARGET").map(str::to_owned))
        .unwrap_or_default();

    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("binaries").join("jaiba"));
        if !triple.is_empty() {
            candidates.push(resource_dir.join("binaries").join(format!("jaiba-{triple}")));
        }
        candidates.push(resource_dir.join("jaiba"));
    }

    if let Ok(exe_dir) = app.path().executable_dir() {
        candidates.push(exe_dir.join("jaiba"));
        if !triple.is_empty() {
            candidates.push(exe_dir.join(format!("jaiba-{triple}")));
        }
        candidates.push(exe_dir.join("../binaries/jaiba"));
        if !triple.is_empty() {
            candidates.push(exe_dir.join(format!("../binaries/jaiba-{triple}")));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        for rel in [
            "binaries/jaiba",
            "target/debug/jaiba",
            "target/release/jaiba",
            "../../target/debug/jaiba",
            "../../target/release/jaiba",
            "../../../target/debug/jaiba",
            "../../../target/release/jaiba",
        ] {
            let path = cwd.join(rel);
            candidates.push(path);
            if !triple.is_empty() && rel.starts_with("binaries/") {
                candidates.push(cwd.join(format!("binaries/jaiba-{triple}")));
            }
        }
    }

    if let Some(path) = candidates.into_iter().find(|path| path.is_file()) {
        return Ok(path.canonicalize().unwrap_or(path));
    }

    if let Some(path) = which("jaiba") {
        return Ok(path);
    }

    Err(
        "no se encontró el binario `jaiba`. Define JAIBA_BIN, ejecuta \
         scripts/prepare-desktop-sidecar.sh o instálalo en PATH."
            .to_owned(),
    )
}

fn resolve_flow_path(app: &AppHandle) -> Result<PathBuf, String> {
    if let Ok(explicit) = std::env::var("JAIBA_FLOW") {
        let path = PathBuf::from(explicit);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "JAIBA_FLOW apunta a un YAML inexistente: {}",
            path.display()
        ));
    }

    let mut candidates = Vec::new();
    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("resources/desktop-local-flow.yaml"));
        candidates.push(resource_dir.join("desktop-local-flow.yaml"));
    }
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("resources/desktop-local-flow.yaml"));
        candidates.push(cwd.join("src-tauri/resources/desktop-local-flow.yaml"));
    }

    if let Some(path) = candidates.into_iter().find(|path| path.is_file()) {
        return Ok(path);
    }

    let data_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("no se pudo resolver app_data_dir: {error}"))?;
    fs::create_dir_all(&data_dir).map_err(|error| error.to_string())?;
    let path = data_dir.join("desktop-local-flow.yaml");
    if !path.is_file() {
        fs::write(&path, include_str!("../resources/desktop-local-flow.yaml"))
            .map_err(|error| format!("no se pudo escribir flow local: {error}"))?;
    }
    Ok(path)
}

fn socket_from_base(api_base: &str) -> Result<SocketAddr, String> {
    let url = api_base.trim().trim_end_matches('/');
    let without_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let host_port = without_scheme.split('/').next().unwrap_or(without_scheme);
    host_port
        .to_socket_addrs()
        .map_err(|error| format!("API base inválida '{api_base}': {error}"))?
        .next()
        .ok_or_else(|| format!("API base sin dirección resoluble: {api_base}"))
}

fn engine_healthy(addr: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(250)) else {
        return false;
    };
    let timeout = Some(Duration::from_millis(500));
    if stream.set_read_timeout(timeout).is_err() || stream.set_write_timeout(timeout).is_err() {
        return false;
    }
    let request = format!(
        "GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok()
        && (response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200"))
        && response.contains("\"service\":\"jaiva\"")
}

fn wait_for_engine(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let started = Instant::now();
    while started.elapsed() < timeout {
        if engine_healthy(addr) {
            return Ok(());
        }
        thread::sleep(HEALTH_POLL);
    }
    Err(format!(
        "el sidecar no abrió {addr} en {}s",
        timeout.as_secs()
    ))
}

fn stop_child(guard: &mut EngineInner) {
    if let Some(mut child) = guard.child.take() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn reap_if_exited(guard: &mut EngineInner) {
    let exited = guard
        .child
        .as_mut()
        .and_then(|child| child.try_wait().ok().flatten());
    if let Some(status) = exited {
        guard.child = None;
        if !status.success() {
            guard.last_error = Some(format!("el sidecar terminó con estado {status}"));
        }
    }
}

fn pipe_child_logs(child: &mut Child) {
    if let Some(stdout) = child.stdout.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines().flatten() {
                eprintln!("[jaiba-sidecar] {line}");
            }
        });
    }
    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().flatten() {
                eprintln!("[jaiba-sidecar] {line}");
            }
        });
    }
}

fn which(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Comandos Tauri expuestos a la UI.
pub mod commands {
    use super::*;

    #[tauri::command]
    pub fn api_base(engine: State<'_, EngineManager>) -> String {
        engine.api_base()
    }

    #[tauri::command]
    pub fn engine_status(engine: State<'_, EngineManager>) -> EngineStatus {
        engine.status()
    }

    #[tauri::command]
    pub fn set_engine_mode(
        app: AppHandle,
        engine: State<'_, EngineManager>,
        mode: String,
    ) -> Result<EngineStatus, String> {
        let parsed = EngineMode::parse(&mode)
            .ok_or_else(|| format!("modo inválido '{mode}' (usa local|remote)"))?;
        engine.set_mode(&app, parsed)
    }

    #[tauri::command]
    pub fn start_local_engine(
        app: AppHandle,
        engine: State<'_, EngineManager>,
    ) -> Result<EngineStatus, String> {
        engine.set_mode(&app, EngineMode::Local)
    }

    #[tauri::command]
    pub fn stop_local_engine(engine: State<'_, EngineManager>) -> Result<EngineStatus, String> {
        engine.stop_local()
    }
}

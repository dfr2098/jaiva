//! Catálogo comunitario de plugins externos (`jaiba plugins …`).
//!
//! El catálogo es un JSON (`catalog_version: 1`) con plugins y sus versiones.
//! Instalar descarga cada archivo y verifica su `sha256`, o hace `pull` de una
//! imagen fijada por digest, y escribe `JAIBA_PLUGIN_DIR/<id>/plugin.yaml` a
//! partir de la entrada del catálogo. `jaiba serve` no usa este módulo:
//! instalar es una acción del operador.

use std::{
    collections::HashSet,
    fmt, fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use jaiba_plugin_sdk::isolated::PROTOCOL_VERSION;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

pub use crate::processors::external_plugin::{PLUGIN_DIR_ENV, container_runtime};
use crate::{
    error::FlowError,
    processors::external_plugin::{ContainerSpec, MANIFEST_FILE, PluginManifest, valid_plugin_id},
};

pub const CATALOG_ENV: &str = "JAIBA_PLUGIN_CATALOG";
pub const DEFAULT_CATALOG: &str =
    "https://raw.githubusercontent.com/dfr2098/jaiva/main/catalog/plugins.json";
pub const CATALOG_FORMAT: u32 = 1;
pub const INSTALL_RECORD: &str = ".jaiba-install.json";
const MAX_CATALOG_BYTES: u64 = 1 << 20;
const MAX_FILE_BYTES: u64 = 64 << 20;

fn invalid(message: impl Into<String>) -> FlowError {
    FlowError::Configuration(message.into())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub catalog_version: u32,
    pub plugins: Vec<CatalogPlugin>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogPlugin {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub license: Option<String>,
    #[serde(default)]
    pub homepage: Option<String>,
    pub versions: Vec<CatalogVersion>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogVersion {
    /// `MAYOR.MENOR.PARCHE`; sin `@versión`, `install` toma la mayor.
    pub version: String,
    pub protocol_version: u16,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub files: Vec<CatalogFile>,
    #[serde(default)]
    pub container: Option<ContainerSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogFile {
    /// Ruta dentro de la carpeta del plugin.
    pub path: String,
    /// URL `https://…` o relativa a la ubicación del catálogo.
    pub url: String,
    pub sha256: String,
    #[serde(default)]
    pub executable: bool,
}

impl Catalog {
    pub fn parse(bytes: &[u8]) -> Result<Self, FlowError> {
        let catalog: Catalog = serde_json::from_slice(bytes)
            .map_err(|error| invalid(format!("catálogo inválido: {error}")))?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn validate(&self) -> Result<(), FlowError> {
        if self.catalog_version != CATALOG_FORMAT {
            return Err(invalid(format!(
                "el catálogo usa el formato {} y esta versión de Jaiba entiende el {CATALOG_FORMAT}; actualiza Jaiba",
                self.catalog_version
            )));
        }
        let mut ids = HashSet::new();
        for plugin in &self.plugins {
            let at =
                |message: String| invalid(format!("catálogo, plugin '{}': {message}", plugin.id));
            if !valid_plugin_id(&plugin.id) {
                return Err(at(
                    "id inválido (usa letras minúsculas, números, '-' o '_')".to_owned(),
                ));
            }
            if !ids.insert(plugin.id.as_str()) {
                return Err(at("id duplicado".to_owned()));
            }
            if plugin.versions.is_empty() {
                return Err(at("no tiene versiones".to_owned()));
            }
            let mut versions = HashSet::new();
            for version in &plugin.versions {
                if !versions.insert(version.version.as_str()) {
                    return Err(at(format!("versión {} duplicada", version.version)));
                }
                version
                    .validate(plugin)
                    .map_err(|message| at(format!("{}: {message}", version.version)))?;
            }
        }
        Ok(())
    }

    pub fn plugin(&self, id: &str) -> Option<&CatalogPlugin> {
        self.plugins.iter().find(|plugin| plugin.id == id)
    }

    /// Plugins cuyo id o descripción contienen `query` (sin distinguir mayúsculas).
    pub fn search(&self, query: &str) -> Vec<&CatalogPlugin> {
        let query = query.to_lowercase();
        let mut found: Vec<_> = self
            .plugins
            .iter()
            .filter(|plugin| {
                plugin.id.contains(&query) || plugin.description.to_lowercase().contains(&query)
            })
            .collect();
        found.sort_by(|left, right| left.id.cmp(&right.id));
        found
    }
}

impl CatalogPlugin {
    pub fn latest(&self) -> &CatalogVersion {
        self.versions
            .iter()
            .max_by_key(|version| parse_version(&version.version))
            .expect("validate() exige al menos una versión")
    }

    pub fn version(&self, version: &str) -> Option<&CatalogVersion> {
        self.versions
            .iter()
            .find(|candidate| candidate.version == version)
    }
}

impl CatalogVersion {
    pub fn kind(&self) -> &'static str {
        if self.container.is_some() {
            "contenedor"
        } else {
            "proceso"
        }
    }

    fn manifest(&self, plugin: &CatalogPlugin) -> PluginManifest {
        PluginManifest {
            id: plugin.id.clone(),
            version: self.version.clone(),
            description: plugin.description.clone(),
            command: self.command.clone(),
            container: self.container.clone(),
            args: self.args.clone(),
        }
    }

    fn validate(&self, plugin: &CatalogPlugin) -> Result<(), String> {
        if parse_version(&self.version).is_none() {
            return Err("la versión debe ser MAYOR.MENOR.PARCHE (p. ej. 0.1.0)".to_owned());
        }
        self.manifest(plugin).validate()?;
        match &self.container {
            Some(spec) if !spec.pinned_by_digest() => {
                return Err(format!(
                    "la imagen '{}' debe fijarse por digest (…@sha256:<64 hex>), no por tag",
                    spec.image
                ));
            }
            Some(_) if !self.files.is_empty() => {
                return Err("un plugin en contenedor no lleva `files`".to_owned());
            }
            None if self.files.is_empty() => {
                return Err("un plugin de proceso necesita `files`".to_owned());
            }
            _ => {}
        }
        let mut paths = HashSet::new();
        for file in &self.files {
            let path = safe_relative_path(&file.path)
                .ok_or_else(|| format!("ruta de archivo inválida '{}'", file.path))?;
            if !paths.insert(path) {
                return Err(format!("archivo '{}' repetido", file.path));
            }
            if !is_sha256_hex(&file.sha256) {
                return Err(format!(
                    "sha256 de '{}' debe tener 64 caracteres hex en minúsculas",
                    file.path
                ));
            }
            if file.url.trim().is_empty() {
                return Err(format!("'{}' no tiene url", file.path));
            }
        }
        Ok(())
    }
}

fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|part| {
        (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| part.parse::<u64>().ok())
            .flatten()
    });
    let parsed = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(parsed)
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Ruta relativa sin `..`, `.`, raíz, prefijos de Windows ni nombres ocultos, y
/// distinta de `plugin.yaml` (ese lo escribe Jaiba).
fn safe_relative_path(path: &str) -> Option<PathBuf> {
    if path.is_empty() || path.contains('\\') {
        return None;
    }
    let mut normalized = PathBuf::new();
    for component in Path::new(path).components() {
        let Component::Normal(part) = component else {
            return None;
        };
        let part = part.to_str()?;
        if part.starts_with('.') || part.contains(':') {
            return None;
        }
        normalized.push(part);
    }
    (!normalized.as_os_str().is_empty() && normalized != Path::new(MANIFEST_FILE))
        .then_some(normalized)
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// De dónde se lee el catálogo y, con URLs relativas, sus archivos.
#[derive(Debug, Clone)]
pub enum CatalogSource {
    Url(Url),
    File(PathBuf),
}

impl CatalogSource {
    pub fn parse(text: &str) -> Result<Self, FlowError> {
        if text.starts_with("https://") || text.starts_with("http://") {
            let url = Url::parse(text).map_err(|error| invalid(format!("{text}: {error}")))?;
            check_url(&url)?;
            Ok(Self::Url(url))
        } else {
            Ok(Self::File(PathBuf::from(text)))
        }
    }

    /// `JAIBA_PLUGIN_CATALOG` o el catálogo comunitario por defecto.
    pub fn from_env() -> Result<Self, FlowError> {
        match std::env::var(CATALOG_ENV) {
            Ok(value) if !value.trim().is_empty() => Self::parse(value.trim()),
            _ => Self::parse(DEFAULT_CATALOG),
        }
    }

    fn join(&self, reference: &str) -> Result<Self, FlowError> {
        if reference.starts_with("https://") || reference.starts_with("http://") {
            return Self::parse(reference);
        }
        match self {
            Self::Url(base) => {
                let url = base
                    .join(reference)
                    .map_err(|error| invalid(format!("{reference}: {error}")))?;
                check_url(&url)?;
                Ok(Self::Url(url))
            }
            Self::File(path) => Ok(Self::File(
                path.parent().unwrap_or(Path::new(".")).join(reference),
            )),
        }
    }

    async fn fetch(&self, client: &reqwest::Client, limit: u64) -> Result<Vec<u8>, FlowError> {
        let too_large = || invalid(format!("{self} excede el límite de {limit} bytes"));
        match self {
            Self::File(path) => {
                let read = |error: std::io::Error| invalid(format!("{}: {error}", path.display()));
                if fs::metadata(path).map_err(read)?.len() > limit {
                    return Err(too_large());
                }
                fs::read(path).map_err(read)
            }
            Self::Url(url) => {
                let failed =
                    |error: reqwest::Error| invalid(format!("no se pudo descargar {url}: {error}"));
                let mut response = client.get(url.clone()).send().await.map_err(failed)?;
                if !response.status().is_success() {
                    return Err(invalid(format!(
                        "no se pudo descargar {url}: HTTP {}",
                        response.status()
                    )));
                }
                if response
                    .content_length()
                    .is_some_and(|length| length > limit)
                {
                    return Err(too_large());
                }
                let mut body = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(failed)? {
                    body.extend_from_slice(&chunk);
                    if body.len() as u64 > limit {
                        return Err(too_large());
                    }
                }
                Ok(body)
            }
        }
    }
}

impl fmt::Display for CatalogSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url(url) => write!(formatter, "{url}"),
            Self::File(path) => write!(formatter, "{}", path.display()),
        }
    }
}

fn allowed_url(url: &Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain(domain)) => domain == "localhost",
            None => false,
        },
        _ => false,
    }
}

fn check_url(url: &Url) -> Result<(), FlowError> {
    if allowed_url(url) {
        Ok(())
    } else {
        Err(invalid(format!(
            "{url}: solo se permiten descargas https (http solo hacia localhost)"
        )))
    }
}

fn http_client() -> Result<reqwest::Client, FlowError> {
    let redirects = reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 5 {
            attempt.error("demasiadas redirecciones")
        } else if allowed_url(attempt.url()) {
            attempt.follow()
        } else {
            attempt.error("redirección a una URL que no es https")
        }
    });
    reqwest::Client::builder()
        .user_agent(concat!("jaiba/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(300))
        .redirect(redirects)
        .build()
        .map_err(|error| invalid(format!("cliente HTTP: {error}")))
}

pub struct InstallOptions {
    /// Reemplaza un plugin ya instalado.
    pub force: bool,
    /// Descarga la imagen de los plugins en contenedor (`<runtime> pull`).
    pub pull: bool,
    pub container_runtime: String,
}

#[derive(Debug)]
pub struct InstallOutcome {
    pub id: String,
    pub version: String,
    pub kind: &'static str,
    pub directory: PathBuf,
    /// Versión que había antes (con `--force`).
    pub replaced: Option<String>,
    pub image: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct InstallRecord {
    catalog: String,
    version: String,
    installed_at: String,
    #[serde(default)]
    files: Vec<RecordedFile>,
}

#[derive(Serialize, Deserialize)]
struct RecordedFile {
    path: String,
    sha256: String,
}

/// Catálogo descargado y validado.
pub struct LoadedCatalog {
    pub source: CatalogSource,
    pub catalog: Catalog,
    client: reqwest::Client,
}

impl LoadedCatalog {
    pub async fn load(source: CatalogSource) -> Result<Self, FlowError> {
        let client = http_client()?;
        let bytes = source.fetch(&client, MAX_CATALOG_BYTES).await?;
        let catalog =
            Catalog::parse(&bytes).map_err(|error| invalid(format!("{source}: {error}")))?;
        Ok(Self {
            source,
            catalog,
            client,
        })
    }

    /// Instala `id` o `id@versión` en `plugin_dir`. Si algo falla (descarga,
    /// checksum, `pull`), no queda nada instalado ni se toca la versión previa.
    pub async fn install(
        &self,
        plugin_dir: &Path,
        request: &str,
        options: &InstallOptions,
    ) -> Result<InstallOutcome, FlowError> {
        let (id, wanted) = match request.split_once('@') {
            Some((id, version)) => (id, Some(version)),
            None => (request, None),
        };
        let plugin = self.catalog.plugin(id).ok_or_else(|| {
            invalid(format!(
                "'{id}' no está en el catálogo {}; búscalo con `jaiba plugins search`",
                self.source
            ))
        })?;
        let version = match wanted {
            Some(wanted) => plugin.version(wanted).ok_or_else(|| {
                let available: Vec<_> =
                    plugin.versions.iter().map(|v| v.version.as_str()).collect();
                invalid(format!(
                    "'{id}' no tiene la versión {wanted}; disponibles: {}",
                    available.join(", ")
                ))
            })?,
            None => plugin.latest(),
        };
        if version.protocol_version != PROTOCOL_VERSION {
            return Err(invalid(format!(
                "{id}@{} usa el protocolo de plugins {} y esta versión de Jaiba usa el {PROTOCOL_VERSION}",
                version.version, version.protocol_version
            )));
        }
        let target = plugin_dir.join(id);
        let replaced = installed_manifest(&target).map(|manifest| manifest.version);
        if target.exists() && !options.force {
            return Err(invalid(format!(
                "'{id}' ya está instalado ({}) en {}; usa --force para reemplazarlo",
                replaced.as_deref().unwrap_or("versión desconocida"),
                target.display()
            )));
        }
        let io = |path: &Path| {
            let path = path.display().to_string();
            move |error: std::io::Error| invalid(format!("{path}: {error}"))
        };
        fs::create_dir_all(plugin_dir).map_err(io(plugin_dir))?;
        let staging = Staging::create(
            plugin_dir.join(format!(".install-{id}-{}", uuid::Uuid::new_v4().simple())),
        )?;

        let mut recorded = Vec::new();
        for file in &version.files {
            let relative = safe_relative_path(&file.path).expect("validate() revisó la ruta");
            let source = self.source.join(&file.url)?;
            let bytes = source.fetch(&self.client, MAX_FILE_BYTES).await?;
            let digest = to_hex(&Sha256::digest(&bytes));
            if digest != file.sha256 {
                return Err(invalid(format!(
                    "checksum distinto en '{}' ({source}): se esperaba {} y llegó {digest}; no se instaló nada",
                    file.path, file.sha256
                )));
            }
            let destination = staging.path.join(&relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(io(parent))?;
            }
            fs::write(&destination, &bytes).map_err(io(&destination))?;
            #[cfg(unix)]
            if file.executable {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&destination, fs::Permissions::from_mode(0o755))
                    .map_err(io(&destination))?;
            }
            recorded.push(RecordedFile {
                path: file.path.clone(),
                sha256: digest,
            });
        }
        if let (Some(spec), true) = (&version.container, options.pull) {
            pull_image(&options.container_runtime, &spec.image).await?;
        }

        let manifest = version.manifest(plugin);
        let yaml = serde_yaml::to_string(&manifest)?;
        let manifest_path = staging.path.join(MANIFEST_FILE);
        fs::write(
            &manifest_path,
            format!(
                "# Instalado con `jaiba plugins install` desde {}\n{yaml}",
                self.source
            ),
        )
        .map_err(io(&manifest_path))?;
        let record = InstallRecord {
            catalog: self.source.to_string(),
            version: version.version.clone(),
            installed_at: chrono::Utc::now().to_rfc3339(),
            files: recorded,
        };
        let record_path = staging.path.join(INSTALL_RECORD);
        fs::write(
            &record_path,
            serde_json::to_vec_pretty(&record).expect("InstallRecord es serializable"),
        )
        .map_err(io(&record_path))?;
        staging.commit(&target)?;

        Ok(InstallOutcome {
            id: id.to_owned(),
            version: version.version.clone(),
            kind: version.kind(),
            directory: target,
            replaced,
            image: version.container.as_ref().map(|spec| spec.image.clone()),
        })
    }
}

async fn pull_image(runtime: &str, image: &str) -> Result<(), FlowError> {
    let status = tokio::process::Command::new(runtime)
        .args(["pull", image])
        .stdin(Stdio::null())
        .status()
        .await
        .map_err(|error| invalid(format!("no se pudo ejecutar `{runtime} pull`: {error}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(invalid(format!(
            "`{runtime} pull {image}` falló ({status}); no se instaló nada"
        )))
    }
}

/// Carpeta temporal dentro de `JAIBA_PLUGIN_DIR` que se borra si la instalación
/// no llega a `commit`.
struct Staging {
    path: PathBuf,
    committed: bool,
}

impl Staging {
    fn create(path: PathBuf) -> Result<Self, FlowError> {
        fs::create_dir(&path).map_err(|error| invalid(format!("{}: {error}", path.display())))?;
        Ok(Self {
            path,
            committed: false,
        })
    }

    /// Mueve la carpeta a `target`; la versión anterior se aparta primero y se
    /// restaura si el movimiento falla.
    fn commit(mut self, target: &Path) -> Result<(), FlowError> {
        let io = |error: std::io::Error| invalid(format!("{}: {error}", target.display()));
        let backup = if target.exists() {
            let backup = target.with_file_name(format!(
                ".old-{}-{}",
                target
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("plugin"),
                uuid::Uuid::new_v4().simple()
            ));
            fs::rename(target, &backup).map_err(io)?;
            Some(backup)
        } else {
            None
        };
        if let Err(error) = fs::rename(&self.path, target) {
            if let Some(backup) = &backup {
                let _ = fs::rename(backup, target);
            }
            return Err(io(error));
        }
        self.committed = true;
        if let Some(backup) = backup {
            let _ = fs::remove_dir_all(backup);
        }
        Ok(())
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn installed_manifest(directory: &Path) -> Option<PluginManifest> {
    let text = fs::read_to_string(directory.join(MANIFEST_FILE)).ok()?;
    serde_yaml::from_str(&text).ok()
}

#[derive(Debug)]
pub struct InstalledPlugin {
    pub id: String,
    pub version: Option<String>,
    pub kind: Option<&'static str>,
    pub description: String,
    /// Catálogo de origen si se instaló con `jaiba plugins install`.
    pub catalog: Option<String>,
    pub problem: Option<String>,
}

/// Plugins en `plugin_dir` (carpetas con `plugin.yaml`; ignora las ocultas).
pub fn list_installed(plugin_dir: &Path) -> Result<Vec<InstalledPlugin>, FlowError> {
    let entries = match fs::read_dir(plugin_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(invalid(format!("{}: {error}", plugin_dir.display()))),
    };
    let mut plugins = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| invalid(format!("{}: {error}", plugin_dir.display())))?;
        let Ok(id) = entry.file_name().into_string() else {
            continue;
        };
        if id.starts_with('.') || !entry.path().is_dir() {
            continue;
        }
        let directory = entry.path();
        let catalog = fs::read(directory.join(INSTALL_RECORD))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<InstallRecord>(&bytes).ok())
            .map(|record| record.catalog);
        let mut plugin = InstalledPlugin {
            id: id.clone(),
            version: None,
            kind: None,
            description: String::new(),
            catalog,
            problem: None,
        };
        match fs::read_to_string(directory.join(MANIFEST_FILE)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => plugin.problem = Some(format!("{MANIFEST_FILE}: {error}")),
            Ok(text) => match serde_yaml::from_str::<PluginManifest>(&text) {
                Err(error) => plugin.problem = Some(format!("{MANIFEST_FILE}: {error}")),
                Ok(manifest) => {
                    plugin.problem = manifest.validate().err().or_else(|| {
                        (manifest.id != id)
                            .then(|| format!("el id '{}' no coincide con la carpeta", manifest.id))
                    });
                    plugin.kind = Some(if manifest.container.is_some() {
                        "contenedor"
                    } else {
                        "proceso"
                    });
                    plugin.version = Some(manifest.version);
                    plugin.description = manifest.description;
                }
            },
        }
        plugins.push(plugin);
    }
    plugins.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(plugins)
}

#[derive(Debug)]
pub struct Removed {
    pub version: Option<String>,
    /// Imagen que queda en el host (Jaiba no la borra).
    pub image: Option<String>,
}

/// Borra `plugin_dir/<id>`; solo si es un plugin (tiene `plugin.yaml`).
pub fn remove_installed(plugin_dir: &Path, id: &str) -> Result<Removed, FlowError> {
    if !valid_plugin_id(id) {
        return Err(invalid(format!("id de plugin inválido '{id}'")));
    }
    let target = plugin_dir.join(id);
    if !target.join(MANIFEST_FILE).is_file() {
        return Err(invalid(format!(
            "'{id}' no está instalado en {}",
            plugin_dir.display()
        )));
    }
    let manifest = installed_manifest(&target);
    let trash = plugin_dir.join(format!(".remove-{id}-{}", uuid::Uuid::new_v4().simple()));
    let io = |error: std::io::Error| invalid(format!("{}: {error}", target.display()));
    fs::rename(&target, &trash).map_err(io)?;
    fs::remove_dir_all(&trash).map_err(io)?;
    Ok(Removed {
        version: manifest.as_ref().map(|manifest| manifest.version.clone()),
        image: manifest.and_then(|manifest| manifest.container.map(|spec| spec.image)),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    const SCRIPT: &[u8] = b"print('hola')\n";

    fn temp_root(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "jaiba-catalog-test-{name}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).unwrap();
        root
    }

    fn sha(bytes: &[u8]) -> String {
        to_hex(&Sha256::digest(bytes))
    }

    fn process_version(version: &str, url: &str, sha256: &str) -> Value {
        json!({
            "version": version,
            "protocol_version": 1,
            "command": "python3",
            "args": ["main.py"],
            "files": [{"path": "main.py", "url": url, "sha256": sha256}]
        })
    }

    fn catalog_with(versions: Vec<Value>) -> Value {
        json!({
            "catalog_version": 1,
            "plugins": [{"id": "demo", "description": "Plugin de prueba", "versions": versions}]
        })
    }

    fn write_catalog(root: &Path, catalog: &Value) -> CatalogSource {
        let path = root.join("catalog/plugins.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, catalog.to_string()).unwrap();
        CatalogSource::File(path)
    }

    fn options(force: bool) -> InstallOptions {
        InstallOptions {
            force,
            pull: true,
            container_runtime: "docker".to_owned(),
        }
    }

    fn leftovers(plugin_dir: &Path) -> Vec<String> {
        fs::read_dir(plugin_dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
                    .filter(|name| name.starts_with('.'))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn rejects_invalid_catalog_entries() {
        let good = process_version("0.1.0", "main.py", &sha(SCRIPT));
        let with = |patch: Value| {
            let mut version = good.clone();
            for (key, value) in patch.as_object().unwrap() {
                version[key] = value.clone();
            }
            catalog_with(vec![version])
        };
        let digest = "a".repeat(64);
        let cases = [
            (json!({"catalog_version": 2, "plugins": []}), "formato 2"),
            (with(json!({"version": "1.0"})), "MAYOR.MENOR.PARCHE"),
            (with(json!({"version": "v1.0.0"})), "MAYOR.MENOR.PARCHE"),
            (
                with(json!({"files": [{"path": "../x.py", "url": "x", "sha256": sha(SCRIPT)}]})),
                "ruta de archivo inválida",
            ),
            (
                with(
                    json!({"files": [{"path": "plugin.yaml", "url": "x", "sha256": sha(SCRIPT)}]}),
                ),
                "ruta de archivo inválida",
            ),
            (
                with(json!({"files": [{"path": "/etc/x", "url": "x", "sha256": sha(SCRIPT)}]})),
                "ruta de archivo inválida",
            ),
            (
                with(json!({"files": [{"path": "main.py", "url": "x", "sha256": "ABC"}]})),
                "64 caracteres",
            ),
            (with(json!({"files": []})), "necesita `files`"),
            (
                with(
                    json!({"command": null, "files": [], "container": {"image": "ghcr.io/a/b:1.0"}}),
                ),
                "fijarse por digest",
            ),
            (
                with(
                    json!({"command": null, "container": {"image": format!("ghcr.io/a/b@sha256:{digest}")}}),
                ),
                "no lleva `files`",
            ),
            (
                with(json!({"container": {"image": format!("ghcr.io/a/b@sha256:{digest}")}})),
                "exactamente uno",
            ),
            (catalog_with(vec![good.clone(), good.clone()]), "duplicada"),
            (
                json!({"catalog_version": 1, "plugins": [{"id": "Demo", "description": "", "versions": [good.clone()]}]}),
                "id inválido",
            ),
        ];
        for (catalog, expected) in cases {
            let error = Catalog::parse(catalog.to_string().as_bytes())
                .err()
                .unwrap_or_else(|| panic!("se esperaba error: {catalog}"))
                .to_string();
            assert!(error.contains(expected), "{expected}: {error}");
        }
        let catalog = Catalog::parse(
            catalog_with(vec![
                process_version("0.10.0", "main.py", &sha(SCRIPT)),
                process_version("0.9.1", "main.py", &sha(SCRIPT)),
            ])
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        assert_eq!(catalog.plugin("demo").unwrap().latest().version, "0.10.0");
        assert_eq!(catalog.search("PRUEBA").len(), 1);
        assert!(catalog.search("nada").is_empty());
    }

    #[tokio::test]
    async fn installs_lists_replaces_and_removes_from_a_local_catalog() {
        let root = temp_root("local");
        let plugin_dir = root.join("plugins");
        fs::write(root.join("main.py"), SCRIPT).unwrap();
        let first = write_catalog(
            &root,
            &catalog_with(vec![process_version("0.1.0", "../main.py", &sha(SCRIPT))]),
        );
        let catalog = LoadedCatalog::load(first).await.unwrap();
        let outcome = catalog
            .install(&plugin_dir, "demo", &options(false))
            .await
            .unwrap();
        assert_eq!(
            (outcome.version.as_str(), outcome.kind),
            ("0.1.0", "proceso")
        );
        assert_eq!(fs::read(plugin_dir.join("demo/main.py")).unwrap(), SCRIPT);
        let manifest: PluginManifest =
            serde_yaml::from_str(&fs::read_to_string(plugin_dir.join("demo/plugin.yaml")).unwrap())
                .unwrap();
        manifest.validate().unwrap();
        assert_eq!(manifest.command.as_deref(), Some("python3"));

        let installed = list_installed(&plugin_dir).unwrap();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].version.as_deref(), Some("0.1.0"));
        assert!(installed[0].catalog.is_some());
        assert!(installed[0].problem.is_none(), "{:?}", installed[0].problem);

        let again = catalog
            .install(&plugin_dir, "demo", &options(false))
            .await
            .unwrap_err()
            .to_string();
        assert!(again.contains("--force"), "{again}");
        let missing = catalog
            .install(&plugin_dir, "demo@9.9.9", &options(true))
            .await
            .unwrap_err()
            .to_string();
        assert!(missing.contains("disponibles: 0.1.0"), "{missing}");

        let second = write_catalog(
            &root,
            &catalog_with(vec![
                process_version("0.1.0", "../main.py", &sha(SCRIPT)),
                process_version("0.2.0", "../main.py", &sha(SCRIPT)),
            ]),
        );
        let outcome = LoadedCatalog::load(second)
            .await
            .unwrap()
            .install(&plugin_dir, "demo", &options(true))
            .await
            .unwrap();
        assert_eq!(outcome.version, "0.2.0");
        assert_eq!(outcome.replaced.as_deref(), Some("0.1.0"));

        let removed = remove_installed(&plugin_dir, "demo").unwrap();
        assert_eq!(removed.version.as_deref(), Some("0.2.0"));
        assert!(list_installed(&plugin_dir).unwrap().is_empty());
        assert!(remove_installed(&plugin_dir, "demo").is_err());
        assert!(
            leftovers(&plugin_dir).is_empty(),
            "{:?}",
            leftovers(&plugin_dir)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn checksum_mismatch_installs_nothing_and_keeps_the_previous_version() {
        let root = temp_root("checksum");
        let plugin_dir = root.join("plugins");
        fs::write(root.join("main.py"), SCRIPT).unwrap();
        let good = write_catalog(
            &root,
            &catalog_with(vec![process_version("0.1.0", "../main.py", &sha(SCRIPT))]),
        );
        LoadedCatalog::load(good)
            .await
            .unwrap()
            .install(&plugin_dir, "demo", &options(false))
            .await
            .unwrap();

        let tampered = write_catalog(
            &root,
            &catalog_with(vec![process_version(
                "0.2.0",
                "../main.py",
                &"0".repeat(64),
            )]),
        );
        let error = LoadedCatalog::load(tampered)
            .await
            .unwrap()
            .install(&plugin_dir, "demo", &options(true))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("checksum distinto"), "{error}");
        let installed = list_installed(&plugin_dir).unwrap();
        assert_eq!(installed[0].version.as_deref(), Some("0.1.0"));
        assert!(
            leftovers(&plugin_dir).is_empty(),
            "{:?}",
            leftovers(&plugin_dir)
        );
        fs::remove_dir_all(root).unwrap();
    }

    async fn serve(routes: Vec<(&'static str, Vec<u8>)>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let routes = routes.clone();
                tokio::spawn(async move {
                    let mut buffer = vec![0; 8192];
                    let read = socket.read(&mut buffer).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    let path = request.split_whitespace().nth(1).unwrap_or("/");
                    let (status, body) = routes
                        .iter()
                        .find(|(route, _)| *route == path)
                        .map(|(_, body)| ("200 OK", body.clone()))
                        .unwrap_or(("404 Not Found", Vec::new()));
                    let head = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn downloads_over_loopback_http_and_only_allows_https_elsewhere() {
        let catalog = catalog_with(vec![process_version(
            "0.1.0",
            "../files/main.py",
            &sha(SCRIPT),
        )]);
        let base = serve(vec![
            ("/catalog/plugins.json", catalog.to_string().into_bytes()),
            ("/files/main.py", SCRIPT.to_vec()),
        ])
        .await;
        let root = temp_root("http");
        let plugin_dir = root.join("plugins");
        let source = CatalogSource::parse(&format!("{base}/catalog/plugins.json")).unwrap();
        let loaded = LoadedCatalog::load(source).await.unwrap();
        loaded
            .install(&plugin_dir, "demo", &options(false))
            .await
            .unwrap();
        assert_eq!(fs::read(plugin_dir.join("demo/main.py")).unwrap(), SCRIPT);

        let missing = CatalogSource::parse(&format!("{base}/no-existe.json")).unwrap();
        let error = LoadedCatalog::load(missing)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("404"), "{error}");

        let error = CatalogSource::parse("http://example.com/plugins.json")
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("solo se permiten"), "{error}");
        let https = CatalogSource::parse("https://example.com/catalog/plugins.json").unwrap();
        let error = https
            .join("http://example.com/x")
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("solo se permiten"), "{error}");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn container_versions_pull_the_pinned_image_or_install_nothing() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_root("container");
        let plugin_dir = root.join("plugins");
        let image = format!("ghcr.io/dfr2098/demo@sha256:{}", "b".repeat(64));
        let catalog = write_catalog(
            &root,
            &json!({
                "catalog_version": 1,
                "plugins": [{"id": "demo", "description": "En contenedor", "versions": [{
                    "version": "1.0.0", "protocol_version": 1,
                    "container": {"image": image, "memory": "128m"}
                }]}]
            }),
        );
        let loaded = LoadedCatalog::load(catalog).await.unwrap();
        let log = root.join("runtime.log");
        let runtime = |exit: i32| {
            let path = root.join(format!("runtime-{exit}"));
            fs::write(
                &path,
                format!(
                    "#!/bin/sh\necho \"$@\" >> '{}'\nexit {exit}\n",
                    log.display()
                ),
            )
            .unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
            path.display().to_string()
        };

        let failing = InstallOptions {
            force: false,
            pull: true,
            container_runtime: runtime(1),
        };
        let error = loaded
            .install(&plugin_dir, "demo", &failing)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("no se instaló nada"), "{error}");
        assert!(list_installed(&plugin_dir).unwrap().is_empty());

        let working = InstallOptions {
            container_runtime: runtime(0),
            ..failing
        };
        let outcome = loaded.install(&plugin_dir, "demo", &working).await.unwrap();
        assert_eq!(outcome.kind, "contenedor");
        assert_eq!(outcome.image.as_deref(), Some(image.as_str()));
        assert_eq!(
            fs::read_to_string(&log).unwrap(),
            format!("pull {image}\npull {image}\n")
        );
        let manifest: PluginManifest =
            serde_yaml::from_str(&fs::read_to_string(plugin_dir.join("demo/plugin.yaml")).unwrap())
                .unwrap();
        let spec = manifest.container.unwrap();
        assert_eq!(
            (spec.image.as_str(), spec.memory.as_str()),
            (image.as_str(), "128m")
        );
        assert!(
            leftovers(&plugin_dir).is_empty(),
            "{:?}",
            leftovers(&plugin_dir)
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn repository_catalog_is_valid_and_matches_its_files() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../catalog/plugins.json");
        let loaded = LoadedCatalog::load(CatalogSource::File(path))
            .await
            .unwrap();
        assert!(!loaded.catalog.plugins.is_empty());
        for plugin in &loaded.catalog.plugins {
            for version in &plugin.versions {
                for file in &version.files {
                    let source = loaded.source.join(&file.url).unwrap();
                    if matches!(source, CatalogSource::Url(_)) {
                        continue;
                    }
                    let bytes = source.fetch(&loaded.client, MAX_FILE_BYTES).await.unwrap();
                    assert_eq!(
                        sha(&bytes),
                        file.sha256,
                        "{}@{} {}: actualiza el sha256 del catálogo",
                        plugin.id,
                        version.version,
                        file.path
                    );
                }
            }
        }
    }
}

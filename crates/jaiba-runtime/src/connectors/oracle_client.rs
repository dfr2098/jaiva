//! Oracle Instant Client gestionado por Jaiba (`jaiba oracle-client`).
//!
//! El cliente se descarga de oracle.com con versión y SHA-256 fijos y se
//! extrae en `<raíz>/instantclient`. La raíz es `JAIBA_ORACLE_CLIENT_DIR` o,
//! por defecto, `~/.jaiba/oracle` (`%LOCALAPPDATA%\Jaiba\oracle` en Windows).
//! Sin cliente instalado, ODPI-C lo busca como siempre (`LD_LIBRARY_PATH`,
//! `PATH`…).

use std::{
    fs,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::FlowError;

pub const CLIENT_DIR_ENV: &str = "JAIBA_ORACLE_CLIENT_DIR";
/// Evita relanzar el proceso más de una vez (ver [`relaunch_with_client`]).
#[cfg(target_os = "linux")]
const RELAUNCH_GUARD_ENV: &str = "JAIBA_ORACLE_CLIENT_RELAUNCHED";
const CLIENT_SUBDIR: &str = "instantclient";
const MANIFEST_FILE: &str = ".jaiba-oracle-client.json";
const MAX_DOWNLOAD_BYTES: u64 = 512 * 1024 * 1024;
const GUIDE_URL: &str = "https://github.com/dfr2098/jaiva/blob/main/docs/oracle.md";

/// Paquete Basic Light publicado por Oracle para una plataforma.
#[derive(Debug, Clone, Copy)]
pub struct ClientPackage {
    pub platform: &'static str,
    pub version: &'static str,
    pub url: &'static str,
    /// Publicado en la página de descargas de Oracle.
    pub sha256: &'static str,
}

const PACKAGES: &[ClientPackage] = &[
    ClientPackage {
        platform: "linux-x86_64",
        version: "23.26.3.0.0",
        url: "https://download.oracle.com/otn_software/linux/instantclient/2326300/instantclient-basiclite-linux.x64-23.26.3.0.0.zip",
        sha256: "88c2b59d473f0d34615556dbee51cb90cce61a9104b4c1c772d13c344fba19c9",
    },
    ClientPackage {
        platform: "linux-aarch64",
        version: "23.26.3.0.0",
        url: "https://download.oracle.com/otn_software/linux/instantclient/2326300/instantclient-basiclite-linux.arm64-23.26.3.0.0.zip",
        sha256: "6559d63a81133e821cbf7fd81d023827813f3262a1ea45a32d364eeeccc68579",
    },
    ClientPackage {
        platform: "windows-x86_64",
        version: "23.26.3.0.0",
        url: "https://download.oracle.com/otn_software/nt/instantclient/2326300/instantclient-basiclite-windows.x64-23.26.3.0.0.zip",
        sha256: "9d9b9a8deafbc94086789d4b16c8156deb6a101be9db1a52d3d7c6b714a0fa07",
    },
];

/// Paquete para la plataforma de este binario, si Oracle publica un ZIP.
pub fn platform_package() -> Option<&'static ClientPackage> {
    let platform = format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    PACKAGES.iter().find(|package| package.platform == platform)
}

fn library_file() -> &'static str {
    match std::env::consts::OS {
        "windows" => "oci.dll",
        "macos" => "libclntsh.dylib",
        _ => "libclntsh.so",
    }
}

/// Carpeta donde se instala el cliente.
pub fn client_root() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os(CLIENT_DIR_ENV).filter(|dir| !dir.is_empty()) {
        return Some(PathBuf::from(dir));
    }
    if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(|dir| PathBuf::from(dir).join("Jaiba").join("oracle"))
    } else {
        std::env::var_os("HOME").map(|dir| PathBuf::from(dir).join(".jaiba").join("oracle"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstalledClient {
    #[serde(skip)]
    pub dir: PathBuf,
    pub version: String,
    pub url: String,
    pub sha256: String,
}

/// Cliente instalado por `jaiba oracle-client install`, si existe.
pub fn installed_client() -> Option<InstalledClient> {
    installed_in(&client_root()?)
}

fn installed_in(root: &Path) -> Option<InstalledClient> {
    let dir = root.join(CLIENT_SUBDIR);
    if !dir.join(library_file()).exists() {
        return None;
    }
    let manifest = fs::read(dir.join(MANIFEST_FILE)).ok()?;
    let mut client: InstalledClient = serde_json::from_slice(&manifest).ok()?;
    client.dir = dir;
    Some(client)
}

/// En Linux el cargador solo lee `LD_LIBRARY_PATH` al iniciar el proceso y
/// las bibliotecas de Instant Client no traen `RUNPATH`, así que indicar la
/// carpeta a ODPI-C no basta. Si hay un cliente instalado que no está en la
/// ruta, el proceso se relanza (`exec`) con la carpeta al frente. Debe
/// llamarse al inicio de `main`, antes de crear hilos.
pub fn relaunch_with_client() {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;

        if std::env::var_os(RELAUNCH_GUARD_ENV).is_some() {
            return;
        }
        let Some(client) = installed_client() else {
            return;
        };
        let current = std::env::var_os("LD_LIBRARY_PATH").unwrap_or_default();
        if std::env::split_paths(&current).any(|path| path == client.dir) {
            return;
        }
        let Ok(executable) = std::env::current_exe() else {
            return;
        };
        let paths = std::iter::once(client.dir.clone()).chain(std::env::split_paths(&current));
        let Ok(library_path) = std::env::join_paths(paths) else {
            return;
        };
        let error = std::process::Command::new(executable)
            .args(std::env::args_os().skip(1))
            .env("LD_LIBRARY_PATH", library_path)
            .env(RELAUNCH_GUARD_ENV, "1")
            .exec();
        eprintln!(
            "aviso: no se pudo relanzar jaiba con Oracle Instant Client en {}: {error}",
            client.dir.display()
        );
    }
}

/// Inicializa ODPI-C una vez, con el cliente instalado si lo hay. Si falla
/// (cliente ausente) no queda cacheado: la siguiente llamada reintenta, así
/// que instalar el cliente no exige reiniciar en Windows.
pub fn ensure_initialized() -> Result<(), oracle::Error> {
    if oracle::InitParams::is_initialized() {
        return Ok(());
    }
    let mut params = oracle::InitParams::new();
    params.load_error_url(GUIDE_URL)?;
    // En Linux la ruta llega por LD_LIBRARY_PATH (ver `relaunch_with_client`).
    if !cfg!(target_os = "linux")
        && let Some(client) = installed_client()
    {
        params.oracle_client_lib_dir(client.dir)?;
    }
    params.init().map(|_| ())
}

/// Versión del cliente que ODPI-C logró cargar.
pub fn loaded_client_version() -> Result<String, oracle::Error> {
    ensure_initialized()?;
    oracle::Version::client().map(|version| version.to_string())
}

/// Hallazgos sobre dependencias del sistema que Instant Client necesita.
pub fn missing_system_libraries() -> Vec<&'static str> {
    if !cfg!(target_os = "linux") {
        return Vec::new();
    }
    let dirs = [
        "/lib64",
        "/usr/lib64",
        "/lib",
        "/usr/lib",
        "/lib/x86_64-linux-gnu",
        "/usr/lib/x86_64-linux-gnu",
        "/lib/aarch64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
    ];
    if dirs
        .iter()
        .any(|dir| Path::new(dir).join("libaio.so.1").exists())
    {
        Vec::new()
    } else {
        vec!["libaio.so.1"]
    }
}

pub struct InstallOptions {
    pub force: bool,
    /// Sustituye el paquete de la plataforma (p. ej. un espejo interno).
    pub package: Option<ClientPackage>,
}

/// Descarga, verifica y extrae el cliente. `progress` recibe bytes
/// descargados y el total si el servidor lo informa.
pub async fn install(
    options: InstallOptions,
    mut progress: impl FnMut(u64, Option<u64>),
) -> Result<InstalledClient, FlowError> {
    let package = match options.package {
        Some(package) => package,
        None => *platform_package().ok_or_else(|| {
            config(format!(
                "Oracle no publica Instant Client en ZIP para {}-{}; instálalo a mano ({GUIDE_URL})",
                std::env::consts::OS,
                std::env::consts::ARCH
            ))
        })?,
    };
    if !package.url.starts_with("https://") {
        return Err(config(format!(
            "la URL del cliente debe usar https: {}",
            package.url
        )));
    }
    let root = client_root().ok_or_else(|| {
        config(format!(
            "no se encontró la carpeta personal; define {CLIENT_DIR_ENV}"
        ))
    })?;
    if let Some(existing) = installed_in(&root)
        && !options.force
    {
        return Err(config(format!(
            "ya hay un cliente {} en {}; usa --force para reinstalar",
            existing.version,
            existing.dir.display()
        )));
    }
    fs::create_dir_all(&root).map_err(|error| io_error(&root, error))?;
    let staging = Staging::new(&root)?;

    let archive = staging.path.join("client.zip");
    download(&package, &archive, &mut progress).await?;

    let extracted = staging.path.join(CLIENT_SUBDIR);
    let archive_for_task = archive.clone();
    let extracted_for_task = extracted.clone();
    tokio::task::spawn_blocking(move || extract(&archive_for_task, &extracted_for_task))
        .await
        .map_err(|error| config(error.to_string()))??;
    if !extracted.join(library_file()).exists() {
        return Err(config(format!(
            "el ZIP no contiene {}; ¿es un paquete Basic/Basic Light?",
            library_file()
        )));
    }
    let client = InstalledClient {
        dir: root.join(CLIENT_SUBDIR),
        version: package.version.to_owned(),
        url: package.url.to_owned(),
        sha256: package.sha256.to_owned(),
    };
    let manifest = serde_json::to_vec_pretty(&client).map_err(|error| config(error.to_string()))?;
    fs::write(extracted.join(MANIFEST_FILE), manifest)
        .map_err(|error| io_error(&extracted, error))?;

    let previous = staging.path.join("previous");
    if client.dir.exists() {
        fs::rename(&client.dir, &previous).map_err(|error| io_error(&client.dir, error))?;
    }
    if let Err(error) = fs::rename(&extracted, &client.dir) {
        if previous.exists() {
            let _ = fs::rename(&previous, &client.dir);
        }
        return Err(io_error(&client.dir, error));
    }
    Ok(client)
}

/// Borra el cliente instalado. Devuelve `false` si no había ninguno.
pub fn remove() -> Result<bool, FlowError> {
    let Some(root) = client_root() else {
        return Ok(false);
    };
    let dir = root.join(CLIENT_SUBDIR);
    if !dir.exists() {
        return Ok(false);
    }
    fs::remove_dir_all(&dir).map_err(|error| io_error(&dir, error))?;
    Ok(true)
}

async fn download(
    package: &ClientPackage,
    destination: &Path,
    progress: &mut impl FnMut(u64, Option<u64>),
) -> Result<(), FlowError> {
    let redirects = reqwest::redirect::Policy::custom(|attempt| {
        if attempt.previous().len() >= 5 {
            attempt.error("demasiadas redirecciones")
        } else if attempt.url().scheme() == "https" {
            attempt.follow()
        } else {
            attempt.error("redirección a una URL que no es https")
        }
    });
    let client = reqwest::Client::builder()
        .user_agent(concat!("jaiba/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(120))
        .redirect(redirects)
        .build()
        .map_err(|error| config(format!("cliente HTTP: {error}")))?;
    let mut response = client
        .get(package.url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(|error| download_error(package, error))?;
    let total = response.content_length();
    if total.is_some_and(|total| total > MAX_DOWNLOAD_BYTES) {
        return Err(config(format!(
            "la descarga supera {} MB",
            MAX_DOWNLOAD_BYTES / 1024 / 1024
        )));
    }
    let mut file = fs::File::create(destination).map_err(|error| io_error(destination, error))?;
    let mut hasher = Sha256::new();
    let mut received = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| download_error(package, error))?
    {
        received += chunk.len() as u64;
        if received > MAX_DOWNLOAD_BYTES {
            return Err(config("la descarga es demasiado grande".to_owned()));
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .map_err(|error| io_error(destination, error))?;
        progress(received, total);
    }
    file.sync_all()
        .map_err(|error| io_error(destination, error))?;
    let actual = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if !actual.eq_ignore_ascii_case(package.sha256) {
        return Err(config(format!(
            "SHA-256 no coincide para {}: se esperaba {}, se obtuvo {actual}",
            package.url, package.sha256
        )));
    }
    Ok(())
}

/// Extrae el contenido de la carpeta superior del ZIP (`instantclient_XX_YY`)
/// en `destination`, sin salir de ella y conservando los enlaces de Linux.
fn extract(archive: &Path, destination: &Path) -> Result<(), FlowError> {
    let file = fs::File::open(archive).map_err(|error| io_error(archive, error))?;
    let mut zip = zip::ZipArchive::new(file).map_err(zip_error)?;
    fs::create_dir_all(destination).map_err(|error| io_error(destination, error))?;
    for index in 0..zip.len() {
        let mut entry = zip.by_index(index).map_err(zip_error)?;
        let Some(name) = entry.enclosed_name() else {
            return Err(config(format!(
                "entrada del ZIP con ruta insegura: {}",
                entry.name().unwrap_or_default()
            )));
        };
        let mut components = name.components();
        let Some(Component::Normal(top)) = components.next() else {
            continue;
        };
        if top == "META-INF" || !top.to_string_lossy().starts_with("instantclient_") {
            continue;
        }
        let relative = components.as_path();
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        if entry.is_dir() {
            fs::create_dir_all(&target).map_err(|error| io_error(&target, error))?;
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
        }
        if entry.is_symlink() {
            let mut link = String::new();
            io::Read::read_to_string(&mut entry, &mut link)
                .map_err(|error| io_error(&target, error))?;
            create_symlink(&link, &target)?;
            continue;
        }
        let mut output = fs::File::create(&target).map_err(|error| io_error(&target, error))?;
        io::copy(&mut entry, &mut output).map_err(|error| io_error(&target, error))?;
        #[cfg(unix)]
        if let Some(mode) = entry.unix_mode() {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o755))
                .map_err(|error| io_error(&target, error))?;
        }
    }
    Ok(())
}

/// Solo se aceptan enlaces a un archivo hermano (`libclntsh.so` →
/// `libclntsh.so.23.1`), nunca rutas que salgan de la carpeta.
fn create_symlink(link: &str, target: &Path) -> Result<(), FlowError> {
    let mut parts = Path::new(link).components();
    if !matches!(
        (parts.next(), parts.next()),
        (Some(Component::Normal(_)), None)
    ) {
        return Err(config(format!(
            "enlace inseguro en el ZIP: {} -> {link}",
            target.display()
        )));
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(link, target).map_err(|error| io_error(target, error))
    }
    #[cfg(not(unix))]
    {
        let source = target.with_file_name(link);
        fs::copy(&source, target)
            .map(|_| ())
            .map_err(|error| io_error(target, error))
    }
}

/// Carpeta temporal dentro de la raíz; se borra al terminar, con o sin error.
struct Staging {
    path: PathBuf,
}

impl Staging {
    fn new(root: &Path) -> Result<Self, FlowError> {
        let path = root.join(format!(".staging-{}", std::process::id()));
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|error| io_error(&path, error))?;
        }
        fs::create_dir_all(&path).map_err(|error| io_error(&path, error))?;
        Ok(Self { path })
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn config(message: String) -> FlowError {
    FlowError::Configuration(message)
}

fn io_error(path: &Path, error: io::Error) -> FlowError {
    config(format!("{}: {error}", path.display()))
}

fn zip_error(error: zip::result::ZipError) -> FlowError {
    config(format!("ZIP del cliente Oracle: {error}"))
}

fn download_error(package: &ClientPackage, error: reqwest::Error) -> FlowError {
    FlowError::DatabaseConnector(format!("descarga de {}: {error}", package.url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_zip(path: &Path, entries: &[(&str, Option<&str>, &[u8])]) {
        use zip::write::SimpleFileOptions;
        let stored =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        let mut writer = zip::ZipWriter::new(fs::File::create(path).unwrap());
        for (name, link, content) in entries {
            match link {
                Some(target) => writer.add_symlink(*name, *target, stored).unwrap(),
                None => {
                    writer
                        .start_file(*name, stored.unix_permissions(0o755))
                        .unwrap();
                    writer.write_all(content).unwrap();
                }
            }
        }
        writer.finish().unwrap();
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("jaiba-oracle-client-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn extracts_top_folder_and_sibling_symlinks() {
        let dir = temp_dir("extract");
        let archive = dir.join("client.zip");
        write_zip(
            &archive,
            &[
                ("META-INF/MANIFEST.MF", None, b"x"),
                ("instantclient_23_26/libclntsh.so.23.1", None, b"library"),
                (
                    "instantclient_23_26/libclntsh.so",
                    Some("libclntsh.so.23.1"),
                    b"",
                ),
                ("instantclient_23_26/network/admin/README", None, b"readme"),
            ],
        );
        let output = dir.join("out");
        extract(&archive, &output).unwrap();
        assert_eq!(fs::read(output.join("libclntsh.so")).unwrap(), b"library");
        assert_eq!(
            fs::read(output.join("network/admin/README")).unwrap(),
            b"readme"
        );
        assert!(!output.join("META-INF").exists());
        #[cfg(unix)]
        assert!(
            output
                .join("libclntsh.so")
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rejects_symlinks_that_leave_the_folder() {
        let dir = temp_dir("symlink");
        let archive = dir.join("client.zip");
        write_zip(
            &archive,
            &[(
                "instantclient_23_26/libclntsh.so",
                Some("../../etc/passwd"),
                b"",
            )],
        );
        let error = extract(&archive, &dir.join("out")).unwrap_err();
        assert!(error.to_string().contains("enlace inseguro"), "{error}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn every_platform_package_is_pinned() {
        for package in PACKAGES {
            assert!(package.url.starts_with("https://download.oracle.com/"));
            assert!(package.url.contains(package.version));
            assert_eq!(package.sha256.len(), 64);
        }
    }
}

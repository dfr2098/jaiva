//! `jaiba oracle-client install|status|remove`.

use jaiba_runtime::error::FlowError;

#[cfg(feature = "oracle-driver")]
const USAGE: &str = "Uso:
  jaiba oracle-client install [--force]   Descarga Oracle Instant Client (versión y SHA-256 fijos)
  jaiba oracle-client status              Muestra el cliente instalado y si se puede cargar
  jaiba oracle-client remove              Borra el cliente instalado

Se instala en JAIBA_ORACLE_CLIENT_DIR o en ~/.jaiba/oracle (%LOCALAPPDATA%\\Jaiba\\oracle
en Windows) y Jaiba lo usa sin configurar LD_LIBRARY_PATH ni PATH. Guía: docs/oracle.md";

#[cfg(not(feature = "oracle-driver"))]
pub async fn command(_arguments: &[String]) -> Result<(), FlowError> {
    Err(FlowError::Configuration(
        "este binario no incluye Oracle; compílalo con --features oracle-driver (o all-drivers)"
            .to_owned(),
    ))
}

#[cfg(feature = "oracle-driver")]
pub async fn command(arguments: &[String]) -> Result<(), FlowError> {
    let flags = arguments.get(2..).unwrap_or_default();
    let force = flags.iter().any(|flag| flag == "--force");
    if let Some(unknown) = flags.iter().find(|flag| *flag != "--force") {
        return Err(FlowError::Configuration(format!(
            "opción desconocida '{unknown}'.\n{USAGE}"
        )));
    }
    match arguments.get(1).map(String::as_str) {
        Some("install") => install(force).await,
        Some("status") => {
            status();
            Ok(())
        }
        Some("remove") => remove(),
        None | Some("-h" | "--help" | "help") => {
            println!("{USAGE}");
            Ok(())
        }
        Some(other) => Err(FlowError::Configuration(format!(
            "subcomando desconocido 'oracle-client {other}'.\n{USAGE}"
        ))),
    }
}

#[cfg(feature = "oracle-driver")]
async fn install(force: bool) -> Result<(), FlowError> {
    use std::io::Write;

    use jaiba_runtime::connectors::oracle_client::{self, InstallOptions};

    let package = oracle_client::platform_package();
    if let Some(existing) = oracle_client::installed_client()
        && !force
    {
        if package.is_some_and(|package| package.sha256 == existing.sha256) {
            println!(
                "Oracle Instant Client {} ya está instalado en {}.",
                existing.version,
                existing.dir.display()
            );
            report_system_libraries();
            return Ok(());
        }
        return Err(FlowError::Configuration(format!(
            "ya hay un cliente {} en {}; usa --force para reemplazarlo",
            existing.version,
            existing.dir.display()
        )));
    }
    if let Some(package) = package {
        println!("Descargando Oracle Instant Client {} ...", package.version);
        println!("  {}", package.url);
    }
    let mut last_reported = 0_u64;
    let client = oracle_client::install(
        InstallOptions {
            force,
            package: None,
        },
        |received, total| {
            if received - last_reported >= 8 * 1024 * 1024 || Some(received) == total {
                last_reported = received;
                let megabytes = received / 1024 / 1024;
                match total {
                    Some(total) => print!("\r  {megabytes} / {} MB", total / 1024 / 1024),
                    None => print!("\r  {megabytes} MB"),
                }
                let _ = std::io::stdout().flush();
            }
        },
    )
    .await?;
    println!();
    println!("SHA-256 verificado: {}", client.sha256);
    println!("Instalado en {}", client.dir.display());
    report_system_libraries();
    if cfg!(target_os = "linux") {
        println!(
            "Listo: los próximos comandos `jaiba` lo usarán solos. Si Jaiba ya estaba corriendo, reinícialo."
        );
    } else {
        println!("Listo: Jaiba lo usará sin configurar PATH.");
    }
    Ok(())
}

#[cfg(feature = "oracle-driver")]
fn status() {
    use jaiba_runtime::connectors::oracle_client;

    match oracle_client::client_root() {
        Some(root) => println!("Carpeta:     {}", root.display()),
        None => println!(
            "Carpeta:     (sin carpeta personal; define {})",
            oracle_client::CLIENT_DIR_ENV
        ),
    }
    match oracle_client::installed_client() {
        Some(client) => println!(
            "Instalado:   {} en {}",
            client.version,
            client.dir.display()
        ),
        None => println!("Instalado:   no (usa `jaiba oracle-client install`)"),
    }
    match oracle_client::platform_package() {
        Some(package) => println!("Disponible:  {} ({})", package.version, package.platform),
        None => println!(
            "Disponible:  Oracle no publica ZIP para {}-{}; instálalo a mano",
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
    }
    match oracle_client::loaded_client_version() {
        Ok(version) => println!("Cargado:     sí, cliente {version}"),
        Err(error) => println!("Cargado:     no — {error}"),
    }
    report_system_libraries();
}

#[cfg(feature = "oracle-driver")]
fn remove() -> Result<(), FlowError> {
    if jaiba_runtime::connectors::oracle_client::remove()? {
        println!("Cliente Oracle borrado.");
    } else {
        println!("No había un cliente Oracle instalado por Jaiba.");
    }
    Ok(())
}

#[cfg(feature = "oracle-driver")]
fn report_system_libraries() {
    for library in jaiba_runtime::connectors::oracle_client::missing_system_libraries() {
        println!(
            "Falta {library} del sistema (Instant Client la necesita):\n  \
             Debian/Ubuntu: sudo apt install libaio1t64 && sudo ln -sf \
             /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1\n  \
             Fedora/RHEL:   sudo dnf install libaio"
        );
    }
}

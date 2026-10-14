//! `jaiba plugins search|install|list|remove`.

use std::path::PathBuf;

use jaiba_runtime::{
    error::FlowError,
    plugin_catalog::{
        CatalogSource, InstallOptions, LoadedCatalog, PLUGIN_DIR_ENV, container_runtime,
        list_installed, remove_installed,
    },
};

const USAGE: &str = "Uso:
  jaiba plugins search [TEXTO] [--catalog URL|ARCHIVO]
  jaiba plugins install <ID>[@VERSIÓN] [--force] [--no-pull] [--catalog URL|ARCHIVO]
  jaiba plugins list
  jaiba plugins remove <ID>

El catálogo sale de --catalog, de JAIBA_PLUGIN_CATALOG o del catálogo comunitario.
Los plugins se instalan en JAIBA_PLUGIN_DIR. Guía: docs/plugins.md";

#[derive(Default)]
struct Options {
    positional: Vec<String>,
    force: bool,
    no_pull: bool,
    catalog: Option<String>,
}

fn parse(arguments: &[String]) -> Result<Options, FlowError> {
    let mut options = Options::default();
    let mut iterator = arguments.iter();
    while let Some(argument) = iterator.next() {
        match argument.as_str() {
            "--force" => options.force = true,
            "--no-pull" => options.no_pull = true,
            "--catalog" => {
                options.catalog = Some(iterator.next().cloned().ok_or_else(|| {
                    FlowError::Configuration("--catalog necesita una URL o un archivo".to_owned())
                })?);
            }
            flag if flag.starts_with('-') => {
                return Err(FlowError::Configuration(format!(
                    "opción desconocida '{flag}'.\n{USAGE}"
                )));
            }
            value => options.positional.push(value.to_owned()),
        }
    }
    Ok(options)
}

fn usage_error(message: &str) -> FlowError {
    FlowError::Configuration(format!("{message}.\n{USAGE}"))
}

pub async fn command(arguments: &[String]) -> Result<(), FlowError> {
    let options = parse(arguments.get(2..).unwrap_or_default())?;
    match arguments.get(1).map(String::as_str) {
        Some("search") => search(options).await,
        Some("install") => install(options).await,
        Some("list") => list(options),
        Some("remove") => remove(options),
        None | Some("-h" | "--help" | "help") => {
            println!("{USAGE}");
            Ok(())
        }
        Some(other) => Err(usage_error(&format!(
            "subcomando desconocido 'plugins {other}'"
        ))),
    }
}

fn plugin_dir() -> Result<PathBuf, FlowError> {
    std::env::var_os(PLUGIN_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            FlowError::Configuration(format!(
                "define {PLUGIN_DIR_ENV} con la carpeta de plugins (p. ej. export {PLUGIN_DIR_ENV}=$HOME/.jaiba/plugins)"
            ))
        })
}

async fn load_catalog(options: &Options) -> Result<LoadedCatalog, FlowError> {
    let source = match &options.catalog {
        Some(catalog) => CatalogSource::parse(catalog)?,
        None => CatalogSource::from_env()?,
    };
    LoadedCatalog::load(source).await
}

fn print_table<const N: usize>(header: [&str; N], rows: &[[String; N]]) {
    let mut widths = header.map(|title| title.chars().count());
    for row in rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let line = |cells: Vec<&str>| {
        let mut text = String::new();
        for (index, (cell, width)) in cells.iter().zip(widths).enumerate() {
            if index + 1 == N {
                text.push_str(cell);
            } else {
                text.push_str(&format!("{cell:<width$}  "));
            }
        }
        println!("{}", text.trim_end());
    };
    line(header.to_vec());
    for row in rows {
        line(row.iter().map(String::as_str).collect());
    }
}

async fn search(options: Options) -> Result<(), FlowError> {
    let query = options.positional.join(" ");
    let catalog = load_catalog(&options).await?;
    let found = catalog.catalog.search(&query);
    if found.is_empty() {
        println!("Sin resultados para '{query}' en {}.", catalog.source);
        return Ok(());
    }
    let rows: Vec<_> = found
        .iter()
        .map(|plugin| {
            let latest = plugin.latest();
            [
                plugin.id.clone(),
                latest.version.clone(),
                latest.kind().to_owned(),
                plugin.description.clone(),
            ]
        })
        .collect();
    print_table(["ID", "VERSIÓN", "TIPO", "DESCRIPCIÓN"], &rows);
    println!("\nInstala con: jaiba plugins install <ID>");
    Ok(())
}

async fn install(options: Options) -> Result<(), FlowError> {
    let [request] = options.positional.as_slice() else {
        return Err(usage_error("install necesita exactamente un <ID>"));
    };
    let plugin_dir = plugin_dir()?;
    let catalog = load_catalog(&options).await?;
    let runtime = container_runtime();
    let outcome = catalog
        .install(
            &plugin_dir,
            request,
            &InstallOptions {
                force: options.force,
                pull: !options.no_pull,
                container_runtime: runtime.clone(),
            },
        )
        .await?;
    println!(
        "Instalado {} {} ({}) en {}",
        outcome.id,
        outcome.version,
        outcome.kind,
        outcome.directory.display()
    );
    if let Some(previous) = &outcome.replaced {
        println!("Reemplaza la versión {previous}.");
    }
    if let (Some(image), true) = (&outcome.image, options.no_pull) {
        println!("Descarga la imagen antes de usarlo: {runtime} pull {image}");
    }
    println!(
        "\nÚsalo en un flujo:\n  - id: {id}\n    type: external_plugin\n    config:\n      plugin: {id}",
        id = outcome.id
    );
    Ok(())
}

fn list(options: Options) -> Result<(), FlowError> {
    if !options.positional.is_empty() {
        return Err(usage_error("list no recibe argumentos"));
    }
    let plugin_dir = plugin_dir()?;
    let plugins = list_installed(&plugin_dir)?;
    if plugins.is_empty() {
        println!("No hay plugins instalados en {}.", plugin_dir.display());
        return Ok(());
    }
    let rows: Vec<_> = plugins
        .iter()
        .map(|plugin| {
            [
                plugin.id.clone(),
                plugin.version.clone().unwrap_or_else(|| "?".to_owned()),
                plugin.kind.unwrap_or("?").to_owned(),
                if plugin.catalog.is_some() {
                    "catálogo"
                } else {
                    "manual"
                }
                .to_owned(),
                plugin.description.clone(),
            ]
        })
        .collect();
    print_table(["ID", "VERSIÓN", "TIPO", "ORIGEN", "DESCRIPCIÓN"], &rows);
    for plugin in &plugins {
        if let Some(problem) = &plugin.problem {
            println!("! {}: {problem}", plugin.id);
        }
    }
    Ok(())
}

fn remove(options: Options) -> Result<(), FlowError> {
    let [id] = options.positional.as_slice() else {
        return Err(usage_error("remove necesita exactamente un <ID>"));
    };
    let removed = remove_installed(&plugin_dir()?, id)?;
    println!(
        "Eliminado {id}{}",
        removed
            .version
            .map(|version| format!(" {version}"))
            .unwrap_or_default()
    );
    if let Some(image) = removed.image {
        println!(
            "La imagen {image} sigue en el host; si ya no la usas: {} image rm {image}",
            container_runtime()
        );
    }
    Ok(())
}

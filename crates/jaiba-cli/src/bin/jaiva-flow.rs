//! Alias temporal compatible con las instalaciones de Jaiva anteriores.

#[tokio::main]
async fn main() -> std::process::ExitCode {
    jaiba_cli::run_and_report().await
}

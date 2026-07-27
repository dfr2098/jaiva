//! Alias temporal compatible con las instalaciones de Jaiva anteriores.

#[tokio::main]
async fn main() -> Result<(), jaiba_runtime::error::FlowError> {
    jaiba_cli::run().await
}

#[tokio::main]
async fn main() -> Result<(), jaiba_runtime::error::FlowError> {
    jaiba_cli::run().await
}

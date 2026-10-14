fn main() -> std::process::ExitCode {
    jaiba_cli::prepare_process();
    tokio::runtime::Runtime::new()
        .expect("Tokio runtime")
        .block_on(jaiba_cli::run_and_report())
}

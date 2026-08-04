//! Shell de escritorio Jaiba (fases 9B + 10A).
//!
//! - 9B: UI embebida + modo remoto hacia `jaiba serve` en loopback.
//! - 10A: sidecar opcional (`jaiba serve`) gestionado por el shell, con
//!   conmutación local/remoto.

mod sidecar;

use sidecar::{commands, EngineManager, EngineMode};
use tauri::{Manager, RunEvent};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            commands::api_base,
            commands::engine_status,
            commands::set_engine_mode,
            commands::start_local_engine,
            commands::stop_local_engine,
        ])
        .setup(|app| {
            let manager = EngineManager::new(app.handle());
            if manager.status().mode == EngineMode::Local {
                if let Err(error) = manager.start_local(app.handle()) {
                    eprintln!("[jaiba-desktop] no se pudo auto-arrancar sidecar: {error}");
                }
            }
            app.manage(manager);

            if let Some(window) = app.get_webview_window("main") {
                let _ = window.set_title("Jaiba");
            }
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building Jaiba desktop")
        .run(|app, event| {
            if matches!(event, RunEvent::Exit | RunEvent::ExitRequested { .. }) {
                if let Some(engine) = app.try_state::<EngineManager>() {
                    engine.shutdown();
                }
            }
        });
}

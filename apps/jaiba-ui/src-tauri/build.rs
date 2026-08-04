fn main() {
    // Expone el triple del host al crate para localizar binaries/jaiba-<triple>.
    if let Ok(target) = std::env::var("TARGET") {
        println!("cargo:rustc-env=TARGET={target}");
    }
    tauri_build::build()
}

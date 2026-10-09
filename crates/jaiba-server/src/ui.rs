//! Consola web en `/`. Con la feature `embedded-ui` los archivos de
//! `apps/jaiba-ui/dist` van dentro del binario; sin ella, `/` explica cómo
//! obtener la consola.

use axum::{
    http::{StatusCode, Uri, header},
    response::{IntoResponse, Response},
};

#[cfg(feature = "embedded-ui")]
use rust_embed::Embed;

#[cfg(feature = "embedded-ui")]
#[derive(Embed)]
#[folder = "../../apps/jaiba-ui/dist"]
struct Console;

/// La consola llama a la API en el mismo origen desde el que se cargó.
#[cfg(feature = "embedded-ui")]
const API_BASE_SCRIPT: &str = "<script>window.__JAIBA_API_BASE__=window.location.origin</script>";

const NOT_INCLUDED_HTML: &str = "<!doctype html><html lang=\"es\"><meta charset=\"utf-8\">\
<title>Jaiba</title><body style=\"font-family:sans-serif;max-width:40rem;margin:3rem auto\">\
<h1>Jaiba</h1><p>Este binario no incluye la consola web. La trae el binario de \
<a href=\"https://github.com/dfr2098/jaiva/releases\">Releases</a>; para compilarla: \
<code>npm run build</code> en <code>apps/jaiba-ui</code> y \
<code>cargo build --features embedded-ui</code>.</p>\
<p>La API sigue disponible: <a href=\"/health\">/health</a>, <code>/api/v1/…</code>.</p>";

/// Fallback del router: lo que no es API ni métricas es la consola.
pub(crate) async fn console(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path.starts_with("api/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    serve(path)
}

/// Indica en el log dónde abrir la consola (solo si va incluida).
pub(crate) fn log_location(scheme: &str, address: std::net::SocketAddr) {
    if cfg!(feature = "embedded-ui") {
        tracing::info!("consola web: {scheme}://{address}/");
    }
}

#[cfg(feature = "embedded-ui")]
fn serve(path: &str) -> Response {
    if let Some(file) = Console::get(path).filter(|_| !path.is_empty()) {
        let cache = if path.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        return (
            [
                (header::CONTENT_TYPE, file.metadata.mimetype().to_owned()),
                (header::CACHE_CONTROL, cache.to_owned()),
            ],
            file.data,
        )
            .into_response();
    }
    let looks_like_file = path
        .rsplit('/')
        .next()
        .is_some_and(|name| name.contains('.'));
    if looks_like_file {
        return StatusCode::NOT_FOUND.into_response();
    }
    match Console::get("index.html") {
        Some(index) => {
            let html = String::from_utf8_lossy(&index.data).replacen(
                "</head>",
                &format!("{API_BASE_SCRIPT}</head>"),
                1,
            );
            (
                [
                    (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                    (header::CACHE_CONTROL, "no-cache"),
                ],
                html,
            )
                .into_response()
        }
        None => not_included(),
    }
}

#[cfg(not(feature = "embedded-ui"))]
fn serve(_path: &str) -> Response {
    not_included()
}

fn not_included() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        NOT_INCLUDED_HTML,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn get(path: &str) -> (StatusCode, String) {
        let response = console(path.parse().unwrap()).await;
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    #[tokio::test]
    async fn unknown_api_paths_stay_404() {
        let (status, body) = get("/api/v1/no-existe").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.is_empty());
    }

    #[cfg(not(feature = "embedded-ui"))]
    #[tokio::test]
    async fn without_embedded_ui_root_explains_how_to_get_it() {
        let (status, body) = get("/").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body.contains("embedded-ui"), "{body}");
    }

    #[cfg(feature = "embedded-ui")]
    #[tokio::test]
    async fn embedded_ui_serves_index_with_same_origin_api() {
        let (status, body) = get("/").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("__JAIBA_API_BASE__"), "{body}");
        assert!(body.contains("<div id=\"root\">"), "{body}");
        assert_eq!(get("/assets/no-existe.js").await.0, StatusCode::NOT_FOUND);
    }
}

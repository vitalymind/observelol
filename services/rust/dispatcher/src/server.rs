use axum::Router;
#[cfg(unix)]
use tracing::info;

#[cfg(unix)]
pub async fn serve_unix_socket(app: Router, socket_path: &str) {
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tokio::net::UnixListener;

    let path = Path::new(socket_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("Failed to create Unix socket parent directory");
    }
    if path.exists() {
        std::fs::remove_file(path).expect("Failed to remove stale Unix socket file");
    }

    let listener = UnixListener::bind(path).expect("Failed to bind Unix Domain Socket");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))
        .expect("Failed to set 0666 permissions on Unix Domain Socket");

    info!(socket_path, "Dispatcher listening on Unix Domain Socket");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Dispatcher server terminated with error");
}

#[cfg(not(unix))]
pub async fn serve_unix_socket(_app: Router, _socket_path: &str) {
    panic!("Dispatcher Unix Domain Socket listener requires a Unix/Linux host container");
}

#[cfg(unix)]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutdown signal received");
}

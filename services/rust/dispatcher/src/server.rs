use std::{fs::File, io::BufReader, sync::Arc};

use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as AutoBuilder,
    service::TowerToHyperService,
};
use rustls::ServerConfig;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{info, warn};

pub async fn serve_tls(app: Router, bind_addr: &str, cert_path: &str, key_path: &str) {
    let tls_acceptor = build_tls_acceptor(cert_path, key_path);

    let listener = TcpListener::bind(bind_addr)
        .await
        .expect("Failed to bind TCP listener");

    info!(bind_addr, "Dispatcher listening for HTTPS traffic");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received, stopping HTTPS listener");
                break;
            }
            accept_res = listener.accept() => {
                let (tcp_stream, _) = match accept_res {
                    Ok(conn) => conn,
                    Err(err) => {
                        warn!(error = %err, "Failed to accept TCP connection");
                        continue;
                    }
                };

                let _ = tcp_stream.set_nodelay(true);
                let tls_acceptor = tls_acceptor.clone();
                let app = app.clone();

                tokio::spawn(async move {
                    let tls_stream = match tls_acceptor.accept(tcp_stream).await {
                        Ok(stream) => stream,
                        Err(err) => {
                            warn!(error = %err, "TLS handshake failed");
                            return;
                        }
                    };

                    let io = TokioIo::new(tls_stream);
                    let service = TowerToHyperService::new(app);

                    if let Err(err) = AutoBuilder::new(TokioExecutor::new())
                        .serve_connection(io, service)
                        .await
                    {
                        warn!(error = %err, "HTTPS connection terminated with error");
                    }
                });
            }
        }
    }
}

fn build_tls_acceptor(cert_path: &str, key_path: &str) -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cert_file = File::open(cert_path).expect("Failed to open TLS certificate file");
    let mut cert_reader = BufReader::new(cert_file);
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse TLS certificate chain");

    let key_file = File::open(key_path).expect("Failed to open TLS private key file");
    let mut key_reader = BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .expect("Failed to read TLS private key")
        .expect("No private key found in TLS key file");

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("Invalid TLS certificate or private key");

    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    TlsAcceptor::from(Arc::new(config))
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    if let Ok(mut sig) = signal(SignalKind::terminate()) {
        sig.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
    info!("SIGTERM shutdown signal received");
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

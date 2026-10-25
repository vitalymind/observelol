mod config;
mod handlers;
mod redis;
mod server;
mod telemetry;

use std::sync::Arc;

use dashmap::DashMap;

use crate::{
    config::DispatcherConfig,
    handlers::{AppState, build_router, spawn_reaper},
    redis::RedisStorage,
    server::serve_unix_socket,
    telemetry::{DispatcherMetrics, init_telemetry},
};

#[tokio::main]
async fn main() {
    let config = DispatcherConfig::from_env();
    let (tracer_provider, meter_provider) = init_telemetry(&config.otel_endpoint);

    let storage = RedisStorage::connect(&config.redis_url)
        .await
        .expect("Failed to initialize Redis storage");

    let state = AppState {
        storage,
        config: config.clone(),
        metrics: Arc::new(DispatcherMetrics::new()),
        activity: Arc::new(DashMap::with_capacity(16_384)),
    };

    spawn_reaper(state.clone());

    let app = build_router(state);
    serve_unix_socket(app, &config.socket_path).await;

    let _ = tracer_provider.shutdown();
    let _ = meter_provider.shutdown();
}

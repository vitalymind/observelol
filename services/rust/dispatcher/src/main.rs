mod config;
mod handlers;
mod redis;
mod server;
mod telemetry;

use std::{sync::Arc, time::Instant};

use dashmap::DashMap;

use crate::{
    config::DispatcherConfig,
    handlers::{AppState, FastUuidBuildHasher, build_router, new_http_client, spawn_reaper},
    redis::RedisStorage,
    server::serve_tls,
    telemetry::{DispatcherMetrics, init_telemetry},
};

#[tokio::main]
async fn main() {
    let config = DispatcherConfig::from_env();
    let (tracer_provider, meter_provider) = init_telemetry(&config.otel_endpoint);

    let storage = RedisStorage::connect(&config.redis_url, config.redis_event_ttl_sec * 2)
        .await
        .expect("Failed to initialize Redis storage");

    let state = AppState {
        storage,
        config: config.clone(),
        metrics: Arc::new(DispatcherMetrics::new()),
        activity: Arc::new(DashMap::with_capacity_and_hasher(
            16_384,
            FastUuidBuildHasher::default(),
        )),
        epoch: Instant::now(),
        http_client: new_http_client(),
    };

    spawn_reaper(state.clone());

    let app = build_router(state);
    serve_tls(
        app,
        &config.bind_addr,
        &config.tls_cert_path,
        &config.tls_key_path,
    )
    .await;

    let _ = tracer_provider.shutdown();
    let _ = meter_provider.shutdown();
}

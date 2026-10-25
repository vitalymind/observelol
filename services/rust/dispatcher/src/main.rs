mod config;
mod handlers;
mod redis;
mod server;
mod telemetry;

use std::sync::Arc;

use ::redis::{Script, aio::ConnectionManager};

use crate::{
    config::DispatcherConfig,
    handlers::{AppState, build_router},
    redis::{INGEST_EVENT_LUA, ensure_consumer_group},
    server::serve_unix_socket,
    telemetry::{DispatcherMetrics, init_telemetry},
};

#[tokio::main]
async fn main() {
    let config = DispatcherConfig::from_env();
    let (tracer_provider, meter_provider) = init_telemetry(&config.otel_endpoint);

    let client =
        ::redis::Client::open(config.redis_url.as_str()).expect("Invalid REDIS_URL provided");
    let mut redis = ConnectionManager::new(client)
        .await
        .expect("Failed to establish multiplexed connection to Redis");

    ensure_consumer_group(&mut redis).await;

    let state = AppState {
        redis,
        ingest_script: Arc::new(Script::new(INGEST_EVENT_LUA)),
        config: config.clone(),
        metrics: Arc::new(DispatcherMetrics::new()),
    };

    let app = build_router(state);
    serve_unix_socket(app, &config.socket_path).await;

    let _ = tracer_provider.shutdown();
    let _ = meter_provider.shutdown();
}

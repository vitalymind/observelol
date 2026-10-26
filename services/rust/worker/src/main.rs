mod config;
mod database;
mod process;
mod redis;
mod replay;
mod telemetry;

use std::sync::Arc;

use tracing::info;

use crate::{
    config::WorkerConfig,
    database::WorkerDatabase,
    process::{AppState, run_worker},
    redis::RedisStreamClient,
    telemetry::{WorkerMetrics, init_telemetry},
};

#[tokio::main]
async fn main() {
    let config = WorkerConfig::from_env();
    let (tracer_provider, meter_provider) = init_telemetry(&config.otel_endpoint);

    info!(
        consumer = %config.consumer_name,
        db_mode = ?config.db_mode,
        "Starting ObserveLoL Worker"
    );

    let redis = RedisStreamClient::connect_with_retry(&config.redis_url).await;
    let db = WorkerDatabase::connect(&config).await;

    let state = AppState {
        redis,
        db,
        config,
        metrics: Arc::new(WorkerMetrics::new()),
    };

    run_worker(state).await;

    let _ = tracer_provider.shutdown();
    let _ = meter_provider.shutdown();
}

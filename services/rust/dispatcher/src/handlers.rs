use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use common::{
    ACTIVE_SNAPSHOT_KEY, EventType, IncomingEvent, NewGameResponse, READY_GAMES_STREAM,
    StoredEvent, game_events_key,
};
use opentelemetry::KeyValue;
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use tracing::{error, warn};
use uuid::Uuid;

use crate::{config::DispatcherConfig, telemetry::DispatcherMetrics};

#[derive(Clone)]
pub struct AppState {
    pub redis: ConnectionManager,
    pub ingest_script: Arc<Script>,
    pub config: DispatcherConfig,
    pub metrics: Arc<DispatcherMetrics>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/new-game/", get(handle_new_game))
        .route("/api/event/", post(handle_event))
        .route("/health", get(handle_health))
        .with_state(state)
}

async fn handle_new_game(State(state): State<AppState>) -> impl IntoResponse {
    let game_id = Uuid::now_v7();
    let mut conn = state.redis.clone();

    let start = Instant::now();
    let raw_snapshot: redis::RedisResult<Option<String>> = conn.get(ACTIVE_SNAPSHOT_KEY).await;
    state.metrics.redis_latency_ms.record(
        start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "get_snapshot")],
    );

    let snapshot = match raw_snapshot {
        Ok(Some(json_str)) => match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(val) => val,
            Err(err) => {
                error!(error = %err, "Corrupted snapshot JSON in Redis");
                state
                    .metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "parse_snapshot")]);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        },
        Ok(None) => serde_json::json!({ "heroes": [], "items": [] }),
        Err(err) => {
            error!(error = %err, "Failed to read snapshot from Redis");
            state
                .metrics
                .errors_total
                .add(1, &[KeyValue::new("stage", "read_snapshot")]);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    state.metrics.new_games_total.add(1, &[]);
    (StatusCode::OK, Json(NewGameResponse { game_id, snapshot })).into_response()
}

async fn handle_event(
    State(state): State<AppState>,
    Json(incoming): Json<IncomingEvent>,
) -> impl IntoResponse {
    let game_id = incoming.game_id;
    let event_type = incoming.event_type;
    let stored = StoredEvent::from(incoming);

    let serialized = match serde_json::to_string(&stored) {
        Ok(s) => s,
        Err(err) => {
            error!(error = %err, %game_id, "Failed to serialize StoredEvent");
            state
                .metrics
                .errors_total
                .add(1, &[KeyValue::new("stage", "serialize_event")]);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let key = game_events_key(&game_id);
    let mut conn = state.redis.clone();

    let start = Instant::now();
    let res: redis::RedisResult<i64> = state
        .ingest_script
        .key(&key)
        .arg(&serialized)
        .arg(state.config.redis_event_ttl_sec)
        .invoke_async(&mut conn)
        .await;
    state.metrics.redis_latency_ms.record(
        start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "ingest_lua")],
    );

    if let Err(err) = res {
        error!(error = %err, %game_id, "Failed to push event to Redis");
        state
            .metrics
            .errors_total
            .add(1, &[KeyValue::new("stage", "redis_rpush")]);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    state.metrics.events_total.add(
        1,
        &[KeyValue::new("event_type", (event_type as u8).to_string())],
    );

    if event_type == EventType::GameEnds {
        let mut bg_redis = state.redis.clone();
        let grace_ms = state.config.game_end_grace_ms;
        let metrics = state.metrics.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(grace_ms)).await;
            let xadd_res: redis::RedisResult<String> = redis::cmd("XADD")
                .arg(READY_GAMES_STREAM)
                .arg("*")
                .arg("game_id")
                .arg(game_id.to_string())
                .query_async(&mut bg_redis)
                .await;

            if let Err(err) = xadd_res {
                warn!(error = %err, %game_id, "Failed to XADD finished game to ready_games stream");
                metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "redis_xadd")]);
            }
        });
    }

    StatusCode::OK
}

async fn handle_health(State(state): State<AppState>) -> impl IntoResponse {
    let mut conn = state.redis.clone();
    let res: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
    match res {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            error!(error = %err, "Healthcheck Redis PING failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

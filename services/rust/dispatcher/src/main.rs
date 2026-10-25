use std::{sync::Arc, time::Duration, time::Instant};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use common::{
    ACTIVE_SNAPSHOT_KEY, EventType, IncomingEvent, NewGameResponse, READY_GAMES_STREAM,
    StoredEvent, WORKERS_CONSUMER_GROUP, game_events_key,
};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

/// Lua script for atomic event ingestion:
/// Appends serialized event to `game:{id}:events` and sets safety TTL only on the first event.
const INGEST_EVENT_LUA: &str = r#"
local len = redis.call('RPUSH', KEYS[1], ARGV[1])
if len == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return len
"#;

#[derive(Clone)]
struct DispatcherConfig {
    redis_url: String,
    socket_path: String,
    otel_endpoint: String,
    redis_event_ttl_sec: u64,
    game_end_grace_ms: u64,
}

impl DispatcherConfig {
    fn from_env() -> Self {
        let redis_url =
            std::env::var("REDIS_URL").expect("Missing required environment variable: REDIS_URL");
        let socket_path = std::env::var("DISPATCHER_SOCKET_PATH")
            .expect("Missing required environment variable: DISPATCHER_SOCKET_PATH");
        let otel_endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .expect("Missing required environment variable: OTEL_EXPORTER_OTLP_ENDPOINT");
        let redis_event_ttl_sec = std::env::var("REDIS_EVENT_TTL_SEC")
            .expect("Missing required environment variable: REDIS_EVENT_TTL_SEC")
            .parse::<u64>()
            .expect("REDIS_EVENT_TTL_SEC must be a valid u64 integer");
        let game_end_grace_ms = std::env::var("GAME_END_GRACE_MS")
            .expect("Missing required environment variable: GAME_END_GRACE_MS")
            .parse::<u64>()
            .expect("GAME_END_GRACE_MS must be a valid u64 integer");

        Self {
            redis_url,
            socket_path,
            otel_endpoint,
            redis_event_ttl_sec,
            game_end_grace_ms,
        }
    }
}

struct DispatcherMetrics {
    events_total: Counter<u64>,
    new_games_total: Counter<u64>,
    errors_total: Counter<u64>,
    redis_latency_ms: Histogram<f64>,
}

impl DispatcherMetrics {
    fn new() -> Self {
        let meter = global::meter("observelol.dispatcher");
        Self {
            events_total: meter
                .u64_counter("ingest.events_total")
                .with_description("Total number of game events ingested")
                .build(),
            new_games_total: meter
                .u64_counter("ingest.new_games_total")
                .with_description("Total number of new games initialized")
                .build(),
            errors_total: meter
                .u64_counter("ingest.errors_total")
                .with_description("Total number of ingestion errors")
                .build(),
            redis_latency_ms: meter
                .f64_histogram("ingest.redis_latency_ms")
                .with_description("Latency of Redis operations in milliseconds")
                .build(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    redis: ConnectionManager,
    ingest_script: Arc<Script>,
    config: DispatcherConfig,
    metrics: Arc<DispatcherMetrics>,
}

fn init_telemetry(otel_endpoint: &str) -> (SdkTracerProvider, SdkMeterProvider) {
    let resource = Resource::builder()
        .with_service_name("observelol-dispatcher")
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("Failed to initialize OTLP gRPC span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("Failed to initialize OTLP gRPC metric exporter");

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("observelol-dispatcher");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .with(otel_layer)
        .init();

    (tracer_provider, meter_provider)
}

async fn ensure_consumer_group(redis: &mut ConnectionManager) {
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(READY_GAMES_STREAM)
        .arg(WORKERS_CONSUMER_GROUP)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(redis)
        .await;

    match res {
        Ok(()) => info!(
            stream = READY_GAMES_STREAM,
            group = WORKERS_CONSUMER_GROUP,
            "Created Redis consumer group"
        ),
        Err(err) if err.to_string().contains("BUSYGROUP") => {
            info!(
                stream = READY_GAMES_STREAM,
                group = WORKERS_CONSUMER_GROUP,
                "Redis consumer group already exists"
            );
        }
        Err(err) => {
            panic!("Failed to create Redis consumer group '{WORKERS_CONSUMER_GROUP}': {err}");
        }
    }
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

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/new-game/", get(handle_new_game))
        .route("/api/event/", post(handle_event))
        .route("/health", get(handle_health))
        .with_state(state)
}

#[cfg(unix)]
async fn serve_unix_socket(app: Router, socket_path: &str) {
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
async fn serve_unix_socket(_app: Router, _socket_path: &str) {
    panic!("Dispatcher Unix Domain Socket listener requires a Unix/Linux host container");
}

#[cfg(unix)]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutdown signal received");
}

#[tokio::main]
async fn main() {
    let config = DispatcherConfig::from_env();
    let (tracer_provider, meter_provider) = init_telemetry(&config.otel_endpoint);

    let client =
        redis::Client::open(config.redis_url.as_str()).expect("Invalid REDIS_URL provided");
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

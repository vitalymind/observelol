use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, Uri, Version, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::{EventType, IncomingEvent, NewGameResponse};
use dashmap::DashMap;
use opentelemetry::KeyValue;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    config::{DispatcherConfig, EVENT_BUFFER_CAPACITY},
    redis::Storage,
    telemetry::DispatcherMetrics,
};

pub type HttpClient =
    hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, Body>;

pub fn new_http_client() -> HttpClient {
    hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build_http()
}

#[derive(Clone)]
pub struct AppState<S: Storage> {
    pub storage: S,
    pub config: DispatcherConfig,
    pub metrics: Arc<DispatcherMetrics>,
    pub activity: Arc<DashMap<Uuid, Instant>>,
    pub http_client: HttpClient,
}

pub fn build_router<S: Storage>(state: AppState<S>) -> Router {
    Router::new()
        .route("/api/new-game/", get(handle_new_game::<S>))
        .route(
            "/api/event/",
            post(handle_event::<S>).layer(DefaultBodyLimit::max(4096)),
        )
        .route("/health", get(handle_health::<S>))
        .fallback(handle_nginx_proxy::<S>)
        .with_state(state)
}

pub fn spawn_reaper<S: Storage>(state: AppState<S>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let check_interval = Duration::from_secs(5);
        let mut interval = tokio::time::interval(check_interval);

        loop {
            interval.tick().await;

            let now = Instant::now();
            let ttl = Duration::from_secs(state.config.redis_event_ttl_sec);
            let mut expired_games = Vec::new();

            for entry in state.activity.iter() {
                if now.duration_since(*entry.value()) >= ttl {
                    expired_games.push(*entry.key());
                }
            }

            for game_id in expired_games {
                if let Some((_, last_seen)) = state.activity.remove(&game_id) {
                    if now.duration_since(last_seen) >= ttl {
                        warn!(%game_id, "Reaping abandoned game after inactivity TTL");
                        if let Err(err) = state.storage.unlink_game(&game_id).await {
                            warn!(error = %err, %game_id, "Failed to unlink abandoned game from Redis");
                        }
                        state
                            .metrics
                            .errors_total
                            .add(1, &[KeyValue::new("stage", "game_inactivity_reaped")]);
                    } else {
                        state.activity.insert(game_id, last_seen);
                    }
                }
            }
        }
    })
}

async fn handle_new_game<S: Storage>(State(state): State<AppState<S>>) -> impl IntoResponse {
    let game_id = Uuid::now_v7();

    let start = Instant::now();
    let raw_snapshot = state.storage.get_snapshot().await;
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

async fn handle_event<S: Storage>(
    State(state): State<AppState<S>>,
    Json(incoming): Json<IncomingEvent>,
) -> impl IntoResponse {
    let game_id = incoming.game_id;
    let event_type = incoming.event_type;

    let mut buf = [0u8; EVENT_BUFFER_CAPACITY];
    let encoded = match postcard::to_slice(&incoming.stored, &mut buf) {
        Ok(bytes) => bytes,
        Err(err) => {
            error!(error = %err, %game_id, "Failed to serialize StoredEvent with postcard");
            state
                .metrics
                .errors_total
                .add(1, &[KeyValue::new("stage", "serialize_event")]);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    let start = Instant::now();
    let res = state.storage.ingest_event(&game_id, encoded).await;
    let now = Instant::now();
    state.metrics.redis_latency_ms.record(
        now.duration_since(start).as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "rpush")],
    );

    if let Err(err) = res {
        error!(error = %err, %game_id, "Failed to push event to storage");
        state
            .metrics
            .errors_total
            .add(1, &[KeyValue::new("stage", "redis_rpush")]);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    state.activity.insert(game_id, now);

    state
        .metrics
        .events_total
        .add(1, &[KeyValue::new("event_type", event_type.as_str())]);

    if event_type == EventType::GameEnds {
        let storage = state.storage.clone();
        let grace_ms = state.config.game_end_grace_ms;
        let metrics = state.metrics.clone();
        let activity = state.activity.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(grace_ms)).await;
            activity.remove(&game_id);
            if let Err(err) = storage.notify_game_ended(&game_id).await {
                warn!(error = %err, %game_id, "Failed to notify game ended");
                metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "redis_xadd")]);
            }
        });
    }

    StatusCode::OK
}

async fn handle_health<S: Storage>(State(state): State<AppState<S>>) -> impl IntoResponse {
    match state.storage.ping().await {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            error!(error = %err, "Healthcheck storage PING failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn handle_nginx_proxy<S: Storage>(
    State(state): State<AppState<S>>,
    mut req: Request,
) -> Response {
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    let target_uri = format!(
        "{}{}",
        state.config.nginx_upstream_url.trim_end_matches('/'),
        path_and_query
    );

    let uri: Uri = match target_uri.parse() {
        Ok(u) => u,
        Err(err) => {
            error!(error = %err, %target_uri, "Invalid upstream URI for nginx proxy");
            return StatusCode::BAD_GATEWAY.into_response();
        }
    };

    let host_header = if !req.headers().contains_key(header::HOST) {
        req.uri()
            .authority()
            .and_then(|auth| header::HeaderValue::from_str(auth.as_str()).ok())
    } else {
        None
    };

    if let Some(host_val) = host_header {
        req.headers_mut().insert(header::HOST, host_val);
    }

    *req.uri_mut() = uri;
    *req.version_mut() = Version::HTTP_11;

    match state.http_client.request(req).await {
        Ok(res) => res.map(Body::new),
        Err(err) => {
            warn!(error = %err, "Failed to proxy request to nginx upstream");
            StatusCode::BAD_GATEWAY.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use common::StoredEvent;
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    type IngestedEvent = (Uuid, Vec<u8>);

    #[derive(Clone)]
    struct MockStorage {
        snapshot: Arc<Mutex<Result<Option<String>, String>>>,
        ingested: Arc<Mutex<Vec<IngestedEvent>>>,
        game_ended: Arc<Mutex<Vec<Uuid>>>,
        unlinked: Arc<Mutex<Vec<Uuid>>>,
        should_fail_ping: Arc<Mutex<bool>>,
    }

    impl Default for MockStorage {
        fn default() -> Self {
            Self {
                snapshot: Arc::new(Mutex::new(Ok(None))),
                ingested: Arc::new(Mutex::new(Vec::new())),
                game_ended: Arc::new(Mutex::new(Vec::new())),
                unlinked: Arc::new(Mutex::new(Vec::new())),
                should_fail_ping: Arc::new(Mutex::new(false)),
            }
        }
    }

    impl Storage for MockStorage {
        async fn get_snapshot(&self) -> Result<Option<String>, String> {
            self.snapshot.lock().unwrap().clone()
        }

        async fn ingest_event(&self, game_id: &Uuid, bytes: &[u8]) -> Result<i64, String> {
            let mut list = self.ingested.lock().unwrap();
            list.push((*game_id, bytes.to_vec()));
            Ok(list.len() as i64)
        }

        async fn notify_game_ended(&self, game_id: &Uuid) -> Result<(), String> {
            self.game_ended.lock().unwrap().push(*game_id);
            Ok(())
        }

        async fn unlink_game(&self, game_id: &Uuid) -> Result<(), String> {
            self.unlinked.lock().unwrap().push(*game_id);
            Ok(())
        }

        async fn ping(&self) -> Result<(), String> {
            if *self.should_fail_ping.lock().unwrap() {
                Err("Storage down".into())
            } else {
                Ok(())
            }
        }
    }

    fn test_state(storage: MockStorage) -> AppState<MockStorage> {
        AppState {
            storage,
            config: DispatcherConfig {
                redis_url: "mock://".into(),
                bind_addr: "127.0.0.1:0".into(),
                tls_cert_path: "/tmp/cert.pem".into(),
                tls_key_path: "/tmp/key.pem".into(),
                nginx_upstream_url: "http://127.0.0.1:80".into(),
                otel_endpoint: "http://mock".into(),
                redis_event_ttl_sec: 1, // 1 second for fast test
                game_end_grace_ms: 10,
            },
            metrics: Arc::new(DispatcherMetrics::new()),
            activity: Arc::new(DashMap::new()),
            http_client: new_http_client(),
        }
    }

    #[tokio::test]
    async fn test_new_game_success() {
        let storage = MockStorage::default();
        let app = build_router(test_state(storage));

        let req = Request::builder()
            .uri("/api/new-game/")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body_bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert!(body.get("game_id").is_some());
        assert_eq!(body["snapshot"], json!({ "heroes": [], "items": [] }));
    }

    #[tokio::test]
    async fn test_new_game_snapshot_error() {
        let storage = MockStorage::default();
        *storage.snapshot.lock().unwrap() = Err("Redis error".into());
        let app = build_router(test_state(storage));

        let req = Request::builder()
            .uri("/api/new-game/")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_event_valid_hero_selected() {
        let storage = MockStorage::default();
        let state = test_state(storage.clone());
        let app = build_router(state.clone());
        let game_id = Uuid::now_v7();

        let payload = json!({
            "game_id": game_id,
            "timestamp_ms": 100,
            "event_index": 1,
            "event_type": 2, // HeroSelected
            "payload": {
                "slot": 0,
                "hero_id": 10,
                "version_id": 1
            }
        });

        let req = Request::builder()
            .uri("/api/event/")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let ingested = storage.ingested.lock().unwrap();
        assert_eq!(ingested.len(), 1);
        assert_eq!(ingested[0].0, game_id);

        let decoded: StoredEvent = postcard::from_bytes(&ingested[0].1).unwrap();
        assert_eq!(
            decoded,
            StoredEvent::HeroSelected {
                timestamp_ms: 100,
                event_index: 1,
                slot: 0,
                hero_id: 10,
                version_id: 1,
            }
        );

        // Verify game is registered in the activity tracker
        assert!(state.activity.contains_key(&game_id));
    }

    #[tokio::test]
    async fn test_event_unknown_fields_rejected() {
        let storage = MockStorage::default();
        let app = build_router(test_state(storage.clone()));

        let payload = json!({
            "game_id": Uuid::now_v7(),
            "timestamp_ms": 100,
            "event_index": 1,
            "event_type": 2,
            "payload": {
                "slot": 0,
                "hero_id": 10,
                "version_id": 1,
                "unknown_malicious_key": "drop this"
            }
        });

        let req = Request::builder()
            .uri("/api/event/")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(storage.ingested.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_event_mismatched_type_and_payload() {
        let storage = MockStorage::default();
        let app = build_router(test_state(storage.clone()));

        let payload = json!({
            "game_id": Uuid::now_v7(),
            "timestamp_ms": 100,
            "event_index": 1,
            "event_type": 2,
            "payload": {
                "killer_slot": 1,
                "victim_slot": 2
            }
        });

        let req = Request::builder()
            .uri("/api/event/")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(storage.ingested.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_event_game_ends_triggers_notification_and_cleans_tracker() {
        let storage = MockStorage::default();
        let state = test_state(storage.clone());
        let app = build_router(state.clone());
        let game_id = Uuid::now_v7();

        state.activity.insert(game_id, Instant::now());

        let payload = json!({
            "game_id": game_id,
            "timestamp_ms": 5000,
            "event_index": 50,
            "event_type": 7, // GameEnds
            "payload": {}
        });

        let req = Request::builder()
            .uri("/api/event/")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        tokio::time::sleep(Duration::from_millis(30)).await;

        // Activity tracker should be cleared after grace period finishes
        assert!(!state.activity.contains_key(&game_id));

        let ended = storage.game_ended.lock().unwrap();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0], game_id);
    }

    #[tokio::test]
    async fn test_inactivity_reaper_cleans_dead_games() {
        let storage = MockStorage::default();
        let state = test_state(storage.clone());
        let game_id = Uuid::now_v7();

        // Seed with a timestamp in the past (> 1 sec TTL)
        state
            .activity
            .insert(game_id, Instant::now() - Duration::from_secs(5));

        let reaper_handle = spawn_reaper(state.clone());

        // Wait for reaper interval (runs every 5s, let's wait 6s)
        tokio::time::sleep(Duration::from_millis(6000)).await;

        reaper_handle.abort();

        // Verify the game was unlinked and removed from tracker
        let unlinked = storage.unlinked.lock().unwrap();
        assert_eq!(unlinked.len(), 1);
        assert_eq!(unlinked[0], game_id);
        assert!(!state.activity.contains_key(&game_id));
    }

    #[tokio::test]
    async fn test_health_check() {
        let storage = MockStorage::default();
        let app = build_router(test_state(storage.clone()));

        let req = Request::builder()
            .uri("/health")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        *storage.should_fail_ping.lock().unwrap() = true;

        let req2 = Request::builder()
            .uri("/health")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let res2 = build_router(test_state(storage))
            .oneshot(req2)
            .await
            .unwrap();
        assert_eq!(res2.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_fallback_proxies_to_nginx() {
        let mock_nginx = Router::new().fallback(|| async { (StatusCode::OK, "ok\n") });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let nginx_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, mock_nginx).await;
        });

        let mut state = test_state(MockStorage::default());
        state.config.nginx_upstream_url = format!("http://{addr}");
        let app = build_router(state);

        let req = Request::builder()
            .uri("/admin/dashboard")
            .method("GET")
            .body(Body::empty())
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body_bytes = res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body_bytes[..], b"ok\n");

        nginx_handle.abort();
    }
}

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use common::{EventType, IncomingEvent, NewGameResponse, StoredEvent};
use opentelemetry::KeyValue;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{config::DispatcherConfig, redis::Storage, telemetry::DispatcherMetrics};

#[derive(Clone)]
pub struct AppState<S: Storage> {
    pub storage: S,
    pub config: DispatcherConfig,
    pub metrics: Arc<DispatcherMetrics>,
}

pub fn build_router<S: Storage>(state: AppState<S>) -> Router {
    Router::new()
        .route("/api/new-game/", get(handle_new_game::<S>))
        .route(
            "/api/event/",
            post(handle_event::<S>).layer(DefaultBodyLimit::max(4096)),
        )
        .route("/health", get(handle_health::<S>))
        .with_state(state)
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
    let stored = match StoredEvent::try_from(incoming) {
        Ok(s) => s,
        Err(_) => {
            state
                .metrics
                .errors_total
                .add(1, &[KeyValue::new("stage", "invalid_payload")]);
            return StatusCode::BAD_REQUEST;
        }
    };

    let encoded = match postcard::to_allocvec(&stored) {
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
    let res = state
        .storage
        .ingest_event(&game_id, &encoded, state.config.redis_event_ttl_sec)
        .await;
    state.metrics.redis_latency_ms.record(
        start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "ingest_lua")],
    );

    if let Err(err) = res {
        error!(error = %err, %game_id, "Failed to push event to storage");
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
        let storage = state.storage.clone();
        let grace_ms = state.config.game_end_grace_ms;
        let metrics = state.metrics.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(grace_ms)).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    #[derive(Clone)]
    struct MockStorage {
        snapshot: Arc<Mutex<Result<Option<String>, String>>>,
        ingested: Arc<Mutex<Vec<(Uuid, Vec<u8>)>>>,
        game_ended: Arc<Mutex<Vec<Uuid>>>,
        should_fail_ping: Arc<Mutex<bool>>,
    }

    impl Default for MockStorage {
        fn default() -> Self {
            Self {
                snapshot: Arc::new(Mutex::new(Ok(None))),
                ingested: Arc::new(Mutex::new(Vec::new())),
                game_ended: Arc::new(Mutex::new(Vec::new())),
                should_fail_ping: Arc::new(Mutex::new(false)),
            }
        }
    }

    impl Storage for MockStorage {
        async fn get_snapshot(&self) -> Result<Option<String>, String> {
            self.snapshot.lock().unwrap().clone()
        }

        async fn ingest_event(
            &self,
            game_id: &Uuid,
            bytes: &[u8],
            _ttl_sec: u64,
        ) -> Result<i64, String> {
            let mut list = self.ingested.lock().unwrap();
            list.push((*game_id, bytes.to_vec()));
            Ok(list.len() as i64)
        }

        async fn notify_game_ended(&self, game_id: &Uuid) -> Result<(), String> {
            self.game_ended.lock().unwrap().push(*game_id);
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
                socket_path: "/tmp/mock.sock".into(),
                otel_endpoint: "http://mock".into(),
                redis_event_ttl_sec: 3600,
                game_end_grace_ms: 10,
            },
            metrics: Arc::new(DispatcherMetrics::new()),
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
        let app = build_router(test_state(storage.clone()));
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

        // Claimed event_type is HeroSelected (2), but payload is Kill
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
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        assert!(storage.ingested.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_event_game_ends_triggers_notification() {
        let storage = MockStorage::default();
        let app = build_router(test_state(storage.clone()));
        let game_id = Uuid::now_v7();

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

        // Wait for background grace period task (configured to 10ms in test_state)
        tokio::time::sleep(Duration::from_millis(30)).await;

        let ended = storage.game_ended.lock().unwrap();
        assert_eq!(ended.len(), 1);
        assert_eq!(ended[0], game_id);
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

        // Simulate storage failure
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
}

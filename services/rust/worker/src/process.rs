use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use opentelemetry::KeyValue;
use tracing::{error, info, warn};

use crate::{
    config::{DbMode, WorkerConfig},
    database::{Database, REPLAY_FORMAT_VERSION},
    redis::{RedisQueue, StreamEntry},
    replay::{ValidationError, validate_and_compile_replay},
    telemetry::WorkerMetrics,
};

#[derive(Clone)]
pub struct AppState<R: RedisQueue, D: Database> {
    pub redis: R,
    pub db: D,
    pub config: WorkerConfig,
    pub metrics: Arc<WorkerMetrics>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutcome {
    SavedReplay,
    RecordedError(ValidationError),
    SkippedEmpty,
}

pub async fn run_worker<R: RedisQueue, D: Database>(state: AppState<R, D>) {
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let autoclaim_interval = Duration::from_secs(15);
    let mut last_autoclaim = Instant::now()
        .checked_sub(autoclaim_interval)
        .unwrap_or_else(Instant::now);
    let mut drain_own_pending = true;

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("Shutdown signal received; stopping worker loop");
                break;
            }
            step_res = worker_step(
                &state,
                &mut last_autoclaim,
                autoclaim_interval,
                &mut drain_own_pending,
            ) => {
                if let Err(err) = step_res {
                    warn!(error = %err, "Worker step encountered an error; backing off for 2s");
                    drain_own_pending = true;
                    tokio::select! {
                        _ = &mut shutdown => {
                            info!("Shutdown signal received during backoff; exiting");
                            break;
                        }
                        _ = wait_until_healthy(&state) => {}
                    }
                }
            }
        }
    }
}

async fn worker_step<R: RedisQueue, D: Database>(
    state: &AppState<R, D>,
    last_autoclaim: &mut Instant,
    autoclaim_interval: Duration,
    drain_own_pending: &mut bool,
) -> Result<(), String> {
    // 1. On startup or after recovering from a transient DB error, immediately drain any
    // unacknowledged PEL entries already owned by this consumer before blocking on '>'
    if *drain_own_pending {
        let pending = state
            .redis
            .read_pending(&state.config.consumer_name, state.config.batch_size)
            .await
            .inspect_err(|_| {
                state
                    .metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "redis_xreadgroup_pending")]);
            })?;

        if pending.is_empty() {
            *drain_own_pending = false;
        } else {
            info!(
                count = pending.len(),
                "Resuming own pending stream entries from PEL"
            );
            process_batch(state, &pending).await?;
            return Ok(());
        }
    }

    // 2. Periodically reclaim abandoned messages from idle/crashed consumers
    if last_autoclaim.elapsed() >= autoclaim_interval {
        *last_autoclaim = Instant::now();
        let autoclaim_start = Instant::now();
        match state
            .redis
            .autoclaim(
                &state.config.consumer_name,
                state.config.autoclaim_threshold_ms,
                state.config.batch_size,
            )
            .await
        {
            Ok(claimed) => {
                state.metrics.redis_latency_ms.record(
                    autoclaim_start.elapsed().as_secs_f64() * 1000.0,
                    &[KeyValue::new("op", "xautoclaim")],
                );
                if !claimed.is_empty() {
                    info!(
                        count = claimed.len(),
                        "Reclaimed abandoned games via XAUTOCLAIM"
                    );
                    process_batch(state, &claimed).await?;
                }
            }
            Err(err) => {
                state
                    .metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "redis_xautoclaim")]);
                return Err(err);
            }
        }
    }

    // 3. Block waiting for new ready_games entries in the consumer group
    let read_start = Instant::now();
    let entries = state
        .redis
        .read_group(
            &state.config.consumer_name,
            state.config.batch_size,
            state.config.block_ms,
        )
        .await
        .inspect_err(|_| {
            state
                .metrics
                .errors_total
                .add(1, &[KeyValue::new("stage", "redis_xreadgroup")]);
        })?;

    state.metrics.redis_latency_ms.record(
        read_start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "xreadgroup")],
    );

    if !entries.is_empty() {
        process_batch(state, &entries).await?;
    }

    Ok(())
}

async fn process_batch<R: RedisQueue, D: Database>(
    state: &AppState<R, D>,
    entries: &[StreamEntry],
) -> Result<(), String> {
    for entry in entries {
        process_entry(state, entry).await?;
    }
    Ok(())
}

async fn process_entry<R: RedisQueue, D: Database>(
    state: &AppState<R, D>,
    entry: &StreamEntry,
) -> Result<ProcessOutcome, String> {
    let start = Instant::now();

    let Some(game_id) = entry.game_id else {
        warn!(
            stream_id = %entry.stream_id,
            "Stream entry missing valid game_id; acknowledging and deleting"
        );
        state.redis.ack_and_delete(&entry.stream_id).await?;
        state
            .metrics
            .dropped_games_total
            .add(1, &[KeyValue::new("reason", "invalid_game_id")]);
        return Ok(ProcessOutcome::SkippedEmpty);
    };

    let lrange_start = Instant::now();
    let raw_events = state.redis.fetch_game_events(&game_id).await?;
    state.metrics.redis_latency_ms.record(
        lrange_start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "lrange")],
    );

    // Empty list means either a duplicate GameEnds stream message or expired Redis TTL
    if raw_events.is_empty() {
        state
            .redis
            .ack_and_cleanup(&entry.stream_id, &game_id)
            .await?;
        state
            .metrics
            .dropped_games_total
            .add(1, &[KeyValue::new("reason", "empty_or_expired")]);
        return Ok(ProcessOutcome::SkippedEmpty);
    }

    let outcome = match validate_and_compile_replay(&raw_events, state.config.compression_level) {
        Ok(compressed_replay) => {
            let db_start = Instant::now();
            let res = state
                .db
                .save_replay(&game_id, REPLAY_FORMAT_VERSION, &compressed_replay)
                .await;
            state.metrics.db_latency_ms.record(
                db_start.elapsed().as_secs_f64() * 1000.0,
                &[KeyValue::new("op", "save_replay")],
            );

            if let Err(err) = res {
                error!(
                    error = %err,
                    %game_id,
                    "Failed to persist replay to database; keeping events in Redis for retry"
                );
                state
                    .metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "db_save_replay")]);
                return Err(err);
            }

            state.metrics.games_processed_total.add(1, &[]);
            if state.config.db_mode == DbMode::Drop {
                state
                    .metrics
                    .dropped_games_total
                    .add(1, &[KeyValue::new("reason", "drop_mode")]);
            }

            ProcessOutcome::SavedReplay
        }
        Err(validation_err) => {
            let err_code = validation_err.code();
            let details = validation_err.details();
            warn!(
                %game_id,
                error_code = %err_code,
                reason = %validation_err,
                "Game failed replay validation"
            );
            state
                .metrics
                .validation_failures_total
                .add(1, &[KeyValue::new("error_code", err_code)]);

            let db_start = Instant::now();
            let res = state
                .db
                .save_error(&game_id, err_code, details.as_ref())
                .await;
            state.metrics.db_latency_ms.record(
                db_start.elapsed().as_secs_f64() * 1000.0,
                &[KeyValue::new("op", "save_error")],
            );

            if let Err(err) = res {
                error!(
                    error = %err,
                    %game_id,
                    "Failed to persist validation error to database; keeping events in Redis for retry"
                );
                state
                    .metrics
                    .errors_total
                    .add(1, &[KeyValue::new("stage", "db_save_error")]);
                return Err(err);
            }

            ProcessOutcome::RecordedError(validation_err)
        }
    };

    // Atomic cleanup in Redis ONLY after DB write (or Stage 1 drop) succeeds
    let cleanup_start = Instant::now();
    state
        .redis
        .ack_and_cleanup(&entry.stream_id, &game_id)
        .await?;
    state.metrics.redis_latency_ms.record(
        cleanup_start.elapsed().as_secs_f64() * 1000.0,
        &[KeyValue::new("op", "ack_and_cleanup")],
    );

    state
        .metrics
        .processing_latency_ms
        .record(start.elapsed().as_secs_f64() * 1000.0, &[]);

    Ok(outcome)
}

/// Pauses consumption from Redis when either Redis or PostgreSQL is unhealthy,
/// re-verifying the Redis consumer group and database connectivity before resuming.
async fn wait_until_healthy<R: RedisQueue, D: Database>(state: &AppState<R, D>) {
    let retry_delay = Duration::from_secs(2);

    loop {
        tokio::time::sleep(retry_delay).await;

        if let Err(err) = state.redis.ensure_group().await {
            warn!(error = %err, "Redis consumer group check failed during recovery; retrying");
            continue;
        }

        if let Err(err) = state.db.ping().await {
            warn!(
                error = %err,
                "Database health check failed; pausing Redis consumption (backpressure active)"
            );
            continue;
        }

        info!("Redis and Database health verified; resuming worker consumption");
        break;
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("Ctrl+C received");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::tests::{build_valid_game_events, encode_events};
    use common::StoredEvent;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct MockRedisQueue {
        events_by_game: Arc<Mutex<HashMap<Uuid, Vec<Vec<u8>>>>>,
        acked_and_cleaned: Arc<Mutex<Vec<(String, Uuid)>>>,
        acked_only: Arc<Mutex<Vec<String>>>,
    }

    impl RedisQueue for MockRedisQueue {
        async fn ensure_group(&self) -> Result<(), String> {
            Ok(())
        }

        async fn read_pending(
            &self,
            _consumer: &str,
            _count: usize,
        ) -> Result<Vec<StreamEntry>, String> {
            Ok(Vec::new())
        }

        async fn read_group(
            &self,
            _consumer: &str,
            _count: usize,
            _block_ms: usize,
        ) -> Result<Vec<StreamEntry>, String> {
            Ok(Vec::new())
        }

        async fn autoclaim(
            &self,
            _consumer: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> Result<Vec<StreamEntry>, String> {
            Ok(Vec::new())
        }

        async fn fetch_game_events(&self, game_id: &Uuid) -> Result<Vec<Vec<u8>>, String> {
            Ok(self
                .events_by_game
                .lock()
                .unwrap()
                .get(game_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn ack_and_cleanup(&self, stream_id: &str, game_id: &Uuid) -> Result<(), String> {
            self.events_by_game.lock().unwrap().remove(game_id);
            self.acked_and_cleaned
                .lock()
                .unwrap()
                .push((stream_id.to_string(), *game_id));
            Ok(())
        }

        async fn ack_and_delete(&self, stream_id: &str) -> Result<(), String> {
            self.acked_only.lock().unwrap().push(stream_id.to_string());
            Ok(())
        }

        async fn ping(&self) -> Result<(), String> {
            Ok(())
        }
    }

    type SavedReplayRow = (Uuid, i32, Vec<u8>);
    type SavedErrorRow = (Uuid, String, Option<serde_json::Value>);

    #[derive(Clone, Default)]
    struct MockDatabase {
        replays: Arc<Mutex<Vec<SavedReplayRow>>>,
        errors: Arc<Mutex<Vec<SavedErrorRow>>>,
        fail_writes: Arc<Mutex<bool>>,
    }

    impl Database for MockDatabase {
        async fn save_replay(
            &self,
            game_id: &Uuid,
            version: i32,
            events_data: &[u8],
        ) -> Result<(), String> {
            if *self.fail_writes.lock().unwrap() {
                return Err("Database unavailable".to_string());
            }
            self.replays
                .lock()
                .unwrap()
                .push((*game_id, version, events_data.to_vec()));
            Ok(())
        }

        async fn save_error(
            &self,
            game_id: &Uuid,
            error_code: &str,
            details: Option<&serde_json::Value>,
        ) -> Result<(), String> {
            if *self.fail_writes.lock().unwrap() {
                return Err("Database unavailable".to_string());
            }
            self.errors
                .lock()
                .unwrap()
                .push((*game_id, error_code.to_string(), details.cloned()));
            Ok(())
        }

        async fn ping(&self) -> Result<(), String> {
            if *self.fail_writes.lock().unwrap() {
                Err("Database unavailable".to_string())
            } else {
                Ok(())
            }
        }
    }

    fn test_state(
        redis: MockRedisQueue,
        db: MockDatabase,
    ) -> AppState<MockRedisQueue, MockDatabase> {
        AppState {
            redis,
            db,
            config: WorkerConfig {
                redis_url: "redis://mock".to_string(),
                otel_endpoint: "http://mock".to_string(),
                db_mode: DbMode::Postgres,
                database_url: Some("postgres://mock".to_string()),
                consumer_name: "test-worker".to_string(),
                batch_size: 10,
                block_ms: 100,
                autoclaim_threshold_ms: 5_000,
                compression_level: 3,
            },
            metrics: Arc::new(WorkerMetrics::new()),
        }
    }

    #[tokio::test]
    async fn pipeline_saves_valid_replay_and_cleans_up_redis() {
        let redis = MockRedisQueue::default();
        let db = MockDatabase::default();
        let game_id = Uuid::now_v7();
        let valid_events = build_valid_game_events();

        redis
            .events_by_game
            .lock()
            .unwrap()
            .insert(game_id, encode_events(&valid_events));

        let state = test_state(redis.clone(), db.clone());
        let entry = StreamEntry {
            stream_id: "100-0".to_string(),
            game_id: Some(game_id),
        };

        let outcome = process_entry(&state, &entry).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::SavedReplay);

        let saved = db.replays.lock().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].0, game_id);
        assert!(!saved[0].2.is_empty());

        let cleaned = redis.acked_and_cleaned.lock().unwrap();
        assert_eq!(cleaned.as_slice(), &[("100-0".to_string(), game_id)]);
    }

    #[tokio::test]
    async fn pipeline_records_validation_error_and_cleans_up_redis() {
        let redis = MockRedisQueue::default();
        let db = MockDatabase::default();
        let game_id = Uuid::now_v7();

        let invalid_events = vec![StoredEvent::GameEnds {
            timestamp_ms: 100,
            event_index: 0,
        }];
        redis
            .events_by_game
            .lock()
            .unwrap()
            .insert(game_id, encode_events(&invalid_events));

        let state = test_state(redis.clone(), db.clone());
        let entry = StreamEntry {
            stream_id: "101-0".to_string(),
            game_id: Some(game_id),
        };

        let outcome = process_entry(&state, &entry).await.unwrap();
        assert_eq!(
            outcome,
            ProcessOutcome::RecordedError(ValidationError::MissingGameStarted)
        );

        assert!(db.replays.lock().unwrap().is_empty());
        let errors = db.errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, game_id);
        assert_eq!(errors[0].1, "MISSING_GAME_STARTED");
        assert_eq!(errors[0].2, None);

        let cleaned = redis.acked_and_cleaned.lock().unwrap();
        assert_eq!(cleaned.as_slice(), &[("101-0".to_string(), game_id)]);
    }

    #[tokio::test]
    async fn pipeline_skips_empty_event_list_silently() {
        let redis = MockRedisQueue::default();
        let db = MockDatabase::default();
        let game_id = Uuid::now_v7();

        let state = test_state(redis.clone(), db.clone());
        let entry = StreamEntry {
            stream_id: "102-0".to_string(),
            game_id: Some(game_id),
        };

        let outcome = process_entry(&state, &entry).await.unwrap();
        assert_eq!(outcome, ProcessOutcome::SkippedEmpty);

        assert!(db.replays.lock().unwrap().is_empty());
        assert!(db.errors.lock().unwrap().is_empty());
        assert_eq!(
            redis.acked_and_cleaned.lock().unwrap().as_slice(),
            &[("102-0".to_string(), game_id)]
        );
    }

    #[tokio::test]
    async fn pipeline_retains_redis_events_when_database_fails() {
        let redis = MockRedisQueue::default();
        let db = MockDatabase::default();
        *db.fail_writes.lock().unwrap() = true;

        let game_id = Uuid::now_v7();
        redis
            .events_by_game
            .lock()
            .unwrap()
            .insert(game_id, encode_events(&build_valid_game_events()));

        let state = test_state(redis.clone(), db);
        let entry = StreamEntry {
            stream_id: "103-0".to_string(),
            game_id: Some(game_id),
        };

        let res = process_entry(&state, &entry).await;
        assert!(res.is_err());

        assert!(redis.acked_and_cleaned.lock().unwrap().is_empty());
        assert!(redis.events_by_game.lock().unwrap().contains_key(&game_id));
    }
}

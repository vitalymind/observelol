use std::{
    sync::{
        Arc, LazyLock, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use common::{
    ACTIVE_SNAPSHOT_KEY, DEFAULT_EMPTY_SNAPSHOT_JSON, READY_GAMES_STREAM,
    SNAPSHOT_KEYSPACE_CHANNEL, SNAPSHOT_UPDATES_CHANNEL, game_events_key,
};
use redis::{
    AsyncCommands, IntoConnectionInfo, ProtocolVersion, PushKind,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use serde_json::value::RawValue;
use tracing::{info, warn};
use uuid::Uuid;

const REDIS_POOL_SIZE: usize = 4;
const REDIS_PIPELINE_BUFFER_SIZE: usize = 2048;
const REDIS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const SNAPSHOT_FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(5);

static DEFAULT_SNAPSHOT: LazyLock<Arc<Box<RawValue>>> = LazyLock::new(|| {
    Arc::new(
        RawValue::from_string(DEFAULT_EMPTY_SNAPSHOT_JSON.to_string())
            .expect("DEFAULT_EMPTY_SNAPSHOT_JSON must be valid JSON"),
    )
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    Read(String),
    Parse(String),
}

/// Validates raw snapshot JSON once and wraps it in an atomically reference-counted `RawValue`
/// so `/api/new-game/` can serialize it verbatim without parsing or DOM allocation.
pub fn parse_snapshot_raw(raw: Option<String>) -> Result<Arc<Box<RawValue>>, SnapshotError> {
    match raw {
        Some(json_str) => RawValue::from_string(json_str)
            .map(Arc::new)
            .map_err(|e| SnapshotError::Parse(e.to_string())),
        None => Ok(DEFAULT_SNAPSHOT.clone()),
    }
}

pub trait Storage: Clone + Send + Sync + 'static {
    fn get_snapshot(
        &self,
    ) -> impl std::future::Future<Output = Result<Arc<Box<RawValue>>, SnapshotError>> + Send;
    fn ingest_event(
        &self,
        game_id: &Uuid,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = Result<i64, String>> + Send;
    fn notify_game_ended(
        &self,
        game_id: &Uuid,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn unlink_game(
        &self,
        game_id: &Uuid,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn ping(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

#[derive(Clone)]
pub struct RedisStorage {
    pool: Arc<[ConnectionManager; REDIS_POOL_SIZE]>,
    next_idx: Arc<AtomicUsize>,
    default_ttl_sec: i64,
    snapshot_cache: Arc<RwLock<Result<Arc<Box<RawValue>>, SnapshotError>>>,
}

impl RedisStorage {
    pub async fn connect(redis_url: &str, default_ttl_sec: u64) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let manager_config = ConnectionManagerConfig::new()
            .set_pipeline_buffer_size(REDIS_PIPELINE_BUFFER_SIZE)
            .set_response_timeout(Some(REDIS_RESPONSE_TIMEOUT));

        let mut conns = Vec::with_capacity(REDIS_POOL_SIZE);
        for _ in 0..REDIS_POOL_SIZE {
            conns.push(
                ConnectionManager::new_with_config(client.clone(), manager_config.clone()).await?,
            );
        }
        let pool: [ConnectionManager; REDIS_POOL_SIZE] = conns
            .try_into()
            .unwrap_or_else(|_| unreachable!("pool size matches REDIS_POOL_SIZE"));

        let mut init_conn = pool[0].clone();
        // Best-effort enable keyspace notifications for string/generic events on snapshot:active
        let _: Result<String, _> = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("K$g")
            .query_async(&mut init_conn)
            .await;

        let initial_snapshot = fetch_snapshot_from_redis(&mut init_conn).await;
        let snapshot_cache = Arc::new(RwLock::new(initial_snapshot));

        spawn_snapshot_watcher(redis_url.to_string(), init_conn, snapshot_cache.clone());

        Ok(Self {
            pool: Arc::new(pool),
            next_idx: Arc::new(AtomicUsize::new(0)),
            default_ttl_sec: default_ttl_sec as i64,
            snapshot_cache,
        })
    }

    #[inline]
    fn next_conn(&self) -> ConnectionManager {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % REDIS_POOL_SIZE;
        self.pool[idx].clone()
    }
}

async fn fetch_snapshot_from_redis(
    conn: &mut ConnectionManager,
) -> Result<Arc<Box<RawValue>>, SnapshotError> {
    let raw: Option<String> = conn
        .get(ACTIVE_SNAPSHOT_KEY)
        .await
        .map_err(|e| SnapshotError::Read(e.to_string()))?;
    parse_snapshot_raw(raw)
}

fn update_cache_if_changed(
    cache: &RwLock<Result<Arc<Box<RawValue>>, SnapshotError>>,
    next: Result<Arc<Box<RawValue>>, SnapshotError>,
) {
    if let Ok(guard) = cache.read()
        && let (Ok(curr_val), Ok(next_val)) = (&*guard, &next)
        && curr_val.get() == next_val.get()
    {
        return;
    }

    if let Ok(mut guard) = cache.write() {
        *guard = next;
    }
}

fn spawn_snapshot_watcher(
    redis_url: String,
    mut query_conn: ConnectionManager,
    cache: Arc<RwLock<Result<Arc<Box<RawValue>>, SnapshotError>>>,
) {
    tokio::spawn(async move {
        let mut push_rx = match setup_resp3_watcher(&redis_url).await {
            Ok((_watcher_conn, rx)) => {
                info!(
                    keyspace_channel = SNAPSHOT_KEYSPACE_CHANNEL,
                    updates_channel = SNAPSHOT_UPDATES_CHANNEL,
                    "Subscribed to Redis snapshot notifications for in-memory cache"
                );
                // Keep `_watcher_conn` alive inside the task by moving it into the tuple
                Some((_watcher_conn, rx))
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to initialize RESP3 push listener for snapshot; relying on periodic polling"
                );
                None
            }
        };

        let mut interval = tokio::time::interval(SNAPSHOT_FALLBACK_POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            match &mut push_rx {
                Some((_, rx)) => {
                    tokio::select! {
                        maybe_push = rx.recv() => {
                            match maybe_push {
                                Some(push) => {
                                    if matches!(
                                        push.kind,
                                        PushKind::Message | PushKind::PMessage | PushKind::Disconnection
                                    ) {
                                        let next = fetch_snapshot_from_redis(&mut query_conn).await;
                                        update_cache_if_changed(&cache, next);
                                    }
                                }
                                None => {
                                    push_rx = None;
                                }
                            }
                        }
                        _ = interval.tick() => {
                            let next = fetch_snapshot_from_redis(&mut query_conn).await;
                            update_cache_if_changed(&cache, next);
                        }
                    }
                }
                None => {
                    interval.tick().await;
                    let next = fetch_snapshot_from_redis(&mut query_conn).await;
                    update_cache_if_changed(&cache, next);
                }
            }
        }
    });
}

async fn setup_resp3_watcher(
    redis_url: &str,
) -> Result<
    (
        ConnectionManager,
        tokio::sync::mpsc::UnboundedReceiver<redis::PushInfo>,
    ),
    redis::RedisError,
> {
    let mut conn_info = redis_url.into_connection_info()?;
    let redis_settings = conn_info
        .redis_settings()
        .clone()
        .set_protocol(ProtocolVersion::RESP3);
    conn_info = conn_info.set_redis_settings(redis_settings);

    let client = redis::Client::open(conn_info)?;
    let (push_tx, push_rx) = tokio::sync::mpsc::unbounded_channel();
    let config = ConnectionManagerConfig::new()
        .set_push_sender(push_tx)
        .set_automatic_resubscription()
        .set_response_timeout(Some(REDIS_RESPONSE_TIMEOUT));

    let mut watcher_conn = ConnectionManager::new_with_config(client, config).await?;
    watcher_conn
        .subscribe(&[SNAPSHOT_KEYSPACE_CHANNEL, SNAPSHOT_UPDATES_CHANNEL])
        .await?;

    Ok((watcher_conn, push_rx))
}

impl Storage for RedisStorage {
    #[inline]
    async fn get_snapshot(&self) -> Result<Arc<Box<RawValue>>, SnapshotError> {
        self.snapshot_cache
            .read()
            .expect("snapshot_cache lock poisoned")
            .clone()
    }

    async fn ingest_event(&self, game_id: &Uuid, bytes: &[u8]) -> Result<i64, String> {
        let key = game_events_key(game_id);
        let mut conn = self.next_conn();
        let len: i64 = conn
            .rpush(key.as_str(), bytes)
            .await
            .map_err(|e| e.to_string())?;
        if len == 1 {
            let _: Result<bool, _> = conn.expire(key.as_str(), self.default_ttl_sec).await;
        }
        Ok(len)
    }

    async fn notify_game_ended(&self, game_id: &Uuid) -> Result<(), String> {
        let mut conn = self.next_conn();
        let mut uuid_buf = [0u8; uuid::fmt::Hyphenated::LENGTH];
        let game_id_str = game_id.as_hyphenated().encode_lower(&mut uuid_buf);
        redis::cmd("XADD")
            .arg(READY_GAMES_STREAM)
            .arg("*")
            .arg("game_id")
            .arg(&*game_id_str)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())
    }

    async fn unlink_game(&self, game_id: &Uuid) -> Result<(), String> {
        let key = game_events_key(game_id);
        let mut conn = self.next_conn();
        conn.unlink(key.as_str()).await.map_err(|e| e.to_string())
    }

    async fn ping(&self) -> Result<(), String> {
        let mut conn = self.next_conn();
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_snapshot_raw_returns_default_when_none() {
        let parsed = parse_snapshot_raw(None).unwrap();
        assert_eq!(parsed.get(), DEFAULT_EMPTY_SNAPSHOT_JSON);
    }

    #[test]
    fn parse_snapshot_raw_preserves_valid_json_and_rejects_invalid() {
        let custom = r#"{"heroes":[{"hero_id":1,"version_id":2}],"items":[]}"#.to_string();
        let parsed = parse_snapshot_raw(Some(custom.clone())).unwrap();
        assert_eq!(parsed.get(), custom);

        let err = parse_snapshot_raw(Some("{broken".to_string())).unwrap_err();
        assert!(matches!(err, SnapshotError::Parse(_)));
    }

    #[test]
    fn update_cache_if_changed_skips_identical_json_and_updates_on_change() {
        let initial = parse_snapshot_raw(None).unwrap();
        let ptr_before = Arc::as_ptr(&initial);
        let cache = RwLock::new(Ok(initial));

        // Identical payload should not replace the Arc allocation
        let duplicate = parse_snapshot_raw(Some(DEFAULT_EMPTY_SNAPSHOT_JSON.to_string())).unwrap();
        update_cache_if_changed(&cache, Ok(duplicate));
        let ptr_after_same = Arc::as_ptr(cache.read().unwrap().as_ref().unwrap());
        assert_eq!(ptr_before, ptr_after_same);

        // Modified payload should atomically swap the cached value
        let updated_json = r#"{"heroes":[{"hero_id":7,"version_id":3}],"items":[]}"#.to_string();
        let updated = parse_snapshot_raw(Some(updated_json.clone())).unwrap();
        update_cache_if_changed(&cache, Ok(updated));
        assert_eq!(
            cache.read().unwrap().as_ref().unwrap().get(),
            updated_json.as_str()
        );
    }
}

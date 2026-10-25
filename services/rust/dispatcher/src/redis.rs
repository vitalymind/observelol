use std::sync::Arc;
use common::{
    ACTIVE_SNAPSHOT_KEY, READY_GAMES_STREAM, WORKERS_CONSUMER_GROUP,
    game_events_key,
};
use redis::{AsyncCommands, Script, aio::ConnectionManager};
use tracing::info;
use uuid::Uuid;

/// Lua script for atomic event ingestion:
/// Appends serialized event to `game:{id}:events` and sets safety TTL only on the first event.
pub const INGEST_EVENT_LUA: &str = r#"
local len = redis.call('RPUSH', KEYS[1], ARGV[1])
if len == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return len
"#;

pub trait Storage: Clone + Send + Sync + 'static {
    fn get_snapshot(&self) -> impl std::future::Future<Output = Result<Option<String>, String>> + Send;
    fn ingest_event(&self, game_id: &Uuid, bytes: &[u8], ttl_sec: u64) -> impl std::future::Future<Output = Result<i64, String>> + Send;
    fn notify_game_ended(&self, game_id: &Uuid) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn ping(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

#[derive(Clone)]
pub struct RedisStorage {
    redis: ConnectionManager,
    ingest_script: Arc<Script>,
}

impl RedisStorage {
    pub async fn connect(redis_url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let mut redis = ConnectionManager::new(client).await?;
        ensure_consumer_group(&mut redis).await;

        Ok(Self {
            redis,
            ingest_script: Arc::new(Script::new(INGEST_EVENT_LUA)),
        })
    }
}

impl Storage for RedisStorage {
    async fn get_snapshot(&self) -> Result<Option<String>, String> {
        let mut conn = self.redis.clone();
        conn.get(ACTIVE_SNAPSHOT_KEY)
            .await
            .map_err(|e| e.to_string())
    }

    async fn ingest_event(&self, game_id: &Uuid, bytes: &[u8], ttl_sec: u64) -> Result<i64, String> {
        let key = game_events_key(game_id);
        let mut conn = self.redis.clone();
        self.ingest_script
            .key(&key)
            .arg(bytes)
            .arg(ttl_sec)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| e.to_string())
    }

    async fn notify_game_ended(&self, game_id: &Uuid) -> Result<(), String> {
        let mut conn = self.redis.clone();
        redis::cmd("XADD")
            .arg(READY_GAMES_STREAM)
            .arg("*")
            .arg("game_id")
            .arg(game_id.to_string())
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())
    }

    async fn ping(&self) -> Result<(), String> {
        let mut conn = self.redis.clone();
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub async fn ensure_consumer_group(redis: &mut ConnectionManager) {
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

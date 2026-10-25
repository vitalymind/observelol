use common::{ACTIVE_SNAPSHOT_KEY, READY_GAMES_STREAM, WORKERS_CONSUMER_GROUP, game_events_key};
use redis::{AsyncCommands, aio::ConnectionManager};
use tracing::info;
use uuid::Uuid;

pub trait Storage: Clone + Send + Sync + 'static {
    fn get_snapshot(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<String>, String>> + Send;
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
    redis: ConnectionManager,
    default_ttl_sec: i64,
}

impl RedisStorage {
    pub async fn connect(redis_url: &str, default_ttl_sec: u64) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let mut redis = ConnectionManager::new(client).await?;
        ensure_consumer_group(&mut redis).await;

        Ok(Self {
            redis,
            default_ttl_sec: default_ttl_sec as i64,
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

    async fn ingest_event(&self, game_id: &Uuid, bytes: &[u8]) -> Result<i64, String> {
        let key = game_events_key(game_id);
        let mut conn = self.redis.clone();
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
        let mut conn = self.redis.clone();
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
        let mut conn = self.redis.clone();
        conn.unlink(key.as_str()).await.map_err(|e| e.to_string())
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

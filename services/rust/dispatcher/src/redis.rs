use common::{READY_GAMES_STREAM, WORKERS_CONSUMER_GROUP};
use redis::aio::ConnectionManager;
use tracing::info;

/// Lua script for atomic event ingestion:
/// Appends serialized event to `game:{id}:events` and sets safety TTL only on the first event.
pub const INGEST_EVENT_LUA: &str = r#"
local len = redis.call('RPUSH', KEYS[1], ARGV[1])
if len == 1 then
    redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
end
return len
"#;

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

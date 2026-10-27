use std::time::Duration;

use common::{READY_GAMES_STREAM, WORKERS_CONSUMER_GROUP, game_events_key};
use redis::{
    AsyncCommands, FromRedisValue, Value,
    aio::{ConnectionManager, ConnectionManagerConfig},
    streams::{StreamAutoClaimReply, StreamId, StreamReadReply},
};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    pub stream_id: String,
    pub game_id: Option<Uuid>,
}

impl StreamEntry {
    pub(crate) fn from_stream_id(mut entry: StreamId) -> Self {
        let game_id = entry
            .map
            .remove("game_id")
            .and_then(|val| String::from_redis_value(val).ok())
            .and_then(|s| Uuid::parse_str(s.trim()).ok());

        Self {
            stream_id: entry.id,
            game_id,
        }
    }

    pub(crate) fn from_read_reply(reply: StreamReadReply) -> Vec<Self> {
        let mut entries = Vec::new();
        for key in reply.keys {
            for id in key.ids {
                entries.push(Self::from_stream_id(id));
            }
        }
        entries
    }
}

pub trait RedisQueue: Clone + Send + Sync + 'static {
    fn ensure_group(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn read_pending(
        &self,
        consumer: &str,
        count: usize,
    ) -> impl std::future::Future<Output = Result<Vec<StreamEntry>, String>> + Send;
    fn read_group(
        &self,
        consumer: &str,
        count: usize,
        block_ms: usize,
    ) -> impl std::future::Future<Output = Result<Vec<StreamEntry>, String>> + Send;
    fn autoclaim(
        &self,
        consumer: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> impl std::future::Future<Output = Result<Vec<StreamEntry>, String>> + Send;
    fn fetch_game_events(
        &self,
        game_id: &Uuid,
    ) -> impl std::future::Future<Output = Result<Vec<Vec<u8>>, String>> + Send;
    fn ack_and_cleanup(
        &self,
        stream_id: &str,
        game_id: &Uuid,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn ack_and_delete(
        &self,
        stream_id: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    #[allow(dead_code)]
    fn ping(&self) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

#[derive(Clone)]
pub struct RedisStreamClient {
    redis: ConnectionManager,
}

impl RedisStreamClient {
    pub async fn connect_with_retry(redis_url: &str, block_ms: usize) -> Self {
        let retry_delay = Duration::from_secs(2);

        loop {
            match Self::try_connect(redis_url, block_ms).await {
                Ok(client) => return client,
                Err(err) => {
                    warn!(
                        error = %err,
                        "Failed to connect to Redis or verify consumer group; retrying in 2s"
                    );
                    tokio::time::sleep(retry_delay).await;
                }
            }
        }
    }

    async fn try_connect(redis_url: &str, block_ms: usize) -> Result<Self, String> {
        let client = redis::Client::open(redis_url).map_err(|e| e.to_string())?;
        let response_timeout = Duration::from_millis((block_ms as u64).saturating_mul(2));
        let manager_config =
            ConnectionManagerConfig::new().set_response_timeout(Some(response_timeout));

        let redis = ConnectionManager::new_with_config(client, manager_config)
            .await
            .map_err(|e| e.to_string())?;
        let stream_client = Self { redis };
        stream_client.ensure_group().await?;
        Ok(stream_client)
    }
}

impl RedisQueue for RedisStreamClient {
    async fn ensure_group(&self) -> Result<(), String> {
        let mut conn = self.redis.clone();
        let res: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(READY_GAMES_STREAM)
            .arg(WORKERS_CONSUMER_GROUP)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        match res {
            Ok(()) => {
                info!(
                    stream = READY_GAMES_STREAM,
                    group = WORKERS_CONSUMER_GROUP,
                    "Created Redis consumer group"
                );
                Ok(())
            }
            Err(err) if err.to_string().contains("BUSYGROUP") => {
                info!(
                    stream = READY_GAMES_STREAM,
                    group = WORKERS_CONSUMER_GROUP,
                    "Verified existing Redis consumer group"
                );
                Ok(())
            }
            Err(err) => Err(format!(
                "Failed to verify consumer group '{WORKERS_CONSUMER_GROUP}': {err}"
            )),
        }
    }

    async fn read_pending(&self, consumer: &str, count: usize) -> Result<Vec<StreamEntry>, String> {
        let mut conn = self.redis.clone();
        let raw: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(WORKERS_CONSUMER_GROUP)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("STREAMS")
            .arg(READY_GAMES_STREAM)
            .arg("0")
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;

        if matches!(raw, Value::Nil) {
            return Ok(Vec::new());
        }

        let reply = StreamReadReply::from_redis_value(raw).map_err(|e| e.to_string())?;
        Ok(StreamEntry::from_read_reply(reply))
    }

    async fn read_group(
        &self,
        consumer: &str,
        count: usize,
        block_ms: usize,
    ) -> Result<Vec<StreamEntry>, String> {
        let mut conn = self.redis.clone();
        let raw: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(WORKERS_CONSUMER_GROUP)
            .arg(consumer)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS")
            .arg(READY_GAMES_STREAM)
            .arg(">")
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;

        if matches!(raw, Value::Nil) {
            return Ok(Vec::new());
        }

        let reply = StreamReadReply::from_redis_value(raw).map_err(|e| e.to_string())?;
        Ok(StreamEntry::from_read_reply(reply))
    }

    async fn autoclaim(
        &self,
        consumer: &str,
        min_idle_ms: u64,
        count: usize,
    ) -> Result<Vec<StreamEntry>, String> {
        let mut conn = self.redis.clone();
        let raw: Value = redis::cmd("XAUTOCLAIM")
            .arg(READY_GAMES_STREAM)
            .arg(WORKERS_CONSUMER_GROUP)
            .arg(consumer)
            .arg(min_idle_ms)
            .arg("0-0")
            .arg("COUNT")
            .arg(count)
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;

        if matches!(raw, Value::Nil) {
            return Ok(Vec::new());
        }

        let reply = StreamAutoClaimReply::from_redis_value(raw).map_err(|e| e.to_string())?;
        Ok(reply
            .claimed
            .into_iter()
            .map(StreamEntry::from_stream_id)
            .collect())
    }

    async fn fetch_game_events(&self, game_id: &Uuid) -> Result<Vec<Vec<u8>>, String> {
        let key = game_events_key(game_id);
        let mut conn = self.redis.clone();
        conn.lrange(key.as_str(), 0, -1)
            .await
            .map_err(|e| e.to_string())
    }

    async fn ack_and_cleanup(&self, stream_id: &str, game_id: &Uuid) -> Result<(), String> {
        let key = game_events_key(game_id);
        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        pipe.cmd("XACK")
            .arg(READY_GAMES_STREAM)
            .arg(WORKERS_CONSUMER_GROUP)
            .arg(stream_id)
            .ignore()
            .cmd("XDEL")
            .arg(READY_GAMES_STREAM)
            .arg(stream_id)
            .ignore()
            .cmd("UNLINK")
            .arg(key.as_str())
            .ignore();

        let (): () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn ack_and_delete(&self, stream_id: &str) -> Result<(), String> {
        let mut conn = self.redis.clone();
        let mut pipe = redis::pipe();
        pipe.cmd("XACK")
            .arg(READY_GAMES_STREAM)
            .arg(WORKERS_CONSUMER_GROUP)
            .arg(stream_id)
            .ignore()
            .cmd("XDEL")
            .arg(READY_GAMES_STREAM)
            .arg(stream_id)
            .ignore();

        let (): () = pipe
            .query_async(&mut conn)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn parses_stream_entry_with_valid_uuid() {
        let expected_id = Uuid::now_v7();
        let mut map = HashMap::new();
        map.insert(
            "game_id".to_string(),
            Value::BulkString(expected_id.to_string().into_bytes()),
        );
        let stream_id = StreamId {
            id: "1700000000000-0".to_string(),
            map,
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let parsed = StreamEntry::from_stream_id(stream_id);
        assert_eq!(parsed.stream_id, "1700000000000-0");
        assert_eq!(parsed.game_id, Some(expected_id));
    }

    #[test]
    fn parses_stream_entry_with_invalid_uuid_as_none() {
        let mut map = HashMap::new();
        map.insert(
            "game_id".to_string(),
            Value::BulkString(b"not-a-uuid".to_vec()),
        );
        let stream_id = StreamId {
            id: "1700000000000-1".to_string(),
            map,
            milliseconds_elapsed_from_delivery: None,
            delivered_count: None,
        };

        let parsed = StreamEntry::from_stream_id(stream_id);
        assert_eq!(parsed.stream_id, "1700000000000-1");
        assert_eq!(parsed.game_id, None);
    }

    #[test]
    fn parses_redis_xautoclaim_raw_value() {
        let expected_id = Uuid::now_v7();
        let raw = Value::Array(vec![
            Value::BulkString(b"0-0".to_vec()),
            Value::Array(vec![Value::Array(vec![
                Value::BulkString(b"1700000000000-0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(b"game_id".to_vec()),
                    Value::BulkString(expected_id.to_string().into_bytes()),
                ]),
            ])]),
            Value::Array(vec![]),
        ]);

        let reply = StreamAutoClaimReply::from_redis_value(raw).unwrap();
        let entries: Vec<StreamEntry> = reply
            .claimed
            .into_iter()
            .map(StreamEntry::from_stream_id)
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].stream_id, "1700000000000-0");
        assert_eq!(entries[0].game_id, Some(expected_id));
    }
}

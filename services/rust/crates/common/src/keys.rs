use uuid::Uuid;

/// Redis Stream key for games that have ended and are ready for Worker validation.
pub const READY_GAMES_STREAM: &str = "ready_games";

/// Redis Stream consumer group name for VM3 Workers.
pub const WORKERS_CONSUMER_GROUP: &str = "workers";

/// Redis key storing the currently active Heroes & Items snapshot JSON.
pub const ACTIVE_SNAPSHOT_KEY: &str = "snapshot:active";

/// Redis keyspace notification channel emitted when `snapshot:active` is modified (`SET`, `DEL`, `UNLINK`).
pub const SNAPSHOT_KEYSPACE_CHANNEL: &str = "__keyspace@0__:snapshot:active";

/// Explicit Redis Pub/Sub channel that writers (`control`) may optionally publish to on snapshot updates.
pub const SNAPSHOT_UPDATES_CHANNEL: &str = "snapshot:updates";

/// Default JSON snapshot used when `snapshot:active` is not yet populated in Redis.
pub const DEFAULT_EMPTY_SNAPSHOT_JSON: &str = r#"{"heroes":[],"items":[]}"#;

/// Stack-allocated Redis list key (`game:{game_id}:events`, 48 bytes) to avoid heap allocations on the hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GameEventsKey([u8; 48]);

impl GameEventsKey {
    #[inline]
    pub fn new(game_id: &Uuid) -> Self {
        let mut buf = [0u8; 48];
        buf[..5].copy_from_slice(b"game:");
        game_id.as_hyphenated().encode_lower(&mut buf[5..41]);
        buf[41..].copy_from_slice(b":events");
        Self(buf)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).expect("GameEventsKey is always valid ASCII")
    }
}

impl AsRef<str> for GameEventsKey {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Formats the Redis list key used to buffer events for a specific game on the stack.
#[inline]
pub fn game_events_key(game_id: &Uuid) -> GameEventsKey {
    GameEventsKey::new(game_id)
}

use uuid::Uuid;

/// Redis Stream key for games that have ended and are ready for Worker validation.
pub const READY_GAMES_STREAM: &str = "ready_games";

/// Redis Stream consumer group name for VM3 Workers.
pub const WORKERS_CONSUMER_GROUP: &str = "workers";

/// Redis key storing the currently active Heroes & Items snapshot JSON.
pub const ACTIVE_SNAPSHOT_KEY: &str = "snapshot:active";

/// Formats the Redis list key used to buffer events for a specific game.
pub fn game_events_key(game_id: &Uuid) -> String {
    format!("game:{game_id}:events")
}

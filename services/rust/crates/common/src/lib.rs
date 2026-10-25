use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
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

/// Numeric event type representation (u8) used on the wire and in Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum EventType {
    GameStarted = 1,
    HeroSelected = 2,
    RoundStart = 3,
    ItemBought = 4,
    Kill = 5,
    LevelUp = 6,
    GameEnds = 7,
}

/// Event payload sent by Producers to `POST /api/event/`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncomingEvent {
    pub game_id: Uuid,
    pub timestamp_ms: u32,
    pub event_index: u32,
    pub event_type: EventType,
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// Event structure stored in Redis (`game:{game_id}:events`) after stripping `game_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredEvent {
    pub timestamp_ms: u32,
    pub event_index: u32,
    pub event_type: EventType,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub payload: serde_json::Value,
}

impl From<IncomingEvent> for StoredEvent {
    fn from(event: IncomingEvent) -> Self {
        Self {
            timestamp_ms: event.timestamp_ms,
            event_index: event.event_index,
            event_type: event.event_type,
            payload: event.payload,
        }
    }
}

/// Response returned by `GET /api/new-game/`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewGameResponse {
    pub game_id: Uuid,
    pub snapshot: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn incoming_event_strips_game_id_and_preserves_u8_type() {
        let game_id = Uuid::now_v7();
        let raw_json = json!({
            "game_id": game_id,
            "timestamp_ms": 1500,
            "event_index": 2,
            "event_type": 2,
            "payload": {
                "slot": 0,
                "hero_id": 10,
                "version_id": 1
            }
        });

        let incoming: IncomingEvent = serde_json::from_value(raw_json).unwrap();
        assert_eq!(incoming.event_type, EventType::HeroSelected);

        let stored = StoredEvent::from(incoming);
        let serialized = serde_json::to_value(&stored).unwrap();

        assert!(serialized.get("game_id").is_none());
        assert_eq!(serialized["event_type"], json!(2));
        assert_eq!(serialized["event_index"], json!(2));
    }
}

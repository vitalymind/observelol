use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GameStartedPayload {
    pub started_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeroSelectedPayload {
    pub slot: u8,
    pub hero_id: u32,
    pub version_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ItemBoughtPayload {
    pub slot: u8,
    pub item_id: u32,
    pub version_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KillPayload {
    pub killer_slot: u8,
    pub victim_slot: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LevelUpPayload {
    pub slot: u8,
    pub level: u8,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EmptyPayload {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum EventPayload {
    GameStarted(GameStartedPayload),
    HeroSelected(HeroSelectedPayload),
    ItemBought(ItemBoughtPayload),
    Kill(KillPayload),
    LevelUp(LevelUpPayload),
    Empty(EmptyPayload),
}

impl Default for EventPayload {
    fn default() -> Self {
        EventPayload::Empty(EmptyPayload {})
    }
}

/// Event payload sent by Producers to `POST /api/event/`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncomingEvent {
    pub game_id: Uuid,
    pub timestamp_ms: u32,
    pub event_index: u32,
    pub event_type: EventType,
    #[serde(default)]
    pub payload: EventPayload,
}

impl IncomingEvent {
    pub fn is_valid_payload(&self) -> bool {
        match (&self.event_type, &self.payload) {
            (EventType::GameStarted, EventPayload::GameStarted(_)) => true,
            (EventType::HeroSelected, EventPayload::HeroSelected(_)) => true,
            (EventType::RoundStart, EventPayload::Empty(_)) => true,
            (EventType::ItemBought, EventPayload::ItemBought(_)) => true,
            (EventType::Kill, EventPayload::Kill(_)) => true,
            (EventType::LevelUp, EventPayload::LevelUp(_)) => true,
            (EventType::GameEnds, EventPayload::Empty(_)) => true,
            _ => false,
        }
    }
}

/// Event structure stored in Redis (`game:{game_id}:events`) after stripping `game_id`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredEvent {
    pub timestamp_ms: u32,
    pub event_index: u32,
    pub event_type: EventType,
    #[serde(default)]
    pub payload: EventPayload,
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
        assert!(incoming.is_valid_payload());

        let stored = StoredEvent::from(incoming);
        let serialized = serde_json::to_value(&stored).unwrap();

        assert!(serialized.get("game_id").is_none());
        assert_eq!(serialized["event_type"], json!(2));
        assert_eq!(serialized["event_index"], json!(2));
    }

    #[test]
    fn rejects_payload_with_unknown_fields() {
        let raw_json = json!({
            "game_id": Uuid::now_v7(),
            "timestamp_ms": 1500,
            "event_index": 2,
            "event_type": 2,
            "payload": {
                "slot": 0,
                "hero_id": 10,
                "version_id": 1,
                "malicious_large_data": "drop this"
            }
        });

        let res: Result<IncomingEvent, _> = serde_json::from_value(raw_json);
        assert!(
            res.is_err(),
            "Payload with unknown fields should be rejected"
        );
    }

    #[test]
    fn detects_mismatched_event_type_and_payload() {
        let raw_json = json!({
            "game_id": Uuid::now_v7(),
            "timestamp_ms": 1500,
            "event_index": 2,
            "event_type": 2, // HeroSelected
            "payload": {
                "killer_slot": 1,
                "victim_slot": 2
            }
        });

        let incoming: IncomingEvent = serde_json::from_value(raw_json).unwrap();
        assert!(
            !incoming.is_valid_payload(),
            "Mismatched payload should be detected as invalid"
        );
    }
}

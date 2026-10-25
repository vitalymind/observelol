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

impl EventType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::GameStarted => "1",
            Self::HeroSelected => "2",
            Self::RoundStart => "3",
            Self::ItemBought => "4",
            Self::Kill => "5",
            Self::LevelUp => "6",
            Self::GameEnds => "7",
        }
    }
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
/// Flat binary enum variant for zero-overhead postcard serialization and direct pattern matching.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredEvent {
    GameStarted {
        timestamp_ms: u32,
        event_index: u32,
        started_at: u64,
    },
    HeroSelected {
        timestamp_ms: u32,
        event_index: u32,
        slot: u8,
        hero_id: u32,
        version_id: u32,
    },
    RoundStart {
        timestamp_ms: u32,
        event_index: u32,
    },
    ItemBought {
        timestamp_ms: u32,
        event_index: u32,
        slot: u8,
        item_id: u32,
        version_id: u32,
    },
    Kill {
        timestamp_ms: u32,
        event_index: u32,
        killer_slot: u8,
        victim_slot: u8,
    },
    LevelUp {
        timestamp_ms: u32,
        event_index: u32,
        slot: u8,
        level: u8,
    },
    GameEnds {
        timestamp_ms: u32,
        event_index: u32,
    },
}

impl StoredEvent {
    pub fn event_index(&self) -> u32 {
        match self {
            Self::GameStarted { event_index, .. }
            | Self::HeroSelected { event_index, .. }
            | Self::RoundStart { event_index, .. }
            | Self::ItemBought { event_index, .. }
            | Self::Kill { event_index, .. }
            | Self::LevelUp { event_index, .. }
            | Self::GameEnds { event_index, .. } => *event_index,
        }
    }

    pub fn timestamp_ms(&self) -> u32 {
        match self {
            Self::GameStarted { timestamp_ms, .. }
            | Self::HeroSelected { timestamp_ms, .. }
            | Self::RoundStart { timestamp_ms, .. }
            | Self::ItemBought { timestamp_ms, .. }
            | Self::Kill { timestamp_ms, .. }
            | Self::LevelUp { timestamp_ms, .. }
            | Self::GameEnds { timestamp_ms, .. } => *timestamp_ms,
        }
    }
}

impl TryFrom<IncomingEvent> for StoredEvent {
    type Error = &'static str;

    fn try_from(event: IncomingEvent) -> Result<Self, Self::Error> {
        match (event.event_type, event.payload) {
            (EventType::GameStarted, EventPayload::GameStarted(p)) => Ok(StoredEvent::GameStarted {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
                started_at: p.started_at,
            }),
            (EventType::HeroSelected, EventPayload::HeroSelected(p)) => Ok(StoredEvent::HeroSelected {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
                slot: p.slot,
                hero_id: p.hero_id,
                version_id: p.version_id,
            }),
            (EventType::RoundStart, EventPayload::Empty(_)) => Ok(StoredEvent::RoundStart {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
            }),
            (EventType::ItemBought, EventPayload::ItemBought(p)) => Ok(StoredEvent::ItemBought {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
                slot: p.slot,
                item_id: p.item_id,
                version_id: p.version_id,
            }),
            (EventType::Kill, EventPayload::Kill(p)) => Ok(StoredEvent::Kill {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
                killer_slot: p.killer_slot,
                victim_slot: p.victim_slot,
            }),
            (EventType::LevelUp, EventPayload::LevelUp(p)) => Ok(StoredEvent::LevelUp {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
                slot: p.slot,
                level: p.level,
            }),
            (EventType::GameEnds, EventPayload::Empty(_)) => Ok(StoredEvent::GameEnds {
                timestamp_ms: event.timestamp_ms,
                event_index: event.event_index,
            }),
            _ => Err("Mismatched event type and payload"),
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
    fn incoming_event_strips_game_id_and_converts_to_postcard() {
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

        let stored = StoredEvent::try_from(incoming).unwrap();
        assert_eq!(stored.event_index(), 2);
        assert_eq!(stored.timestamp_ms(), 1500);

        let binary = postcard::to_allocvec(&stored).unwrap();
        let decoded: StoredEvent = postcard::from_bytes(&binary).unwrap();
        assert_eq!(stored, decoded);
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

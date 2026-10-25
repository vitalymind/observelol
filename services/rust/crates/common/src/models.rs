use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::value::RawValue;
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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct GameStartedPayload {
    started_at: u64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct HeroSelectedPayload {
    slot: u8,
    hero_id: u32,
    version_id: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ItemBoughtPayload {
    slot: u8,
    item_id: u32,
    version_id: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct KillPayload {
    killer_slot: u8,
    victim_slot: u8,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct LevelUpPayload {
    slot: u8,
    level: u8,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct EmptyPayload {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIncomingEvent<'a> {
    game_id: Uuid,
    timestamp_ms: u32,
    event_index: u32,
    event_type: EventType,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
}

#[inline]
fn parse_payload<'a, T: Deserialize<'a>, E: de::Error>(raw: Option<&'a RawValue>) -> Result<T, E> {
    let json = raw.map_or("{}", RawValue::get);
    serde_json::from_str(json).map_err(de::Error::custom)
}

/// Event payload sent by Producers to `POST /api/event/`.
/// Deserializes the inner `payload` directly by `event_type` into `StoredEvent`
/// without heap-buffering an untagged enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingEvent {
    pub game_id: Uuid,
    pub event_type: EventType,
    pub stored: StoredEvent,
}

impl<'de> Deserialize<'de> for IncomingEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawIncomingEvent::deserialize(deserializer)?;
        let timestamp_ms = raw.timestamp_ms;
        let event_index = raw.event_index;

        let stored = match raw.event_type {
            EventType::GameStarted => {
                let p: GameStartedPayload = parse_payload(raw.payload)?;
                StoredEvent::GameStarted {
                    timestamp_ms,
                    event_index,
                    started_at: p.started_at,
                }
            }
            EventType::HeroSelected => {
                let p: HeroSelectedPayload = parse_payload(raw.payload)?;
                StoredEvent::HeroSelected {
                    timestamp_ms,
                    event_index,
                    slot: p.slot,
                    hero_id: p.hero_id,
                    version_id: p.version_id,
                }
            }
            EventType::RoundStart => {
                let _: EmptyPayload = parse_payload(raw.payload)?;
                StoredEvent::RoundStart {
                    timestamp_ms,
                    event_index,
                }
            }
            EventType::ItemBought => {
                let p: ItemBoughtPayload = parse_payload(raw.payload)?;
                StoredEvent::ItemBought {
                    timestamp_ms,
                    event_index,
                    slot: p.slot,
                    item_id: p.item_id,
                    version_id: p.version_id,
                }
            }
            EventType::Kill => {
                let p: KillPayload = parse_payload(raw.payload)?;
                StoredEvent::Kill {
                    timestamp_ms,
                    event_index,
                    killer_slot: p.killer_slot,
                    victim_slot: p.victim_slot,
                }
            }
            EventType::LevelUp => {
                let p: LevelUpPayload = parse_payload(raw.payload)?;
                StoredEvent::LevelUp {
                    timestamp_ms,
                    event_index,
                    slot: p.slot,
                    level: p.level,
                }
            }
            EventType::GameEnds => {
                let _: EmptyPayload = parse_payload(raw.payload)?;
                StoredEvent::GameEnds {
                    timestamp_ms,
                    event_index,
                }
            }
        };

        Ok(Self {
            game_id: raw.game_id,
            event_type: raw.event_type,
            stored,
        })
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

        let bytes = serde_json::to_vec(&raw_json).unwrap();
        let incoming: IncomingEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(incoming.event_type, EventType::HeroSelected);
        assert_eq!(incoming.stored.event_index(), 2);
        assert_eq!(incoming.stored.timestamp_ms(), 1500);

        let binary = postcard::to_allocvec(&incoming.stored).unwrap();
        let decoded: StoredEvent = postcard::from_bytes(&binary).unwrap();
        assert_eq!(incoming.stored, decoded);
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

        let bytes = serde_json::to_vec(&raw_json).unwrap();
        let res: Result<IncomingEvent, _> = serde_json::from_slice(&bytes);
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

        let bytes = serde_json::to_vec(&raw_json).unwrap();
        let res: Result<IncomingEvent, _> = serde_json::from_slice(&bytes);
        assert!(
            res.is_err(),
            "Mismatched payload should be rejected during deserialization"
        );
    }
}

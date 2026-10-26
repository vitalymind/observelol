use prost::Message;

use crate::models::StoredEvent;

/// Top-level Protobuf message representing an entire game replay.
#[derive(Clone, PartialEq, Message)]
pub struct ProtoReplay {
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<ProtoEvent>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoEvent {
    #[prost(uint32, tag = "1")]
    pub timestamp_ms: u32,
    #[prost(uint32, tag = "2")]
    pub event_index: u32,
    #[prost(oneof = "ProtoEventPayload", tags = "3, 4, 5, 6, 7, 8, 9")]
    pub payload: Option<ProtoEventPayload>,
}

#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum ProtoEventPayload {
    #[prost(message, tag = "3")]
    GameStarted(ProtoGameStarted),
    #[prost(message, tag = "4")]
    HeroSelected(ProtoHeroSelected),
    #[prost(message, tag = "5")]
    RoundStart(ProtoRoundStart),
    #[prost(message, tag = "6")]
    ItemBought(ProtoItemBought),
    #[prost(message, tag = "7")]
    Kill(ProtoKill),
    #[prost(message, tag = "8")]
    LevelUp(ProtoLevelUp),
    #[prost(message, tag = "9")]
    GameEnds(ProtoGameEnds),
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoGameStarted {
    #[prost(uint64, tag = "1")]
    pub started_at: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoHeroSelected {
    #[prost(uint32, tag = "1")]
    pub slot: u32,
    #[prost(uint32, tag = "2")]
    pub hero_id: u32,
    #[prost(uint32, tag = "3")]
    pub version_id: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoRoundStart {}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoItemBought {
    #[prost(uint32, tag = "1")]
    pub slot: u32,
    #[prost(uint32, tag = "2")]
    pub item_id: u32,
    #[prost(uint32, tag = "3")]
    pub version_id: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoKill {
    #[prost(uint32, tag = "1")]
    pub killer_slot: u32,
    #[prost(uint32, tag = "2")]
    pub victim_slot: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoLevelUp {
    #[prost(uint32, tag = "1")]
    pub slot: u32,
    #[prost(uint32, tag = "2")]
    pub level: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ProtoGameEnds {}

impl From<&StoredEvent> for ProtoEvent {
    fn from(ev: &StoredEvent) -> Self {
        match ev {
            StoredEvent::GameStarted {
                timestamp_ms,
                event_index,
                started_at,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::GameStarted(ProtoGameStarted {
                    started_at: *started_at,
                })),
            },
            StoredEvent::HeroSelected {
                timestamp_ms,
                event_index,
                slot,
                hero_id,
                version_id,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::HeroSelected(ProtoHeroSelected {
                    slot: u32::from(*slot),
                    hero_id: *hero_id,
                    version_id: *version_id,
                })),
            },
            StoredEvent::RoundStart {
                timestamp_ms,
                event_index,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::RoundStart(ProtoRoundStart {})),
            },
            StoredEvent::ItemBought {
                timestamp_ms,
                event_index,
                slot,
                item_id,
                version_id,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::ItemBought(ProtoItemBought {
                    slot: u32::from(*slot),
                    item_id: *item_id,
                    version_id: *version_id,
                })),
            },
            StoredEvent::Kill {
                timestamp_ms,
                event_index,
                killer_slot,
                victim_slot,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::Kill(ProtoKill {
                    killer_slot: u32::from(*killer_slot),
                    victim_slot: u32::from(*victim_slot),
                })),
            },
            StoredEvent::LevelUp {
                timestamp_ms,
                event_index,
                slot,
                level,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::LevelUp(ProtoLevelUp {
                    slot: u32::from(*slot),
                    level: u32::from(*level),
                })),
            },
            StoredEvent::GameEnds {
                timestamp_ms,
                event_index,
            } => ProtoEvent {
                timestamp_ms: *timestamp_ms,
                event_index: *event_index,
                payload: Some(ProtoEventPayload::GameEnds(ProtoGameEnds {})),
            },
        }
    }
}

impl TryFrom<ProtoEvent> for StoredEvent {
    type Error = String;

    fn try_from(proto: ProtoEvent) -> Result<Self, Self::Error> {
        let timestamp_ms = proto.timestamp_ms;
        let event_index = proto.event_index;
        let payload = proto
            .payload
            .ok_or_else(|| "Missing event payload in ProtoEvent".to_string())?;

        match payload {
            ProtoEventPayload::GameStarted(p) => Ok(StoredEvent::GameStarted {
                timestamp_ms,
                event_index,
                started_at: p.started_at,
            }),
            ProtoEventPayload::HeroSelected(p) => Ok(StoredEvent::HeroSelected {
                timestamp_ms,
                event_index,
                slot: p.slot as u8,
                hero_id: p.hero_id,
                version_id: p.version_id,
            }),
            ProtoEventPayload::RoundStart(_) => Ok(StoredEvent::RoundStart {
                timestamp_ms,
                event_index,
            }),
            ProtoEventPayload::ItemBought(p) => Ok(StoredEvent::ItemBought {
                timestamp_ms,
                event_index,
                slot: p.slot as u8,
                item_id: p.item_id,
                version_id: p.version_id,
            }),
            ProtoEventPayload::Kill(p) => Ok(StoredEvent::Kill {
                timestamp_ms,
                event_index,
                killer_slot: p.killer_slot as u8,
                victim_slot: p.victim_slot as u8,
            }),
            ProtoEventPayload::LevelUp(p) => Ok(StoredEvent::LevelUp {
                timestamp_ms,
                event_index,
                slot: p.slot as u8,
                level: p.level as u8,
            }),
            ProtoEventPayload::GameEnds(_) => Ok(StoredEvent::GameEnds {
                timestamp_ms,
                event_index,
            }),
        }
    }
}

/// Encodes a list of `StoredEvent`s into a binary Protobuf byte vector.
pub fn encode_replay_protobuf(events: &[StoredEvent]) -> Vec<u8> {
    let proto = ProtoReplay {
        events: events.iter().map(ProtoEvent::from).collect(),
    };
    proto.encode_to_vec()
}

/// Decodes binary Protobuf bytes back into a list of `StoredEvent`s.
pub fn decode_replay_protobuf(buf: &[u8]) -> Result<Vec<StoredEvent>, String> {
    let proto = ProtoReplay::decode(buf).map_err(|e| e.to_string())?;
    proto
        .events
        .into_iter()
        .map(StoredEvent::try_from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_protobuf_all_events() {
        let events = vec![
            StoredEvent::GameStarted {
                timestamp_ms: 0,
                event_index: 0,
                started_at: 1_700_000_000,
            },
            StoredEvent::HeroSelected {
                timestamp_ms: 100,
                event_index: 1,
                slot: 0,
                hero_id: 10,
                version_id: 1,
            },
            StoredEvent::RoundStart {
                timestamp_ms: 1000,
                event_index: 2,
            },
            StoredEvent::ItemBought {
                timestamp_ms: 1500,
                event_index: 3,
                slot: 0,
                item_id: 20,
                version_id: 1,
            },
            StoredEvent::Kill {
                timestamp_ms: 2000,
                event_index: 4,
                killer_slot: 0,
                victim_slot: 5,
            },
            StoredEvent::LevelUp {
                timestamp_ms: 2500,
                event_index: 5,
                slot: 0,
                level: 2,
            },
            StoredEvent::GameEnds {
                timestamp_ms: 5000,
                event_index: 6,
            },
        ];

        let encoded = encode_replay_protobuf(&events);
        assert!(!encoded.is_empty());

        let decoded = decode_replay_protobuf(&encoded).unwrap();
        assert_eq!(decoded, events);
    }
}

use std::{fmt, io::Cursor};

use common::StoredEvent;

pub(crate) const TOTAL_HERO_SLOTS: u8 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationError {
    EmptyEvents,
    CorruptedEvent(String),
    MissingGameStarted,
    DuplicateGameStarted,
    MissingRoundStart,
    DuplicateRoundStart,
    MissingGameEnds,
    EventAfterGameEnds,
    IndexGap {
        expected: u32,
        found: u32,
    },
    UnorderedTimestamp {
        event_index: u32,
        prev_ts: u32,
        found_ts: u32,
    },
    InvalidSlot(u8),
    HeroSelectedAfterRoundStart {
        slot: u8,
    },
    DuplicateHeroSlot {
        slot: u8,
    },
    MissingHeroSlots {
        assigned_mask: u16,
    },
    GameplayEventBeforeRoundStart {
        event_index: u32,
    },
    CompressionFailed(String),
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.details() {
            Some(details) => write!(f, "{}: {details}", self.code()),
            None => write!(f, "{}", self.code()),
        }
    }
}

impl ValidationError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyEvents => "EMPTY_EVENTS",
            Self::CorruptedEvent(_) => "CORRUPTED_EVENT",
            Self::MissingGameStarted => "MISSING_GAME_STARTED",
            Self::DuplicateGameStarted => "DUPLICATE_GAME_STARTED",
            Self::MissingRoundStart => "MISSING_ROUND_START",
            Self::DuplicateRoundStart => "DUPLICATE_ROUND_START",
            Self::MissingGameEnds => "MISSING_GAME_ENDS",
            Self::EventAfterGameEnds => "EVENT_AFTER_GAME_ENDS",
            Self::IndexGap { .. } => "INDEX_GAP",
            Self::UnorderedTimestamp { .. } => "UNORDERED_TIMESTAMP",
            Self::InvalidSlot(_) => "INVALID_SLOT",
            Self::HeroSelectedAfterRoundStart { .. } => "HERO_AFTER_ROUND_START",
            Self::DuplicateHeroSlot { .. } => "DUPLICATE_HERO_SLOT",
            Self::MissingHeroSlots { .. } => "MISSING_HERO_SLOTS",
            Self::GameplayEventBeforeRoundStart { .. } => "GAMEPLAY_BEFORE_ROUND_START",
            Self::CompressionFailed(_) => "COMPRESSION_FAILED",
        }
    }

    pub fn details(&self) -> Option<serde_json::Value> {
        match self {
            Self::EmptyEvents
            | Self::MissingGameStarted
            | Self::DuplicateGameStarted
            | Self::MissingRoundStart
            | Self::DuplicateRoundStart
            | Self::MissingGameEnds
            | Self::EventAfterGameEnds => None,

            Self::CorruptedEvent(err)
            | Self::CompressionFailed(err) => {
                let clamped: String = err.chars().take(255).collect();
                Some(serde_json::json!({ "lib_err": clamped }))
            }

            Self::IndexGap { expected, found } => {
                Some(serde_json::json!({ "expected": expected, "found": found }))
            }
            Self::UnorderedTimestamp {
                event_index,
                prev_ts,
                found_ts,
            } => Some(serde_json::json!({
                "event_index": event_index,
                "prev_ts": prev_ts,
                "found_ts": found_ts
            })),
            Self::InvalidSlot(slot) => Some(serde_json::json!({ "slot": slot })),
            Self::HeroSelectedAfterRoundStart { slot } => Some(serde_json::json!({ "slot": slot })),
            Self::DuplicateHeroSlot { slot } => Some(serde_json::json!({ "slot": slot })),
            Self::MissingHeroSlots { assigned_mask } => {
                Some(serde_json::json!({ "assigned_mask": format!("{assigned_mask:#012b}") }))
            }
            Self::GameplayEventBeforeRoundStart { event_index } => {
                Some(serde_json::json!({ "event_index": event_index }))
            }
        }
    }
}

impl std::error::Error for ValidationError {}

pub fn validate_and_compile_replay(
    raw_events: &[Vec<u8>],
    compression_level: i32,
) -> Result<Vec<u8>, ValidationError> {
    let validated = validate_events(raw_events)?;
    compile_replay(&validated, compression_level)
}

/// Prepares the event timeline and executes the modular game rules pipeline.
pub(crate) fn validate_events(raw_events: &[Vec<u8>]) -> Result<Vec<StoredEvent>, ValidationError> {
    let events = prepare_events(raw_events)?;
    crate::rules::validate_game_rules(&events)?;
    Ok(events)
}

/// Deserializes raw events from Redis, sorts and deduplicates by event_index,
/// and verifies baseline timeline integrity (strictly contiguous 0-based indices and monotonic timestamps).
fn prepare_events(raw_events: &[Vec<u8>]) -> Result<Vec<StoredEvent>, ValidationError> {
    if raw_events.is_empty() {
        return Err(ValidationError::EmptyEvents);
    }

    let mut events = Vec::with_capacity(raw_events.len());
    for raw in raw_events {
        let ev: StoredEvent = postcard::from_bytes(raw)
            .map_err(|e| ValidationError::CorruptedEvent(e.to_string()))?;
        events.push(ev);
    }

    // Sort by event_index and deduplicate retries sharing the same event_index
    events.sort_by_key(StoredEvent::event_index);
    events.dedup_by_key(|ev| ev.event_index());

    // First event must be GameStarted at index 0
    let first = &events[0];
    if !matches!(first, StoredEvent::GameStarted { .. }) {
        return Err(ValidationError::MissingGameStarted);
    }

    if first.event_index() != 0 {
        return Err(ValidationError::IndexGap {
            expected: 0,
            found: first.event_index(),
        });
    }

    // Verify contiguous 0..N event indices and monotonic timestamps
    let mut prev_ts = first.timestamp_ms();
    for (offset, ev) in events.iter().enumerate() {
        let expected_idx = offset as u32;
        let actual_idx = ev.event_index();
        if actual_idx != expected_idx {
            return Err(ValidationError::IndexGap {
                expected: expected_idx,
                found: actual_idx,
            });
        }

        let ts = ev.timestamp_ms();
        if ts < prev_ts {
            return Err(ValidationError::UnorderedTimestamp {
                event_index: actual_idx,
                prev_ts,
                found_ts: ts,
            });
        }
        prev_ts = ts;
    }

    Ok(events)
}

/// Serializes validated events with Protobuf and compresses them with Zstandard.
fn compile_replay(
    events: &[StoredEvent],
    compression_level: i32,
) -> Result<Vec<u8>, ValidationError> {
    let protobuf_bytes = common::encode_replay_protobuf(events);

    zstd::encode_all(Cursor::new(&protobuf_bytes), compression_level)
        .map_err(|e| ValidationError::CompressionFailed(e.to_string()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn encode_events(events: &[StoredEvent]) -> Vec<Vec<u8>> {
        events
            .iter()
            .map(|e| postcard::to_allocvec(e).unwrap())
            .collect()
    }

    pub(crate) fn build_valid_game_events() -> Vec<StoredEvent> {
        let mut events = Vec::new();
        events.push(StoredEvent::GameStarted {
            timestamp_ms: 0,
            event_index: 0,
            started_at: 1_700_000_000,
        });

        for slot in 0..TOTAL_HERO_SLOTS {
            events.push(StoredEvent::HeroSelected {
                timestamp_ms: 100 + u32::from(slot) * 50,
                event_index: 1 + u32::from(slot),
                slot,
                hero_id: 100 + u32::from(slot),
                version_id: 1,
            });
        }

        events.push(StoredEvent::RoundStart {
            timestamp_ms: 1_000,
            event_index: 11,
        });

        events.push(StoredEvent::ItemBought {
            timestamp_ms: 1_500,
            event_index: 12,
            slot: 0,
            item_id: 200,
            version_id: 1,
        });

        events.push(StoredEvent::Kill {
            timestamp_ms: 2_000,
            event_index: 13,
            killer_slot: 0,
            victim_slot: 5,
        });

        events.push(StoredEvent::LevelUp {
            timestamp_ms: 2_500,
            event_index: 14,
            slot: 0,
            level: 2,
        });

        events.push(StoredEvent::GameEnds {
            timestamp_ms: 5_000,
            event_index: 15,
        });

        events
    }

    #[test]
    fn valid_game_compiles_to_compressed_bytes() {
        let expected_events = build_valid_game_events();
        let raw = encode_events(&expected_events);

        let compressed = validate_and_compile_replay(&raw, 3).unwrap();
        assert!(!compressed.is_empty());
    }

    #[test]
    fn deduplicates_and_sorts_out_of_order_events() {
        let valid_events = build_valid_game_events();
        let mut shuffled = valid_events.clone();
        shuffled.push(valid_events[3].clone());
        shuffled.push(valid_events[12].clone());
        shuffled.reverse();

        let raw = encode_events(&shuffled);
        let validated = validate_events(&raw).unwrap();
        assert_eq!(validated, valid_events);
    }

    #[test]
    fn rejects_missing_game_started_or_end_events() {
        let mut no_start = build_valid_game_events();
        no_start.remove(0);
        assert_eq!(
            validate_events(&encode_events(&no_start)),
            Err(ValidationError::MissingGameStarted)
        );

        let mut no_end = build_valid_game_events();
        no_end.pop();
        assert_eq!(
            validate_events(&encode_events(&no_end)),
            Err(ValidationError::MissingGameEnds)
        );
    }

    #[test]
    fn rejects_index_gaps() {
        let mut events = build_valid_game_events();
        events.remove(12);
        let res = validate_events(&encode_events(&events));
        assert_eq!(
            res,
            Err(ValidationError::IndexGap {
                expected: 12,
                found: 13
            })
        );
    }

    #[test]
    fn rejects_unordered_timestamps() {
        let mut events = build_valid_game_events();
        events[12] = StoredEvent::ItemBought {
            timestamp_ms: 500,
            event_index: 12,
            slot: 0,
            item_id: 200,
            version_id: 1,
        };
        let res = validate_events(&encode_events(&events));
        assert_eq!(
            res,
            Err(ValidationError::UnorderedTimestamp {
                event_index: 12,
                prev_ts: 1_000,
                found_ts: 500
            })
        );
    }

    #[test]
    fn rejects_invalid_or_missing_hero_slots() {
        let mut bad_slot = build_valid_game_events();
        bad_slot[1] = StoredEvent::HeroSelected {
            timestamp_ms: 100,
            event_index: 1,
            slot: 10,
            hero_id: 100,
            version_id: 1,
        };
        assert_eq!(
            validate_events(&encode_events(&bad_slot)),
            Err(ValidationError::InvalidSlot(10))
        );

        let mut missing_slot = Vec::new();
        missing_slot.push(StoredEvent::GameStarted {
            timestamp_ms: 0,
            event_index: 0,
            started_at: 1_700_000_000,
        });
        for slot in 0..9 {
            missing_slot.push(StoredEvent::HeroSelected {
                timestamp_ms: 100 + u32::from(slot) * 10,
                event_index: 1 + u32::from(slot),
                slot,
                hero_id: 100 + u32::from(slot),
                version_id: 1,
            });
        }
        missing_slot.push(StoredEvent::RoundStart {
            timestamp_ms: 500,
            event_index: 10,
        });
        missing_slot.push(StoredEvent::GameEnds {
            timestamp_ms: 1_000,
            event_index: 11,
        });

        assert_eq!(
            validate_events(&encode_events(&missing_slot)),
            Err(ValidationError::MissingHeroSlots {
                assigned_mask: 0x01FF
            })
        );

        let mut dup_slot = build_valid_game_events();
        dup_slot[2] = StoredEvent::HeroSelected {
            timestamp_ms: 150,
            event_index: 2,
            slot: 0,
            hero_id: 101,
            version_id: 1,
        };
        assert_eq!(
            validate_events(&encode_events(&dup_slot)),
            Err(ValidationError::DuplicateHeroSlot { slot: 0 })
        );
    }

    #[test]
    fn rejects_stage_ordering_violations_and_corrupted_bytes() {
        let mut early_gameplay = build_valid_game_events();
        early_gameplay[10] = StoredEvent::ItemBought {
            timestamp_ms: 550,
            event_index: 10,
            slot: 0,
            item_id: 200,
            version_id: 1,
        };
        assert_eq!(
            validate_events(&encode_events(&early_gameplay)),
            Err(ValidationError::GameplayEventBeforeRoundStart { event_index: 10 })
        );

        let mut late_hero = build_valid_game_events();
        late_hero[12] = StoredEvent::HeroSelected {
            timestamp_ms: 1_500,
            event_index: 12,
            slot: 0,
            hero_id: 100,
            version_id: 1,
        };
        assert_eq!(
            validate_events(&encode_events(&late_hero)),
            Err(ValidationError::HeroSelectedAfterRoundStart { slot: 0 })
        );

        let corrupted = vec![vec![0xFF, 0xFF, 0xFF]];
        assert!(matches!(
            validate_events(&corrupted),
            Err(ValidationError::CorruptedEvent(_))
        ));
    }
}

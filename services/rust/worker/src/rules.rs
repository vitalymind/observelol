use common::StoredEvent;

use crate::replay::{TOTAL_HERO_SLOTS, ValidationError};

const ALL_HERO_SLOTS_MASK: u16 = (1 << TOTAL_HERO_SLOTS) - 1;

/// Executes all modular game rules in sequence.
pub(crate) fn validate_game_rules(events: &[StoredEvent]) -> Result<(), ValidationError> {
    check_game_lifecycle(events)?;
    check_gameplay_boundaries(events)?;
    check_hero_draft(events)?;
    Ok(())
}

/// Verifies structural game lifecycle milestones:
/// - Starts with `GameStarted` (and no duplicate `GameStarted`)
/// - Contains `RoundStart` before `GameEnds` (and no duplicate `RoundStart`)
/// - Contains `GameEnds` as the final event with no trailing events
fn check_game_lifecycle(events: &[StoredEvent]) -> Result<(), ValidationError> {
    if events.is_empty() {
        return Err(ValidationError::EmptyEvents);
    }

    if !matches!(events[0], StoredEvent::GameStarted { .. }) {
        return Err(ValidationError::MissingGameStarted);
    }

    let mut round_start_seen = false;
    let mut game_ended = false;

    for (idx, ev) in events.iter().enumerate() {
        if game_ended {
            return Err(ValidationError::EventAfterGameEnds);
        }

        match ev {
            StoredEvent::GameStarted { .. } => {
                if idx > 0 {
                    return Err(ValidationError::DuplicateGameStarted);
                }
            }
            StoredEvent::RoundStart { .. } => {
                if round_start_seen {
                    return Err(ValidationError::DuplicateRoundStart);
                }
                round_start_seen = true;
            }
            StoredEvent::GameEnds { .. } => {
                if !round_start_seen {
                    return Err(ValidationError::MissingRoundStart);
                }
                game_ended = true;
            }
            _ => {}
        }
    }

    if !round_start_seen {
        return Err(ValidationError::MissingRoundStart);
    }

    if !game_ended {
        return Err(ValidationError::MissingGameEnds);
    }

    Ok(())
}

/// Verifies hero selection and drafting rules:
/// - Hero slots must be within 0..TOTAL_HERO_SLOTS
/// - Each slot must be chosen at most once
/// - All 10 hero slots must be filled before RoundStart
/// - No HeroSelected events may occur after RoundStart
fn check_hero_draft(events: &[StoredEvent]) -> Result<(), ValidationError> {
    let mut assigned_mask: u16 = 0;
    let mut round_started = false;

    for ev in events {
        match ev {
            StoredEvent::RoundStart { .. } => {
                if assigned_mask != ALL_HERO_SLOTS_MASK {
                    return Err(ValidationError::MissingHeroSlots { assigned_mask });
                }
                round_started = true;
            }
            StoredEvent::HeroSelected { slot, .. } => {
                if round_started {
                    return Err(ValidationError::HeroSelectedAfterRoundStart { slot: *slot });
                }
                if *slot >= TOTAL_HERO_SLOTS {
                    return Err(ValidationError::InvalidSlot(*slot));
                }

                let bit = 1u16 << *slot;
                if (assigned_mask & bit) != 0 {
                    return Err(ValidationError::DuplicateHeroSlot { slot: *slot });
                }
                assigned_mask |= bit;
            }
            _ => {}
        }
    }

    Ok(())
}

/// Verifies gameplay boundaries and slot identifiers:
/// - In-game actions (ItemBought, Kill, LevelUp) can only occur after RoundStart
/// - Slots referenced in gameplay events must be within valid range
fn check_gameplay_boundaries(events: &[StoredEvent]) -> Result<(), ValidationError> {
    let mut round_started = false;

    for ev in events {
        match ev {
            StoredEvent::RoundStart { .. } => {
                round_started = true;
            }
            StoredEvent::ItemBought {
                slot, event_index, ..
            }
            | StoredEvent::LevelUp {
                slot, event_index, ..
            } => {
                if *slot >= TOTAL_HERO_SLOTS {
                    return Err(ValidationError::InvalidSlot(*slot));
                }
                if !round_started {
                    return Err(ValidationError::GameplayEventBeforeRoundStart {
                        event_index: *event_index,
                    });
                }
            }
            StoredEvent::Kill {
                killer_slot,
                victim_slot,
                event_index,
                ..
            } => {
                if *killer_slot >= TOTAL_HERO_SLOTS {
                    return Err(ValidationError::InvalidSlot(*killer_slot));
                }
                if *victim_slot >= TOTAL_HERO_SLOTS {
                    return Err(ValidationError::InvalidSlot(*victim_slot));
                }
                if !round_started {
                    return Err(ValidationError::GameplayEventBeforeRoundStart {
                        event_index: *event_index,
                    });
                }
            }
            _ => {}
        }
    }

    Ok(())
}

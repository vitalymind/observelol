import math
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from simulation.load_tester.config import SimulationConfig

EVENT_GAME_STARTED = 1
EVENT_HERO_SELECTED = 2
EVENT_ROUND_START = 3
EVENT_ITEM_BOUGHT = 4
EVENT_KILL = 5
EVENT_LEVEL_UP = 6
EVENT_GAME_ENDS = 7

TOTAL_SLOTS = 10


@dataclass
class ScheduledEvent:
    schedule_ms: int
    wire: Dict[str, Any]


def _extract_catalog(
    snapshot: Dict[str, Any] | None,
    key: str,
    id_field: str,
    fallback_base: int,
    fallback_count: int,
) -> List[Tuple[int, int]]:
    """
    Extracts (entity_id, version_id) pairs from the `/api/new-game/` snapshot,
    falling back to deterministic synthetic IDs when Redis snapshot is empty.
    """
    if isinstance(snapshot, dict):
        items = snapshot.get(key)
        if isinstance(items, list) and items:
            parsed: List[Tuple[int, int]] = []
            for entry in items:
                if isinstance(entry, dict):
                    ent_id = entry.get(id_field) or entry.get("id")
                    ver_id = (
                        entry.get("version_id") or entry.get("active_version_id") or 1
                    )
                    if ent_id is not None:
                        parsed.append((int(ent_id), int(ver_id)))
            if parsed:
                return parsed

    return [(fallback_base + i, 1) for i in range(fallback_count)]


def _roll_probability(percentage: int) -> bool:
    if percentage <= 0:
        return False
    if percentage >= 100:
        return True
    return random.randint(1, 100) <= percentage


def generate_game_plan(
    game_id: str,
    snapshot: Dict[str, Any] | None,
    config: SimulationConfig,
) -> Tuple[List[ScheduledEvent], List[str]]:
    """
    Generates a complete sequence of scheduled events for a single simulated game,
    evaluating all configured error injection probabilities independently.

    Returns (scheduled_events, injected_error_codes).
    """
    total_duration_ms = random.randint(
        config.min_game_duration_sec * 1000,
        config.max_game_duration_sec * 1000,
    )
    hero_select_ms = config.hero_select_duration_sec * 1000

    heroes_catalog = _extract_catalog(snapshot, "heroes", "hero_id", 100, 20)
    items_catalog = _extract_catalog(snapshot, "items", "item_id", 200, 30)

    injected_errors: List[str] = [
        code for code, pct in config.error_rates.items() if _roll_probability(pct)
    ]
    error_set = set(injected_errors)

    # 1. Phase 1: GameStarted (timestamp_ms = 0)
    timeline: List[ScheduledEvent] = []
    if "MISSING_GAME_STARTED" not in error_set:
        timeline.append(
            ScheduledEvent(
                schedule_ms=0,
                wire={
                    "game_id": game_id,
                    "timestamp_ms": 0,
                    "event_index": 0,
                    "event_type": EVENT_GAME_STARTED,
                    "payload": {"started_at": int(time.time())},
                },
            )
        )

    # 2. Phase 2: Hero Selection (slots 0..9 distributed across hero_select_ms)
    slots_to_pick = list(range(TOTAL_SLOTS))
    if "MISSING_HERO_SLOTS" in error_set and slots_to_pick:
        slots_to_pick.pop()  # Omit slot 9

    hero_step_ms = max(10, (hero_select_ms - 200) // max(1, len(slots_to_pick) + 2))
    current_ms = 100

    for slot in slots_to_pick:
        hero_id, version_id = random.choice(heroes_catalog)
        timeline.append(
            ScheduledEvent(
                schedule_ms=current_ms,
                wire={
                    "game_id": game_id,
                    "timestamp_ms": current_ms,
                    "event_index": 0,
                    "event_type": EVENT_HERO_SELECTED,
                    "payload": {
                        "slot": slot,
                        "hero_id": hero_id,
                        "version_id": version_id,
                    },
                },
            )
        )
        current_ms = min(hero_select_ms - 50, current_ms + hero_step_ms)

    if "DUPLICATE_HERO_SLOT" in error_set:
        hero_id, version_id = random.choice(heroes_catalog)
        dup_ms = min(hero_select_ms - 20, current_ms + 10)
        timeline.append(
            ScheduledEvent(
                schedule_ms=dup_ms,
                wire={
                    "game_id": game_id,
                    "timestamp_ms": dup_ms,
                    "event_index": 0,
                    "event_type": EVENT_HERO_SELECTED,
                    "payload": {
                        "slot": 0,
                        "hero_id": hero_id,
                        "version_id": version_id,
                    },
                },
            )
        )
        current_ms = dup_ms

    # 3. Phase 3: RoundStart (at hero_select_ms)
    round_start_ms = hero_select_ms
    if "MISSING_ROUND_START" not in error_set:
        timeline.append(
            ScheduledEvent(
                schedule_ms=round_start_ms,
                wire={
                    "game_id": game_id,
                    "timestamp_ms": round_start_ms,
                    "event_index": 0,
                    "event_type": EVENT_ROUND_START,
                    "payload": {},
                },
            )
        )

    # Optional Error: HERO_AFTER_ROUND_START
    # Placed after RoundStart while keeping all 10 initial slots intact before RoundStart
    if "HERO_AFTER_ROUND_START" in error_set:
        late_hero_ms = round_start_ms + 50
        hero_id, version_id = random.choice(heroes_catalog)
        timeline.append(
            ScheduledEvent(
                schedule_ms=late_hero_ms,
                wire={
                    "game_id": game_id,
                    "timestamp_ms": late_hero_ms,
                    "event_index": 0,
                    "event_type": EVENT_HERO_SELECTED,
                    "payload": {
                        "slot": 0,
                        "hero_id": hero_id,
                        "version_id": version_id,
                    },
                },
            )
        )

    # 4. Phase 4: In-Game Action Events (Items, Kills, LevelUps)
    max_target = config.game_events_target
    if max_target > 0:
        min_target = int(math.floor(max_target * 0.95))
        actual_action_count = random.randint(min_target, max_target)
    else:
        actual_action_count = 0

    action_Specs: List[Tuple[int, Dict[str, Any]]] = []

    # Roll items per hero slot (0..6 within [min_items_per_hero, max_items_per_hero])
    for slot in range(TOTAL_SLOTS):
        num_items = random.randint(config.min_items_per_hero, config.max_items_per_hero)
        for _ in range(num_items):
            item_id, version_id = random.choice(items_catalog)
            action_Specs.append(
                (
                    EVENT_ITEM_BOUGHT,
                    {
                        "slot": slot,
                        "item_id": item_id,
                        "version_id": version_id,
                    },
                )
            )

    # Clamp item events if total rolled items exceed actual_action_count
    if len(action_Specs) > actual_action_count:
        random.shuffle(action_Specs)
        action_Specs = action_Specs[:actual_action_count]

    remaining_actions = actual_action_count - len(action_Specs)
    for _ in range(remaining_actions):
        if random.random() < 0.55:
            # Kill event: team A (0..4) vs team B (5..9)
            if random.random() < 0.5:
                killer = random.randint(0, 4)
                victim = random.randint(5, 9)
            else:
                killer = random.randint(5, 9)
                victim = random.randint(0, 4)
            action_Specs.append(
                (
                    EVENT_KILL,
                    {
                        "killer_slot": killer,
                        "victim_slot": victim,
                    },
                )
            )
        else:
            slot = random.randint(0, TOTAL_SLOTS - 1)
            action_Specs.append((EVENT_LEVEL_UP, {"slot": slot, "level": 2}))

    random.shuffle(action_Specs)

    # Assign monotonic levels per slot for LevelUp events
    slot_levels = {s: 1 for s in range(TOTAL_SLOTS)}
    for ev_type, payload in action_Specs:
        if ev_type == EVENT_LEVEL_UP:
            s = payload["slot"]
            slot_levels[s] = min(255, slot_levels[s] + 1)
            payload["level"] = slot_levels[s]

    # Distribute action timestamps semi-evenly across (round_start_ms + 100 .. total_duration_ms - 100)
    action_start_ms = round_start_ms + 100
    action_end_ms = max(action_start_ms + 100, total_duration_ms - 100)
    action_window_ms = action_end_ms - action_start_ms

    num_actions = len(action_Specs)
    action_events: List[ScheduledEvent] = []
    if num_actions > 0:
        raw_offsets = sorted(
            random.randint(0, action_window_ms) for _ in range(num_actions)
        )
        for offset, (ev_type, payload) in zip(raw_offsets, action_Specs):
            ts_ms = action_start_ms + offset
            action_events.append(
                ScheduledEvent(
                    schedule_ms=ts_ms,
                    wire={
                        "game_id": game_id,
                        "timestamp_ms": ts_ms,
                        "event_index": 0,
                        "event_type": ev_type,
                        "payload": payload,
                    },
                )
            )

    # Optional Error: INVALID_SLOT (slot >= 10)
    if "INVALID_SLOT" in error_set:
        if action_events:
            target_ev = random.choice(action_events)
            if "slot" in target_ev.wire["payload"]:
                target_ev.wire["payload"]["slot"] = 10
            elif "killer_slot" in target_ev.wire["payload"]:
                target_ev.wire["payload"]["killer_slot"] = 10
        else:
            # No action events: inject an invalid-slot HeroSelected before RoundStart
            hero_id, version_id = random.choice(heroes_catalog)
            invalid_ms = max(10, round_start_ms - 10)
            timeline.insert(
                max(1, len(timeline) - 1),
                ScheduledEvent(
                    schedule_ms=invalid_ms,
                    wire={
                        "game_id": game_id,
                        "timestamp_ms": invalid_ms,
                        "event_index": 0,
                        "event_type": EVENT_HERO_SELECTED,
                        "payload": {
                            "slot": 10,
                            "hero_id": hero_id,
                            "version_id": version_id,
                        },
                    },
                ),
            )

    timeline.extend(action_events)

    # 5. Phase 5: GameEnds (always sent so Dispatcher triggers ready_games stream)
    timeline.append(
        ScheduledEvent(
            schedule_ms=total_duration_ms,
            wire={
                "game_id": game_id,
                "timestamp_ms": total_duration_ms,
                "event_index": 0,
                "event_type": EVENT_GAME_ENDS,
                "payload": {},
            },
        )
    )

    # 6. Contiguous re-indexing (0..N-1) so structural mutations don't mask as INDEX_GAP
    for idx, ev in enumerate(timeline):
        ev.wire["event_index"] = idx

    # 7. Optional Error: INDEX_GAP
    if "INDEX_GAP" in error_set and len(timeline) >= 2:
        gap_start = max(1, len(timeline) // 2)
        for idx in range(gap_start, len(timeline)):
            timeline[idx].wire["event_index"] += 1

    # 8. Optional Error: UNORDERED_TIMESTAMP
    if "UNORDERED_TIMESTAMP" in error_set and len(timeline) >= 3:
        # Pick an event after index 1 whose predecessor has timestamp_ms > 0
        candidates = [
            i
            for i in range(2, len(timeline))
            if timeline[i - 1].wire["timestamp_ms"] > 0
        ]
        if candidates:
            chosen_idx = random.choice(candidates)
            # Set wire timestamp_ms strictly smaller than predecessor's timestamp_ms
            timeline[chosen_idx].wire["timestamp_ms"] = 0

    return timeline, injected_errors

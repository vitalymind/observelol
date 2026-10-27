from dataclasses import dataclass, field, asdict
from typing import Dict, Any

ERROR_CODES: tuple[str, ...] = (
    "INDEX_GAP",
    "MISSING_ROUND_START",
    "MISSING_GAME_STARTED",
    "DUPLICATE_HERO_SLOT",
    "MISSING_HERO_SLOTS",
    "HERO_AFTER_ROUND_START",
    "UNORDERED_TIMESTAMP",
    "INVALID_SLOT",
)

LIFECYCLE_REQUESTS_PER_GAME = 14  # 1 GET /api/new-game/ + 13 mandatory POST /api/event/


def get_hero_select_duration(min_game_duration_sec: int) -> int:
    """
    Maps minimum game duration (30s..600s) to discrete hero selection duration steps:
      - 30s  - 60s  -> 5s
      - 61s  - 180s -> 10s
      - 181s - 360s -> 15s
      - 361s - 600s -> 20s
    """
    if min_game_duration_sec <= 60:
        return 5
    if min_game_duration_sec <= 180:
        return 10
    if min_game_duration_sec <= 360:
        return 15
    return 20


@dataclass
class SimulationConfig:
    target_host: str = "https://observelol.vbrokh.dev"
    producers: int = 10
    min_game_duration_sec: int = 45
    max_game_duration_sec: int = 75
    initial_delay_pct: int = 100
    game_events_target: int = 100
    min_items_per_hero: int = 0
    max_items_per_hero: int = 6
    error_rates: Dict[str, int] = field(
        default_factory=lambda: {code: 0 for code in ERROR_CODES}
    )

    def validate_and_clamp(self) -> "SimulationConfig":
        self.target_host = self.target_host.strip().rstrip("/") or "https://observelol.vbrokh.dev"
        self.producers = max(1, min(10_000, int(self.producers)))
        self.min_game_duration_sec = max(30, min(600, int(self.min_game_duration_sec)))
        self.max_game_duration_sec = max(30, min(600, int(self.max_game_duration_sec)))
        if self.min_game_duration_sec > self.max_game_duration_sec:
            self.min_game_duration_sec, self.max_game_duration_sec = (
                self.max_game_duration_sec,
                self.min_game_duration_sec,
            )

        self.initial_delay_pct = max(0, min(100, int(round(float(self.initial_delay_pct)))))
        self.game_events_target = max(0, min(50_000, int(self.game_events_target)))

        self.min_items_per_hero = max(0, min(6, int(self.min_items_per_hero)))
        self.max_items_per_hero = max(0, min(6, int(self.max_items_per_hero)))
        if self.min_items_per_hero > self.max_items_per_hero:
            self.min_items_per_hero, self.max_items_per_hero = (
                self.max_items_per_hero,
                self.min_items_per_hero,
            )

        cleaned_errors: Dict[str, int] = {}
        for code in ERROR_CODES:
            raw_val = self.error_rates.get(code, 0)
            cleaned_errors[code] = max(0, min(100, int(round(float(raw_val)))))
        self.error_rates = cleaned_errors
        return self

    @property
    def hero_select_duration_sec(self) -> int:
        return get_hero_select_duration(self.min_game_duration_sec)

    @property
    def avg_game_duration_sec(self) -> float:
        return (self.min_game_duration_sec + self.max_game_duration_sec) / 2.0

    def calculate_estimates(self) -> Dict[str, Any]:
        avg_duration = self.avg_game_duration_sec
        avg_action_events = self.game_events_target * 0.975
        avg_total_requests = LIFECYCLE_REQUESTS_PER_GAME + avg_action_events
        estimated_rps = (self.producers * avg_total_requests) / avg_duration
        games_per_minute = (self.producers * 60.0) / avg_duration

        return {
            "hero_select_sec": self.hero_select_duration_sec,
            "avg_game_duration_sec": round(avg_duration, 1),
            "max_initial_delay_sec": round(
                self.min_game_duration_sec * (self.initial_delay_pct / 100.0), 1
            ),
            "avg_action_events_per_game": round(avg_action_events, 1),
            "avg_total_requests_per_game": round(avg_total_requests, 1),
            "estimated_rps": round(estimated_rps, 2),
            "games_per_minute": round(games_per_minute, 2),
        }

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["estimates"] = self.calculate_estimates()
        return data

    @classmethod
    def from_dict(cls, payload: Dict[str, Any], base: "SimulationConfig | None" = None) -> "SimulationConfig":
        current = asdict(base) if base is not None else asdict(cls())
        for key in (
            "target_host",
            "producers",
            "min_game_duration_sec",
            "max_game_duration_sec",
            "initial_delay_pct",
            "game_events_target",
            "min_items_per_hero",
            "max_items_per_hero",
        ):
            if key in payload and payload[key] is not None:
                current[key] = payload[key]

        if "error_rates" in payload and isinstance(payload["error_rates"], dict):
            merged_errors = dict(current["error_rates"])
            for k, v in payload["error_rates"].items():
                if k in ERROR_CODES and v is not None:
                    merged_errors[k] = v
            current["error_rates"] = merged_errors

        return cls(**current).validate_and_clamp()

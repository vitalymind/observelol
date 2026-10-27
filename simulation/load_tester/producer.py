import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import gevent
from locust import FastHttpUser, task, constant
from locust.runners import STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP

from simulation.load_tester.config import ERROR_CODES, SimulationConfig
from simulation.load_tester.generator import generate_game_plan


@dataclass
class SimulationCounters:
    active_games: int = 0
    games_started: int = 0
    games_completed: int = 0
    games_aborted: int = 0
    games_with_injected_errors: int = 0
    injected_by_code: Dict[str, int] = field(
        default_factory=lambda: {code: 0 for code in ERROR_CODES}
    )

    def reset(self) -> None:
        self.active_games = 0
        self.games_started = 0
        self.games_completed = 0
        self.games_aborted = 0
        self.games_with_injected_errors = 0
        self.injected_by_code = {code: 0 for code in ERROR_CODES}


class SimulationRuntime:
    def __init__(self) -> None:
        self.config: SimulationConfig = SimulationConfig()
        self.counters: SimulationCounters = SimulationCounters()
        self._producer_states: Dict[int, float] = {}
        self._free_ids: set[int] = set()
        self._next_id: int = 0

    def register_producer(self) -> int:
        if self._free_ids:
            pid = min(self._free_ids)
            self._free_ids.remove(pid)
        else:
            pid = self._next_id
            self._next_id += 1
        self._producer_states[pid] = 0.0
        return pid

    def unregister_producer(self, pid: int) -> None:
        self._producer_states.pop(pid, None)
        self._free_ids.add(pid)

    def set_producer_progress(self, pid: int, progress: float) -> None:
        self._producer_states[pid] = max(0.0, min(1.0, progress))

    def get_producer_states(self) -> List[float]:
        if not self._producer_states:
            return []
        max_id = max(self._producer_states.keys())
        return [round(self._producer_states.get(i, 0.0), 2) for i in range(max_id + 1)]

    def reset_producers(self) -> None:
        self._producer_states.clear()
        self._free_ids.clear()
        self._next_id = 0


RUNTIME = SimulationRuntime()


class GameProducerUser(FastHttpUser):
    """
    Simulated ObserveLoL game producer using persistent HTTPS/TLS Keep-Alive
    connections via Locust's FastHttpUser (geventhttpclient).
    """

    host = "https://observelol.vbrokh.dev"
    wait_time = constant(0.0)
    network_timeout = 15.0
    connection_timeout = 10.0
    max_retries = 1

    def on_start(self) -> None:
        self.producer_id = RUNTIME.register_producer()
        self._initial_delay_done = False

    def on_stop(self) -> None:
        if hasattr(self, "producer_id"):
            RUNTIME.unregister_producer(self.producer_id)

    def _is_stopping(self) -> bool:
        runner = getattr(self.environment, "runner", None)
        if runner is None:
            return False
        return runner.state in (STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP)

    def _sleep_interruptible(self, seconds: float) -> bool:
        """
        Sleeps for `seconds` in small increments so a Stop command from the UI
        interrupts long games immediately. Returns False if interrupted by Stop.
        """
        if seconds <= 0:
            return not self._is_stopping()

        end_time = time.perf_counter() + seconds
        while True:
            if self._is_stopping():
                return False
            remaining = end_time - time.perf_counter()
            if remaining <= 0:
                return True
            gevent.sleep(min(0.2, remaining))

    @task
    def run_single_game(self) -> None:
        if self._is_stopping():
            gevent.sleep(0.1)
            return

        cfg = RUNTIME.config
        RUNTIME.set_producer_progress(self.producer_id, 0.0)

        if not getattr(self, "_initial_delay_done", False):
            self._initial_delay_done = True
            max_initial_delay = float(cfg.min_game_duration_sec) * (
                float(cfg.initial_delay_pct) / 100.0
            )
            if max_initial_delay > 0.0:
                initial_delay = random.uniform(0.0, max_initial_delay)
                if not self._sleep_interruptible(initial_delay):
                    return

        # 1. Request new game_id and active Hero/Item snapshot
        with self.client.get(
            "/api/new-game/",
            name="GET /api/new-game/",
            catch_response=True,
        ) as resp:
            if resp.status_code != 200:
                resp.failure(f"HTTP {resp.status_code}")
                gevent.sleep(0.5)
                return
            try:
                data = resp.json()
                game_id = data["game_id"]
                snapshot = data.get("snapshot")
            except Exception as exc:
                resp.failure(f"Invalid JSON from /api/new-game/: {exc}")
                gevent.sleep(0.5)
                return

        # 2. Build scheduled event timeline and apply configured error injections
        timeline, injected_errors = generate_game_plan(game_id, snapshot, cfg)
        total_duration_ms = timeline[-1].schedule_ms if timeline else 1

        RUNTIME.counters.games_started += 1
        RUNTIME.counters.active_games += 1
        if injected_errors:
            RUNTIME.counters.games_with_injected_errors += 1
            for code in injected_errors:
                RUNTIME.counters.injected_by_code[code] = (
                    RUNTIME.counters.injected_by_code.get(code, 0) + 1
                )

        game_start_wall = time.perf_counter()
        completed_cleanly = True

        try:
            # 3. Stream events over persistent Keep-Alive connection
            for scheduled_ev in timeline:
                progress = (
                    min(0.99, scheduled_ev.schedule_ms / total_duration_ms)
                    if total_duration_ms > 0
                    else 0.0
                )
                RUNTIME.set_producer_progress(self.producer_id, progress)

                target_wall = game_start_wall + (scheduled_ev.schedule_ms / 1000.0)
                delay_sec = target_wall - time.perf_counter()
                if delay_sec > 0:
                    if not self._sleep_interruptible(delay_sec):
                        completed_cleanly = False
                        break

                if self._is_stopping():
                    completed_cleanly = False
                    break

                with self.client.post(
                    "/api/event/",
                    json=scheduled_ev.wire,
                    name="POST /api/event/",
                    catch_response=True,
                ) as ev_resp:
                    if ev_resp.status_code != 200:
                        ev_resp.failure(f"HTTP {ev_resp.status_code}")
        finally:
            RUNTIME.counters.active_games = max(0, RUNTIME.counters.active_games - 1)
            if completed_cleanly:
                RUNTIME.counters.games_completed += 1
                RUNTIME.set_producer_progress(self.producer_id, 1.0)
                gevent.sleep(0.4)
            else:
                RUNTIME.counters.games_aborted += 1
                RUNTIME.set_producer_progress(self.producer_id, 0.0)

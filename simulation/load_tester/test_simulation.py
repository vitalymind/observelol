from gevent import monkey

monkey.patch_all()

import sys
import unittest
import unittest.mock
from pathlib import Path
from uuid import uuid4

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from simulation.load_tester.config import (
    ERROR_CODES,
    SimulationConfig,
    get_hero_select_duration,
)
from simulation.load_tester.generator import (
    EVENT_GAME_ENDS,
    EVENT_GAME_STARTED,
    EVENT_HERO_SELECTED,
    EVENT_ROUND_START,
    generate_game_plan,
)


class TestSimulationGenerator(unittest.TestCase):
    def test_hero_select_duration_steps(self) -> None:
        self.assertEqual(get_hero_select_duration(30), 5)
        self.assertEqual(get_hero_select_duration(60), 5)
        self.assertEqual(get_hero_select_duration(61), 10)
        self.assertEqual(get_hero_select_duration(180), 10)
        self.assertEqual(get_hero_select_duration(181), 15)
        self.assertEqual(get_hero_select_duration(360), 15)
        self.assertEqual(get_hero_select_duration(361), 20)
        self.assertEqual(get_hero_select_duration(600), 20)

    def test_valid_game_timeline_invariants(self) -> None:
        cfg = SimulationConfig(
            min_game_duration_sec=30,
            max_game_duration_sec=60,
            game_events_target=100,
            min_items_per_hero=1,
            max_items_per_hero=3,
        )
        game_id = str(uuid4())
        timeline, injected = generate_game_plan(game_id, None, cfg)

        self.assertEqual(injected, [])
        # 13 mandatory events + 95..100 action events
        self.assertGreaterEqual(len(timeline), 13 + 95)
        self.assertLessEqual(len(timeline), 13 + 100)

        # Starts with GameStarted at index 0, ends with GameEnds within [30_000, 60_000] ms
        self.assertEqual(timeline[0].wire["event_type"], EVENT_GAME_STARTED)
        self.assertEqual(timeline[0].wire["event_index"], 0)
        self.assertEqual(timeline[-1].wire["event_type"], EVENT_GAME_ENDS)
        self.assertGreaterEqual(timeline[-1].wire["timestamp_ms"], 30_000)
        self.assertLessEqual(timeline[-1].wire["timestamp_ms"], 60_000)

        # Contiguous indices and monotonic timestamps
        prev_ts = 0
        for idx, ev in enumerate(timeline):
            self.assertEqual(ev.wire["event_index"], idx)
            self.assertGreaterEqual(ev.wire["timestamp_ms"], prev_ts)
            prev_ts = ev.wire["timestamp_ms"]

    def test_each_error_injection_independently(self) -> None:
        for code in ERROR_CODES:
            cfg = SimulationConfig(
                min_game_duration_sec=45,
                max_game_duration_sec=75,
                game_events_target=40,
                error_rates={c: (100 if c == code else 0) for c in ERROR_CODES},
            )
            timeline, injected = generate_game_plan(str(uuid4()), None, cfg)
            self.assertEqual(injected, [code])

            wires = [ev.wire for ev in timeline]
            if code == "MISSING_GAME_STARTED":
                self.assertNotEqual(wires[0]["event_type"], EVENT_GAME_STARTED)
            elif code == "MISSING_ROUND_START":
                self.assertFalse(
                    any(w["event_type"] == EVENT_ROUND_START for w in wires)
                )
            elif code == "INDEX_GAP":
                indices = [w["event_index"] for w in wires]
                self.assertNotEqual(indices, list(range(len(wires))))
            elif code == "UNORDERED_TIMESTAMP":
                timestamps = [w["timestamp_ms"] for w in wires]
                self.assertNotEqual(timestamps, sorted(timestamps))
            elif code == "MISSING_HERO_SLOTS":
                round_idx = next(
                    i
                    for i, w in enumerate(wires)
                    if w["event_type"] == EVENT_ROUND_START
                )
                hero_slots = {
                    w["payload"]["slot"]
                    for w in wires[:round_idx]
                    if w["event_type"] == EVENT_HERO_SELECTED
                }
                self.assertLess(len(hero_slots), 10)
            elif code == "DUPLICATE_HERO_SLOT":
                round_idx = next(
                    i
                    for i, w in enumerate(wires)
                    if w["event_type"] == EVENT_ROUND_START
                )
                hero_picks = [
                    w["payload"]["slot"]
                    for w in wires[:round_idx]
                    if w["event_type"] == EVENT_HERO_SELECTED
                ]
                self.assertGreater(len(hero_picks), len(set(hero_picks)))
            elif code == "HERO_AFTER_ROUND_START":
                round_idx = next(
                    i
                    for i, w in enumerate(wires)
                    if w["event_type"] == EVENT_ROUND_START
                )
                self.assertTrue(
                    any(
                        w["event_type"] == EVENT_HERO_SELECTED
                        for w in wires[round_idx + 1 :]
                    )
                )
            elif code == "INVALID_SLOT":
                has_invalid = any(
                    w["payload"].get("slot", 0) >= 10
                    or w["payload"].get("killer_slot", 0) >= 10
                    for w in wires
                )
                self.assertTrue(has_invalid)


class TestControllerAndEndToEnd(unittest.TestCase):
    def test_controller_api_and_e2e_swarm(self) -> None:
        import gevent
        from gevent.pywsgi import WSGIServer
        from flask import Flask, jsonify, request
        from simulation.load_tester.controller import (
            LoadController,
            create_app,
            find_available_port,
        )

        # 1. Spin up a lightweight mock ObserveLoL ingest server on a local port
        ingested_events = []
        mock_ingest = Flask("mock_ingest")

        @mock_ingest.get("/api/new-game/")
        def new_game():
            return jsonify(
                {
                    "game_id": str(uuid4()),
                    "snapshot": {
                        "heroes": [{"hero_id": 10 + i, "version_id": 2} for i in range(10)],
                        "items": [{"item_id": 50 + i, "version_id": 3} for i in range(10)],
                    },
                }
            )

        @mock_ingest.post("/api/event/")
        def ingest_event():
            ingested_events.append(request.get_json())
            return ("", 200)

        mock_port = find_available_port(8100, 8999)
        mock_server = WSGIServer(("127.0.0.1", mock_port), mock_ingest, log=None)
        mock_server.start()

        try:
            controller = LoadController()
            app = create_app(controller)
            client = app.test_client()

            # Verify dashboard HTML loads
            res_html = client.get("/")
            self.assertEqual(res_html.status_code, 200)
            self.assertIn(b"ObserveLoL Load Simulation Controller", res_html.data)

            # Verify config update and estimate calculation
            res_cfg = client.post(
                "/api/config",
                json={
                    "target_host": f"http://127.0.0.1:{mock_port}",
                    "producers": 2,
                    "min_game_duration_sec": 30,
                    "max_game_duration_sec": 30,
                    "game_events_target": 20,
                    "min_items_per_hero": 0,
                    "max_items_per_hero": 1,
                },
            )
            self.assertEqual(res_cfg.status_code, 200)
            cfg_data = res_cfg.get_json()
            self.assertEqual(cfg_data["estimates"]["hero_select_sec"], 5)
            self.assertEqual(cfg_data["estimates"]["games_per_minute"], 4.0)

            # Start swarm against local mock server (patch one-time startup delay for fast test)
            with unittest.mock.patch(
                "simulation.load_tester.producer.random.uniform", return_value=0.05
            ) as mock_uniform:
                res_start = client.post("/api/swarm/start")
                self.assertEqual(res_start.status_code, 200)

                # Let producers finish one-time delay, initialize games, and send initial events
                gevent.sleep(0.8)
                mock_uniform.assert_called_with(0.0, 30.0)
                self.assertEqual(mock_uniform.call_count, 2)

            res_tel = client.get("/api/telemetry")
            self.assertEqual(res_tel.status_code, 200)
            tel_data = res_tel.get_json()
            self.assertIn(tel_data["state"], ("SPAWNING", "RUNNING"))
            self.assertGreaterEqual(tel_data["games"]["started"], 2)
            self.assertGreater(len(ingested_events), 0)
            self.assertEqual(tel_data["failed_requests"], 0)

            # Stop swarm and verify immediate interrupt of sleeping producers
            res_stop = client.post("/api/swarm/stop")
            self.assertEqual(res_stop.status_code, 200)
            gevent.sleep(0.3)

            res_tel_after = client.get("/api/telemetry").get_json()
            self.assertEqual(res_tel_after["state"], "IDLE")
            self.assertEqual(res_tel_after["games"]["active"], 0)
        finally:
            mock_server.stop()


if __name__ == "__main__":
    unittest.main()

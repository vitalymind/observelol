from gevent import monkey

monkey.patch_all()

import argparse
import json
import os
import random
import socket
import sys
import time
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import gevent
from gevent.pywsgi import WSGIServer
from flask import Flask, Response, jsonify, request, send_from_directory
import psutil
from locust.env import Environment
from locust.runners import (
    STATE_INIT,
    STATE_SPAWNING,
    STATE_RUNNING,
    STATE_CLEANUP,
    STATE_STOPPING,
    STATE_STOPPED,
)
from locust.stats import stats_history

from simulation.load_tester.config import SimulationConfig
from simulation.load_tester.producer import GameProducerUser, RUNTIME

STATIC_DIR = Path(__file__).resolve().parent / "static"


def map_runner_state(raw_state: str | None) -> str:
    if raw_state in (None, STATE_INIT, STATE_STOPPED):
        return "IDLE"
    if raw_state == STATE_SPAWNING:
        return "SPAWNING"
    if raw_state == STATE_RUNNING:
        return "RUNNING"
    if raw_state in (STATE_STOPPING, STATE_CLEANUP):
        return "STOPPING"
    return str(raw_state).upper()


class LoadController:
    def __init__(self) -> None:
        self.env = Environment(
            user_classes=[GameProducerUser],
            host=RUNTIME.config.target_host,
        )
        self.runner = self.env.create_local_runner()
        self.stats_greenlet = gevent.spawn(stats_history, self.runner)
        self.process = psutil.Process(os.getpid())
        # Prime psutil CPU measurement
        self.process.cpu_percent(interval=None)
        self._last_cpu_time = time.monotonic()
        self._cached_cpu_percent = 0.0

    def apply_config(self, payload: Dict[str, Any]) -> SimulationConfig:
        prev_producers = RUNTIME.config.producers
        new_cfg = SimulationConfig.from_dict(payload, base=RUNTIME.config)
        RUNTIME.config = new_cfg
        self.env.host = new_cfg.target_host
        GameProducerUser.host = new_cfg.target_host

        # If swarm is active and producer count changed, dynamically scale runner
        state = map_runner_state(self.runner.state)
        if state in ("SPAWNING", "RUNNING") and new_cfg.producers != prev_producers:
            spawn_rate = max(1.0, float(new_cfg.producers))
            self.runner.start(user_count=new_cfg.producers, spawn_rate=spawn_rate)

        return new_cfg

    def start_swarm(self, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if payload:
            self.apply_config(payload)

        cfg = RUNTIME.config
        self.env.host = cfg.target_host
        GameProducerUser.host = cfg.target_host

        spawn_rate = max(1.0, float(cfg.producers))
        self.runner.start(user_count=cfg.producers, spawn_rate=spawn_rate)
        return self.get_telemetry()

    def stop_swarm(self) -> Dict[str, Any]:
        if map_runner_state(self.runner.state) != "IDLE":
            self.runner.stop()
        RUNTIME.reset_producers()
        return self.get_telemetry()

    def reset_stats(self) -> Dict[str, Any]:
        self.env.stats.reset_all()
        RUNTIME.counters.reset()
        if map_runner_state(self.runner.state) == "IDLE":
            RUNTIME.reset_producers()
        return self.get_telemetry()

    def _get_process_resources(self) -> Dict[str, float]:
        now = time.monotonic()
        if now - self._last_cpu_time >= 0.5:
            self._cached_cpu_percent = self.process.cpu_percent(interval=None)
            self._last_cpu_time = now

        mem_info = self.process.memory_info()
        return {
            "cpu_percent": round(self._cached_cpu_percent, 1),
            "memory_mb": round(mem_info.rss / (1024 * 1024), 1),
        }

    def get_telemetry(self) -> Dict[str, Any]:
        total = self.env.stats.total
        num_reqs = total.num_requests
        num_fails = total.num_failures
        num_success = max(0, num_reqs - num_fails)
        fail_rate_pct = (num_fails / num_reqs * 100.0) if num_reqs > 0 else 0.0

        endpoints = []
        for (name, method), entry in sorted(self.env.stats.entries.items()):
            ep_reqs = entry.num_requests
            ep_fails = entry.num_failures
            endpoints.append(
                {
                    "method": method,
                    "name": name,
                    "num_requests": ep_reqs,
                    "num_failures": ep_fails,
                    "current_rps": round(entry.current_rps, 2),
                    "avg_response_time_ms": round(entry.avg_response_time, 1),
                    "p95_response_time_ms": round(
                        entry.get_response_time_percentile(0.95) or 0.0, 1
                    ),
                }
            )

        return {
            "timestamp": time.time(),
            "state": map_runner_state(self.runner.state),
            "active_producers": self.runner.user_count,
            "target_producers": RUNTIME.config.producers,
            "current_rps": round(total.current_rps, 2),
            "current_fail_per_sec": round(total.current_fail_per_sec, 2),
            "failure_rate_pct": round(fail_rate_pct, 2),
            "total_requests": num_reqs,
            "successful_requests": num_success,
            "failed_requests": num_fails,
            "avg_response_time_ms": round(total.avg_response_time, 1),
            "p95_response_time_ms": round(
                total.get_response_time_percentile(0.95) or 0.0, 1
            ),
            "games": {
                "active": RUNTIME.counters.active_games,
                "started": RUNTIME.counters.games_started,
                "completed": RUNTIME.counters.games_completed,
                "aborted": RUNTIME.counters.games_aborted,
                "with_injected_errors": RUNTIME.counters.games_with_injected_errors,
                "injected_by_code": dict(RUNTIME.counters.injected_by_code),
            },
            "producer_states": RUNTIME.get_producer_states(),
            "process": self._get_process_resources(),
            "endpoints": endpoints,
            "estimates": RUNTIME.config.calculate_estimates(),
        }


def find_available_port(start: int = 8100, end: int = 8999) -> int:
    candidates = list(range(start, end + 1))
    random.shuffle(candidates)
    for port in candidates:
        if port == 3000:
            continue
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free port found in range {start}-{end}")


def create_app(controller: LoadController) -> Flask:
    app = Flask(__name__, static_folder=str(STATIC_DIR))

    @app.get("/")
    def index() -> Any:
        return send_from_directory(app.static_folder, "index.html")

    @app.get("/api/config")
    def get_config() -> Any:
        return jsonify(RUNTIME.config.to_dict())

    @app.post("/api/config")
    def update_config() -> Any:
        payload = request.get_json(silent=True) or {}
        cfg = controller.apply_config(payload)
        return jsonify(cfg.to_dict())

    @app.post("/api/swarm/start")
    def start_swarm() -> Any:
        payload = request.get_json(silent=True)
        telemetry = controller.start_swarm(
            payload if isinstance(payload, dict) else None
        )
        return jsonify({"config": RUNTIME.config.to_dict(), "telemetry": telemetry})

    @app.post("/api/swarm/stop")
    def stop_swarm() -> Any:
        telemetry = controller.stop_swarm()
        return jsonify({"config": RUNTIME.config.to_dict(), "telemetry": telemetry})

    @app.post("/api/swarm/reset")
    def reset_swarm_stats() -> Any:
        telemetry = controller.reset_stats()
        return jsonify({"telemetry": telemetry})

    @app.get("/api/telemetry")
    def get_telemetry() -> Any:
        return jsonify(controller.get_telemetry())

    @app.get("/api/telemetry/stream")
    def stream_telemetry() -> Response:
        def event_stream():
            while True:
                data = json.dumps(controller.get_telemetry())
                yield f"data: {data}\n\n"
                gevent.sleep(1.0)

        return Response(
            event_stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ObserveLoL Locust Load Simulation Controller"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address for the controller web server (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port to bind (default: randomly selected available port in 8100-8999)",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="Override target ObserveLoL URL (default: https://observelol.vbrokh.dev)",
    )
    args = parser.parse_args()

    if args.target:
        RUNTIME.config.target_host = args.target.rstrip("/")

    port = args.port if args.port > 0 else find_available_port(8100, 8999)
    controller = LoadController()
    app = create_app(controller)

    url = f"http://{args.host}:{port}"
    print("=" * 64)
    print(" ObserveLoL Locust Load Simulation Controller")
    print(f" Dashboard URL : {url}")
    print(f" Target Host   : {RUNTIME.config.target_host}")
    print("=" * 64)

    server = WSGIServer((args.host, port), app, log=None)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down controller and stopping active swarm...")
        controller.stop_swarm()


if __name__ == "__main__":
    main()

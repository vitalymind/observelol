from gevent import monkey

monkey.patch_all()

import argparse
import os
import signal
import sys
from pathlib import Path
import psutil
import gevent
from locust.env import Environment
from locust.runners import WorkerRunner

REPO_ROOT = str(Path(__file__).resolve().parents[2])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from simulation.load_tester.config import SimulationConfig
from simulation.load_tester.producer import GameProducerUser, RUNTIME


def telemetry_loop(runner: WorkerRunner) -> None:
    """
    Batched 1 Hz worker heartbeat.
    Periodically sends local game counters, active producer progress values,
    and process CPU/RAM to the Master runner via ZeroMQ.
    """
    proc = psutil.Process(os.getpid())
    proc.cpu_percent(interval=None)  # prime psutil CPU measurement

    while True:
        gevent.sleep(1.0)
        try:
            mem_mb = proc.memory_info().rss / (1024 * 1024)
            cpu_pct = proc.cpu_percent(interval=None)
            payload = {
                "counters": {
                    "active_games": RUNTIME.counters.active_games,
                    "games_started": RUNTIME.counters.games_started,
                    "games_completed": RUNTIME.counters.games_completed,
                    "games_aborted": RUNTIME.counters.games_aborted,
                    "games_with_injected_errors": RUNTIME.counters.games_with_injected_errors,
                    "injected_by_code": dict(RUNTIME.counters.injected_by_code),
                },
                "producer_states": RUNTIME.get_producer_states(),
                "process": {
                    "cpu_percent": round(cpu_pct, 1),
                    "memory_mb": round(mem_mb, 1),
                    "pid": os.getpid(),
                },
            }
            runner.send_message("worker_telemetry", payload)
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="ObserveLoL Locust Load Worker")
    parser.add_argument("--master-host", default="127.0.0.1", help="Master host address")
    parser.add_argument("--master-port", type=int, default=5557, help="Master ZeroMQ port")
    args = parser.parse_args()

    env = Environment(
        user_classes=[GameProducerUser],
        host=RUNTIME.config.target_host,
    )
    runner = env.create_worker_runner(
        master_host=args.master_host,
        master_port=args.master_port,
    )

    def on_sync_config(environment, msg, **kwargs):
        if msg.data:
            RUNTIME.config = SimulationConfig.from_dict(msg.data, base=RUNTIME.config)
            GameProducerUser.host = RUNTIME.config.target_host
            environment.host = RUNTIME.config.target_host

    def on_reset_counters(environment, msg, **kwargs):
        RUNTIME.counters.reset()
        RUNTIME.reset_producers()

    def on_stop_swarm(environment, msg, **kwargs):
        RUNTIME.reset_producers()

    runner.register_message("sync_config", on_sync_config)
    runner.register_message("reset_counters", on_reset_counters)
    runner.register_message("stop_swarm", on_stop_swarm)

    gevent.spawn(telemetry_loop, runner)

    def sig_handler(*_):
        runner.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    try:
        runner.greenlet.join()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.yml"
LOCAL_PORT = 3000
REMOTE_PORT = 3000
RECONNECT_DELAY_SEC = 5


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        else:
            proc.terminate()
            proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def main() -> None:
    if not CONFIG_PATH.exists():
        print(f"Config file not found: {CONFIG_PATH}")
        sys.exit(1)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    project_id = config["GCP_PROJECT_ID"]
    region = config["GCP_REGION"]
    zone_letter = config["infra_definitions"]["zone"]
    vm_name = config["infra_definitions"]["control_vm"]["name"]
    zone = f"{region}-{zone_letter}"

    gcloud = shutil.which("gcloud.cmd") or shutil.which("gcloud") or "gcloud"

    cmd = [
        gcloud,
        "compute",
        "ssh",
        vm_name,
        f"--project={project_id}",
        f"--zone={zone}",
        "--tunnel-through-iap",
        f"--ssh-flag=-L {LOCAL_PORT}:127.0.0.1:{REMOTE_PORT}",
        "--command=while true; do sleep 30; done",
    ]

    print(f"Target SSH IAP tunnel: {vm_name}:{REMOTE_PORT} ({zone})")
    print(f"Grafana UI:            http://localhost:{LOCAL_PORT}")
    print("Press Ctrl+C to close the tunnel.\n")

    proc = None
    try:
        while True:
            try:
                print("Starting multiplexed SSH tunnel...")
                proc = subprocess.Popen(cmd)
                return_code = proc.wait()
                print(
                    f"\nSSH tunnel exited (code {return_code}). "
                    f"Reconnecting in {RECONNECT_DELAY_SEC}s..."
                )
            except OSError as err:
                print(
                    f"\nFailed to launch gcloud ({err}). "
                    f"Retrying in {RECONNECT_DELAY_SEC}s..."
                )
            finally:
                if proc is not None:
                    stop_process(proc)
                    proc = None

            time.sleep(RECONNECT_DELAY_SEC)
    except KeyboardInterrupt:
        if proc is not None:
            stop_process(proc)
        print("\nTunnel closed gracefully.")


if __name__ == "__main__":
    main()

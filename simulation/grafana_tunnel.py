from pathlib import Path
import shutil
import subprocess
import sys
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.yml"
LOCAL_PORT = 3000
REMOTE_PORT = 3000


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

    gcloud = shutil.which("gcloud") or "gcloud"

    cmd = [
        gcloud,
        "compute",
        "start-iap-tunnel",
        vm_name,
        str(REMOTE_PORT),
        f"--project={project_id}",
        f"--zone={zone}",
        f"--local-host-port=localhost:{LOCAL_PORT}",
    ]

    print(f"Opening IAP tunnel to {vm_name}:{REMOTE_PORT} ({zone})...")
    print(f"Grafana UI: http://localhost:{LOCAL_PORT}")
    print("Press Ctrl+C to close the tunnel.\n")

    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\nTunnel closed.")


if __name__ == "__main__":
    main()

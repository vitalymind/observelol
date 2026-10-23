import os
import shutil
import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_APIS = os.getenv("GCP_REQUIRED_APIS")

if not PROJECT_ID:
    print("❌ Missing required environment variable: GCP_PROJECT_ID")
    sys.exit(1)
    
if not RAW_APIS:
    print("❌ Missing required environment variable: GCP_REQUIRED_APIS")
    sys.exit(1)


def parse_api_list(raw: str) -> list[str]:
    raw = raw.strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    return [
        api.strip(" \t\r\n'\"") for api in raw.split(",") if api.strip(" \t\r\n'\"")
    ]


required_apis = parse_api_list(RAW_APIS)

if not required_apis:
    print("❌ No APIs specified in GCP_REQUIRED_APIS")
    sys.exit(1)

GCLOUD = shutil.which("gcloud") or "gcloud"


def run_cmd(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run([GCLOUD, *args], capture_output=True, text=True, check=check)


print(f"--- Checking GCP APIs for project '{PROJECT_ID}' ---")
enabled_proc = run_cmd(
    [
        "services",
        "list",
        "--enabled",
        f"--project={PROJECT_ID}",
        "--format=value(config.name)",
    ]
)
enabled_apis = enabled_proc.stdout.split()

for api in required_apis:
    if api in enabled_apis:
        print(f"✅ API '{api}' is enabled.")
    else:
        print(f"Enabling API '{api}'...")
        res = run_cmd(
            ["services", "enable", api, f"--project={PROJECT_ID}"], check=False
        )
        if res.returncode == 0:
            print(f"✅ Enabled '{api}'.")
        else:
            print(f"❌ Failed to enable '{api}': {res.stderr}")
            sys.exit(1)

import sys
from common import require_env, run_cmd

(
    PROJECT_ID,
    RAW_APIS,
) = require_env(
    "GCP_PROJECT_ID",
    "GCP_REQUIRED_APIS",
)


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

import sys
from common import load_config, run_cmd

(
    PROJECT_ID,
    APIS,
) = load_config(["GCP_PROJECT_ID", "GCP_REQUIRED_APIS"])

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

for api in APIS:
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

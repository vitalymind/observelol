import sys

from common import require_env, run_cmd

(
    PROJECT_ID,
    BUILD_SA_NAME,
) = require_env(
    "GCP_PROJECT_ID",
    "GCP_BUILD_SA_NAME",
)

print(f"--- Setting up Build Service Account ({BUILD_SA_NAME}) ---")
build_sa_email = f"{BUILD_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

check_sa = run_cmd(
    [
        "iam",
        "service-accounts",
        "describe",
        build_sa_email,
        f"--project={PROJECT_ID}",
        "--format=json",
    ],
    check=False,
)

if check_sa.returncode == 0:
    print(f"✅ Service Account '{build_sa_email}' already exists.")
else:
    print(f"Creating Service Account '{build_sa_email}'...")
    create_sa = run_cmd(
        [
            "iam",
            "service-accounts",
            "create",
            BUILD_SA_NAME,
            "--display-name=Cloud Build Runner SA",
            f"--project={PROJECT_ID}",
        ],
        check=False,
    )
    if create_sa.returncode != 0:
        print(f"❌ Failed to create Service Account: {create_sa.stderr}")
        sys.exit(1)
    print(f"✅ Created Service Account '{build_sa_email}'.")

# Grant Cloud Logging writer so builds can write logs
print("Granting roles/logging.logWriter to Build Service Account...")
give_rights = run_cmd(
    [
        "projects",
        "add-iam-policy-binding",
        PROJECT_ID,
        f"--member=serviceAccount:{build_sa_email}",
        "--role=roles/logging.logWriter",
        "--condition=None",
    ]
)

if give_rights.returncode == 0:
    print("✅ Granted logging permissions.")
else:
    print(f"❌ Failed to grant logging rights: {give_rights.stderr}")

import sys
import time
from common import require_env, run_cmd

(
    PROJECT_ID,
    DEPLOY_SA_NAME,
) = require_env(
    "GCP_PROJECT_ID",
    "GCP_DEPLOY_SA_NAME",
)

print(f"--- Setting up GitHub Deployer Service Account ({DEPLOY_SA_NAME}) ---")
deploy_sa_email = f"{DEPLOY_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

# 1. Check if Service Account exists
check_sa = run_cmd(
    [
        "iam",
        "service-accounts",
        "describe",
        deploy_sa_email,
        f"--project={PROJECT_ID}",
        "--format=json",
    ],
    check=False,
)

if check_sa.returncode == 0:
    print(f"✅ Service Account '{deploy_sa_email}' already exists.")
else:
    print(f"Creating Service Account '{deploy_sa_email}'...")
    create_sa = run_cmd(
        [
            "iam",
            "service-accounts",
            "create",
            DEPLOY_SA_NAME,
            "--display-name=GitHub Actions Deployer SA",
            f"--project={PROJECT_ID}",
        ],
        check=False,
    )
    if create_sa.returncode != 0:
        print(f"❌ Failed to create Service Account: {create_sa.stderr}")
        sys.exit(1)
    print(f"✅ Created Service Account '{deploy_sa_email}'.")

# 2. Assign IAM roles to the Service Account (with retry for IAM propagation)
ROLES = [
    "roles/storage.admin",
    "roles/compute.admin",
    "roles/artifactregistry.admin",
    "roles/iam.serviceAccountUser",
]

MAX_RETRIES = 6
RETRY_DELAY_SEC = 5

print(f"Ensuring IAM roles for '{deploy_sa_email}'...")
for role in ROLES:
    for attempt in range(1, MAX_RETRIES + 1):
        grant_res = run_cmd(
            [
                "projects",
                "add-iam-policy-binding",
                PROJECT_ID,
                f"--member=serviceAccount:{deploy_sa_email}",
                f"--role={role}",
                "--condition=None",
            ],
            check=False,
        )
        if grant_res.returncode == 0:
            print(f"✅ Granted {role}.")
            break

        if "does not exist" in grant_res.stderr and attempt < MAX_RETRIES:
            print(
                f"⏳ Waiting for Service Account propagation "
                f"(attempt {attempt}/{MAX_RETRIES}, retrying in {RETRY_DELAY_SEC}s)..."
            )
            time.sleep(RETRY_DELAY_SEC)
        else:
            print(f"❌ Failed to grant {role}: {grant_res.stderr}")
            sys.exit(1)

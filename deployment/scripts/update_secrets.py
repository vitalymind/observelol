import os
from pathlib import Path
import subprocess
import sys
from common import GCLOUD, load_config, run_cmd

(
    PROJECT_ID,
    DEPLOY_SA_NAME,
    VM_SA_NAME,
) = load_config(["gcp_project_id", "gcp_deploy_sa_name", "gcp_vm_sa_name"])

ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"

if not ENV_FILE.exists():
    print(
        f"❌ .env file not found at {ENV_FILE}. Please create one from .env.template first."
    )
    sys.exit(1)

# Mapping of .env variable names to Secret Manager secret IDs
SECRETS_TO_SYNC = {
    "POSTGRES_PASSWORD": "postgres-password",
}

print(
    f"--- Synchronizing secrets from .env to Google Secret Manager for project '{PROJECT_ID}' ---"
)

deploy_sa_email = f"{DEPLOY_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
vm_sa_email = f"{VM_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

for env_var, secret_id in SECRETS_TO_SYNC.items():
    secret_value = os.getenv(env_var)
    if not secret_value:
        print(f"⚠️  Variable '{env_var}' is empty or not set in .env; skipping.")
        continue

    print(f"Processing secret '{secret_id}'...")

    # 1. Ensure the secret resource exists in Secret Manager
    check_secret = run_cmd(
        ["secrets", "describe", secret_id, f"--project={PROJECT_ID}"],
        check=False,
    )

    if check_secret.returncode != 0:
        print(f"Creating secret '{secret_id}' in Secret Manager...")
        create_res = run_cmd(
            [
                "secrets",
                "create",
                secret_id,
                "--replication-policy=automatic",
                f"--project={PROJECT_ID}",
            ],
            check=False,
        )
        if create_res.returncode != 0:
            print(f"❌ Failed to create secret '{secret_id}': {create_res.stderr}")
            sys.exit(1)
        print(f"✅ Created secret '{secret_id}'.")
    else:
        print(f"✅ Secret '{secret_id}' exists.")

    # 2. Add secret version via stdin (avoids leaking password in process args)
    print(f"Adding new version payload for '{secret_id}'...")
    add_version_proc = subprocess.run(
        [
            GCLOUD,
            "--quiet",
            "secrets",
            "versions",
            "add",
            secret_id,
            f"--project={PROJECT_ID}",
            "--data-file=-",
        ],
        input=secret_value,
        text=True,
        capture_output=True,
    )

    if add_version_proc.returncode != 0:
        print(f"❌ Failed to add secret version: {add_version_proc.stderr}")
        sys.exit(1)
    print(f"✅ Updated version for '{secret_id}'.")

    # 3. Ensure both Deployer SA and VM SA have accessor rights to this secret
    for sa_email in [deploy_sa_email, vm_sa_email]:
        run_cmd(
            [
                "secrets",
                "add-iam-policy-binding",
                secret_id,
                f"--member=serviceAccount:{sa_email}",
                "--role=roles/secretmanager.secretAccessor",
                f"--project={PROJECT_ID}",
            ],
            check=False,
        )
        print(f"✅ Granted Secret Accessor on '{secret_id}' to {sa_email}")

print("✅ Secrets synchronization complete.")

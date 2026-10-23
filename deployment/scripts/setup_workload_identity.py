import sys
from common import require_env, run_cmd

(
    PROJECT_ID,
    PROJECT_NUMBER,
    GITHUB_OWNER,
    GITHUB_REPO,
    POOL_ID,
    PROVIDER_ID,
    DEPLOY_SA_NAME,
) = require_env(
    "GCP_PROJECT_ID",
    "GCP_PROJECT_NUMBER",
    "GITHUB_OWNER",
    "GITHUB_REPO",
    "GCP_WORKLOAD_IDENTITY_POOL",
    "GCP_WORKLOAD_IDENTITY_PROVIDER",
    "GCP_DEPLOY_SA_NAME",
)

deploy_sa_email = f"{DEPLOY_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
repo_path = f"{GITHUB_OWNER}/{GITHUB_REPO}"

print(f"--- Setting up Workload Identity Federation for '{repo_path}' ---")

# 1. Workload Identity Pool
check_pool = run_cmd(
    [
        "iam",
        "workload-identity-pools",
        "describe",
        POOL_ID,
        "--location=global",
        f"--project={PROJECT_ID}",
        "--format=value(state)",
    ],
    check=False,
)

pool_state = check_pool.stdout.strip() if check_pool.returncode == 0 else None

if pool_state == "ACTIVE":
    print(f"✅ Workload Identity Pool '{POOL_ID}' already exists and is ACTIVE.")
elif pool_state == "DELETED":
    print(f"Restoring soft-deleted Workload Identity Pool '{POOL_ID}'...")
    undelete_pool = run_cmd(
        [
            "iam",
            "workload-identity-pools",
            "undelete",
            POOL_ID,
            "--location=global",
            f"--project={PROJECT_ID}",
        ],
        check=False,
    )
    if undelete_pool.returncode != 0:
        print(f"❌ Failed to restore pool: {undelete_pool.stderr}")
        sys.exit(1)
    print(f"✅ Restored Workload Identity Pool '{POOL_ID}'.")
else:
    print(f"Creating Workload Identity Pool '{POOL_ID}'...")
    create_pool = run_cmd(
        [
            "iam",
            "workload-identity-pools",
            "create",
            POOL_ID,
            "--location=global",
            f"--project={PROJECT_ID}",
            "--display-name=GitHub Actions Pool",
        ],
        check=False,
    )
    if create_pool.returncode != 0:
        print(f"❌ Failed to create pool: {create_pool.stderr}")
        sys.exit(1)
    print(f"✅ Created Workload Identity Pool '{POOL_ID}'.")

# 2. OIDC Provider for GitHub Actions
check_provider = run_cmd(
    [
        "iam",
        "workload-identity-pools",
        "providers",
        "describe",
        PROVIDER_ID,
        f"--workload-identity-pool={POOL_ID}",
        "--location=global",
        f"--project={PROJECT_ID}",
        "--format=value(state)",
    ],
    check=False,
)

provider_state = (
    check_provider.stdout.strip() if check_provider.returncode == 0 else None
)

if provider_state == "ACTIVE":
    print(f"✅ OIDC Provider '{PROVIDER_ID}' already exists and is ACTIVE.")
elif provider_state == "DELETED":
    print(f"Restoring soft-deleted OIDC Provider '{PROVIDER_ID}'...")
    undelete_provider = run_cmd(
        [
            "iam",
            "workload-identity-pools",
            "providers",
            "undelete",
            PROVIDER_ID,
            f"--workload-identity-pool={POOL_ID}",
            "--location=global",
            f"--project={PROJECT_ID}",
        ],
        check=False,
    )
    if undelete_provider.returncode != 0:
        print(f"❌ Failed to restore provider: {undelete_provider.stderr}")
        sys.exit(1)
    print(f"✅ Restored OIDC Provider '{PROVIDER_ID}'.")
else:
    print(f"Creating OIDC Provider '{PROVIDER_ID}'...")
    create_provider = run_cmd(
        [
            "iam",
            "workload-identity-pools",
            "providers",
            "create-oidc",
            PROVIDER_ID,
            f"--workload-identity-pool={POOL_ID}",
            "--location=global",
            f"--project={PROJECT_ID}",
            "--issuer-uri=https://token.actions.githubusercontent.com",
            "--attribute-mapping=google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner",
            f"--attribute-condition=assertion.repository_owner == '{GITHUB_OWNER}'",
        ],
        check=False,
    )
    if create_provider.returncode != 0:
        print(f"❌ Failed to create provider: {create_provider.stderr}")
        sys.exit(1)
    print(f"✅ Created OIDC Provider '{PROVIDER_ID}'.")

# 3. Allow GitHub Repository to impersonate the Service Account
print(f"Binding GitHub repo '{repo_path}' to Service Account '{deploy_sa_email}'...")
member_principal = (
    f"principalSet://iam.googleapis.com/projects/{PROJECT_NUMBER}"
    f"/locations/global/workloadIdentityPools/{POOL_ID}/attribute.repository/{repo_path}"
)

bind_res = run_cmd(
    [
        "iam",
        "service-accounts",
        "add-iam-policy-binding",
        deploy_sa_email,
        f"--project={PROJECT_ID}",
        "--role=roles/iam.workloadIdentityUser",
        f"--member={member_principal}",
        "--condition=None",
    ],
    check=False,
)

if bind_res.returncode == 0:
    print("✅ Service Account bound to Workload Identity Pool.")
else:
    print(f"❌ Failed to bind Workload Identity: {bind_res.stderr}")
    sys.exit(1)

# Summary of settings for GitHub Actions workflow
provider_resource_name = f"projects/{PROJECT_NUMBER}/locations/global/workloadIdentityPools/{POOL_ID}/providers/{PROVIDER_ID}"

# Sets repository variables in GitHub Actions using GitHub CLI ('gh')

import shutil
import subprocess
import sys
from common import require_env

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

repo_name = f"{GITHUB_OWNER}/{GITHUB_REPO}"
provider_val = f"projects/{PROJECT_NUMBER}/locations/global/workloadIdentityPools/{POOL_ID}/providers/{PROVIDER_ID}"
sa_val = f"{DEPLOY_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

variables_to_set = {
    "GCP_WORKLOAD_IDENTITY_PROVIDER": provider_val,
    "GCP_SERVICE_ACCOUNT": sa_val,
    "GCP_PROJECT_ID": PROJECT_ID,
}

print(f"--- Setting GitHub Actions Variables for repository '{repo_name}' ---")

gh_bin = shutil.which("gh")

# Check if gh CLI is installed and authenticated
gh_authenticated = False
if gh_bin:
    auth_check = subprocess.run(
        [gh_bin, "auth", "status"], capture_output=True, text=True
    )
    if auth_check.returncode == 0:
        gh_authenticated = True

if gh_authenticated:
    for var_name, var_value in variables_to_set.items():
        print(f"Setting GitHub variable '{var_name}'...")
        res = subprocess.run(
            [
                gh_bin,
                "variable",
                "set",
                var_name,
                "--body",
                var_value,
                "--repo",
                repo_name,
            ],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0:
            print(f"✅ Set {var_name}")
        else:
            print(f"❌ Failed to set {var_name}: {res.stderr.strip()}")
            sys.exit(1)
    print("\n✅ All GitHub Actions variables configured successfully!")
else:
    print("⚠️ GitHub CLI (`gh`) is not logged in.")
    print(f"Please run 'gh auth login' or manually add these variables at:")
    print(f"👉 https://github.com/{repo_name}/settings/variables/actions\n")
    for var_name, var_value in variables_to_set.items():
        print(f"  {var_name}:")
        print(f"    {var_value}\n")

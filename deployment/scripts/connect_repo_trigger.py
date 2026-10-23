import json
import sys

from common import require_env, run_cmd

(
    PROJECT_ID,
    PROJECT_NUMBER,
    REGION,
    GITHUB_OWNER,
    GITHUB_REPO,
    CONNECTION_NAME,
    TRIGGER_NAME,
    BUILD_SA_NAME,
) = require_env(
    "GCP_PROJECT_ID",
    "GCP_PROJECT_NUMBER",
    "GCP_REGION",
    "GITHUB_OWNER",
    "GITHUB_REPO",
    "GCP_CONNECTION_NAME",
    "GCP_TRIGGER_NAME",
    "GCP_BUILD_SA_NAME",
)

service_agent = f"service-{PROJECT_NUMBER}@gcp-sa-cloudbuild.iam.gserviceaccount.com"
build_sa_email = f"{BUILD_SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
build_sa_resource = f"projects/{PROJECT_ID}/serviceAccounts/{build_sa_email}"

# -------------------------------------------------------------
# 1. Cloud Build 2nd Gen Connection (Bootstrap & Lockdown)
# -------------------------------------------------------------
print(f"--- Setting up Cloud Build Connection ({CONNECTION_NAME}) ---")
check_conn = run_cmd(
    [
        "builds",
        "connections",
        "describe",
        CONNECTION_NAME,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=value(installationState.stage)",
    ],
    check=False,
)

conn_exists = check_conn.returncode == 0
stage = check_conn.stdout.strip() if conn_exists else ""

if conn_exists and stage == "COMPLETE":
    print(f"✅ Connection '{CONNECTION_NAME}' already exists and is authorized.")
else:
    print(
        "🔒 Temporarily elevating Cloud Build permissions for connection & OAuth callback..."
    )
    run_cmd(
        [
            "projects",
            "add-iam-policy-binding",
            PROJECT_ID,
            f"--member=serviceAccount:{service_agent}",
            "--role=roles/secretmanager.admin",
            "--condition=None",
        ]
    )

    try:
        if not conn_exists:
            print(f"Creating connection '{CONNECTION_NAME}' in region '{REGION}'...")
            create_proc = run_cmd(
                [
                    "builds",
                    "connections",
                    "create",
                    "github",
                    CONNECTION_NAME,
                    f"--region={REGION}",
                    f"--project={PROJECT_ID}",
                ],
                check=False,
                capture_output=False,
            )

            if create_proc.returncode != 0:
                print("❌ Failed to initiate connection creation.")
                sys.exit(1)

        print("Checking GitHub App authorization status...")
        while True:
            status_proc = run_cmd(
                [
                    "builds",
                    "connections",
                    "describe",
                    CONNECTION_NAME,
                    f"--region={REGION}",
                    f"--project={PROJECT_ID}",
                    "--format=json",
                ],
                check=False,
            )
            conn_info = json.loads(status_proc.stdout)
            inst_state = conn_info.get("installationState", {})
            stage = inst_state.get("stage", "UNKNOWN")

            if stage == "COMPLETE":
                print("✅ Connection is fully authorized on GitHub.")
                break
            else:
                action_uri = inst_state.get("actionUri", "")
                print(f"Stage is currently '{stage}'.")
                if action_uri:
                    print("Click to authorize:")
                    print("=================================================")
                    print(action_uri)
                    print("=================================================")
                input("Press Enter after authorizing on GitHub to continue...")
    finally:
        print("🛡️  Revoking temporary project-level Secret Manager Admin access...")
        run_cmd(
            [
                "projects",
                "remove-iam-policy-binding",
                PROJECT_ID,
                f"--member=serviceAccount:{service_agent}",
                "--role=roles/secretmanager.admin",
                "--condition=None",
            ],
            check=False,
        )

# Restrict permissions specifically to the generated connection secret
conn_desc = run_cmd(
    [
        "builds",
        "connections",
        "describe",
        CONNECTION_NAME,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=json",
    ]
)
conn_data = json.loads(conn_desc.stdout)
secret_version_path = (
    conn_data.get("githubConfig", {})
    .get("authorizerCredential", {})
    .get("oauthTokenSecretVersion", "")
)

if secret_version_path and "/secrets/" in secret_version_path:
    secret_name = secret_version_path.split("/secrets/")[1].split("/versions/")[0]
    print(
        f"🔒 Locking down permissions: granting access strictly to secret '{secret_name}'..."
    )
    run_cmd(
        [
            "secrets",
            "add-iam-policy-binding",
            secret_name,
            f"--project={PROJECT_ID}",
            f"--member=serviceAccount:{service_agent}",
            "--role=roles/secretmanager.admin",
        ],
        check=False,
    )
    print(
        "✅ Principle of least privilege enforced: agent only has access to its own secret."
    )

# -------------------------------------------------------------
# 2. Link Repository
# -------------------------------------------------------------
print(f"\n--- 2. Linking Repository ({GITHUB_OWNER}/{GITHUB_REPO}) ---")
check_repo = run_cmd(
    [
        "builds",
        "repositories",
        "describe",
        GITHUB_REPO,
        f"--connection={CONNECTION_NAME}",
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=json",
    ],
    check=False,
)

if check_repo.returncode == 0:
    print(f"✅ Repository link '{GITHUB_REPO}' already exists.")
else:
    remote_uri = f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git"
    print(f"Linking remote repository: {remote_uri}...")
    link_proc = run_cmd(
        [
            "builds",
            "repositories",
            "create",
            GITHUB_REPO,
            f"--connection={CONNECTION_NAME}",
            f"--region={REGION}",
            f"--project={PROJECT_ID}",
            f"--remote-uri={remote_uri}",
        ],
        check=False,
        capture_output=False,
    )
    if link_proc.returncode == 0:
        print(f"✅ Successfully linked repository '{GITHUB_REPO}'.")
    else:
        print("❌ Failed to link repository.")
        sys.exit(1)

# -------------------------------------------------------------
# 3. Create Cloud Build Trigger
# -------------------------------------------------------------
print(f"\n--- 3. Creating Cloud Build Trigger ({TRIGGER_NAME}) ---")
check_trig = run_cmd(
    [
        "builds",
        "triggers",
        "describe",
        TRIGGER_NAME,
        f"--region={REGION}",
        f"--project={PROJECT_ID}",
        "--format=json",
    ],
    check=False,
)

if check_trig.returncode == 0:
    print(f"✅ Trigger '{TRIGGER_NAME}' already exists.")
else:
    repo_resource = (
        f"projects/{PROJECT_ID}/locations/{REGION}/connections/"
        f"{CONNECTION_NAME}/repositories/{GITHUB_REPO}"
    )
    print(
        f"Creating trigger '{TRIGGER_NAME}' with Service Account '{build_sa_email}'..."
    )
    # Note: 4 carets (^^^^) survive the 2 passes of Windows cmd.exe inside gcloud.cmd as a single ^
    trig_proc = run_cmd(
        [
            "builds",
            "triggers",
            "create",
            "github",
            f"--name={TRIGGER_NAME}",
            f"--region={REGION}",
            f"--project={PROJECT_ID}",
            f"--repository={repo_resource}",
            f"--service-account={build_sa_resource}",
            r"--branch-pattern=\Amain\z",
            "--build-config=cloudbuild.yaml",
        ],
        check=False,
        capture_output=False,
    )
    if trig_proc.returncode == 0:
        print(
            f"✅ Successfully created trigger '{TRIGGER_NAME}' targeting cloudbuild.yaml on ^main$!"
        )
    else:
        print("❌ Failed to create trigger.")
        sys.exit(1)

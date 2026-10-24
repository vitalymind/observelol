import sys
from common import run_cmd, load_config

(
    PROJECT_ID,
    REGION,
    BUCKET_NAME,
) = load_config(
    ["GCP_PROJECT_ID", "GCP_REGION", "GCP_STATE_BUCKET_NAME"],
)

print(f"--- Setting up OpenTofu State Bucket: gs://{BUCKET_NAME} ---")
bucket_url = f"gs://{BUCKET_NAME}"

check_bucket = run_cmd(
    ["storage", "buckets", "describe", bucket_url, f"--project={PROJECT_ID}"],
    check=False,
)

if check_bucket.returncode == 0:
    print(f"✅ Bucket '{bucket_url}' already exists.")
else:
    print(f"Creating bucket '{bucket_url}' in region '{REGION}'...")
    create_res = run_cmd(
        [
            "storage",
            "buckets",
            "create",
            bucket_url,
            f"--project={PROJECT_ID}",
            f"--location={REGION}",
            "--uniform-bucket-level-access",
        ],
        check=False,
    )
    if create_res.returncode == 0:
        print(f"✅ Bucket '{bucket_url}' created.")
    else:
        print(f"❌ Failed to create bucket: {create_res.stderr}")
        sys.exit(1)

print(f"Ensuring versioning is enabled on '{bucket_url}'...")
update_res = run_cmd(
    [
        "storage",
        "buckets",
        "update",
        bucket_url,
        f"--project={PROJECT_ID}",
        "--versioning",
    ],
    check=False,
)
if update_res.returncode == 0:
    print("✅ Bucket versioning enabled.")
else:
    print(f"❌ Failed to enable bucket versioning: {update_res.stderr}")
    sys.exit(1)

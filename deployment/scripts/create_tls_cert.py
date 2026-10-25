import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from common import run_cmd, load_config

(
    PROJECT_ID,
    BUCKET_NAME,
    INGEST_CONFIG,
) = load_config(
    ["GCP_PROJECT_ID", "GCP_STATE_BUCKET_NAME", "ingest_config"],
)

DOMAIN = INGEST_CONFIG["domain"]
GCS_CERT_DIR = f"gs://{BUCKET_NAME}/certs/{DOMAIN}"

print(f"--- Checking TLS Certificate for {DOMAIN} in {GCS_CERT_DIR} ---")

check_cert = run_cmd(
    ["storage", "ls", f"{GCS_CERT_DIR}/fullchain.pem", f"--project={PROJECT_ID}"],
    check=False,
)

if check_cert.returncode == 0:
    print(f"✅ Certificate for '{DOMAIN}' already exists in '{GCS_CERT_DIR}'.")
    sys.exit(0)

print(f"Generating new Let's Encrypt certificate for '{DOMAIN}' via DNS challenge...")
temp_dir = Path(tempfile.mkdtemp(prefix="observelol_certs_"))

try:
    subprocess.run(
        [
            "docker",
            "run",
            "-it",
            "--rm",
            "-v",
            f"{temp_dir}:/etc/letsencrypt",
            "certbot/certbot",
            "certonly",
            "--manual",
            "--preferred-challenges",
            "dns",
            "-d",
            DOMAIN,
        ],
        check=True,
    )

    archive_dir = temp_dir / "archive" / DOMAIN
    fullchain_src = archive_dir / "fullchain1.pem"
    privkey_src = archive_dir / "privkey1.pem"

    if not fullchain_src.exists() or not privkey_src.exists():
        print(f"❌ Expected certificate files not found in {archive_dir}")
        sys.exit(1)

    fullchain = temp_dir / "fullchain.pem"
    privkey = temp_dir / "privkey.pem"
    shutil.copyfile(fullchain_src, fullchain)
    shutil.copyfile(privkey_src, privkey)

    print(f"Uploading certificate files to '{GCS_CERT_DIR}'...")
    upload_res = run_cmd(
        [
            "storage",
            "cp",
            str(fullchain),
            str(privkey),
            f"{GCS_CERT_DIR}/",
            f"--project={PROJECT_ID}",
        ],
        check=False,
    )

    if upload_res.returncode == 0:
        print(f"✅ Certificate uploaded to '{GCS_CERT_DIR}'.")
    else:
        print(f"❌ Failed to upload certificate: {upload_res.stderr}")
        sys.exit(1)
finally:
    shutil.rmtree(temp_dir, ignore_errors=True)

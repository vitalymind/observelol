from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parent

scripts = [
    BASE_DIR / "scripts" / "enable_apis.py",
    BASE_DIR / "scripts" / "create_deploy_sa.py",
    BASE_DIR / "scripts" / "create_bucket.py",
    BASE_DIR / "scripts" / "setup_workload_identity.py",
    BASE_DIR / "scripts" / "set_github_vars.py",
]

for script in scripts:
    print(f"\n=== Running {script.name} ===")
    subprocess.run([sys.executable, str(script)], check=True)

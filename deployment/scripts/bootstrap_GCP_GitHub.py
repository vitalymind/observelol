from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parent

scripts = [
    BASE_DIR / "enable_apis.py",
    BASE_DIR / "create_deploy_sa.py",
    BASE_DIR / "create_vm_sa.py",
    BASE_DIR / "create_bucket.py",
    BASE_DIR / "setup_workload_identity.py",
]

for script in scripts:
    print(f"\n=== Running {script.name} ===")
    subprocess.run([sys.executable, str(script)], check=True)

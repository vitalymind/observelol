import subprocess
import sys

scripts = [
		"deployment/scripts/enable_apis.py",
		"deployment/scripts/create_build_sa.py",
		"deployment/scripts/connect_repo_trigger.py",
]

for script in scripts:
		print(f"\n=== Running {script} ===")
		subprocess.run([sys.executable, script], check=True)
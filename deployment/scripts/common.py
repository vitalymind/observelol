import os
import shutil
import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()

GCLOUD = shutil.which("gcloud") or "gcloud"


def run_cmd(
    args: list[str], check: bool = True, capture_output: bool = True
) -> subprocess.CompletedProcess:
    """Run a gcloud command. Captures output by default, or streams live if capture_output=False."""
    return subprocess.run(
        [GCLOUD, *args], capture_output=capture_output, text=True, check=check
    )


def require_env(*names: str):
    """Validate required environment variables and return them for unpacking.

    Exits with code 1 if any are missing.
    Returns a single string if 1 name is provided, or a tuple of strings if multiple.
    """
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    values = [os.environ[name] for name in names]
    return values[0] if len(values) == 1 else tuple(values)

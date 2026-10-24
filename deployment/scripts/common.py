import os
from pathlib import Path
import shutil
import subprocess
import sys
from dotenv import find_dotenv, load_dotenv
import yaml
from typing import Any

load_dotenv(find_dotenv(".env"))

GCLOUD = shutil.which("gcloud") or "gcloud"


def run_cmd(
    args: list[str], check: bool = True, capture_output: bool = True
) -> subprocess.CompletedProcess:
    """Run a gcloud command. Captures output by default, or streams live if capture_output=False."""
    return subprocess.run(
        [GCLOUD, "--quiet", *args],
        capture_output=capture_output,
        text=True,
        check=check,
    )


def load_config(names: list[str], config_path: str = "config.yml"):
    file = Path("./", config_path).resolve()

    if not file.exists():
        print(f"❌ Config file not found: {file}")
        sys.exit(1)

    with open(file, "r") as f:
        config = yaml.safe_load(f) or {}

    missing = [name for name in names if name not in config or config[name] is None]
    if missing:
        print(f"❌ Missing required configuration variables: {', '.join(missing)}")
        sys.exit(1)

    values = [config[name] for name in names]
    return values[0] if len(values) == 1 else tuple(values)

import os
import sys

IGNORED = {".git", ".venv", "venv", "__pycache__", ".github"}
errors = []

for root, dirs, files in os.walk("."):
    dirs[:] = [d for d in dirs if d not in IGNORED]
    for file in files:
        if file.endswith(".py"):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    compile(f.read(), filepath, "exec")
            except SyntaxError as err:
                errors.append(f"{filepath}:{err.lineno}: {err.msg}")

if errors:
    print("❌ Syntax errors detected:")
    for err in errors:
        print(f"  - {err}")
    sys.exit(1)

print("✅ All Python files passed syntax check.")

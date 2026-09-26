"""
scripts/stop_hook.py
Antigravity Stop Hook Handler for Football Narrative Observatory.
Prevents the agent from declaring an implementation complete with broken tests or syntax errors.
Guarantees zero infinite loops by checking executionNum and working tree state.
"""

import sys
import json
import os
import subprocess
import shutil

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_python_bin():
    if sys.platform == "win32":
        venv_py = os.path.join(REPO_ROOT, ".venv", "Scripts", "python.exe")
    else:
        venv_py = os.path.join(REPO_ROOT, ".venv", "bin", "python")
    if os.path.exists(venv_py):
        return venv_py
    return sys.executable


def main():
    # Parse stdin safely
    payload = {}
    try:
        if not sys.stdin.isatty():
            input_text = sys.stdin.read().strip()
            if input_text:
                payload = json.loads(input_text)
    except Exception:
        pass

    execution_num = payload.get("executionNum", 1)

    # Invariant 1: Never loop. If already re-entered (executionNum > 1), allow stop immediately.
    if execution_num > 1:
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)

    # Invariant 2: Check if any code files are modified.
    git_bin = shutil.which("git")
    if not git_bin:
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)

    try:
        status_output = subprocess.check_output(
            [git_bin, "status", "--porcelain", "scripts/", "tests/", "infra/"],
            cwd=REPO_ROOT,
            text=True
        ).strip()
    except Exception:
        status_output = ""

    # If no code files are modified, this is an exploratory or conversational turn. Allow stop.
    if not status_output:
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)

    # Invariant 3: Code has been modified. Run fast deterministic verification.
    verify_script = os.path.join(REPO_ROOT, "scripts", "verify_feature.py")
    if not os.path.exists(verify_script):
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)

    python_bin = get_python_bin()
    result = subprocess.run(
        [python_bin, verify_script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        # Verification passed; allow stop cleanly.
        print(json.dumps({"decision": "allow"}))
        sys.exit(0)
    else:
        # Verification failed; send agent back to fix deterministic errors.
        error_lines = result.stdout.splitlines()[-10:]
        error_excerpt = "\n".join(error_lines)
        print(json.dumps({
            "decision": "continue",
            "reason": (
                "Deterministic verification failed (pytest or flake8 errors detected). "
                f"Please fix all failures before stopping:\n{error_excerpt}"
            )
        }))
        sys.exit(0)


if __name__ == "__main__":
    main()

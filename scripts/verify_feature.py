"""
scripts/verify_feature.py
Deterministic feature verification script for Football Narrative Observatory.
Runs the required offline pytest test suite and fatal flake8 check.
Returns exit code 0 if all checks pass, or 1 if any check fails.
"""

import sys
import os
import subprocess
import shutil

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_command(cmd, desc):
    print(f"\n{'=' * 60}")
    print(f"RUNNING: {desc}")
    print(f"COMMAND: {' '.join(cmd)}")
    print(f"{'=' * 60}")
    
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        print(f"\n[FAILED] {desc} exited with code {result.returncode}")
        return False
    print(f"\n[PASSED] {desc}")
    return True


def get_git_info():
    git_bin = shutil.which("git")
    if not git_bin:
        return
    try:
        branch = subprocess.check_output([git_bin, "branch", "--show-current"], cwd=REPO_ROOT, text=True).strip()
        head = subprocess.check_output([git_bin, "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True).strip()
        status = subprocess.check_output([git_bin, "status", "--short"], cwd=REPO_ROOT, text=True).strip()
        print(f"\n{'=' * 60}")
        print("GIT REPOSITORY STATUS")
        print(f"{'=' * 60}")
        print(f"Active Branch : {branch}")
        print(f"Current HEAD  : {head}")
        if status:
            print("Working Tree (Uncommitted / Modified Files):")
            for line in status.splitlines():
                print(f"  {line}")
        else:
            print("Working Tree  : Clean (no uncommitted changes)")
    except Exception as e:
        print(f"Could not retrieve git status: {e}")


def get_python_bin():
    if sys.platform == "win32":
        venv_py = os.path.join(REPO_ROOT, ".venv", "Scripts", "python.exe")
    else:
        venv_py = os.path.join(REPO_ROOT, ".venv", "bin", "python")
    if os.path.exists(venv_py):
        return venv_py
    return sys.executable


def main():
    print("Football Narrative Observatory - Deterministic Feature Verifier")
    get_git_info()

    python_bin = get_python_bin()

    # Check 1: Fatal Flake8 Linting (CI parity)
    # flake8 . --count --select=E9,F63,F7,F82 --exclude=.venv --show-source --statistics
    flake8_cmd = [
        python_bin, "-m", "flake8", ".",
        "--count",
        "--select=E9,F63,F7,F82",
        "--exclude=.venv",
        "--show-source",
        "--statistics"
    ]
    flake8_passed = run_command(flake8_cmd, "Fatal Syntax & Name Error Check (flake8)")

    # Check 2: Offline Pytest Suite
    # pytest tests/
    pytest_cmd = [python_bin, "-m", "pytest", "tests/"]
    pytest_passed = run_command(pytest_cmd, "Offline Unit & Regression Test Suite (pytest)")

    print(f"\n{'=' * 60}")
    print("VERIFICATION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Fatal Flake8 Check : {'PASS' if flake8_passed else 'FAIL'}")
    print(f"Offline Pytest Suite : {'PASS' if pytest_passed else 'FAIL'}")

    if not flake8_passed or not pytest_passed:
        print("\nOVERALL STATUS: FAILED")
        sys.exit(1)

    print("\nOVERALL STATUS: PASSED - Ready for external review protocol.")
    sys.exit(0)


if __name__ == "__main__":
    main()

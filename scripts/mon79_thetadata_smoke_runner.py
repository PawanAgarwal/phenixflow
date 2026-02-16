#!/usr/bin/env python3
"""MON-79 smoke runner with preflight checks and optional retries."""

import os
import subprocess
import sys
import time

REQUIRED_VARS = [
    "THETADATA_DOWNLOAD_PATH",
    "THETADATA_BASE_URL (if THETADATA_DOWNLOAD_PATH is relative)",
    "ThetaTerminal running with valid creds.txt",
]


def _parse_retry_delays_seconds() -> list:
    value = os.environ.get("MON79_RUNNER_RETRY_DELAYS_SEC", "2,5,15").strip()
    if not value:
        return []
    try:
        return [int(piece) for piece in value.split(",") if piece]
    except ValueError:
        raise ValueError("MON79_RUNNER_RETRY_DELAYS_SEC must be comma-separated integers")


def _run(command: list) -> subprocess.CompletedProcess:
    return subprocess.run(command, check=False, text=True, capture_output=True)


def _preflight() -> bool:
    result = _run(["python3", "scripts/mon79_thetadata_config_check.py"])
    sys.stdout.write(result.stdout)
    if result.returncode == 0:
        return True

    sys.stderr.write("MON-79 preflight blocked: missing ThetaData configuration\n")
    for item in REQUIRED_VARS:
        sys.stderr.write(f"- {item}\n")
    return False


def main() -> int:
    try:
        retry_delays = _parse_retry_delays_seconds()
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        return 1

    if not _preflight():
        return 1

    attempts = len(retry_delays) + 1
    for attempt in range(1, attempts + 1):
        smoke = _run(["node", "scripts/mon79-thetadata-smoke.js"])
        sys.stdout.write(smoke.stdout)
        if smoke.returncode == 0:
            return 0

        sys.stderr.write(smoke.stderr)
        if attempt < attempts:
            delay = retry_delays[attempt - 1]
            sys.stderr.write(f"MON-79 smoke attempt {attempt}/{attempts} failed; retrying in {delay}s\n")
            time.sleep(delay)

    sys.stderr.write(f"MON-79 smoke failed after {attempts} attempts\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())

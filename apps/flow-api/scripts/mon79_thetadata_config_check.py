#!/usr/bin/env python3
"""Validate MON-79 ThetaData runtime configuration."""

import json
import os
import re
import sys


def _is_absolute_http_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _is_placeholder(value: str) -> bool:
    lowered = value.lower()
    placeholder_tokens = ["example", "changeme", "your_", "<", "placeholder"]
    return any(token in lowered for token in placeholder_tokens)


def evaluate_env(env: dict) -> dict:
    issues = []
    warnings = []

    base_url = env.get("THETADATA_BASE_URL", "").strip()
    download_path = env.get("THETADATA_DOWNLOAD_PATH", "").strip()
    creds_file = env.get("THETADATA_CREDS_FILE", "").strip()

    if not download_path:
        issues.append("Missing THETADATA_DOWNLOAD_PATH")

    download_is_absolute = _is_absolute_http_url(download_path)

    if download_path and not download_is_absolute and not base_url:
        issues.append("Missing THETADATA_BASE_URL for relative endpoint paths")

    if download_path and _is_placeholder(download_path):
        issues.append("THETADATA_DOWNLOAD_PATH still looks like a placeholder")

    if base_url and _is_placeholder(base_url):
        issues.append("THETADATA_BASE_URL still looks like a placeholder")

    if base_url and not _is_absolute_http_url(base_url):
        issues.append("THETADATA_BASE_URL must start with http:// or https://")

    auth_mode = "terminal_creds"
    if creds_file and not os.path.exists(creds_file):
        warnings.append("THETADATA_CREDS_FILE is set but file does not exist")

    retry_delays = env.get("THETADATA_RETRY_DELAYS_MS", "").strip()
    if retry_delays and not re.match(r"^\d+(,\d+)*$", retry_delays):
        warnings.append("THETADATA_RETRY_DELAYS_MS should be comma-separated integers, e.g. 2000,5000,15000")

    result = {
        "ok": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "summary": {
            "hasBaseUrl": bool(base_url),
            "downloadPathIsAbsoluteUrl": download_is_absolute,
            "authMode": auth_mode,
        },
    }
    return result


def main() -> int:
    result = evaluate_env(os.environ)
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

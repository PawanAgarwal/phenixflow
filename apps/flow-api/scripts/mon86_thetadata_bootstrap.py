#!/usr/bin/env python3
"""MON-86 ThetaData bootstrap + runtime preflight guard.

Required outcomes covered:
1) Download ThetaData jar if missing (idempotent)
2) Start service only if not already running (duplicate-start guard)
3) Health check gate before success exit
4) Emit env guidance for MON-79/80/81/82 flows
5) Clear failure taxonomy
6) Dry-run mode for CI/local verification
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


FAIL_MISSING_CREDS = "MON86_ERR_MISSING_CREDS"
FAIL_PORT_IN_USE = "MON86_ERR_PORT_IN_USE"
FAIL_DOWNLOAD = "MON86_ERR_DOWNLOAD_FAILED"
FAIL_HEALTH_TIMEOUT = "MON86_ERR_HEALTH_TIMEOUT"
FAIL_CONFIG = "MON86_ERR_CONFIG"


@dataclass
class RunSummary:
    ok: bool
    dryRun: bool
    alreadyRunning: bool
    jarExisted: bool
    jarPath: str
    jarUrl: str | None
    baseUrl: str
    healthUrl: str
    logPath: str | None
    pid: int | None
    errorCode: str | None
    message: str
    timestamp: str



def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def _is_http_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")



def _check_terminal_creds(env: dict[str, str], jar_parent: Path) -> tuple[str, str | None]:
    creds_file = (env.get("THETADATA_CREDS_FILE") or str(jar_parent / "creds.txt")).strip()
    path = Path(creds_file).expanduser()
    if not path.is_absolute():
        path = (jar_parent / path).resolve()
    if not path.exists():
        return "missing", f"Missing creds file at {path}. Create creds.txt with email on line 1 and password on line 2."
    try:
        lines = [ln.strip() for ln in path.read_text(encoding='utf-8').splitlines() if ln.strip()]
    except Exception as exc:  # pylint: disable=broad-except
        return "invalid", f"Unable to read creds file {path}: {exc}"
    if len(lines) < 2:
        return "invalid", f"Creds file {path} must contain email on line 1 and password on line 2"
    return "creds_file", None



def _parse_base_url(base_url: str) -> tuple[str, int]:
    parsed = urllib.parse.urlparse(base_url)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError("THETADATA_BASE_URL must be a valid http(s) URL")

    if parsed.port is not None:
        port = parsed.port
    else:
        port = 443 if parsed.scheme == "https" else 80

    return parsed.hostname, int(port)



def _port_open(host: str, port: int, timeout_sec: float = 0.6) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout_sec)
        sock.connect((host, port))
        return True
    except OSError:
        return False
    finally:
        sock.close()



def _health_check(health_url: str, timeout_sec: float = 2.0) -> tuple[bool, str]:
    request = urllib.request.Request(health_url, method="GET", headers={"User-Agent": "phenixflow-mon86-bootstrap/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            body = response.read(200).decode("utf-8", errors="replace")
            return response.status < 500, f"status={response.status} body={body[:120]}"
    except urllib.error.HTTPError as exc:
        snippet = ""
        try:
            snippet = exc.read(200).decode("utf-8", errors="replace")
        except Exception:
            snippet = ""
        return False, f"http_error={getattr(exc, 'code', '?')} body={snippet[:120]}"
    except Exception as exc:  # pylint: disable=broad-except
        return False, str(exc)



def _download_jar(jar_url: str, jar_path: Path, dry_run: bool) -> tuple[bool, str]:
    jar_path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        return True, f"[dry-run] would download {jar_url} -> {jar_path}"

    try:
        with urllib.request.urlopen(jar_url, timeout=45) as response:
            data = response.read()
        if len(data) < 1024:
            return False, f"download too small ({len(data)} bytes); expected a jar"
        jar_path.write_bytes(data)
        return True, f"downloaded {len(data)} bytes"
    except Exception as exc:  # pylint: disable=broad-except
        return False, str(exc)



def _emit_env_guidance(base_url: str, jar_path: Path):
    lines = [
        "MON-86 ENV GUIDANCE (MON-79/80/81/82)",
        "export THETADATA_BASE_URL=\"%s\"" % base_url,
        "export THETADATA_DOWNLOAD_PATH=\"/v3/stock/list/symbols?format=json\"",
        "export THETADATA_INGEST_PATH=\"$THETADATA_DOWNLOAD_PATH\"",
        "export THETADATA_OUTPUT_DIR=\"artifacts/mon-79\"",
        "# ThetaTerminal creds file (email line1, password line2)",
        "export THETADATA_CREDS_FILE=\"./creds.txt\"",
        "# Optional bootstrap tuning",
        "export THETADATA_JAR_PATH=\"%s\"" % str(jar_path),
        "export THETADATA_HEALTH_PATH=\"/\"",
    ]
    print("\n".join(lines))



def _write_summary(summary: RunSummary, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = output_dir / f"mon86-bootstrap-{stamp}.json"
    out.write_text(json.dumps(asdict(summary), indent=2), encoding="utf-8")
    return out



def main() -> int:
    parser = argparse.ArgumentParser(description="MON-86 ThetaData bootstrap + preflight")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; do not download or start Java")
    parser.add_argument("--health-timeout-sec", type=float, default=35.0, help="Max wait for health after start")
    parser.add_argument("--health-poll-sec", type=float, default=1.0, help="Poll cadence while waiting for health")
    args = parser.parse_args()

    env = os.environ
    base_url = (env.get("THETADATA_BASE_URL") or "http://127.0.0.1:25503").strip()
    health_path = (env.get("THETADATA_HEALTH_PATH") or "/").strip()
    health_url = base_url.rstrip("/") + (health_path if health_path.startswith("/") else f"/{health_path}")

    jar_path = Path((env.get("THETADATA_JAR_PATH") or "artifacts/thetadata/ThetaTerminal.jar").strip()).resolve()
    jar_url = (env.get("THETADATA_JAR_URL") or "").strip() or None
    java_bin = (env.get("THETADATA_JAVA_BIN") or "java").strip()
    java_args = shlex.split((env.get("THETADATA_JAVA_ARGS") or "").strip())
    bootstrap_output_dir = Path((env.get("THETADATA_BOOTSTRAP_OUTPUT_DIR") or "artifacts/mon-86").strip()).resolve()

    auth_mode, auth_error = _check_terminal_creds(dict(env), jar_path.parent)
    if auth_error:
        summary = RunSummary(
            ok=False,
            dryRun=args.dry_run,
            alreadyRunning=False,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=FAIL_MISSING_CREDS,
            message=f"Authentication preflight failed ({auth_mode}): {auth_error}",
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
        sys.stderr.write(f"SUMMARY={artifact}\n")
        _emit_env_guidance(base_url, jar_path)
        return 1

    if not _is_http_url(base_url):
        summary = RunSummary(
            ok=False,
            dryRun=args.dry_run,
            alreadyRunning=False,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=FAIL_CONFIG,
            message="THETADATA_BASE_URL must start with http:// or https://",
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
        sys.stderr.write(f"SUMMARY={artifact}\n")
        return 1

    try:
        host, port = _parse_base_url(base_url)
    except ValueError as exc:
        summary = RunSummary(
            ok=False,
            dryRun=args.dry_run,
            alreadyRunning=False,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=FAIL_CONFIG,
            message=str(exc),
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
        sys.stderr.write(f"SUMMARY={artifact}\n")
        return 1

    healthy, health_detail = _health_check(health_url)
    if healthy:
        summary = RunSummary(
            ok=True,
            dryRun=args.dry_run,
            alreadyRunning=True,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=None,
            message=f"ThetaData already healthy; duplicate start skipped ({health_detail})",
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        print(summary.message)
        print(f"SUMMARY={artifact}")
        _emit_env_guidance(base_url, jar_path)
        return 0

    if _port_open(host, port):
        summary = RunSummary(
            ok=False,
            dryRun=args.dry_run,
            alreadyRunning=False,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=FAIL_PORT_IN_USE,
            message=(
                f"Port {host}:{port} is already in use but health check failed ({health_detail}). "
                "Refusing duplicate/unsafe start."
            ),
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
        sys.stderr.write(f"SUMMARY={artifact}\n")
        return 1

    auth_mode, auth_error = _check_terminal_creds(dict(env), jar_path.parent)
    if auth_error:
        summary = RunSummary(
            ok=False,
            dryRun=args.dry_run,
            alreadyRunning=False,
            jarExisted=jar_path.exists(),
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=FAIL_MISSING_CREDS,
            message=f"Credentials preflight failed ({auth_mode}): {auth_error}",
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
        sys.stderr.write(f"SUMMARY={artifact}\n")
        _emit_env_guidance(base_url, jar_path)
        return 1

    jar_existed = jar_path.exists()
    if not jar_existed:
        if not jar_url:
            if args.dry_run:
                print("[dry-run] jar is missing and THETADATA_JAR_URL is unset; real run would fail with MON86_ERR_DOWNLOAD_FAILED")
            else:
                summary = RunSummary(
                    ok=False,
                    dryRun=args.dry_run,
                    alreadyRunning=False,
                    jarExisted=False,
                    jarPath=str(jar_path),
                    jarUrl=jar_url,
                    baseUrl=base_url,
                    healthUrl=health_url,
                    logPath=None,
                    pid=None,
                    errorCode=FAIL_DOWNLOAD,
                    message="Jar missing and THETADATA_JAR_URL is not set",
                    timestamp=_now_iso(),
                )
                artifact = _write_summary(summary, bootstrap_output_dir)
                sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
                sys.stderr.write(f"SUMMARY={artifact}\n")
                return 1

        if jar_url:
            ok, detail = _download_jar(jar_url, jar_path, args.dry_run)
            if not ok:
                summary = RunSummary(
                    ok=False,
                    dryRun=args.dry_run,
                    alreadyRunning=False,
                    jarExisted=False,
                    jarPath=str(jar_path),
                    jarUrl=jar_url,
                    baseUrl=base_url,
                    healthUrl=health_url,
                    logPath=None,
                    pid=None,
                    errorCode=FAIL_DOWNLOAD,
                    message=f"Jar download failed: {detail}",
                    timestamp=_now_iso(),
                )
                artifact = _write_summary(summary, bootstrap_output_dir)
                sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
                sys.stderr.write(f"SUMMARY={artifact}\n")
                return 1
            print(f"Jar bootstrap: {detail}")

    if args.dry_run:
        summary = RunSummary(
            ok=True,
            dryRun=True,
            alreadyRunning=False,
            jarExisted=jar_existed,
            jarPath=str(jar_path),
            jarUrl=jar_url,
            baseUrl=base_url,
            healthUrl=health_url,
            logPath=None,
            pid=None,
            errorCode=None,
            message="Dry run complete: preflight passed; would launch ThetaData and wait for health",
            timestamp=_now_iso(),
        )
        artifact = _write_summary(summary, bootstrap_output_dir)
        print(summary.message)
        print(f"SUMMARY={artifact}")
        _emit_env_guidance(base_url, jar_path)
        return 0

    log_path = bootstrap_output_dir / f"thetadata-service-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [java_bin, *java_args, "-jar", str(jar_path)]
    with log_path.open("a", encoding="utf-8") as log_handle:
        log_handle.write(f"[{_now_iso()}] launching: {' '.join(cmd)}\n")
        proc = subprocess.Popen(
            cmd,
            stdout=log_handle,
            stderr=log_handle,
            text=True,
            cwd=jar_path.parent,
        )

    deadline = time.time() + args.health_timeout_sec
    last_detail = health_detail
    while time.time() < deadline:
        ok, detail = _health_check(health_url)
        last_detail = detail
        if ok:
            summary = RunSummary(
                ok=True,
                dryRun=False,
                alreadyRunning=False,
                jarExisted=jar_existed,
                jarPath=str(jar_path),
                jarUrl=jar_url,
                baseUrl=base_url,
                healthUrl=health_url,
                logPath=str(log_path),
                pid=proc.pid,
                errorCode=None,
                message="ThetaData bootstrap succeeded and health gate passed",
                timestamp=_now_iso(),
            )
            artifact = _write_summary(summary, bootstrap_output_dir)
            print(summary.message)
            print(f"PID={proc.pid}")
            print(f"LOG={log_path}")
            print(f"SUMMARY={artifact}")
            _emit_env_guidance(base_url, jar_path)
            return 0

        if proc.poll() is not None:
            break
        time.sleep(max(args.health_poll_sec, 0.1))

    try:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=5)
    except Exception:  # pylint: disable=broad-except
        try:
            proc.kill()
        except Exception:
            pass

    summary = RunSummary(
        ok=False,
        dryRun=False,
        alreadyRunning=False,
        jarExisted=jar_existed,
        jarPath=str(jar_path),
        jarUrl=jar_url,
        baseUrl=base_url,
        healthUrl=health_url,
        logPath=str(log_path),
        pid=proc.pid,
        errorCode=FAIL_HEALTH_TIMEOUT,
        message=(
            f"Timed out waiting for healthy ThetaData service at {health_url} after "
            f"{args.health_timeout_sec:.1f}s (last detail: {last_detail})"
        ),
        timestamp=_now_iso(),
    )
    artifact = _write_summary(summary, bootstrap_output_dir)
    sys.stderr.write(f"{summary.errorCode}: {summary.message}\n")
    sys.stderr.write(f"PID={proc.pid}\n")
    sys.stderr.write(f"LOG={log_path}\n")
    sys.stderr.write(f"SUMMARY={artifact}\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())

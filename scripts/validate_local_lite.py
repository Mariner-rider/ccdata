from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPORT = Path("docs/local-lite-verification-report.md")


def run(cmd: str) -> tuple[bool, str]:
    p = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    return p.returncode == 0, (p.stdout + p.stderr).strip()


def main() -> int:
    lines = ["# Local-lite Verification Report", f"Timestamp: {datetime.now(timezone.utc).isoformat()}", f"OS: {platform.platform()}", f"Python: {sys.version.split()[0]}", ""]

    checks = []
    checks.append(("python-compile",) + run("python -m compileall services tests"))
    checks.append(("pytest",) + run("pytest -q"))

    docker_ok = shutil.which("docker") is not None
    compose_ok = False
    if docker_ok:
        compose_ok = run("docker compose version")[0]

    checks.append(("docker-cli", docker_ok, "found" if docker_ok else "missing"))
    checks.append(("docker-compose", compose_ok, "ok" if compose_ok else "unavailable"))

    if docker_ok and compose_ok:
        checks.append(("compose-build",) + run("docker compose -f docker-compose.local-lite.yml build"))
        checks.append(("compose-up",) + run("docker compose -f docker-compose.local-lite.yml up -d --build"))
        checks.append(("health",) + run("curl -fsS http://127.0.0.1:8000/health"))
        checks.append(("health-db",) + run("curl -fsS http://127.0.0.1:8000/health/db"))
        checks.append(("health-redis",) + run("curl -fsS http://127.0.0.1:8000/health/redis"))
        checks.append(("health-webclaw",) + run("curl -fsS http://127.0.0.1:8000/health/webclaw"))

    checks.append(("extract-test",) + run("python -m services.lite_pipeline.main extract:test --url https://example.com"))
    checks.append(("crawl-single",) + run("python -m services.lite_pipeline.main crawl:single --url https://example.com"))
    checks.append(("size-report",) + run("python scripts/docker_size_report.py"))

    passed = 0
    for name, ok, out in checks:
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        lines.append(f"## {name}: {status}")
        lines.append("```")
        lines.append((out or "(no output)")[:3000])
        lines.append("```")

    lines.append("")
    lines.append(f"Summary: {passed}/{len(checks)} checks passed")
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0 if passed == len(checks) else 2


if __name__ == "__main__":
    raise SystemExit(main())

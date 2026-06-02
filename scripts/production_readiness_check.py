#!/usr/bin/env python3
"""Production readiness checks for CollegeCue."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def check(condition: bool, name: str, reason: str = "") -> tuple[str, bool, str]:
    return name, condition, reason


def main() -> int:
    results: list[tuple[str, bool, str]] = []

    dead = [
        "REDMI.md",
        "requirements.txt",
        "requirements-dev.txt",
        "requirements-full.txt",
        "docker-compose.yml",
        "docker-compose.local-full.yml",
        "nutch",
        "docs",
    ]
    present = [path for path in dead if (ROOT / path).exists()]
    results.append(check(not present, "Dead files absent", f"present: {', '.join(present)}"))

    try:
        from services.common.user_agents import USER_AGENT_POOL

        bad = [
            ua
            for ua in USER_AGENT_POOL
            if any(word in ua.lower() for word in ["bot", "crawler", "scraper", "spider", "python", "requests", "httpx", "ccdata", "collegecue"])
        ]
        results.append(check(len(USER_AGENT_POOL) == 24 and not bad, "No bot strings in USER_AGENT_POOL (24 agents)", f"count={len(USER_AGENT_POOL)} bad={bad}"))
    except Exception as exc:
        results.append(check(False, "No bot strings in USER_AGENT_POOL (24 agents)", str(exc)))

    compose = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    leaked = [value for value in ["minio123", "change-me", "admin/admin", "changeme"] if value in compose]
    results.append(check(not leaked, "No hardcoded credentials", f"found: {', '.join(leaked)}"))

    gitignore = ROOT / ".gitignore"
    results.append(check(gitignore.exists() and ".env" in gitignore.read_text(encoding="utf-8"), ".gitignore contains .env", ".gitignore missing or lacks .env"))
    results.append(check(not (ROOT / ".env").exists(), ".env absent from repo root", ".env exists"))

    migrations_ok = all(list((ROOT / "migrations").glob(f"{idx:03d}_*.sql")) for idx in range(1, 6))
    results.append(check(migrations_ok, "Migration files 001-005 present", "missing numbered migration"))

    modules = [
        "services.common.user_agents",
        "services.deep_crawler.crawler",
        "services.institutions.crawler",
        "services.admissions.crawler",
        "services.jobs.crawler",
        "services.news.crawler",
        "services.research.crawler",
        "services.test_series.crawler",
    ]
    import_errors = []
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as exc:
            import_errors.append(f"{module}: {exc}")
    results.append(check(not import_errors, "All service packages importable", "; ".join(import_errors)))

    env_example = (ROOT / ".env.production.example").read_text(encoding="utf-8")
    missing_keys = [key for key in ["POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD", "SERVICE_API_KEY", "AIRFLOW_FERNET_KEY"] if key not in env_example]
    results.append(check(not missing_keys, ".env.production.example required keys present", f"missing: {', '.join(missing_keys)}"))

    results.append(check((ROOT / ".github/workflows/ci.yml.disabled").exists(), "CI skeleton exists", ".github/workflows/ci.yml.disabled missing"))

    try:
        import yaml

        yaml.safe_load((ROOT / "docker-compose.production.yml").read_text(encoding="utf-8"))
        yaml.safe_load((ROOT / "docker-compose.local-lite.yml").read_text(encoding="utf-8"))
        results.append(check(True, "Compose files parse as valid YAML"))
    except Exception as exc:
        results.append(check(False, "Compose files parse as valid YAML", str(exc)))

    failures = 0
    print("══════════════════════════════")
    print("CollegeCue Production Readiness")
    print("══════════════════════════════")
    for name, ok, reason in results:
        if ok:
            print(f"✓ {name}")
        else:
            failures += 1
            print(f"✗ {name}: {reason}")
    print("══════════════════════════════")
    if failures:
        print(f"RESULT: NOT READY — fix {failures} issue(s) above")
        return 1
    print("RESULT: READY TO DEPLOY ✓")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

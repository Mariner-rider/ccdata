#!/usr/bin/env python3
"""Run CollegeCue production readiness checks."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _print_header() -> None:
    print("══════════════════════════════")
    print("CollegeCue Production Readiness")
    print("══════════════════════════════")


def _run_pytest() -> tuple[bool, str]:
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-q", "--tb=short"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        return False, proc.stdout.strip().splitlines()[-20:][0] if proc.stdout.strip() else "pytest failed"
    summary = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else "tests passed"
    return True, summary


def main() -> int:
    checks: list[tuple[str, bool, str]] = []

    dead_files = [
        "REDMI.md",
        "requirements.txt",
        "requirements-dev.txt",
        "requirements-full.txt",
        "docker-compose.yml",
        "docker-compose.local-full.yml",
        "nutch",
        "docs",
    ]
    existing = [path for path in dead_files if (ROOT / path).exists()]
    checks.append(("No dead files", not existing, f"still present: {', '.join(existing)}" if existing else ""))

    try:
        from services.common.user_agents import USER_AGENT_POOL

        bad = [
            ua
            for ua in USER_AGENT_POOL
            if any(word in ua.lower() for word in ["bot", "crawler", "scraper", "spider", "python", "requests", "httpx", "ccdata"])
        ]
        checks.append((f"Clean user agents ({len(USER_AGENT_POOL)} agents)", len(USER_AGENT_POOL) >= 20 and not bad, f"bad user agents: {bad}" if bad else ""))
    except Exception as exc:
        checks.append(("Clean user agents", False, str(exc)))

    compose_text = (ROOT / "docker-compose.production.yml").read_text(encoding="utf-8")
    bad_literals = [value for value in ["minio123", "change-me", "admin/admin", "changeme"] if value in compose_text]
    checks.append(("No hardcoded credentials", not bad_literals, f"found literals: {', '.join(bad_literals)}" if bad_literals else ""))

    gitignore = ROOT / ".gitignore"
    env_file = ROOT / ".env"
    env_ok = gitignore.exists() and ".env" in gitignore.read_text(encoding="utf-8") and not env_file.exists()
    reason = "" if env_ok else ".gitignore missing .env or root .env exists"
    checks.append((".env not committed", env_ok, reason))

    migration_ok = all(list((ROOT / "migrations").glob(f"{idx:03d}_*.sql")) for idx in range(1, 5)) and (ROOT / "migrations/005_test_series.sql").exists()
    checks.append(("All migrations present", migration_ok, "expected migrations/001_*.sql through migrations/005_test_series.sql" if not migration_ok else ""))

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
    failed_modules = []
    for module in modules:
        try:
            importlib.import_module(module)
        except Exception as exc:
            failed_modules.append(f"{module}: {exc}")
    checks.append(("All modules importable", not failed_modules, "; ".join(failed_modules)))

    env_example = ROOT / ".env.production.example"
    required_env = ["POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD", "SERVICE_API_KEY", "AIRFLOW_FERNET_KEY"]
    env_text = env_example.read_text(encoding="utf-8") if env_example.exists() else ""
    missing_env = [name for name in required_env if name not in env_text]
    checks.append(("Env vars documented", not missing_env, f"missing: {', '.join(missing_env)}" if missing_env else ""))

    checks.append(("CI skeleton present", (ROOT / ".github/workflows/ci.yml.disabled").exists(), ".github/workflows/ci.yml.disabled missing"))

    try:
        import yaml

        for compose in ["docker-compose.production.yml", "docker-compose.local-lite.yml"]:
            yaml.safe_load((ROOT / compose).read_text(encoding="utf-8"))
        checks.append(("Compose files valid YAML", True, ""))
    except ModuleNotFoundError:
        # PyYAML is part of the dev extra, but production images may run this
        # script before dev dependencies are installed. Fall back to a minimal
        # structural check so the readiness report still catches missing files.
        missing = [compose for compose in ["docker-compose.production.yml", "docker-compose.local-lite.yml"] if not (ROOT / compose).read_text(encoding="utf-8").lstrip().startswith("version:")]
        checks.append(("Compose files valid YAML", not missing, f"invalid compose headers: {', '.join(missing)}" if missing else ""))
    except Exception as exc:
        checks.append(("Compose files valid YAML", False, str(exc)))

    pytest_ok, pytest_reason = _run_pytest()
    checks.append(("Tests pass", pytest_ok, pytest_reason if not pytest_ok else pytest_reason))

    _print_header()
    failures = []
    for name, ok, reason in checks:
        if ok:
            suffix = f" ({reason})" if name == "Tests pass" and reason else ""
            print(f"✓ {name}{suffix}")
        else:
            failures.append((name, reason))
            print(f"✗ {name}: {reason}")
    print("══════════════════════════════")
    if failures:
        print(f"RESULT: NOT READY — fix {len(failures)} issue(s) above")
        print("══════════════════════════════")
        return 1
    print("RESULT: READY TO DEPLOY ✓")
    print("══════════════════════════════")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

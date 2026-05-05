from __future__ import annotations
import os, sqlite3, subprocess, sys
from pathlib import Path

ENV = os.environ | {
    "RUNTIME_PROFILE": "no-docker",
    "DATABASE_URL": "sqlite:///./collegecue_local.db",
    "QUEUE_BACKEND": "memory",
    "WEBCLAW_ENABLED": "false",
}

def run(cmd: list[str]) -> tuple[int, str]:
    p = subprocess.run(cmd, capture_output=True, text=True, env=ENV)
    return p.returncode, (p.stdout + p.stderr)

report = [f"Python: {sys.version}"]
code, out = run([sys.executable, "-m", "services.lite_pipeline.main", "init-db"])
report.append(f"init-db: {code}")
code, out = run([sys.executable, "-m", "services.lite_pipeline.main", "extract:test", "--url", "file://tests/fixtures/college_sample.html"])
report.append(f"extract:test: {code}")
code, out = run([sys.executable, "-m", "services.lite_pipeline.main", "source:add", "--entity-type", "college", "--entity-name", "Fixture", "--url", "file://tests/fixtures/college_sample.html"])
report.append(f"source:add: {code}")
code, out = run([sys.executable, "-m", "services.lite_pipeline.main", "source:crawl", "--id", "1"])
report.append(f"source:crawl: {code}")

con = sqlite3.connect("collegecue_local.db")
exists = con.execute("SELECT count(*) FROM crawler_records").fetchone()[0]
con.close()
report.append(f"db_records: {exists}")

code, out = run([sys.executable, "-m", "pytest", "-q"])
report.append(f"pytest: {code}")

Path("docs").mkdir(exist_ok=True)
Path("docs/no-docker-verification-report.md").write_text("\n".join(["# No-Docker Verification Report", *report]))
print("\n".join(report))
if code != 0 or exists < 1:
    raise SystemExit(1)

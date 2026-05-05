from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

LIMITS_MB = {
    "core": 1536,
    "worker": 1536,
    "browser-worker": 3072,
    "local-lite-total": 5120,
}


def parse_size_to_mb(size_text: str) -> float:
    s = size_text.strip().upper()
    if s.endswith("GB"):
        return float(s[:-2]) * 1024
    if s.endswith("MB"):
        return float(s[:-2])
    if s.endswith("KB"):
        return float(s[:-2]) / 1024
    if s.endswith("B"):
        return float(s[:-1]) / 1024 / 1024
    return 0.0


def main() -> int:
    allow_large = os.getenv("ALLOW_LARGE_IMAGES", "false").lower() == "true"
    out_file = Path("docs/docker-size-report.md")
    lines = ["# Docker Size Report", f"Generated: {datetime.now(timezone.utc).isoformat()}", ""]

    try:
        output = subprocess.check_output(["docker", "images", "--format", "{{json .}}"], text=True)
        imgs = [json.loads(x) for x in output.splitlines() if x.strip()]
    except Exception as exc:  # noqa: BLE001
        out_file.write_text(f"# Docker Size Report\n\nUnavailable: {exc}\n", encoding="utf-8")
        return 0

    lines.append("| Image | Tag | Size | Target | Status |")
    lines.append("|---|---:|---:|---:|---:|")

    failures = 0
    lite_total = 0.0
    for img in imgs:
        repo = img.get("Repository", "")
        tag = img.get("Tag", "")
        size = img.get("Size", "0B")
        size_mb = parse_size_to_mb(size)
        status = "N/A"
        target = "-"

        if "core" in repo:
            target = "<=1.5GB"
            status = "PASS" if size_mb <= LIMITS_MB["core"] else "FAIL"
        elif "worker" in repo and "browser" not in repo:
            target = "<=1.5GB"
            status = "PASS" if size_mb <= LIMITS_MB["worker"] else "FAIL"
        elif "browser" in repo:
            target = "<=3GB"
            status = "PASS" if size_mb <= LIMITS_MB["browser-worker"] else "FAIL"

        if any(k in repo for k in ["core", "worker", "browser-worker"]):
            lite_total += size_mb
        if status == "FAIL":
            failures += 1

        lines.append(f"| {repo} | {tag} | {size} | {target} | {status} |")

    total_status = "PASS" if lite_total <= LIMITS_MB["local-lite-total"] else "FAIL"
    if total_status == "FAIL":
        failures += 1

    lines.append("")
    lines.append(f"Local-lite total size: {lite_total:.2f}MB (target <= {LIMITS_MB['local-lite-total']}MB) => **{total_status}**")

    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if failures and not allow_large:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

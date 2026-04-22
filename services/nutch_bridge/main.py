"""Bridge Apache Nutch discovery output into Kafka crawl queue."""

from __future__ import annotations

import json
import subprocess
import tempfile
import time
from pathlib import Path

from services.common.config import settings
from services.common.kafka_client import build_producer


def run_nutch_cycle() -> Path:
    workdir = Path("/opt/nutch/runtime/local")
    output_file = Path(tempfile.mkstemp(prefix="nutch_urls_", suffix=".json")[1])

    # Discovery crawl by Apache Nutch (inject -> generate -> fetch -> parse -> updatedb).
    subprocess.run(["/opt/nutch/bin/crawl", "-i", "-Dmapreduce.job.queuename=default", "/seed", "-depth", "1", "-topN", "500"], check=True)

    # Export discovered URLs from crawldb as JSON lines.
    with output_file.open("w", encoding="utf-8") as handle:
        subprocess.run(
            ["/opt/nutch/bin/nutch", "readdb", "-dump", "crawl/crawldb", "-format", "csv"],
            check=True,
            cwd=workdir,
            stdout=handle,
        )
    return output_file


def publish_urls(dump_file: Path) -> int:
    producer = build_producer()
    count = 0
    for line in dump_file.read_text(encoding="utf-8").splitlines():
        if line.startswith("http"):
            url = line.split(",", 1)[0].strip()
            msg = {"source_id": None, "url": url, "priority": "discovered", "max_depth": settings.max_depth}
            producer.send(settings.crawl_queue_topic, msg)
            count += 1
    producer.flush()
    return count


def main() -> None:
    interval = int(__import__("os").getenv("NUTCH_DISCOVERY_INTERVAL_SECONDS", "900"))
    while True:
        dump_file = run_nutch_cycle()
        count = publish_urls(dump_file)
        print(json.dumps({"event": "nutch_publish", "count": count}))
        time.sleep(interval)


if __name__ == "__main__":
    main()

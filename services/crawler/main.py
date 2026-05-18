import hashlib
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config
from scrapy import Request, Spider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from w3lib.url import canonicalize_url

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer
from services.common.user_agents import add_jitter, get_headers, get_random_ua


UPSERT_PAGE_STATE = """
INSERT INTO page_state (url, url_hash, content_hash, s3_key, http_status, etag, last_modified, last_crawled_at)
VALUES (%(url)s, %(url_hash)s, %(content_hash)s, %(s3_key)s, %(http_status)s, %(etag)s, %(last_modified)s, NOW())
ON CONFLICT (url_hash)
DO UPDATE SET
    content_hash = EXCLUDED.content_hash,
    s3_key = EXCLUDED.s3_key,
    http_status = EXCLUDED.http_status,
    etag = EXCLUDED.etag,
    last_modified = EXCLUDED.last_modified,
    last_crawled_at = NOW();
"""

SELECT_PAGE_STATE = """
SELECT etag, last_modified, content_hash
FROM page_state
WHERE url_hash = %s;
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""


class SingleURLSpider(Spider):
    name = "single_url"

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": add_jitter(settings.per_domain_delay_seconds),
        "AUTOTHROTTLE_ENABLED": True,
        "USER_AGENT": get_random_ua(),
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, task: dict[str, Any], s3_client, existing_state: dict[str, Any] | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task = task
        self.s3 = s3_client
        self.existing_state = existing_state or {}

    def start_requests(self):
        headers = get_headers(self.task["url"])
        if self.existing_state.get("etag"):
            headers["If-None-Match"] = self.existing_state["etag"]
        if self.existing_state.get("last_modified"):
            headers["If-Modified-Since"] = self.existing_state["last_modified"]
        yield Request(self.task["url"], callback=self.parse_page, headers=headers, dont_filter=True)

    def parse_page(self, response):
        url = canonicalize_url(response.url)
        source_id = self.task.get("source_id")
        url_hash = hashlib.sha256(url.encode()).hexdigest()

        if response.status == 304:
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (source_id, url, "not_modified", "HTTP 304"))
                conn.commit()
            return

        raw_html = response.body
        content_hash = hashlib.sha256(raw_html).hexdigest()
        if content_hash == self.existing_state.get("content_hash"):
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (source_id, url, "not_modified", "hash unchanged"))
                conn.commit()
            return

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        s3_key = f"raw/{url_hash[:2]}/{url_hash}_{ts}.html"
        self.s3.put_object(Bucket=settings.s3_bucket, Key=s3_key, Body=raw_html, ContentType="text/html")

        with get_conn() as (conn, cur):
            cur.execute(
                UPSERT_PAGE_STATE,
                {
                    "url": url,
                    "url_hash": url_hash,
                    "content_hash": content_hash,
                    "s3_key": s3_key,
                    "http_status": response.status,
                    "etag": response.headers.get("ETag", b"").decode("utf-8") or None,
                    "last_modified": response.headers.get("Last-Modified", b"").decode("utf-8") or None,
                },
            )
            cur.execute(INSERT_LOG, (source_id, url, "fetched", f"stored={s3_key}"))
            conn.commit()


def run_task(task: dict[str, Any], s3_client):
    normalized = canonicalize_url(task["url"])
    url_hash = hashlib.sha256(normalized.encode()).hexdigest()

    existing_state = None
    with get_conn() as (conn, cur):
        cur.execute(SELECT_PAGE_STATE, (url_hash,))
        row = cur.fetchone()
        if row:
            existing_state = {"etag": row[0], "last_modified": row[1], "content_hash": row[2]}
            cur.execute(INSERT_LOG, (task.get("source_id"), normalized, "recrawl", "existing url hash found"))
            conn.commit()

    proc = CrawlerProcess(settings=get_project_settings())
    proc.crawl(SingleURLSpider, task=task, s3_client=s3_client, existing_state=existing_state)
    proc.start()


def main() -> None:
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )
    consumer = build_consumer("scrapy-crawler", settings.crawl_queue_topic)
    producer = build_producer()

    for msg in consumer:
        task = msg.value
        try:
            run_task(task, s3_client)
            producer.send(settings.crawl_results_topic, {"url": task["url"], "status": "done"})
            consumer.commit()
        except Exception as exc:  # noqa: BLE001
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (task.get("source_id"), task["url"], "error", str(exc)[:2000]))
                conn.commit()
            producer.send(settings.crawl_results_topic, {"url": task["url"], "status": "error", "error": str(exc)})
            consumer.commit()


if __name__ == "__main__":
    main()

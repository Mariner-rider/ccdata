import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    postgres_dsn: str = _env("POSTGRES_DSN", "postgresql://crawler:crawler@postgres:5432/crawler")
    kafka_bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    crawl_queue_topic: str = _env("CRAWL_QUEUE_TOPIC", "crawl.queue")
    crawl_results_topic: str = _env("CRAWL_RESULTS_TOPIC", "crawl.results")
    parse_results_topic: str = _env("PARSE_RESULTS_TOPIC", "parse.results")
    schema_mapped_topic: str = _env("SCHEMA_MAPPED_TOPIC", "schema.mapped")
    enriched_results_topic: str = _env("ENRICHED_RESULTS_TOPIC", "content.enriched")
    college_page_events_topic: str = _env("COLLEGE_PAGE_EVENTS_TOPIC", "college.pages.events")
    chatbot_postgres_dsn: str = _env("CHATBOT_POSTGRES_DSN", "postgresql://crawler:crawler@postgres:5432/crawler")
    elasticsearch_url: str = _env("ELASTICSEARCH_URL", "http://elasticsearch:9200")
    realtime_crawl_request_topic: str = _env("REALTIME_CRAWL_REQUEST_TOPIC", "crawl.requests.realtime")
    review_events_topic: str = _env("REVIEW_EVENTS_TOPIC", "reviews.events")
    freshness_days_threshold: int = int(_env("FRESHNESS_DAYS_THRESHOLD", "30"))
    freshness_scan_interval_seconds: int = int(_env("FRESHNESS_SCAN_INTERVAL_SECONDS", "300"))
    freshness_max_events_per_cycle: int = int(_env("FRESHNESS_MAX_EVENTS_PER_CYCLE", "200"))
    s3_endpoint_url: str = _env("S3_ENDPOINT_URL", "http://minio:9000")
    s3_bucket: str = _env("S3_BUCKET", "raw-html")
    s3_access_key: str = _env("S3_ACCESS_KEY", "minio")
    s3_secret_key: str = _env("S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", ""))
    per_domain_delay_seconds: float = float(_env("PER_DOMAIN_DELAY_SECONDS", "1.0"))
    max_depth: int = int(_env("MAX_DEPTH", "2"))


settings = Settings()

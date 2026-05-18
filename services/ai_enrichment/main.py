import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from services.common.user_agents import get_headers
from duckduckgo_search import DDGS
from transformers import pipeline

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""

UPSERT_ENRICHED = """
INSERT INTO enriched_records (
    source_url,
    category,
    record_hash,
    payload,
    enriched_at
)
VALUES (%s, %s, %s, %s::jsonb, NOW())
ON CONFLICT (record_hash)
DO UPDATE SET payload = EXCLUDED.payload, enriched_at = NOW();
"""

CHECK_DUPLICATE = """
SELECT record_hash
FROM enriched_records
WHERE record_hash = %s
LIMIT 1;
"""


@dataclass
class EnrichmentModels:
    classifier: Any
    summarizer: Any
    sentiment: Any
    fake_detector: Any


def _safe_pipeline(task: str, model: str, fallback: str):
    try:
        return pipeline(task, model=model)
    except Exception:  # noqa: BLE001
        return pipeline(task, model=fallback)


def _build_models() -> EnrichmentModels:
    classifier = _safe_pipeline(
        "zero-shot-classification",
        os.getenv("CLASSIFIER_MODEL", "valhalla/distilbart-mnli-12-1"),
        "typeform/distilbert-base-uncased-mnli",
    )
    summarizer = _safe_pipeline(
        "summarization",
        os.getenv("SUMMARIZER_MODEL", "sshleifer/distilbart-cnn-12-6"),
        "sshleifer/distilbart-cnn-12-6",
    )
    sentiment = _safe_pipeline(
        "sentiment-analysis",
        os.getenv("SENTIMENT_MODEL", "sshleifer/tiny-distilbert-base-uncased-finetuned-sst-2-english"),
        "distilbert-base-uncased-finetuned-sst-2-english",
    )
    fake_detector = _safe_pipeline(
        "text-classification",
        os.getenv("FAKE_DETECTOR_MODEL", "mrm8488/bert-tiny-finetuned-fake-news-detection"),
        "distilbert-base-uncased-finetuned-sst-2-english",
    )
    return EnrichmentModels(classifier=classifier, summarizer=summarizer, sentiment=sentiment, fake_detector=fake_detector)


def _flatten_record(record: dict[str, Any]) -> str:
    values = []
    for key, value in record.get("data", {}).items():
        if isinstance(value, list):
            values.append(f"{key}: {' | '.join(str(x) for x in value)}")
        else:
            values.append(f"{key}: {value}")
    return "\n".join(values)


def _web_search_enrich(entity: str, category: str) -> dict[str, Any]:
    # Lightweight web lookup to fill missing fields.
    results = []
    query = f"{entity} {category} official admissions fees placements reviews"
    with DDGS() as ddgs:
        for item in ddgs.text(query, max_results=5):
            results.append(
                {
                    "title": item.get("title"),
                    "href": item.get("href"),
                    "snippet": item.get("body"),
                }
            )
    return {"query": query, "results": results}


def _llm_fallback(prompt: str) -> dict[str, Any] | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    body = {
        "model": os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        "input": prompt,
        "temperature": 0.1,
    }
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={**get_headers("https://api.openai.com/v1/responses"), "Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        data=json.dumps(body),
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    text = payload.get("output", [{}])[0].get("content", [{}])[0].get("text", "")
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}


def classify_content(models: EnrichmentModels, text: str) -> dict[str, Any]:
    labels = ["college", "job", "news", "scholarship"]
    out = models.classifier(text, candidate_labels=labels, multi_label=False)
    return {"label": out["labels"][0], "score": float(out["scores"][0])}


def generate_summary(models: EnrichmentModels, text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    clipped = text[:2200]
    summary = models.summarizer(clipped, max_length=140, min_length=40, do_sample=False)
    return summary[0]["summary_text"]


def extract_sentiment(models: EnrichmentModels, text: str) -> dict[str, Any]:
    if not text:
        return {"label": "NEUTRAL", "score": 0.0}
    out = models.sentiment(text[:512])[0]
    return {"label": out["label"], "score": float(out["score"])}


def detect_fake(models: EnrichmentModels, text: str) -> dict[str, Any]:
    if not text:
        return {"label": "unknown", "score": 0.0}
    model_out = models.fake_detector(text[:512])[0]
    heuristic_flags = []
    if re.search(r"guaranteed\s+100%|miracle|secret\s+trick", text, re.IGNORECASE):
        heuristic_flags.append("marketing_hype")
    if text.count("!") > 8:
        heuristic_flags.append("excessive_punctuation")

    return {
        "label": model_out["label"],
        "score": float(model_out["score"]),
        "heuristics": heuristic_flags,
    }


def fill_missing_fields(record: dict[str, Any], search_data: dict[str, Any]) -> dict[str, Any]:
    data = record.get("data", {})
    snippets = " ".join(filter(None, [r.get("snippet") for r in search_data.get("results", [])]))
    if not data.get("fees"):
        m = re.findall(r"\$\s?\d[\d,]*", snippets)
        if m:
            data["fees"] = list(dict.fromkeys(m[:5]))
    if not data.get("admission_links") and search_data.get("results"):
        links = [r.get("href") for r in search_data["results"] if r.get("href")]
        if links:
            data["admission_links"] = links[:3]
    record["data"] = data
    return record


def compute_record_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def is_duplicate(record_hash: str) -> bool:
    with get_conn() as (conn, cur):
        cur.execute(CHECK_DUPLICATE, (record_hash,))
        row = cur.fetchone()
        conn.commit()
    return bool(row)


def enrich_record(models: EnrichmentModels, mapped_record: dict[str, Any]) -> dict[str, Any]:
    text = _flatten_record(mapped_record)

    classification = classify_content(models, text)
    summary = generate_summary(models, text)
    sentiment = extract_sentiment(models, text)
    fake_detection = detect_fake(models, text)

    entity = mapped_record.get("data", {}).get("name") or mapped_record.get("source_url") or ""
    search_data = _web_search_enrich(str(entity), classification["label"])
    enriched = fill_missing_fields(mapped_record, search_data)

    enriched["ai_enrichment"] = {
        "classification": classification,
        "summary": summary,
        "reviews_sentiment": sentiment,
        "fake_content": fake_detection,
        "web_search": search_data,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }

    hash_value = compute_record_hash(enriched)
    enriched["ai_enrichment"]["duplicate"] = is_duplicate(hash_value)

    if classification["score"] < 0.55 or not summary:
        llm_data = _llm_fallback(
            "Return JSON with improved category, summary, and missing fields for this record:\n"
            + json.dumps(enriched, ensure_ascii=False)
        )
        if llm_data:
            enriched["ai_enrichment"]["llm_fallback"] = llm_data

    enriched["record_hash"] = hash_value
    return enriched


def main() -> None:
    models = _build_models()
    consumer = build_consumer("ai-enrichment", settings.schema_mapped_topic)
    producer = build_producer()

    for msg in consumer:
        record = msg.value
        try:
            enriched = enrich_record(models, record)
            with get_conn() as (conn, cur):
                cur.execute(
                    UPSERT_ENRICHED,
                    (
                        enriched.get("source_url"),
                        enriched.get("category", "unknown"),
                        enriched["record_hash"],
                        json.dumps(enriched),
                    ),
                )
                cur.execute(
                    INSERT_LOG,
                    (
                        None,
                        enriched.get("source_url") or "",
                        "ai_enriched",
                        f"category={enriched.get('category')} duplicate={enriched['ai_enrichment']['duplicate']}",
                    ),
                )
                conn.commit()
            producer.send(settings.enriched_results_topic, enriched)
            print(json.dumps(enriched))
            consumer.commit()
        except Exception as exc:  # noqa: BLE001
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (None, record.get("source_url", ""), "ai_enrich_error", str(exc)[:2000]))
                conn.commit()
            consumer.commit()


if __name__ == "__main__":
    main()

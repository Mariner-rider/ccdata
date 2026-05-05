import threading
from typing import Any

from elasticsearch import Elasticsearch
from fastapi import Depends, FastAPI, Query, Request
from pydantic import BaseModel

from services.common.config import settings
from services.common.kafka_client import build_consumer
from services.common.security import enforce_rate_limit, verify_api_key

INDEX_NAME = "college_pages"

INDEX_BODY = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "refresh_interval": "1s",
        "analysis": {
            "normalizer": {
                "lc": {
                    "type": "custom",
                    "filter": ["lowercase", "asciifolding"],
                }
            }
        },
    },
    "mappings": {
        "properties": {
            "page_id": {"type": "long"},
            "slug": {"type": "keyword", "normalizer": "lc"},
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "country": {"type": "keyword"},
            "courses": {"type": "keyword"},
            "fees_min": {"type": "double"},
            "fees_max": {"type": "double"},
            "rating": {"type": "float"},
            "popularity": {"type": "float"},
            "suggest": {"type": "completion"},
            "raw": {"type": "object", "enabled": True},
        }
    },
}

app = FastAPI(title="ccdata-elasticsearch-search", version="1.0.0")
es = Elasticsearch(settings.elasticsearch_url, request_timeout=2)


class IndexDoc(BaseModel):
    page_id: int
    slug: str
    name: str
    country: str | None = None
    courses: list[str] = []
    fees_min: float | None = None
    fees_max: float | None = None
    rating: float = 0.0
    popularity: float = 0.0
    raw: dict[str, Any] = {}


def ensure_index() -> None:
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=INDEX_BODY)


def _extract_doc(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {})
    ai = payload.get("ai_enrichment", {})

    courses = data.get("courses", []) or []
    fees = data.get("fees", []) or []
    numeric_fees = []
    for fee in fees:
        try:
            numeric_fees.append(float(str(fee).replace("$", "").replace(",", "").strip()))
        except Exception:  # noqa: BLE001
            continue

    rating = float(data.get("rating", 0) or ai.get("rating", 0) or 0)
    popularity = float(data.get("popularity", 0) or 0)

    name = data.get("name") or payload.get("slug") or "unknown"
    country = data.get("country") or data.get("country_code") or ""

    doc = {
        "page_id": payload.get("page_id", 0),
        "slug": payload.get("slug", ""),
        "name": name,
        "country": country,
        "courses": [str(c).strip().lower() for c in courses if str(c).strip()],
        "fees_min": min(numeric_fees) if numeric_fees else None,
        "fees_max": max(numeric_fees) if numeric_fees else None,
        "rating": rating,
        "popularity": popularity,
        "suggest": {
            "input": [name, payload.get("slug", "")],
            "weight": int(max(1, popularity * 10 + rating * 20)),
        },
        "raw": payload,
    }
    return doc


def consume_events() -> None:
    consumer = build_consumer("search-indexer", settings.college_page_events_topic)
    for msg in consumer:
        payload = msg.value
        try:
            source = payload.get("raw") if isinstance(payload.get("raw"), dict) else payload
            doc = _extract_doc(source)
            if not doc.get("slug"):
                consumer.commit()
                continue
            es.index(index=INDEX_NAME, id=doc["slug"], document=doc, refresh=False)
            consumer.commit()
        except Exception:  # noqa: BLE001
            consumer.commit()


@app.on_event("startup")
def startup() -> None:
    ensure_index()
    thread = threading.Thread(target=consume_events, daemon=True)
    thread.start()


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "index": INDEX_NAME}


@app.post("/index")
def index_doc(doc: IndexDoc, _: None = Depends(verify_api_key), request: Request = None) -> dict[str, Any]:
    enforce_rate_limit(request.client.host if request and request.client else "unknown")
    ensure_index()
    body = doc.model_dump()
    body["suggest"] = {"input": [body["name"], body["slug"]], "weight": int(max(1, body["popularity"] * 10 + body["rating"] * 20))}
    es.index(index=INDEX_NAME, id=doc.slug, document=body, refresh=True)
    return {"indexed": True, "id": doc.slug}


@app.get("/search")
def search(
    q: str = Query("", description="Search query"),
    country: str | None = Query(None),
    course: str | None = Query(None),
    fees_max: float | None = Query(None),
    size: int = Query(20, ge=1, le=100),
    _: None = Depends(verify_api_key),
    request: Request = None,
) -> dict[str, Any]:
    enforce_rate_limit(request.client.host if request and request.client else "unknown")
    filters = []
    if country:
        filters.append({"term": {"country": country}})
    if course:
        filters.append({"term": {"courses": course.lower()}})
    if fees_max is not None:
        filters.append({"range": {"fees_min": {"lte": fees_max}}})

    base_query: dict[str, Any]
    if q:
        base_query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["name^3", "courses^2", "raw.data.summary"],
                            "fuzziness": "AUTO",
                        }
                    }
                ],
                "filter": filters,
            }
        }
    else:
        base_query = {"bool": {"must": [{"match_all": {}}], "filter": filters}}

    query = {
        "size": size,
        "track_total_hits": False,
        "timeout": "150ms",
        "query": {
            "function_score": {
                "query": base_query,
                "boost_mode": "sum",
                "score_mode": "sum",
                "functions": [
                    {"field_value_factor": {"field": "popularity", "factor": 1.0, "missing": 0}},
                    {"field_value_factor": {"field": "rating", "factor": 2.0, "missing": 0}},
                ],
            }
        },
        "sort": ["_score"],
    }

    result = es.search(index=INDEX_NAME, body=query)
    hits = [
        {
            "score": h.get("_score"),
            "slug": h["_source"].get("slug"),
            "name": h["_source"].get("name"),
            "country": h["_source"].get("country"),
            "courses": h["_source"].get("courses"),
            "fees_min": h["_source"].get("fees_min"),
            "fees_max": h["_source"].get("fees_max"),
            "rating": h["_source"].get("rating"),
            "popularity": h["_source"].get("popularity"),
        }
        for h in result.get("hits", {}).get("hits", [])
    ]
    return {"count": len(hits), "results": hits}


@app.get("/suggest")
def suggest(q: str = Query(..., min_length=1), size: int = Query(8, ge=1, le=20), _: None = Depends(verify_api_key), request: Request = None) -> dict[str, Any]:
    enforce_rate_limit(request.client.host if request and request.client else "unknown")
    body = {
        "suggest": {
            "college-suggest": {
                "prefix": q,
                "completion": {
                    "field": "suggest",
                    "size": size,
                    "skip_duplicates": True,
                },
            }
        }
    }
    result = es.search(index=INDEX_NAME, body=body)
    options = result.get("suggest", {}).get("college-suggest", [{}])[0].get("options", [])
    suggestions = [{"text": opt.get("text"), "score": opt.get("_score")} for opt in options]
    return {"query": q, "suggestions": suggestions}

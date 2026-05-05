import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""

UPSERT_NORMALIZED = """
INSERT INTO normalized_records (
    source_url,
    category,
    record_hash,
    payload,
    mapped_at
)
VALUES (%s, %s, %s, %s::jsonb, NOW())
ON CONFLICT (record_hash)
DO UPDATE SET payload = EXCLUDED.payload, mapped_at = NOW();
"""


@dataclass
class MatchResult:
    target_field: str
    confidence: float
    method: str


class SchemaMappingEngine:
    CATEGORY_SCHEMA = {
        "school": {
            "name": ["school", "institution", "academy name"],
            "fees": ["tuition fee", "annual fee", "cost"],
            "admission_links": ["admission", "apply now", "enroll"],
            "courses": ["program", "curriculum", "subjects"],
            "faculty": ["teachers", "staff", "faculty"],
            "placements": ["placement", "career outcomes", "jobs"],
        },
        "colleges": {
            "name": ["college name", "institution", "university"],
            "fees": ["tuition fee", "fee structure", "cost of study"],
            "admission_links": ["admission", "application form", "apply"],
            "courses": ["courses", "programs", "degrees"],
            "faculty": ["faculty", "professors", "departments"],
            "placements": ["placements", "median package", "recruiters"],
        },
        "jobs": {
            "title": ["job title", "position", "role"],
            "company": ["company", "employer", "organization"],
            "location": ["location", "city", "onsite/remote"],
            "salary": ["salary", "ctc", "compensation"],
            "apply_link": ["apply", "application", "job link"],
            "description": ["description", "responsibilities", "job summary"],
        },
        "scholarships": {
            "name": ["scholarship", "grant name", "fellowship"],
            "eligibility": ["eligibility", "criteria", "requirements"],
            "amount": ["amount", "award", "funding"],
            "deadline": ["deadline", "last date", "closing date"],
            "apply_link": ["apply", "application form", "register"],
            "provider": ["provider", "sponsor", "organization"],
        },
        "news": {
            "headline": ["headline", "title", "news title"],
            "summary": ["summary", "snippet", "abstract"],
            "published_at": ["date", "published", "timestamp"],
            "author": ["author", "reporter", "byline"],
            "source": ["source", "publisher", "news outlet"],
            "url": ["url", "article link", "story link"],
        },
    }

    def __init__(self):
        # Build semantic vocabulary for NLP-style matching
        self._category_docs: dict[str, list[str]] = {}
        self._vectorizers: dict[str, TfidfVectorizer] = {}
        self._schema_vectors: dict[str, Any] = {}

        for category, schema in self.CATEGORY_SCHEMA.items():
            docs = [f"{field} {' '.join(synonyms)}" for field, synonyms in schema.items()]
            vectorizer = TfidfVectorizer(ngram_range=(1, 2))
            schema_vectors = vectorizer.fit_transform(docs)
            self._category_docs[category] = list(schema.keys())
            self._vectorizers[category] = vectorizer
            self._schema_vectors[category] = schema_vectors

    @staticmethod
    def _normalize_key(key: str) -> str:
        key = key.replace("_", " ").replace("-", " ").lower().strip()
        return re.sub(r"\s+", " ", key)

    def detect_category(self, raw: dict[str, Any], default: str = "colleges") -> str:
        flattened = " ".join(self._normalize_key(k) for k in raw.keys())
        best_cat, best_score = default, 0.0
        for category, schema in self.CATEGORY_SCHEMA.items():
            category_tokens = " ".join([category] + [s for aliases in schema.values() for s in aliases])
            score = fuzz.token_set_ratio(flattened, category_tokens) / 100.0
            if score > best_score:
                best_cat, best_score = category, score
        return best_cat

    def _best_schema_match(self, category: str, source_key: str) -> MatchResult | None:
        schema = self.CATEGORY_SCHEMA[category]
        normalized = self._normalize_key(source_key)

        # 1) direct exact alias
        for target, aliases in schema.items():
            if normalized == target or normalized in aliases:
                return MatchResult(target_field=target, confidence=0.99, method="exact")

        # 2) fuzzy alias similarity
        fuzzy_best = (None, 0.0)
        for target, aliases in schema.items():
            candidates = [target, *aliases]
            for candidate in candidates:
                score = fuzz.token_sort_ratio(normalized, candidate) / 100.0
                if score > fuzzy_best[1]:
                    fuzzy_best = (target, score)

        # 3) semantic similarity (TF-IDF cosine)
        vectorizer = self._vectorizers[category]
        source_vec = vectorizer.transform([normalized])
        sims = cosine_similarity(source_vec, self._schema_vectors[category]).flatten()
        semantic_idx = int(sims.argmax())
        semantic_field = self._category_docs[category][semantic_idx]
        semantic_score = float(sims[semantic_idx])

        fuzzy_target, fuzzy_score = fuzzy_best
        if fuzzy_score >= semantic_score and fuzzy_score >= 0.55:
            return MatchResult(target_field=str(fuzzy_target), confidence=round(fuzzy_score, 2), method="fuzzy")
        if semantic_score >= 0.35:
            return MatchResult(target_field=semantic_field, confidence=round(semantic_score, 2), method="semantic")
        return None

    @staticmethod
    def _clean_value(value: Any) -> Any:
        if isinstance(value, dict) and "value" in value:
            value = value["value"]

        if isinstance(value, str):
            cleaned = re.sub(r"\s+", " ", value).strip()
            return cleaned
        if isinstance(value, list):
            out = []
            for item in value:
                if isinstance(item, str):
                    item = re.sub(r"\s+", " ", item).strip()
                    if item:
                        out.append(item)
                elif item is not None:
                    out.append(item)
            return out
        return value

    def map_record(self, raw: dict[str, Any], category: str | None = None) -> dict[str, Any]:
        chosen_category = category or self.detect_category(raw)
        schema = self.CATEGORY_SCHEMA[chosen_category]

        mapped: dict[str, Any] = {field: None for field in schema.keys()}
        meta: dict[str, Any] = {
            "category": chosen_category,
            "mapped_at": datetime.now(timezone.utc).isoformat(),
            "field_mapping": {},
        }

        for source_key, raw_value in raw.items():
            if source_key in {"url", "overall_confidence", "render_mode"}:
                continue
            match = self._best_schema_match(chosen_category, source_key)
            if not match:
                continue
            cleaned_value = self._clean_value(raw_value)
            if cleaned_value in (None, "", []):
                continue

            current = mapped.get(match.target_field)
            if current in (None, "", []):
                mapped[match.target_field] = cleaned_value
            elif isinstance(current, list):
                if isinstance(cleaned_value, list):
                    mapped[match.target_field] = list(dict.fromkeys(current + cleaned_value))
                else:
                    mapped[match.target_field] = list(dict.fromkeys(current + [cleaned_value]))

            meta["field_mapping"][source_key] = {
                "target": match.target_field,
                "confidence": match.confidence,
                "method": match.method,
            }

        cleaned = {
            "category": chosen_category,
            "source_url": raw.get("url"),
            "data": {k: v for k, v in mapped.items() if v not in (None, "", [])},
            "meta": meta,
        }
        return cleaned


def compute_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    import hashlib

    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def main() -> None:
    engine = SchemaMappingEngine()
    consumer = build_consumer("schema-mapper", settings.parse_results_topic)
    producer = build_producer()

    for msg in consumer:
        raw = msg.value
        try:
            mapped = engine.map_record(raw)
            record_hash = compute_hash(mapped)
            with get_conn() as (conn, cur):
                cur.execute(
                    UPSERT_NORMALIZED,
                    (
                        mapped.get("source_url"),
                        mapped["category"],
                        record_hash,
                        json.dumps(mapped),
                    ),
                )
                cur.execute(
                    INSERT_LOG,
                    (None, mapped.get("source_url") or "", "schema_mapped", f"category={mapped['category']} hash={record_hash[:12]}"),
                )
                conn.commit()
            producer.send(settings.schema_mapped_topic, mapped)
            print(json.dumps(mapped))
            consumer.commit()
        except Exception as exc:  # noqa: BLE001
            with get_conn() as (conn, cur):
                cur.execute(INSERT_LOG, (None, raw.get("url", ""), "schema_map_error", str(exc)[:2000]))
                conn.commit()
            consumer.commit()


if __name__ == "__main__":
    main()

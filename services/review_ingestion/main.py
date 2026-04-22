import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config
from bs4 import BeautifulSoup
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.linear_model import SGDClassifier
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from w3lib.url import canonicalize_url

from services.common.config import settings
from services.common.db import get_conn
from services.common.kafka_client import build_consumer, build_producer
from services.common.security import enforce_rate_limit, verify_api_key

SELECT_LATEST_HTML = """
SELECT s3_key
FROM page_state
WHERE url_hash = %s
ORDER BY last_crawled_at DESC
LIMIT 1;
"""

INSERT_REVIEW = """
INSERT INTO reviews (
    user_id,
    target_type,
    target_id,
    rating,
    review_title,
    review_text,
    sentiment_label,
    sentiment_score,
    is_verified,
    attrs,
    created_at,
    updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW(), NOW())
RETURNING id;
"""

INSERT_LOG = """
INSERT INTO crawl_logs(source_id, url, status, detail, event_ts)
VALUES (%s, %s, %s, %s, NOW());
"""


class UserReviewIn(BaseModel):
    user_id: int
    target_type: str = Field(pattern="^(college|institute|job|scholarship)$")
    target_id: int
    review_title: str = ""
    review_text: str
    rating: int = Field(ge=1, le=5)


class ReviewIngestionEngine:
    def __init__(self):
        self.sentiment = SentimentIntensityAnalyzer()
        self.vectorizer = HashingVectorizer(n_features=2**16, alternate_sign=False)
        self.fake_model = SGDClassifier(loss="log_loss", random_state=42)
        self._train_fake_model()

    def _train_fake_model(self) -> None:
        # Lightweight warm-start model (replace with offline-trained model in prod).
        texts = [
            "Great campus and genuine placement support.",
            "Faculty helped me with projects and internships.",
            "Best college ever guaranteed 100 percent job!!!",
            "Miracle admission no exam pay now limited seats!!!",
            "Hostel was decent and food quality improved.",
            "Secret trick to get instant scholarship click now!!!",
        ]
        labels = [0, 0, 1, 1, 0, 1]  # 1 => fake/suspicious
        X = self.vectorizer.transform(texts)
        self.fake_model.partial_fit(X, labels, classes=[0, 1])

    def sentiment_score(self, text: str) -> tuple[str, float]:
        score = float(self.sentiment.polarity_scores(text).get("compound", 0.0))
        if score > 0.2:
            label = "POSITIVE"
        elif score < -0.2:
            label = "NEGATIVE"
        else:
            label = "NEUTRAL"
        return label, score

    def fake_probability(self, text: str) -> float:
        X = self.vectorizer.transform([text])
        prob = float(self.fake_model.predict_proba(X)[0][1])
        # heuristic boost for obvious spam cues
        if re.search(r"guaranteed\s+100%|limited\s+seats|click\s+now|miracle", text, re.IGNORECASE):
            prob = min(1.0, prob + 0.2)
        if text.count("!") > 5:
            prob = min(1.0, prob + 0.15)
        return prob

    def extract_reviews_from_html(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        extracted: list[dict[str, Any]] = []

        candidates = soup.select(".review, .reviews li, [itemprop='review'], .testimonial")
        for node in candidates[:200]:
            text = node.get_text(" ", strip=True)
            if len(text) < 20:
                continue
            rating = 4
            rating_match = re.search(r"([1-5](?:\.0)?)\s*/\s*5", text)
            if rating_match:
                rating = max(1, min(5, int(float(rating_match.group(1)))))
            extracted.append({"title": "Web Review", "text": text[:4000], "rating": rating})

        if not extracted:
            for p in soup.find_all("p")[:300]:
                text = p.get_text(" ", strip=True)
                if len(text) > 60 and any(k in text.lower() for k in ["review", "faculty", "placement", "hostel", "course"]):
                    extracted.append({"title": "Web Review", "text": text[:4000], "rating": 4})
                    if len(extracted) >= 50:
                        break
        return extracted


engine = ReviewIngestionEngine()
app = FastAPI(title="ccdata-review-ingestion", version="1.0.0")


def load_html_from_s3(url: str) -> str | None:
    canonical = canonicalize_url(url)
    url_hash = hashlib.sha256(canonical.encode()).hexdigest()

    with get_conn() as (conn, cur):
        cur.execute(SELECT_LATEST_HTML, (url_hash,))
        row = cur.fetchone()
        conn.commit()
    if not row:
        return None

    s3_key = row[0]
    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4"),
    )
    obj = s3_client.get_object(Bucket=settings.s3_bucket, Key=s3_key)
    return obj["Body"].read().decode("utf-8", errors="ignore")


def store_review(user_id: int, target_type: str, target_id: int, title: str, text: str, rating: int, is_verified: bool, attrs: dict[str, Any]) -> int:
    sent_label, sent_score = engine.sentiment_score(text)
    fake_prob = engine.fake_probability(text)
    attrs = {**attrs, "fake_probability": fake_prob}

    with get_conn() as (conn, cur):
        cur.execute(
            INSERT_REVIEW,
            (
                user_id,
                target_type,
                target_id,
                rating,
                title,
                text,
                sent_label,
                sent_score,
                is_verified and fake_prob < 0.75,
                json.dumps(attrs),
            ),
        )
        review_id = cur.fetchone()[0]
        cur.execute(
            INSERT_LOG,
            (
                None,
                attrs.get("source_url", ""),
                "review_ingested",
                f"review_id={review_id} fake_prob={fake_prob:.3f} sentiment={sent_label}:{sent_score:.3f}",
            ),
        )
        conn.commit()
    return review_id


def crawl_review_loop() -> None:
    consumer = build_consumer("review-crawler", settings.crawl_results_topic)
    producer = build_producer()

    for msg in consumer:
        event = msg.value
        if event.get("status") != "done":
            consumer.commit()
            continue
        url = event.get("url")
        if not url:
            consumer.commit()
            continue

        html = load_html_from_s3(url)
        if not html:
            consumer.commit()
            continue

        reviews = engine.extract_reviews_from_html(html)
        for rv in reviews[:100]:
            review_id = store_review(
                user_id=0,
                target_type="college",
                target_id=event.get("target_id", 0),
                title=rv["title"],
                text=rv["text"],
                rating=rv["rating"],
                is_verified=False,
                attrs={"source": "web_crawl", "source_url": url},
            )
            producer.send(settings.review_events_topic, {"review_id": review_id, "source": "web_crawl", "url": url})
        consumer.commit()


@app.on_event("startup")
def startup() -> None:
    thread = threading.Thread(target=crawl_review_loop, daemon=True)
    thread.start()


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok"}


@app.post("/reviews/user")
def ingest_user_review(review: UserReviewIn, _: None = Depends(verify_api_key), request: Request = None) -> dict[str, Any]:
    enforce_rate_limit(request.client.host if request and request.client else "unknown")
    if len(review.review_text.strip()) < 20:
        raise HTTPException(status_code=400, detail="review_text is too short")

    review_id = store_review(
        user_id=review.user_id,
        target_type=review.target_type,
        target_id=review.target_id,
        title=review.review_title,
        text=review.review_text,
        rating=review.rating,
        is_verified=True,
        attrs={"source": "user_submission", "submitted_at": datetime.now(timezone.utc).isoformat()},
    )

    fake_prob = engine.fake_probability(review.review_text)
    sent_label, sent_score = engine.sentiment_score(review.review_text)
    return {
        "review_id": review_id,
        "fake_probability": fake_prob,
        "sentiment": {"label": sent_label, "score": sent_score},
    }

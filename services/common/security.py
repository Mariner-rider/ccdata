import hmac
import os
import time
from collections import defaultdict, deque
from fastapi import Header, HTTPException


SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "change-me")
RATE_LIMIT_PER_MINUTE = int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "120"))

_request_windows: dict[str, deque[float]] = defaultdict(deque)


def verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if SERVICE_API_KEY in {"", "change-me"}:
        raise HTTPException(status_code=503, detail="Service API key is not configured securely")

    if not x_api_key or not hmac.compare_digest(x_api_key, SERVICE_API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")


def enforce_rate_limit(client_id: str) -> None:
    now = time.time()
    bucket = _request_windows[client_id]
    while bucket and now - bucket[0] > 60:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    bucket.append(now)

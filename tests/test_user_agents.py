from services.common.user_agents import ACCEPT_LANGUAGES, USER_AGENT_POOL, add_jitter, get_headers


def test_pool_size():
    assert len(USER_AGENT_POOL) >= 20


def test_no_bot_strings():
    forbidden = ("bot", "crawler", "scraper", "spider", "python", "requests", "httpx", "ccdata")
    for user_agent in USER_AGENT_POOL:
        lowered = user_agent.lower()
        assert not any(word in lowered for word in forbidden)


def test_get_headers_keys():
    headers = get_headers("https://iimb.ac.in/courses")
    required = {
        "User-Agent",
        "Accept",
        "Accept-Language",
        "Accept-Encoding",
        "Connection",
        "Upgrade-Insecure-Requests",
        "Sec-Fetch-Dest",
        "Sec-Fetch-Mode",
        "Sec-Fetch-Site",
        "Sec-Fetch-User",
        "Cache-Control",
        "DNT",
    }
    assert required <= set(headers)
    assert headers["User-Agent"] in USER_AGENT_POOL
    assert headers["Accept-Language"] in ACCEPT_LANGUAGES


def test_accept_language_rotates():
    languages = {get_headers("https://iimb.ac.in/")["Accept-Language"] for _ in range(20)}
    assert len(languages) >= 2


def test_jitter_range():
    for _ in range(100):
        assert 0.5 <= add_jitter(1.0) <= 2.5


def test_referer_set_for_subpage():
    headers = get_headers("https://iimb.ac.in/courses")
    assert headers["Referer"] == "https://iimb.ac.in"
    assert headers["Sec-Fetch-Site"] == "same-origin"

from services.common.user_agents import USER_AGENT_POOL, add_jitter, get_headers, get_random_ua


def test_user_agent_pool_has_browser_coverage():
    assert len(USER_AGENT_POOL) >= 20
    joined = "\n".join(USER_AGENT_POOL)
    assert "Chrome/" in joined
    assert "Firefox/" in joined
    assert "Safari/" in joined
    assert "Edg/" in joined


def test_user_agents_do_not_contain_bot_strings():
    forbidden = ("bot", "crawler", "scraper", "ccdata")
    for user_agent in USER_AGENT_POOL:
        lowered = user_agent.lower()
        assert not any(word in lowered for word in forbidden)


def test_get_headers_returns_required_browser_keys():
    headers = get_headers("https://example.edu/admissions/apply")
    required = {
        "User-Agent",
        "Accept",
        "Accept-Language",
        "Accept-Encoding",
        "Connection",
        "Sec-Fetch-Dest",
        "Sec-Fetch-Mode",
        "Sec-Fetch-Site",
        "Referer",
        "Cache-Control",
        "DNT",
    }
    assert required <= set(headers)
    assert headers["Referer"] == "https://example.edu/"
    assert headers["User-Agent"] in USER_AGENT_POOL


def test_add_jitter_stays_in_expected_range():
    base = 1.5
    for _ in range(100):
        value = add_jitter(base)
        assert base - 0.3 <= value <= base + 0.8


def test_random_user_agent_always_from_pool():
    for _ in range(100):
        assert get_random_ua() in USER_AGENT_POOL

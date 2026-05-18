from services.common.user_agents import USER_AGENT_POOL, add_jitter, get_headers


def test_pool_size():
    assert len(USER_AGENT_POOL) >= 20


def test_no_bot_strings():
    bad_words = ["bot", "crawler", "scraper", "spider", "python", "requests", "httpx", "ccdata"]
    assert not [ua for ua in USER_AGENT_POOL if any(word in ua.lower() for word in bad_words)]


def test_get_headers_keys():
    headers = get_headers("https://iimb.ac.in/courses")
    for key in [
        "User-Agent", "Accept", "Accept-Language", "Accept-Encoding", "Connection",
        "Upgrade-Insecure-Requests", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site",
        "Sec-Fetch-User", "Cache-Control", "DNT", "Referer",
    ]:
        assert key in headers


def test_accept_language_rotates():
    langs = {get_headers("https://iimb.ac.in/about")["Accept-Language"] for _ in range(20)}
    assert len(langs) >= 2


def test_jitter_range():
    vals = [add_jitter(1.0) for _ in range(100)]
    assert all(0.5 <= v <= 2.5 for v in vals)


def test_referer_set_for_subpage():
    headers = get_headers("https://iimb.ac.in/courses")
    assert headers["Referer"] == "https://www.iimb.ac.in"

from services.deep_crawler.crawler import DeepCrawler


def test_extract_courses_from_table():
    html = "<table><tr><th>Course</th><th>Duration</th></tr><tr><td>MBA</td><td>2 years</td></tr></table>"
    out = DeepCrawler()._extract_structured(html, "https://x.com/courses")
    assert out["courses"][0]["name"] == "MBA"


def test_extract_fees_rupee_pattern():
    html = "<p>Tuition fee is ₹45,000 per year for MBA students. " + ("a" * 400) + "</p>"
    out = DeepCrawler()._extract_structured(html, "https://x.com/fees")
    assert out["fees"]["tuition_per_year"] == 45000


def test_extract_contact_phone():
    html = "<p>Contact +919876543210 Email info@example.edu " + ("a" * 400) + "</p>"
    out = DeepCrawler()._extract_structured(html, "https://x.com/contact")
    assert out["contact"]["phone"] == "+919876543210"


def test_skip_url_binary():
    c = DeepCrawler(); c._base_domain = "x.com"
    assert c._skip_url("https://x.com/doc.pdf") is True


def test_skip_url_login():
    c = DeepCrawler(); c._base_domain = "x.com"
    assert c._skip_url("https://x.com/login") is True


def test_merge_pages_dedup_courses():
    c = DeepCrawler()
    out = c._merge_pages([
        {"courses": [{"name": "MBA", "duration": "2y", "fees_inr": None, "eligibility": None}]},
        {"courses": [{"name": "MBA", "duration": "2y", "fees_inr": None, "eligibility": None}]},
    ])
    assert len(out["courses"]) == 1


def test_merge_pages_longest_about():
    c = DeepCrawler()
    out = c._merge_pages([{"about": "short"}, {"about": "this is a much longer about section"}])
    assert out["about"] == "this is a much longer about section"


def test_is_useful_page_rejects_empty():
    c = DeepCrawler()
    assert c._is_useful_page("<html><nav>Home</nav></html>") is False

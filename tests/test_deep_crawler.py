from services.deep_crawler.crawler import DeepCrawler


def test_extract_courses_from_table():
    html = """
    <html><body><table>
    <tr><th>Course</th><th>Duration</th><th>Fee</th></tr>
    <tr><td>MBA</td><td>2 years</td><td>₹45,000</td></tr>
    </table></body></html>
    """
    data = DeepCrawler()._extract_structured(html, "https://x.com/courses")
    assert data["courses"][0]["name"] == "MBA"


def test_extract_fees_rupee_pattern():
    html = "<html><body><p>Tuition fee is ₹45,000 per year for the programme.</p></body></html>"
    data = DeepCrawler()._extract_structured(html, "https://x.com/fees")
    assert data["fees"]["tuition_per_year"] == 45000


def test_extract_contact_phone():
    html = "<html><body><p>Contact office at +91 9876543210 for admissions.</p></body></html>"
    data = DeepCrawler()._extract_structured(html, "https://x.com/contact")
    assert data["contact"]["phone"] == "+91 9876543210"


def test_skip_url_binary():
    assert DeepCrawler()._skip_url("https://x.com/doc.pdf") is True


def test_skip_url_login():
    assert DeepCrawler()._skip_url("https://x.com/login") is True


def test_merge_pages_dedup_courses():
    merged = DeepCrawler()._merge_pages([
        {"courses": [{"name": "MBA", "duration": "2 years"}]},
        {"courses": [{"name": "MBA", "duration": "2 years"}]},
    ])
    assert len(merged["courses"]) == 1


def test_merge_pages_longest_about():
    page1 = {"about": "Short about."}
    page2 = {"about": "This is the longer about text with richer details about the institution."}
    merged = DeepCrawler()._merge_pages([page1, page2])
    assert merged["about"] == page2["about"]


def test_is_useful_page_rejects_empty():
    html = "<html><body><nav>Home About Contact</nav></body></html>"
    assert DeepCrawler()._is_useful_page(html) is False

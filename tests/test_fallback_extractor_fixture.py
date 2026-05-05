from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback


def test_fixture_college_extraction():
    out = extract_fallback("file://tests/fixtures/college_sample.html")
    assert out["location"]
    assert out["courses"]
    assert out["fees"]
    assert out["admission_link"]
    assert out["placement"]
    assert out["faculty"]
    assert out["hostel"]

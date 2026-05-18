import asyncio
import importlib.util

import pytest

from services.admissions.crawler import (
    AdmissionNotice,
    AdmissionsCrawler,
    AdmissionsRepository,
    classify_status,
)


def write_admission_site(tmp_path):
    form = tmp_path / "apply-form.html"
    form.write_text("<html><body>Application portal OK</body></html>", encoding="utf-8")
    page = tmp_path / "admissions.html"
    page.write_text(
        f"""
        <html><body>
          <section class="notice">
            <h2>B.Tech Computer Science admission 2026</h2>
            <p>Applications open from 01 May 2026. Last date: 20 June 2026.</p>
            <p>Exam date: 10 July 2026. Result date: 01 August 2026.</p>
            <p>Eligibility: 10+2 with PCM. Application fee INR 1200. Mode online. Maharashtra.</p>
            <a href="{form.as_uri()}">Apply online registration form</a>
          </section>
        </body></html>
        """,
        encoding="utf-8",
    )
    index = tmp_path / "index.html"
    index.write_text(f'<html><body><a href="{page.as_uri()}">Admissions notice</a></body></html>', encoding="utf-8")
    return index.as_uri(), form.as_uri()


def test_classify_status_from_dates(monkeypatch):
    monkeypatch.setenv("ADMISSIONS_TODAY", "2026-05-17")
    assert classify_status("2026-06-01", "2026-07-01") == "upcoming"
    assert classify_status("2026-05-01", "2026-07-01") == "ongoing"
    assert classify_status("2026-01-01", "2026-05-01") == "closed"


def test_admissions_crawler_extracts_direct_application_link(tmp_path, monkeypatch):
    monkeypatch.setenv("ADMISSIONS_TODAY", "2026-05-17")
    start_url, form_url = write_admission_site(tmp_path)
    crawler = AdmissionsCrawler(max_pages=5, rate_limit_seconds=0)

    notices = asyncio.run(crawler.crawl_source(entity_id=7, entity_name="Example Institute", source_url=start_url, intake_year=2026))

    assert len(notices) == 1
    notice = notices[0]
    assert notice.entity_id == 7
    assert notice.admission_type == "UG"
    assert notice.program_name.startswith("B.Tech Computer Science")
    assert notice.intake_year == 2026
    assert notice.application_start_date == "2026-05-01"
    assert notice.application_end_date == "2026-06-20"
    assert notice.exam_date == "2026-07-10"
    assert notice.application_link == form_url
    assert notice.status == "ongoing"
    assert notice.state == "MH"
    assert notice.fee_inr == 1200


def test_admissions_repository_upserts_and_queries(tmp_path, monkeypatch):
    monkeypatch.setenv("ADMISSIONS_TODAY", "2026-05-17")
    repo = AdmissionsRepository(f"sqlite:///{tmp_path / 'admissions.db'}")
    first = AdmissionNotice(entity_id=1, entity_name="A", admission_type="UG", program_name="B.Tech", intake_year=2026, application_start_date="2026-06-01", application_end_date="2026-07-01", application_link="file:///tmp/apply", status="upcoming", state="UP")
    second = AdmissionNotice(entity_id=1, entity_name="A", admission_type="UG", program_name="B.Tech", intake_year=2026, application_start_date="2026-05-01", application_end_date="2026-05-10", application_link="file:///tmp/apply2", status="closed", state="UP")

    assert repo.upsert_many([first]) == 1
    assert repo.upsert_many([second]) == 1

    rows = repo.list(state="UP", admission_type="UG")
    assert len(rows) == 1
    assert rows[0]["application_link"] == "file:///tmp/apply2"
    assert rows[0]["status"] == "closed"
    assert repo.get(rows[0]["id"])["program_name"] == "B.Tech"


def test_admissions_upcoming_query(tmp_path, monkeypatch):
    monkeypatch.setenv("ADMISSIONS_TODAY", "2026-05-17")
    repo = AdmissionsRepository(f"sqlite:///{tmp_path / 'admissions.db'}")
    repo.upsert_many([
        AdmissionNotice(entity_id=2, entity_name="A", admission_type="PG", program_name="MBA", intake_year=2026, application_start_date="2026-05-20", application_end_date="2026-06-15", application_link="file:///tmp/mba", status="upcoming"),
        AdmissionNotice(entity_id=3, entity_name="B", admission_type="UG", program_name="BSc", intake_year=2026, application_start_date="2026-08-20", application_end_date="2026-09-15", application_link="file:///tmp/bsc", status="upcoming"),
    ])

    rows = repo.upcoming(days=30)
    assert [row["program_name"] for row in rows] == ["MBA"]


@pytest.mark.skipif(importlib.util.find_spec("fastapi") is None, reason="fastapi is not installed")
def test_admissions_api_endpoints(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from services.lite_pipeline import api

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'api.db'}")
    repo = AdmissionsRepository(f"sqlite:///{tmp_path / 'api.db'}")
    repo.upsert_many([AdmissionNotice(entity_id=4, entity_name="API", admission_type="UG", program_name="BA", intake_year=2026, application_start_date="2026-05-20", application_end_date="2026-06-15", application_link="file:///tmp/ba", status="ongoing", state="UP")])
    client = TestClient(api.app)

    listing = client.get("/admissions", params={"status": "ongoing", "state": "UP", "type": "UG"})
    assert listing.status_code == 200
    admission_id = listing.json()["results"][0]["id"]
    assert client.get(f"/admissions/{admission_id}").json()["program_name"] == "BA"
    assert client.get("/admissions/upcoming", params={"days": 30}).status_code == 200

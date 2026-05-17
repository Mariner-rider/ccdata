import asyncio
import importlib.util
import json

import pytest

from services.jobs.crawler import JobPosting, JobsCrawler, JobsRepository, classify_status, validate_application_link


def write_job_site(tmp_path):
    apply = tmp_path / "apply.html"
    apply.write_text("<html><body>Apply form OK</body></html>", encoding="utf-8")
    pdf = tmp_path / "notice.pdf"
    pdf.write_bytes(b"%PDF-1.4 fixture")
    recruitment = tmp_path / "recruitment.html"
    recruitment.write_text(
        f"""
        <html><body>
          <article class="recruitment">
            <h1>Railway Group D Recruitment 2026</h1>
            <p>Organization: Railway Recruitment Board.</p>
            <p>Vacancies: 32000 posts. Eligibility: 10th pass. Age limit 18-33 years.</p>
            <p>Pay scale ₹18000 per month. Job location: Pan India.</p>
            <p>Application start date: 01 May 2026. Last date: 30 June 2026.</p>
            <p>Exam date: 15 August 2026. Result date: 30 September 2026.</p>
            <a href="{apply.as_uri()}">Apply Online</a>
            <a href="{pdf.as_uri()}">Official notification PDF</a>
          </article>
        </body></html>
        """,
        encoding="utf-8",
    )
    index = tmp_path / "index.html"
    index.write_text(f'<html><body><a href="{recruitment.as_uri()}">Recruitment</a></body></html>', encoding="utf-8")
    return index.as_uri(), apply.as_uri()


def test_jobs_status_and_login_validation(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBS_TODAY", "2026-05-17")
    assert classify_status("2026-06-01", "2026-07-01") == "upcoming"
    assert classify_status("2026-05-01", "2026-07-01") == "ongoing"
    assert classify_status("2026-01-01", "2026-05-01") == "closed"
    login = tmp_path / "login.html"
    login.write_text("Sign in to apply for this role", encoding="utf-8")
    assert validate_application_link(login.as_uri()) == (True, True)


def test_jobs_crawler_extracts_govt_job_and_pdf(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBS_TODAY", "2026-05-17")
    monkeypatch.setenv("JOBS_S3_ENABLED", "false")
    start_url, apply_url = write_job_site(tmp_path)
    crawler = JobsCrawler(max_pages=5, rate_limit_seconds=0)

    postings = asyncio.run(crawler.crawl(seed_urls=[start_url], job_type="govt"))

    assert len(postings) == 1
    job = postings[0]
    assert job.title == "Railway Group D Recruitment 2026"
    assert job.organization.startswith("Railway Recruitment Board")
    assert job.job_type == "govt"
    assert job.category == "railway"
    assert job.vacancies == 32000
    assert job.application_start_date == "2026-05-01"
    assert job.application_end_date == "2026-06-30"
    assert job.application_link == apply_url
    assert job.status == "ongoing"
    assert job.raw_payload["notification_pdf_object"].startswith("disabled://job-notifications/")


def test_jobs_repository_filters_internships_search_and_dedup(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBS_TODAY", "2026-05-17")
    repo = JobsRepository(f"sqlite:///{tmp_path / 'jobs.db'}")
    first = JobPosting(title="Data Analyst Internship", organization="Acme", job_type="internship", category="tech", pay_scale="Stipend ₹8000", location="remote", application_end_date="2026-06-01", application_link="file:///tmp/apply", status="ongoing")
    updated = JobPosting(title="Data Analyst Internship", organization="Acme", job_type="internship", category="tech", pay_scale="Stipend ₹10000", location="remote", application_end_date="2026-06-01", application_link="file:///tmp/apply2", status="ongoing")
    other = JobPosting(title="Bank PO", organization="IBPS", job_type="govt", category="banking", location="pan-india", state="UP", application_end_date="2026-06-10", application_link="file:///tmp/po", status="ongoing")

    assert repo.upsert_many([first, updated, other]) == 3
    internships = repo.list(job_type="internship", location="remote", stipend_min=5000)
    assert len(internships) == 1
    assert internships[0]["application_link"] == "file:///tmp/apply2"
    assert repo.list(job_type="govt", category="banking", state="UP", status="ongoing")[0]["title"] == "Bank PO"
    assert repo.search("analyst")[0]["organization"] == "Acme"


@pytest.mark.skipif(importlib.util.find_spec("fastapi") is None, reason="fastapi is not installed")
def test_jobs_api_endpoints(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from services.lite_pipeline import api

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'api_jobs.db'}")
    repo = JobsRepository(f"sqlite:///{tmp_path / 'api_jobs.db'}")
    repo.upsert_many([JobPosting(title="Remote Internship", organization="Acme", job_type="internship", category="tech", pay_scale="₹6000", location="remote", application_end_date="2026-06-01", application_link="file:///tmp/apply", status="ongoing")])
    client = TestClient(api.app)

    listing = client.get("/jobs/internships", params={"stipend_min": 5000, "location": "remote"})
    assert listing.status_code == 200
    job_id = listing.json()["results"][0]["id"]
    assert client.get(f"/jobs/{job_id}").json()["title"] == "Remote Internship"
    assert client.get("/jobs/search", params={"q": "remote"}).json()["results"][0]["organization"] == "Acme"

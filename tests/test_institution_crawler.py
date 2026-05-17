import json
import sqlite3
from pathlib import Path

from services.institutions.crawler import InstitutionCrawler, read_bulk_csv
from services.lite_pipeline.main import institution_crawl


def write_site(tmp_path: Path) -> str:
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "index.html").write_text(
        """
        <html><head><title>Example Institute</title></head><body>
        <h1>Example Institute</h1>
        <p>Example Institute is a leading college in Pune, India.</p>
        <a href="about.html">About</a>
        <a href="courses-fees.html">Courses</a>
        <a href="faculty.html">Faculty</a>
        <a href="hostel.html">Hostel</a>
        <a href="placements.html">Placements</a>
        <a href="contact.html">Contact</a>
        </body></html>
        """,
        encoding="utf-8",
    )
    (tmp_path / "about.html").write_text(
        """
        <html><body><h1>About Example Institute</h1>
        <section id="about"><p>Example Institute was founded with a mission to deliver excellent education, research, and community service.</p></section>
        <p>NAAC A+ accredited. NIRF rank 45 in 2025.</p>
        </body></html>
        """,
        encoding="utf-8",
    )
    (tmp_path / "courses-fees.html").write_text(
        """
        <html><body><h1>Courses and Fee Structure</h1>
        <ul><li>B.Tech Computer Science 4 years Tuition ₹150000 Eligibility 10+2 Seats 120</li>
        <li>MBA 2 years Tuition ₹250000 Eligibility Graduation Seats 60</li></ul>
        <p>Application fee ₹1000</p><p>Tuition per year ₹150000</p>
        </body></html>
        """,
        encoding="utf-8",
    )
    (tmp_path / "faculty.html").write_text(
        """
        <html><body><h1>Faculty</h1><div class="faculty"><img src="prof.jpg" />Dr Asha Rao - Professor Computer Science Ph.D.</div></body></html>
        """,
        encoding="utf-8",
    )
    (tmp_path / "hostel.html").write_text(
        "<html><body><h1>Hostel</h1><p>Hostel available capacity 500 fees ₹80000 wifi mess laundry security.</p></body></html>",
        encoding="utf-8",
    )
    (tmp_path / "placements.html").write_text(
        "<html><body><h1>Placement</h1><p>Average package 8 LPA</p><p>Highest package 32 LPA</p><p>Placement 92%</p><p>Top recruiters TCS, Infosys, Google</p></body></html>",
        encoding="utf-8",
    )
    (tmp_path / "contact.html").write_text(
        "<html><body><h1>Contact</h1><address>Pune, Maharashtra, India</address><p>Phone +919876543210 Email info@example.edu.in</p></body></html>",
        encoding="utf-8",
    )
    return (tmp_path / "index.html").as_uri()


def test_institution_crawler_deep_crawls_and_merges_one_profile(tmp_path, monkeypatch):
    monkeypatch.setenv("INSTITUTION_S3_ENABLED", "false")
    crawler = InstitutionCrawler(max_pages=10, max_depth=2, rate_limit_seconds=0)
    result = __import__("asyncio").run(crawler.crawl(write_site(tmp_path), "college"))

    assert result.status == "created"
    assert result.pages_crawled >= 6
    record = result.record
    fields = record["fields"]
    assert fields["name"] in {"Example Institute", "About Example Institute"}
    assert len(fields["courses"]) == 2
    assert fields["hostel"]["available"] is True
    assert fields["placement"]["highest_package_lpa"] == 32
    assert fields["faculty"][0]["name"].startswith("Dr Asha Rao")
    assert record["source_url"].endswith("index.html")
    assert record["metadata"]["page_count"] == result.pages_crawled


def test_institution_crawl_cli_saves_to_crawl_records(tmp_path, monkeypatch):
    db = tmp_path / "cc.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db}")
    monkeypatch.setenv("INSTITUTION_S3_ENABLED", "false")

    out = institution_crawl(write_site(tmp_path / "site"), "college")

    assert out["status"] == "created"
    with sqlite3.connect(db) as conn:
        rows = conn.execute("select entity_type,payload from crawler_records").fetchall()
        quarantine = conn.execute("select count(*) from quarantine_records").fetchone()[0]
    assert len(rows) == 1
    assert quarantine == 0
    payload = json.loads(rows[0][1])
    assert payload["fields"]["courses"][0]["name"].startswith("B.Tech")


def test_bulk_csv_reader_accepts_type_and_url(tmp_path):
    csv_file = tmp_path / "urls.csv"
    csv_file.write_text("url,type,name\nhttps://example.edu,university,Example\n", encoding="utf-8")
    assert read_bulk_csv(str(csv_file)) == [{"url": "https://example.edu", "entity_type": "university", "name": "Example"}]

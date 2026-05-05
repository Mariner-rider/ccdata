import sqlite3
from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
from services.lite_pipeline.main import Repo, _load_config, crawl_source


def test_fixture_extraction_file_scheme():
    out = extract_fallback('file://tests/fixtures/college_sample.html')
    assert out['name']


def test_file_crawl_and_record(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_load_config().database_url); repo.init_db()
    repo.add_source({'entity_type':'college','entity_name':'Fixture','official_url':'file://tests/fixtures/college_sample.html','country':'','trust_tier':'official','crawl_frequency_days':7})
    crawl_source(1)
    con=sqlite3.connect(repo.path)
    count=con.execute('select count(*) from crawler_records').fetchone()[0]
    assert count>=1

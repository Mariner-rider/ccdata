import sqlite3
from services.lite_pipeline.main import _strip_label, Repo, _cfg, crawl_source, pilot_college, discover


def test_label_strip():
    assert _strip_label('Faculty: Dr. Lisa, Dr. Kumar')=='Dr. Lisa, Dr. Kumar'
    assert _strip_label('Hostel: AC rooms')=='AC rooms'


def test_allowlist_block(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('CRAWL_ALLOWED_DOMAINS','allowed.com')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'X','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    plan=discover('file://tests/fixtures/site/index.html',_cfg(),repo)
    assert plan


def test_pilot_dry_run_no_write(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    out=pilot_college('Fixture','file://tests/fixtures/site/index.html',dry=True,save=False)
    con=sqlite3.connect(tmp_path/'db.sqlite'); cnt=con.execute('select count(*) from crawler_records').fetchone()[0]
    assert out['result']['dry_run'] and cnt==0


def test_quality_report_shape(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'X','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    out=crawl_source(1,True)
    assert 'quality_report' in out and 'pages_discovered' in out['quality_report']

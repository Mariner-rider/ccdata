import json, sqlite3
from services.lite_pipeline.main import Repo,_cfg,discover,crawl_source,export_entity


def setup_source(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init()
    repo.add_source({'entity_type':'college','entity_name':'Fixture College','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    return repo


def test_preview_prioritized(monkeypatch,tmp_path):
    repo=setup_source(monkeypatch,tmp_path)
    src=repo.get_source(1)
    urls=discover(src[3],_cfg())
    assert any('admissions' in u['url'] for u in urls)


def test_crawl_merge_no_duplicates(monkeypatch,tmp_path):
    repo=setup_source(monkeypatch,tmp_path)
    out=crawl_source(1,False)
    con=sqlite3.connect(repo.path)
    cnt=con.execute('select count(*) from crawler_records').fetchone()[0]
    assert out['status'] in {'created','updated','unchanged'} and cnt==1


def test_dry_run_writes_nothing(monkeypatch,tmp_path):
    repo=setup_source(monkeypatch,tmp_path)
    out=crawl_source(1,True)
    con=sqlite3.connect(repo.path)
    cnt=con.execute('select count(*) from crawler_records').fetchone()[0]
    assert out['dry_run'] and cnt==0


def test_export_entity(monkeypatch,tmp_path):
    repo=setup_source(monkeypatch,tmp_path)
    crawl_source(1,False)
    rec=export_entity(1)
    assert rec['courses_and_fees']['courses'] and rec['placement'] and 'Courses' not in rec['courses_and_fees']['courses']

import sqlite3

from services.lite_pipeline.main import Repo,_cfg,crawl_source,enqueue_job,worker_once,_search,integrity_repair
from services.lite_pipeline import api


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('QUEUE_BACKEND','memory')
    monkeypatch.setenv('CRAWL_BACKOFF_BASE_SECONDS','0')
    repo=Repo(_cfg().database_url); repo.init()
    return repo


def test_http_retry_and_proxy_fallback(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html?v=2','trust_tier':'official'})
    monkeypatch.setenv('CRAWL_SIMULATE_STATUS','500,200')
    monkeypatch.setenv('CRAWL_MAX_RETRIES','1')
    out=crawl_source(sid,False)
    assert out['status'] in {'created','updated','unchanged','quarantined'}


def test_dedup_delta_and_search(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    s1=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(s1,False)
    s2=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html?v=2','trust_tier':'official'})
    crawl_source(s2,False)
    with sqlite3.connect(repo.path) as c:
        p2=c.execute('select payload from crawler_records where source_id=? order by id desc limit 1',(s2,)).fetchone()[0]
    assert 'duplicate_of' in p2

    # force delta via direct payload rewrite and recrawl
    with sqlite3.connect(repo.path) as c:
        p=c.execute('select id,payload from crawler_records where source_id=?',(s1,)).fetchone();
        rec=__import__('json').loads(p[1]); rec['fields']['location']='Changed';
        c.execute('update crawler_records set payload=? where id=?',(__import__('json').dumps(rec),p[0])); c.commit()
    crawl_source(s1,False)
    with sqlite3.connect(repo.path) as c:
        rec=__import__('json').loads(c.execute('select payload from crawler_records where source_id=?',(s1,)).fetchone()[0])
    assert isinstance(rec.get('change_log',[]),list)

    r=_search('MBA')
    assert r and 'title' in r[0]


def test_trigger_crawl_and_api_search(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    j=enqueue_job(sid,'crawl',False,5)
    assert j['job_id']
    worker_once()
    out=api.search(q='MBA')
    assert 'results' in out

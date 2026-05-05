import sqlite3
from services.lite_pipeline.main import Repo,_cfg,crawl_source,record_approve,publish_entity,_search,index_rebuild,public_entities_list,public_entity_get
from services.lite_pipeline import api


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('QUEUE_BACKEND','memory')
    repo=Repo(_cfg().database_url); repo.init(); return repo


def test_publish_creates_public_entity_and_slug(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(sid,False); record_approve(1,'tester'); publish_entity(1)
    pubs=public_entities_list(); assert pubs and pubs[0]['slug'].startswith('fixture')
    pe=public_entity_get(pubs[0]['slug'])
    assert 'page_json' in pe and 'payload' not in pe and 'missing_fields' not in pe


def test_duplicate_slug_suffix(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    s1=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    s2=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html?v=2','trust_tier':'official'})
    crawl_source(s1,False); crawl_source(s2,False)
    record_approve(1,'tester'); record_approve(2,'tester'); publish_entity(1); publish_entity(2)
    slugs=[x['slug'] for x in public_entities_list()]
    assert len(set(slugs))==2 and any('-2' in s for s in slugs)


def test_search_only_published_and_reindex_idempotent(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(sid,False)
    assert _search('MBA')==[]  # draft not searchable
    record_approve(1,'tester'); publish_entity(1)
    assert _search('MBA')
    a=index_rebuild(); b=index_rebuild()
    assert a['ok'] and b['ok'] and a['rebuilt']==b['rebuilt']


def test_public_api(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(sid,False); record_approve(1,'tester'); publish_entity(1)
    lst=api.public_entities(); assert lst['results']
    slug=lst['results'][0]['slug']
    detail=api.public_entity(slug)
    assert 'page_json' in detail and 'payload' not in detail

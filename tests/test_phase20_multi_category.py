from services.lite_pipeline.main import Repo,_cfg,crawl_source,record_approve,publish_entity,public_entities_list,_search
from services.lite_pipeline import api

CASES=[
 ('college','file://tests/fixtures/site/index.html'),
 ('institute','file://tests/fixtures/site/institute_sample.html'),
 ('admission','file://tests/fixtures/site/admission_sample.html'),
 ('job','file://tests/fixtures/site/job_sample.html'),
 ('scholarship','file://tests/fixtures/site/scholarship_sample.html'),
 ('news','file://tests/fixtures/site/news_sample.html'),
 ('education_loan','file://tests/fixtures/site/education_loan_sample.html'),
]

def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init(); return repo


def test_multi_category_publish_and_search(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    for i,(et,url) in enumerate(CASES, start=1):
        sid=repo.add_source({'entity_type':et,'entity_name':f'{et}-fixture','url':url,'trust_tier':'official'})
        out=crawl_source(sid,False); assert out['status'] in {'created','updated','unchanged'}
        import sqlite3
        with sqlite3.connect(repo.path) as c: rid=c.execute('select id from crawler_records where source_id=? order by id desc limit 1',(sid,)).fetchone()[0]
        record_approve(rid,'tester'); publish_entity(rid)
    pubs=public_entities_list()
    assert len(pubs)>=7
    assert _search('admission-fixture',entity_type='admission')
    assert _search('scholarship-fixture',entity_type='scholarship')


def test_public_api_filters(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    sid=repo.add_source({'entity_type':'job','entity_name':'job-fixture','url':'file://tests/fixtures/site/job_sample.html','trust_tier':'official'})
    out=crawl_source(sid,False); assert out['status'] in {'created','updated','unchanged'}
    import sqlite3
    with sqlite3.connect(repo.path) as c: rid=c.execute('select id from crawler_records where source_id=? order by id desc limit 1',(sid,)).fetchone()[0]
    record_approve(rid,'tester'); publish_entity(rid)
    res=api.public_entities(entity_type='job')
    assert res['results'] and all(r['entity_type']=='job' for r in res['results'])
    s=api.search(q='job-fixture',entity_type='job')
    assert s['results']

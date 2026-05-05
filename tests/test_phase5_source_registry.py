import json, sqlite3
from services.lite_pipeline.main import Repo, _load_config, crawl_source, map_record


def test_source_registry_add_list(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_load_config().database_url); repo.init_db()
    repo.add_source({'entity_type':'college','entity_name':'Fixture College','official_url':'file://tests/fixtures/college_sample.html','country':'IN','trust_tier':'official','crawl_frequency_days':7})
    rows=repo.list_sources(); assert len(rows)==1 and rows[0][2]=='Fixture College'


def test_controlled_crawl_and_dedup(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_load_config().database_url); repo.init_db()
    repo.add_source({'entity_type':'college','entity_name':'Fixture College','official_url':'file://tests/fixtures/college_sample.html','country':'IN','trust_tier':'official','crawl_frequency_days':7})
    r1=crawl_source(1); r2=crawl_source(1)
    con=sqlite3.connect(repo.path)
    cnt=con.execute('select count(*) from crawler_records').fetchone()[0]
    assert cnt>=1 and r1['source_id']==1 and r2['source_id']==1


def test_schema_mapping_profiles():
    sample={'name':'X','location':'Y','official_website':'u','courses':['a'],'fees':['1'],'admission_link':['a'],'placement':['p'],'faculty':['f'],'hostel':['h']}
    rec=map_record('college','X','file://x',sample,'official')
    assert rec['entity_type']=='college' and rec['freshness_status']=='fresh' and rec['confidence_score']>0.9


def test_missing_fields_task_creation(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_load_config().database_url); repo.init_db()
    repo.add_source({'entity_type':'admission','entity_name':'Fixture Admission','official_url':'file://tests/fixtures/college_sample.html','country':'IN','trust_tier':'official','crawl_frequency_days':7})
    crawl_source(1)
    con=sqlite3.connect(repo.path)
    tasks=con.execute('select count(*) from crawl_tasks').fetchone()[0]
    assert tasks>=1

from services.lite_pipeline.main import Repo,_cfg,crawl_source,record_list,record_approve,record_reject,publish_entity,chatbot_sync
from services.lite_pipeline import api
import pytest


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})


def test_record_approve_publish_sync(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    crawl_source(1,False)
    drafts=record_list('draft')
    assert drafts
    record_approve(1,'admin')
    out=publish_entity(1)
    assert out['status']=='published'
    s=chatbot_sync(1)
    assert s['status']=='queued'


def test_reject(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path); crawl_source(1,False)
    record_reject(1,'admin','bad')
    with pytest.raises(RuntimeError):
        publish_entity(1)


def test_api_roundtrip(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    assert api.health()['status']=='ok'
    api.crawl(1,False)
    from services.lite_pipeline.main import worker_once
    worker_once()
    api.approve_record(1,'admin')

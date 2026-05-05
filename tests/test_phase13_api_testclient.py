import os, pytest

pytest.importorskip('fastapi')
httpx = pytest.importorskip('httpx')
from fastapi.testclient import TestClient
from services.lite_pipeline.api import app
from services.lite_pipeline.main import Repo,_cfg


def setup_db(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.delenv('ADMIN_API_KEY', raising=False)
    repo=Repo(_cfg().database_url); repo.init()


def test_roundtrip(monkeypatch,tmp_path):
    setup_db(monkeypatch,tmp_path)
    c=TestClient(app)
    sid=c.post('/sources',json={'entity_name':'Fixture','url':'file://tests/fixtures/site/index.html'}).json()['id']
    assert c.get('/sources').status_code==200
    assert c.post(f'/sources/{sid}/crawl?dry_run=false').status_code==200
    assert c.post('/records/1/approve').status_code==200
    assert c.post('/records/1/publish').status_code==200
    assert c.post('/chatbot/sync/1').status_code==200
    assert c.get('/records/1/export').status_code==200


def test_auth(monkeypatch,tmp_path):
    setup_db(monkeypatch,tmp_path)
    monkeypatch.setenv('ADMIN_API_KEY','secret')
    from importlib import reload
    import services.lite_pipeline.api as m
    reload(m)
    c=TestClient(m.app)
    assert c.post('/sources',json={'entity_name':'X','url':'file://tests/fixtures/site/index.html'}).status_code==401

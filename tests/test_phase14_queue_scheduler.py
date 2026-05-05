import pytest
from services.lite_pipeline.main import Repo,_cfg,enqueue_job,worker_once,job_get,jobs_list,scheduler_run_once
from services.lite_pipeline import api


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('QUEUE_BACKEND','memory')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})


def test_enqueue_and_worker(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    j=enqueue_job(1,'crawl',True)
    assert j['job_id']
    worker_once()
    s=job_get(j['job_id'])
    assert s['status'] in {'completed','failed'}


def test_idempotent_job(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    a=enqueue_job(1,'crawl',True,idempotency_key='k1')
    b=enqueue_job(1,'crawl',True,idempotency_key='k1')
    assert a['job_id']==b['job_id']


def test_api_returns_job_id(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    out=api.crawl(1,True)
    assert 'job_id' in out


def test_scheduler(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    out=scheduler_run_once()
    assert out['enqueued']


def test_redis_missing(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    monkeypatch.setenv('QUEUE_BACKEND','redis')
    with pytest.raises(RuntimeError):
        enqueue_job(1,'crawl',True)

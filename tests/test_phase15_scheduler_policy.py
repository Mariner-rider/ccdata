from datetime import datetime, timezone, timedelta
from services.lite_pipeline.main import Repo,_cfg,scheduler_run_once,enqueue_job,worker_once,jobs_cancel,job_get


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('QUEUE_BACKEND','memory')
    repo=Repo(_cfg().database_url); repo.init()
    return repo


def test_due_and_not_due(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    with __import__('sqlite3').connect(repo.path) as c:
        c.execute("insert into source_registry(entity_type,entity_name,official_url,is_active,last_crawled_at,crawl_frequency_days) values('college','A','file://tests/fixtures/site/index.html',1,?,7)",(datetime.now(timezone.utc).isoformat(),))
        c.execute("insert into source_registry(entity_type,entity_name,official_url,is_active,last_crawled_at,crawl_frequency_days) values('college','B','file://tests/fixtures/site/index.html',1,?,7)",((datetime.now(timezone.utc)-timedelta(days=8)).isoformat(),)); c.commit()
    r=scheduler_run_once()
    assert r['jobs_enqueued']==1 and r['skipped_not_due']>=1


def test_budget_domain_cooldown(monkeypatch,tmp_path):
    repo=setup(monkeypatch,tmp_path)
    monkeypatch.setenv('DAILY_MAX_JOBS','1'); monkeypatch.setenv('DAILY_MAX_JOBS_PER_DOMAIN','1'); monkeypatch.setenv('MAX_FAILED_JOBS_PER_SOURCE','0')
    with __import__('sqlite3').connect(repo.path) as c:
        c.execute("insert into source_registry(entity_type,entity_name,official_url,is_active,last_crawled_at,crawl_frequency_days) values('college','A','file://tests/fixtures/site/index.html',1,NULL,1)")
        c.execute("insert into source_registry(entity_type,entity_name,official_url,is_active,last_crawled_at,crawl_frequency_days) values('college','B','file://tests/fixtures/site/index.html',1,NULL,1)"); c.commit()
    r=scheduler_run_once(); assert r['jobs_enqueued']<=1


def test_retry_and_cancel(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    # failed retry path
    j=enqueue_job(999,'crawl',False)  # invalid source -> fail/retry
    out=worker_once(); s=job_get(j['job_id'])
    assert out['status'] in {'retry_queued','failed'}
    j2=enqueue_job(999,'crawl',False)
    jobs_cancel(j2['job_id'])
    s2=job_get(j2['job_id']); assert s2['status']=='cancelled'

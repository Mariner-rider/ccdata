import sqlite3
from datetime import datetime, timedelta, timezone

from services.lite_pipeline.main import (
    Repo,
    _cfg,
    chatbot_sync,
    crawl_source,
    integrity_check,
    integrity_repair,
    metrics_summary,
    publish_entity,
    record_approve,
    scheduler_run_once,
    sources_freshness,
)


def setup(monkeypatch, tmp_path):
    monkeypatch.setenv('DATABASE_URL', f'sqlite:///{tmp_path}/db.sqlite')
    monkeypatch.setenv('QUEUE_BACKEND', 'memory')
    repo = Repo(_cfg().database_url)
    repo.init()
    sid = repo.add_source({'entity_type': 'college', 'entity_name': 'Fixture', 'url': 'file://tests/fixtures/site/index.html', 'trust_tier': 'official'})
    return repo, sid


def test_freshness_metrics_and_lineage(monkeypatch, tmp_path):
    repo, sid = setup(monkeypatch, tmp_path)

    fresh_before = sources_freshness()
    src_before = [r for r in fresh_before if r['id'] == sid][0]
    assert src_before['last_crawled_at'] is None

    metrics_before = metrics_summary()
    assert metrics_before['due_sources'] >= 1

    out = crawl_source(sid, False)
    assert out['status'] in {'created', 'updated', 'unchanged'}

    fresh_after = sources_freshness()
    src_after = [r for r in fresh_after if r['id'] == sid][0]
    assert src_after['last_crawled_at'] is not None

    metrics_after = metrics_summary()
    assert metrics_after['due_sources'] <= metrics_before['due_sources']

    record_approve(1, 'tester')
    publish_entity(1)
    chatbot_sync(1)

    with sqlite3.connect(repo.path) as c:
        psrc = c.execute('select source_id from published_records where entity_record_id=1 order by id desc limit 1').fetchone()[0]
        ssrc = c.execute('select source_id from chatbot_sync_logs where entity_record_id=1 order by id desc limit 1').fetchone()[0]
    assert psrc == sid
    assert ssrc == sid


def test_scheduler_due_vs_not_due(monkeypatch, tmp_path):
    repo, sid = setup(monkeypatch, tmp_path)
    now = datetime.now(timezone.utc)
    with sqlite3.connect(repo.path) as c:
        c.execute('update source_registry set last_crawled_at=?, crawl_frequency_days=7 where id=?', (now.isoformat(), sid))
        c.execute("insert into source_registry(entity_type,entity_name,official_url,trust_tier,is_active,last_crawled_at,crawl_frequency_days) values('college','Due','file://tests/fixtures/site/index.html','official',1,?,7)", ((now - timedelta(days=8)).isoformat(),))
        c.commit()

    out = scheduler_run_once()
    assert out['jobs_enqueued'] >= 1
    assert out['skipped_not_due'] >= 1


def test_integrity_duplicate_and_repair(monkeypatch, tmp_path):
    repo, _ = setup(monkeypatch, tmp_path)
    with sqlite3.connect(repo.path) as c:
        c.execute("insert into source_registry(entity_type,entity_name,official_url,trust_tier,is_active) values('college','Dup1','file://dup.example','official',1)")
        c.execute("insert into source_registry(entity_type,entity_name,official_url,trust_tier,is_active) values('college','Dup2','file://dup.example','official',1)")
        c.commit()

    chk = integrity_check()
    assert chk['duplicate_active_sources_by_url'] >= 1

    before_active = sqlite3.connect(repo.path).execute("select count(*) from source_registry where official_url='file://dup.example' and is_active=1").fetchone()[0]
    dry = integrity_repair(apply=False)
    after_dry = sqlite3.connect(repo.path).execute("select count(*) from source_registry where official_url='file://dup.example' and is_active=1").fetchone()[0]
    assert dry['dry_run'] is True
    assert before_active == after_dry

    applied = integrity_repair(apply=True)
    after_apply = sqlite3.connect(repo.path).execute("select count(*) from source_registry where official_url='file://dup.example' and is_active=1").fetchone()[0]
    assert applied['dry_run'] is False
    assert after_apply == 1

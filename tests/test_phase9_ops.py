import json
from services.lite_pipeline.main import GENERIC_HEADINGS, _clean_list, export_validate, readiness_check, audit_export, pilot_http_smoke, Repo, _cfg, crawl_source


def test_heading_pollution_removed():
    out=_clean_list(['Courses & Fees','Placements','Faculty','Dr. A'], GENERIC_HEADINGS)
    assert out==['Dr. A']


def test_export_validate_success_failure(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'X','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(1,False)
    ok=export_validate(1)
    assert ok['ok']
    bad=export_validate(999)
    assert not bad['ok']


def test_readiness_shape(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    out=readiness_check()
    assert 'runtime_profile' in out and 'crawler_limits' in out


def test_audit_export_shape(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    out=audit_export()
    assert 'crawl_logs' in out and 'quarantine_records' in out


def test_http_smoke_mocked(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    out=pilot_http_smoke('file://tests/fixtures/site/index.html','Fixture')
    assert out['safe_completed'] and 'quality_report' in out


def test_invalid_url(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    try:
        pilot_http_smoke('bad://url','X')
        assert False
    except RuntimeError:
        assert True

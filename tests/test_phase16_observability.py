import json
from services.lite_pipeline.main import Repo,_cfg,metrics_summary,sources_freshness,jobs_failures,quality_report_summary,_log_event,crawl_source
from services.lite_pipeline import api


def setup(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init(); repo.add_source({'entity_type':'college','entity_name':'Fixture','url':'file://tests/fixtures/site/index.html','trust_tier':'official'})
    crawl_source(1,False)


def test_shapes(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    assert 'total_sources' in metrics_summary()
    assert isinstance(sources_freshness(), list)
    assert isinstance(jobs_failures(), list)
    assert 'records_by_lifecycle_state' in quality_report_summary()


def test_api_shapes(monkeypatch,tmp_path):
    setup(monkeypatch,tmp_path)
    assert 'total_sources' in api.metrics()
    assert isinstance(api.freshness(), list)
    assert isinstance(api.failures(), list)
    assert 'records_by_lifecycle_state' in api.quality()


def test_json_log(monkeypatch,tmp_path,capsys):
    monkeypatch.setenv('LOG_FORMAT','json')
    _log_event('x',a=1)
    out=capsys.readouterr().out.strip()
    assert json.loads(out)['event']=='x'

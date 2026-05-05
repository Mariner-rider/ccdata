import json
from pathlib import Path
from services.lite_pipeline.main import discover,_cfg,Repo


def test_indian_synonym_prioritization(monkeypatch,tmp_path):
    monkeypatch.setenv('DATABASE_URL',f'sqlite:///{tmp_path}/db.sqlite')
    repo=Repo(_cfg().database_url); repo.init()
    plan=discover('file://tests/fixtures/site/indian_university_index.html',_cfg(),repo)
    urls=' '.join(x['url'] for x in plan)
    assert 'programmes' in urls and 'fee-structure' in urls and 'career-development' in urls and 'contact-directory' in urls


def test_live_output_fixture_analysis():
    data=json.loads(Path('tests/fixtures/site/live_pilot_output_iimb.json').read_text())
    missing_before=set(data['quality_report']['missing_fields'])
    # expected tuning target keeps same or fewer missing after synonym expansion in real runs
    assert {'fees','hostel','placement'}.issubset(missing_before)

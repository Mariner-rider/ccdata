from __future__ import annotations
import json, os, sqlite3, subprocess, sys
from pathlib import Path
ENV=os.environ|{"DATABASE_URL":"sqlite:///./collegecue_local.db"}

def run(cmd):
    p=subprocess.run(cmd,capture_output=True,text=True,env=ENV)
    return p.returncode,p.stdout+p.stderr

r=[f"Python: {sys.version}"]
for cmd,label in [([sys.executable,"-m","services.lite_pipeline.main","init-db"],"init-db"),([sys.executable,"-m","services.lite_pipeline.main","source:add","--entity-type","college","--entity-name","Fixture Site","--url","file://tests/fixtures/site/index.html"],"source:add")]:
    c,o=run(cmd); r.append(f"{label}: {c}")
c,o=run([sys.executable,"-m","services.lite_pipeline.main","source:preview","--id","1"]); r.append(f"source:preview: {c}")
preview=json.loads(o)
urls=[u['url'] for u in preview['urls']]
assert any('admissions' in u for u in urls) and any('courses-fees' in u for u in urls)
c,o=run([sys.executable,"-m","services.lite_pipeline.main","source:crawl","--id","1","--dry-run"]); r.append(f"source:crawl:dry-run: {c}")
c,o=run([sys.executable,"-m","services.lite_pipeline.main","source:crawl","--id","1"]); r.append(f"source:crawl: {c}")
c,o=run([sys.executable,"-m","services.lite_pipeline.main","export:entity","--id","1","--format","json"]); r.append(f"export:entity: {c}")
rec=json.loads(o)
for f in ['courses_and_fees','faculty','hostel','placement','info']:
    assert rec.get(f)
con=sqlite3.connect('collegecue_local.db'); exists=con.execute("select count(*) from crawler_records where source_url='file://tests/fixtures/site/index.html'").fetchone()[0]; con.close(); r.append(f"entity_records: {exists}")
c,o=run([sys.executable,"-m","pytest","-q"]); r.append(f"pytest: {c}")
Path('docs').mkdir(exist_ok=True); Path('docs/no-docker-verification-report.md').write_text('\n'.join(['# No-Docker Verification Report',*r]))
print('\n'.join(r))
if c!=0 or exists!=1: raise SystemExit(1)

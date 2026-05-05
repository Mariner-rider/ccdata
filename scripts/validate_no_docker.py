from __future__ import annotations
import json, os, sqlite3, subprocess, sys
from pathlib import Path
ENV=os.environ|{"DATABASE_URL":"sqlite:///./collegecue_local.db"}

def run(cmd):
    p=subprocess.run(cmd,capture_output=True,text=True,env=ENV)
    return p.returncode,p.stdout+p.stderr

r=[f"Python: {sys.version}"]
steps=[([sys.executable,"-m","services.lite_pipeline.main","init-db"],"init-db"),([sys.executable,"-m","services.lite_pipeline.main","source:add","--entity-type","college","--entity-name","Fixture Site","--url","file://tests/fixtures/site/index.html"],"source:add"),([sys.executable,"-m","services.lite_pipeline.main","source:crawl","--id","1"],"source:crawl"),([sys.executable,"-m","services.lite_pipeline.main","record:list","--state","draft"],"record:list:draft"),([sys.executable,"-m","services.lite_pipeline.main","record:approve","--id","1","--reviewed-by","local-admin"],"record:approve"),([sys.executable,"-m","services.lite_pipeline.main","publish:entity","--id","1"],"publish:entity"),([sys.executable,"-m","services.lite_pipeline.main","chatbot:sync","--entity-id","1"],"chatbot:sync"),([sys.executable,"-m","services.lite_pipeline.main","export:validate","--id","1"],"export:validate")]
for cmd,label in steps:
    c,o=run(cmd); r.append(f"{label}: {c}")
c,o=run([sys.executable,"-m","pytest","-q"]); r.append(f"pytest: {c}")
Path('docs').mkdir(exist_ok=True); Path('docs/no-docker-verification-report.md').write_text('\n'.join(['# No-Docker Verification Report',*r]))
print('\n'.join(r))
if c!=0: raise SystemExit(1)

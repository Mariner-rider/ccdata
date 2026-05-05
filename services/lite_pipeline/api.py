from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os
from services.lite_pipeline.main import Repo,_cfg,discover,crawl_source,export_entity,review_list,review_decide,publish_entity,chatbot_sync,record_approve,enqueue_job,job_get,metrics_summary,sources_freshness,jobs_failures,quality_report_summary,_search

app=FastAPI(title='collegecue-local-lite')
ADMIN_API_KEY=os.getenv('ADMIN_API_KEY','')

def _guard(key):
    if ADMIN_API_KEY and key!=ADMIN_API_KEY: raise HTTPException(status_code=401, detail='invalid admin api key')

class SourceIn(BaseModel):
    entity_type:str='college'; entity_name:str; url:str; trust_tier:str='official'

@app.get('/health')
def health(): return {'status':'ok'}

@app.post('/sources')
def add_source(s:SourceIn, x_api_key:str|None=Header(default=None)):
    _guard(x_api_key); repo=Repo(_cfg().database_url); repo.init(); sid=repo.add_source(s.model_dump()); return {'id':sid}

@app.get('/sources')
def list_sources():
    repo=Repo(_cfg().database_url); repo.init(); return [{'id':r[0],'entity_type':r[1],'entity_name':r[2],'official_url':r[3]} for r in repo.list_sources()]

@app.post('/sources/{id}/preview')
def preview(id:int):
    repo=Repo(_cfg().database_url); s=repo.get_source(id); return {'source_id':id,'urls':discover(s[3],_cfg(),repo)}

@app.post('/sources/{id}/crawl')
def crawl(id:int,dry_run:bool=True,idempotency_key:str|None=Header(default=None),x_api_key:str|None=Header(default=None)):
    _guard(x_api_key); return enqueue_job(id,'crawl',dry_run,5,None,idempotency_key)

@app.get('/records/{id}/export')
def export(id:int): return export_entity(id)

@app.get('/review')
def review(): return review_list()

@app.post('/review/{id}/approve')
def appr(id:int,reviewed_by:str='admin'): return review_decide(id,'approved',reviewed_by)

@app.post('/review/{id}/reject')
def rej(id:int,reviewed_by:str='admin',notes:str=''): return review_decide(id,'rejected',reviewed_by,notes)

@app.post('/records/{id}/publish')
def pub(id:int, idempotency_key:str|None=Header(default=None), x_api_key:str|None=Header(default=None)):
    _guard(x_api_key); return publish_entity(id,idempotency_key)

@app.post('/chatbot/sync/{entity_id}')
def sync(entity_id:int, idempotency_key:str|None=Header(default=None), x_api_key:str|None=Header(default=None)):
    _guard(x_api_key); return chatbot_sync(entity_id,idempotency_key)

@app.post('/records/{id}/approve')
def approve_record(id:int,reviewed_by:str='admin', x_api_key:str|None=Header(default=None)):
    _guard(x_api_key); return record_approve(id,reviewed_by)


@app.get('/jobs/{id}')
def job(id:int):
    return job_get(id)


@app.get('/metrics/summary')
def metrics(): return metrics_summary()

@app.get('/sources/freshness')
def freshness(): return sources_freshness()

@app.get('/jobs/failures')
def failures(): return jobs_failures()

@app.get('/quality/report')
def quality(): return quality_report_summary()

@app.get('/search')
def search(q:str, entity_type:str|None=None, location:str|None=None):
    return {"results":_search(q,entity_type,location)}

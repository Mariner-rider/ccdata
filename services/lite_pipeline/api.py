from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
import os
from services.lite_pipeline.main import Repo,_cfg,discover,crawl_source,export_entity,review_list,review_decide,publish_entity,chatbot_sync,record_approve,enqueue_job,job_get,metrics_summary,sources_freshness,jobs_failures,quality_report_summary,_search,public_entities_list,public_entity_get,admissions_list,admissions_get,admissions_upcoming,job_postings_list,job_postings_get,job_postings_search,news_articles_list,news_articles_featured,news_articles_get

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


@app.get('/crawl-jobs/{id}')
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
def search(q:str, entity_type:str|None=None, location:str|None=None, country:str|None=None):
    return {"results":_search(q,entity_type,location,country)}

@app.get('/public/entities')
def public_entities(entity_type:str|None=None, country:str|None=None, location:str|None=None):
    return {"results":public_entities_list(entity_type,country,location)}

@app.get('/public/entities/{slug}')
def public_entity(slug:str):
    out=public_entity_get(slug)
    if not out: raise HTTPException(status_code=404,detail='not found')
    return out


@app.get('/admissions')
def list_admissions(status:str|None=None,state:str|None=None,type:str|None=None,country:str|None=None,limit:int=100):
    return {"results":admissions_list(status=status,state=state,admission_type=type,country=country,limit=limit)}

@app.get('/admissions/upcoming')
def upcoming_admissions(days:int=30):
    return {"results":admissions_upcoming(days)}

@app.get('/admissions/{id}')
def admission_detail(id:int):
    out=admissions_get(id)
    if not out: raise HTTPException(status_code=404,detail='not found')
    return out


@app.get('/jobs/search')
def search_jobs(q:str,limit:int=100):
    return {"results":job_postings_search(q,limit)}

@app.get('/jobs/internships')
def internships(stipend_min:int|None=None,location:str|None=None,status:str|None=None,limit:int=100):
    return {"results":job_postings_list(job_type='internship',location=location,stipend_min=stipend_min,status=status,limit=limit)}

@app.get('/jobs')
def list_job_postings(type:str|None=None,category:str|None=None,state:str|None=None,status:str|None=None,location:str|None=None,limit:int=100):
    return {"results":job_postings_list(job_type=type,category=category,state=state,status=status,location=location,limit=limit)}

@app.get('/jobs/{id}')
def job_posting_detail(id:int):
    out=job_postings_get(id)
    if not out: raise HTTPException(status_code=404,detail='not found')
    return out


@app.get('/news/featured')
def featured_news(limit:int=100):
    return {"results":news_articles_featured(limit)}

@app.get('/news')
def list_news(category:str|None=None,days:int|None=None,entity_id:int|None=None,limit:int=100):
    return {"results":news_articles_list(category=category,days=days,entity_id=entity_id,limit=limit)}

@app.get('/news/{id}')
def news_detail(id:int):
    out=news_articles_get(id)
    if not out: raise HTTPException(status_code=404,detail='not found')
    return out

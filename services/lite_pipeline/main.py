from __future__ import annotations
import argparse, hashlib, json, os, sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback
try:
    import psycopg
except Exception:
    psycopg=None
try:
    import redis
except Exception:
    redis=None

TRUST={"official":1.0,"government/regulator":0.95,"recognized news":0.75,"aggregator":0.60,"user/review":0.40}
REQ_COLLEGE=["name","location","official_website","courses","fees","admission_link","placement","faculty","hostel"]
KEYWORDS=["admission","admissions","programme","program","academics","departments","courses","fees","fee-structure","fee","placement","placements","career-development","career","faculty","people","hostel","campus-life","infrastructure","scholarship","contact","directory","about","overview"]
LABELS=["Faculty:","Hostel:","Placement:","Contact:","Fees:","Courses:","Address:","Location:","Admission:"]
GENERIC_HEADINGS={"courses & fees","courses and fees","placements","placement","faculty","hostel","contact","admissions","admission","gallery","infrastructure","about","overview","reviews","scholarships"}

class LinkParser(HTMLParser):
    def __init__(self): super().__init__(); self.links=[]
    def handle_starttag(self, tag, attrs):
        if tag=="a":
            h=dict(attrs).get("href")
            if h: self.links.append(h)

@dataclass
class Cfg: database_url:str; max_pages:int; timeout:float; same_domain:bool; allowlist:set[str]

def _cfg():
    allowed={d.strip() for d in os.getenv("CRAWL_ALLOWED_DOMAINS","").split(',') if d.strip()}
    return Cfg(os.getenv("DATABASE_URL","sqlite:///./collegecue_local.db"),int(os.getenv("CRAWL_MAX_PAGES_PER_SOURCE","25")),float(os.getenv("CRAWL_TIMEOUT_SECONDS","15")),os.getenv("CRAWL_SAME_DOMAIN_ONLY","true").lower()=="true",allowed)



def _is_pg():
    return _cfg().database_url.startswith('postgresql://')

def _exec_sql(sql, params=()):
    db=_cfg().database_url
    if db.startswith('sqlite:///'):
        with sqlite3.connect(Repo(db).path) as c:
            cur=c.execute(sql, params); c.commit(); return cur.fetchall() if sql.strip().lower().startswith('select') else []
    if db.startswith('postgresql://'):
        if psycopg is None: raise RuntimeError('psycopg not installed for postgresql path')
        with psycopg.connect(db) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                out=cur.fetchall() if sql.strip().lower().startswith('select') else []
            conn.commit(); return out
    raise RuntimeError('unsupported DATABASE_URL')

MIGRATIONS=[
"CREATE TABLE IF NOT EXISTS source_registry(id SERIAL PRIMARY KEY,entity_type TEXT,entity_name TEXT,official_url TEXT,trust_tier TEXT,is_active INTEGER DEFAULT 1)",
"CREATE TABLE IF NOT EXISTS crawler_records(id SERIAL PRIMARY KEY,entity_type TEXT,title TEXT,source_url TEXT UNIQUE,official_url TEXT,payload TEXT,missing_fields TEXT,confidence_score REAL,trust_tier TEXT,content_hash TEXT,last_crawled_at TEXT)",
"CREATE TABLE IF NOT EXISTS crawl_logs(id SERIAL PRIMARY KEY,source_url TEXT,status TEXT,detail TEXT,event_ts TEXT)",
"CREATE TABLE IF NOT EXISTS crawl_tasks(id SERIAL PRIMARY KEY,source_id INTEGER,url TEXT,reason TEXT,created_at TEXT)",
"CREATE TABLE IF NOT EXISTS audit_logs(id SERIAL PRIMARY KEY,entity_record_id INTEGER,action TEXT,notes TEXT,reviewed_by TEXT,created_at TEXT)",
"CREATE TABLE IF NOT EXISTS quarantine_records(id SERIAL PRIMARY KEY,source_url TEXT,payload TEXT,reason TEXT,created_at TEXT)",
"CREATE TABLE IF NOT EXISTS review_queue(id SERIAL PRIMARY KEY,entity_record_id INTEGER,entity_type TEXT,title TEXT,confidence_score REAL,missing_fields TEXT,quality_gate_status TEXT,suggested_action TEXT,created_at TEXT,reviewed_at TEXT,reviewed_by TEXT,decision TEXT,notes TEXT)",
"CREATE TABLE IF NOT EXISTS published_records(id SERIAL PRIMARY KEY,entity_record_id INTEGER,version INTEGER,payload TEXT,published_at TEXT,idempotency_key TEXT)",
"CREATE TABLE IF NOT EXISTS chatbot_sync_logs(id SERIAL PRIMARY KEY,entity_record_id INTEGER,title TEXT,published_version INTEGER,fields_synced TEXT,status TEXT,created_at TEXT,idempotency_key TEXT)",
]

def db_migrate():
    for m in MIGRATIONS: _exec_sql(m)
    return {'ok':True,'applied':len(MIGRATIONS)}

def db_status():
    if _cfg().database_url.startswith('sqlite:///'):
        path=Repo(_cfg().database_url).path
        exists=Path(path).exists()
        return {'database_url':_cfg().database_url,'backend':'sqlite','file_exists':exists,'migrations_known':len(MIGRATIONS)}
    return {'database_url':_cfg().database_url,'backend':'postgresql','psycopg_installed':psycopg is not None,'migrations_known':len(MIGRATIONS)}

def _canon(u): p=urlparse(u); return f"{p.scheme}://{p.netloc}{p.path}" if p.scheme else u

def _robots(url):
    if url.startswith("file://"): return True,"file_url"
    p=urlparse(url); rp=robotparser.RobotFileParser(); rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
    try: rp.read(); ok=rp.can_fetch("ccdata-lite-bot",url); return ok,"robots_checked"
    except Exception: return True,"robots_error_allow"

def _ptype(u):
    lu=u.lower()
    for t,k in [("admission","admission"),("courses_fees","course"),("courses_fees","fees"),("faculty","faculty"),("placement","placement"),("hostel","hostel"),("gallery","gallery"),("contact","contact")]:
        if k in lu: return t
    return "homepage" if "index" in lu else "unknown"

class Repo:
    def __init__(self,db): self.path=db.replace("sqlite:///","")
    def init(self):
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS source_registry(id INTEGER PRIMARY KEY,entity_type TEXT,entity_name TEXT,official_url TEXT,trust_tier TEXT,is_active INTEGER DEFAULT 1,last_crawled_at TEXT,crawl_frequency_days INTEGER DEFAULT 7,updated_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS crawler_records(id INTEGER PRIMARY KEY,source_id INTEGER,entity_type TEXT,title TEXT,source_url TEXT UNIQUE,official_url TEXT,payload TEXT,missing_fields TEXT,confidence_score REAL,trust_tier TEXT,content_hash TEXT,last_crawled_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS quarantine_records(id INTEGER PRIMARY KEY,source_url TEXT,payload TEXT,reason TEXT,created_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS crawl_logs(id INTEGER PRIMARY KEY,source_url TEXT,status TEXT,detail TEXT,event_ts TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS review_queue(id INTEGER PRIMARY KEY,entity_record_id INTEGER,entity_type TEXT,title TEXT,confidence_score REAL,missing_fields TEXT,quality_gate_status TEXT,suggested_action TEXT,created_at TEXT,reviewed_at TEXT,reviewed_by TEXT,decision TEXT,notes TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS published_records(id INTEGER PRIMARY KEY,entity_record_id INTEGER,version INTEGER,payload TEXT,published_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS chatbot_sync_logs(id INTEGER PRIMARY KEY,entity_record_id INTEGER,title TEXT,published_version INTEGER,fields_synced TEXT,status TEXT,created_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS audit_logs(id INTEGER PRIMARY KEY,entity_record_id INTEGER,action TEXT,notes TEXT,reviewed_by TEXT,created_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS crawl_jobs(id INTEGER PRIMARY KEY,source_id INTEGER,job_type TEXT,status TEXT,dry_run INTEGER,priority INTEGER,payload_json TEXT,result_json TEXT,error_message TEXT,idempotency_key TEXT,created_at TEXT,started_at TEXT,completed_at TEXT,retry_count INTEGER DEFAULT 0,next_retry_at TEXT,last_error TEXT)")

            try: c.execute('ALTER TABLE published_records ADD COLUMN idempotency_key TEXT')
            except Exception: pass
            try: c.execute('ALTER TABLE chatbot_sync_logs ADD COLUMN idempotency_key TEXT')
            except Exception: pass
            for q in ["ALTER TABLE source_registry ADD COLUMN last_crawled_at TEXT","ALTER TABLE source_registry ADD COLUMN crawl_frequency_days INTEGER DEFAULT 7","ALTER TABLE source_registry ADD COLUMN updated_at TEXT","ALTER TABLE crawl_jobs ADD COLUMN retry_count INTEGER DEFAULT 0","ALTER TABLE crawl_jobs ADD COLUMN next_retry_at TEXT","ALTER TABLE crawl_jobs ADD COLUMN last_error TEXT","ALTER TABLE crawler_records ADD COLUMN source_id INTEGER","ALTER TABLE published_records ADD COLUMN source_id INTEGER","ALTER TABLE chatbot_sync_logs ADD COLUMN source_id INTEGER"]:
                try: c.execute(q)
                except Exception: pass
            c.commit()
    def log(self,u,s,d):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO crawl_logs(source_url,status,detail,event_ts) VALUES(?,?,?,?)",(u,s,d,datetime.now(timezone.utc).isoformat())); c.commit()
    def add_source(self,e):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO source_registry(entity_type,entity_name,official_url,trust_tier,is_active) VALUES(?,?,?,?,1)",(e['entity_type'],e['entity_name'],e.get('url') or e.get('official_url'),e.get('trust_tier','official'))); c.commit(); return c.execute('select last_insert_rowid()').fetchone()[0]
    def get_source(self,id):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,trust_tier FROM source_registry WHERE id=?",(id,)).fetchone()
    def list_sources(self):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,trust_tier,is_active FROM source_registry").fetchall()
    def save_entity(self, rec):
        with sqlite3.connect(self.path) as c:
            row=c.execute("SELECT id,content_hash FROM crawler_records WHERE source_url=?",(rec['source_url'],)).fetchone()
            if row and row[1]==rec['content_hash']: return 'unchanged'
            if row:
                c.execute("UPDATE crawler_records SET payload=?,missing_fields=?,confidence_score=?,content_hash=?,last_crawled_at=? WHERE id=?",(json.dumps(rec),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['content_hash'],rec['last_crawled_at'],row[0])); c.commit(); return 'updated'
            state='draft' if (not rec['missing_fields'] and rec['confidence_score']>=0.85) else 'needs_review'
            rec['lifecycle_state']=state
            c.execute("INSERT INTO crawler_records(source_id,entity_type,title,source_url,official_url,payload,missing_fields,confidence_score,trust_tier,content_hash,last_crawled_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(rec.get('source_id'),rec['entity_type'],rec['title'],rec['source_url'],rec['official_url'],json.dumps(rec),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['trust_tier'],rec['content_hash'],rec['last_crawled_at']))
            rid=c.execute('select last_insert_rowid()').fetchone()[0]
            if state=='needs_review':
                c.execute("INSERT INTO review_queue(entity_record_id,entity_type,title,confidence_score,missing_fields,quality_gate_status,suggested_action,created_at) VALUES(?,?,?,?,?,?,?,?)",(rid,rec['entity_type'],rec['title'],rec['confidence_score'],json.dumps(rec['missing_fields']),'fail','review',datetime.now(timezone.utc).isoformat()))
            c.commit(); return 'created'
    def save_quarantine(self,source_url,payload,reason):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO quarantine_records(source_url,payload,reason,created_at) VALUES(?,?,?,?)",(source_url,json.dumps(payload),reason,datetime.now(timezone.utc).isoformat())); c.commit()
    def get_entity(self,id):
        with sqlite3.connect(self.path) as c: r=c.execute("SELECT payload,source_id FROM crawler_records WHERE id=?",(id,)).fetchone(); return json.loads(r[0]) if r else None

def _strip_label(s):
    x=" ".join(str(s).split()).strip()
    for l in LABELS:
        if x.lower().startswith(l.lower()): x=x[len(l):].strip()
    return x

def _clean_list(vals, headings):
    out=[]
    for v in vals or []:
        s=_strip_label(v)
        if not s or s.lower() in headings or s.lower() in GENERIC_HEADINGS: continue
        if s not in out: out.append(s)
    return out

def discover(seed,cfg,repo=None):
    txt=Path((urlparse(seed).netloc+urlparse(seed).path)).read_text(encoding='utf-8') if seed.startswith('file://') else ''
    p=LinkParser(); p.feed(txt); base=urlparse(seed); cand=[]
    for h in p.links:
        u=_canon(urljoin(seed,h)); lu=u.lower(); score=sum(2 for k in KEYWORDS if k in lu)
        if cfg.same_domain and not u.startswith('file://') and urlparse(u).netloc!=base.netloc:
            if repo: repo.log(u,'skipped','cross_domain'); continue
        if cfg.allowlist and not u.startswith('file://') and urlparse(u).netloc not in cfg.allowlist:
            if repo: repo.log(u,'skipped','allowlist_blocked'); continue
        if any(lu.endswith(ext) for ext in ['.pdf','.zip','.jpg','.png','.gif']):
            if repo: repo.log(u,'skipped','binary'); continue
        ok,reason=_robots(u)
        cand.append({"url":u,"page_type":_ptype(u),"priority":score,"reason":"keyword_match" if score else "internal_link","robots_allowed":ok,"robots_reason":reason})
    cand=sorted(cand,key=lambda x:(x['priority'],x['url']), reverse=True)
    seed_ok,seed_reason=_robots(seed)
    return [{"url":seed,"page_type":"homepage","priority":99,"reason":"seed","robots_allowed":seed_ok,"robots_reason":seed_reason}]+cand[:cfg.max_pages-1]

def merge_pages(entity_type,name,official_url,pages,trust):
    headings={"courses","course","placement","placements","faculty","hostel","fees","admission","contact","gallery","courses & fees"}
    merged={"name":name,"official_website":official_url,"location":"","courses":[],"fees":[],"admission_link":[],"placement":[],"faculty":[],"hostel":[],"gallery":[],"contact":[]}
    fs={}
    for p in pages:
        ex=p['extract']
        for f in ["location","courses","fees","admission_link","placement","faculty","hostel","gallery","contact"]:
            v=ex.get(f); conf=ex.get('field_details',{}).get(f,{}).get('confidence',0.5)
            v=_clean_list(v,headings) if isinstance(v,list) else _strip_label(v)
            if not v: continue
            if conf>fs.get(f,{}).get('confidence',-1): merged[f]=v; fs[f]={"source_url":p['url'],"confidence":conf,"method":ex.get('field_details',{}).get(f,{}).get('extraction_method','heuristic')}
    if isinstance(merged['faculty'],list):
        flat=[]
        for i in merged['faculty']: flat.extend([x.strip() for x in i.split(',') if x.strip()])
        merged['faculty']=flat
    missing=[f for f in REQ_COLLEGE if not merged.get(f)]
    comp=1-len(missing)/len(REQ_COLLEGE); conf=round(comp*0.6+TRUST.get(trust,0.6)*0.25+0.15,3)
    return {"entity_type":entity_type,"title":merged.get('name') or name,"source_url":official_url,"official_url":official_url,"info":{"name":merged['name'],"location":merged['location'],"contact":merged['contact']},"courses_and_fees":{"courses":merged['courses'],"fees":merged['fees'],"admission_link":merged['admission_link']},"gallery":merged['gallery'],"faculty":merged['faculty'],"hostel":merged['hostel'],"placement":merged['placement'],"reviews":[],"metadata":{"field_sources":fs,"page_count":len(pages)},"fields":merged,"missing_fields":missing,"confidence_score":conf,"trust_tier":trust,"content_hash":hashlib.sha256(json.dumps(merged,sort_keys=True).encode()).hexdigest(),"last_crawled_at":datetime.now(timezone.utc).isoformat()}

def quality_report(plan,pages,rec,gate,reason=''):
    return {"pages_discovered":len(plan),"pages_crawled":len(pages),"page_types_detected":sorted({p['page_type'] for p in pages}),"required_fields_found":[f for f in REQ_COLLEGE if f not in rec['missing_fields']],"missing_fields":rec['missing_fields'],"confidence_score":rec['confidence_score'],"quality_gate":gate,"quarantine_reason":reason,"top_field_sources":rec['metadata']['field_sources']}

def crawl_source(id,dry=False):
    cfg=_cfg(); repo=Repo(cfg.database_url); repo.init(); sid,etype,name,url,trust=repo.get_source(id)
    plan=discover(url,cfg,repo); pages=[]
    for p in plan:
        if not p['robots_allowed']: repo.log(p['url'],'blocked','robots'); continue
        try: pages.append({"url":p['url'],"extract":extract_fallback(p['url'], timeout=cfg.timeout),"page_type":p['page_type']})
        except Exception as e: repo.log(p['url'],'error',str(e))
    rec=merge_pages(etype,name,url,pages,trust); rec['source_id']=sid
    valid=rec['confidence_score']>=0.65 and (1-len(rec['missing_fields'])/len(REQ_COLLEGE))>=0.7 and rec['trust_tier'] in TRUST and rec['content_hash']
    qr=quality_report(plan,pages,rec,'pass' if valid else 'fail','quality_gate_failed' if not valid else '')
    if dry: return {"dry_run":True,"quality_report":qr,"record":rec}
    st=repo.save_entity(rec) if valid else (repo.save_quarantine(url,rec,'quality_gate_failed') or 'quarantined')
    if st in {'created','updated','unchanged'}:
        with sqlite3.connect(repo.path) as c:
            c.execute('UPDATE source_registry SET last_crawled_at=?,updated_at=? WHERE id=?',(datetime.now(timezone.utc).isoformat(),datetime.now(timezone.utc).isoformat(),sid)); c.commit()
    return {"source_id":sid,"status":st,"quality_report":qr}

def export_entity(eid): repo=Repo(_cfg().database_url); repo.init(); return repo.get_entity(eid)

def pilot_college(name,url,dry,save):
    cfg=_cfg(); cfg.max_pages=min(cfg.max_pages,10)
    repo=Repo(cfg.database_url); repo.init(); sid=repo.add_source({'entity_type':'college','entity_name':name,'url':url,'trust_tier':'official'})
    res=crawl_source(sid,dry=not save)
    return {"pilot_source_id":sid,"preview":discover(url,cfg,repo),"result":res}



def export_validate(eid):
    rec=export_entity(eid)
    if not rec: return {"ok":False,"errors":["entity_not_found"]}
    errs=[]
    fields=rec.get('fields',{})
    for k,v in fields.items():
        if isinstance(v,list):
            if '' in [str(x).strip() for x in v]: errs.append(f"{k}:empty")
            if len(v)!=len(list(dict.fromkeys(v))): errs.append(f"{k}:duplicate")
            if any(str(x).strip().lower() in GENERIC_HEADINGS for x in v): errs.append(f"{k}:heading_pollution")
        elif isinstance(v,str) and not v.strip() and k in REQ_COLLEGE: errs.append(f"{k}:empty")
    if rec.get('missing_fields'): errs.append('missing_required')
    if not rec.get('confidence_score'): errs.append('missing_confidence')
    srcs=rec.get('metadata',{}).get('field_sources',{})
    if not srcs: errs.append('missing_sources')
    return {"ok":len(errs)==0,"errors":errs}

def readiness_check():
    cfg=_cfg(); repo=Repo(cfg.database_url); repo.init();
    with sqlite3.connect(repo.path) as c:
        pending=0
        quarantine=c.execute('select count(*) from quarantine_records').fetchone()[0]
        errs=c.execute("select count(*) from crawl_logs where status='error'").fetchone()[0]
    return {"deps":{"sqlite3":True,"httpx_optional":True},"runtime_profile":os.getenv('RUNTIME_PROFILE','no-docker'),"db_connectivity":True,"queue_backend":os.getenv('QUEUE_BACKEND','memory'),"webclaw_enabled":os.getenv('WEBCLAW_ENABLED','false'),"crawler_limits":{"max_pages":cfg.max_pages,"timeout":cfg.timeout,"same_domain":cfg.same_domain},"allowed_domains":sorted(cfg.allowlist),"storage_status":{"db_path":repo.path},"pending_crawl_tasks":pending,"quarantine_count":quarantine,"last_crawl_log_errors":errs}

def audit_export():
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        logs=[dict(source_url=r[0],status=r[1],detail=r[2],event_ts=r[3]) for r in c.execute('select source_url,status,detail,event_ts from crawl_logs order by id desc limit 200').fetchall()]
        q=[dict(id=r[0],source_url=r[1],reason=r[2],created_at=r[3]) for r in c.execute('select id,source_url,reason,created_at from quarantine_records order by id desc limit 200').fetchall()]
    return {"crawl_logs":logs,"quarantine_records":q,"audit_logs":[],"crawl_tasks_summary":{"pending":0}}

def pilot_http_smoke(url,name):
    if not url.startswith(('http://','https://','file://')): raise RuntimeError('invalid url')
    old=os.environ.get('CRAWL_MAX_PAGES_PER_SOURCE'); os.environ['CRAWL_MAX_PAGES_PER_SOURCE']='5'
    old2=os.environ.get('CRAWL_SAME_DOMAIN_ONLY'); os.environ['CRAWL_SAME_DOMAIN_ONLY']='true'
    rep=Repo(_cfg().database_url); rep.init(); sid=rep.add_source({'entity_type':'college','entity_name':name,'url':url,'trust_tier':'official'})
    out=crawl_source(sid,True)
    comp=audit_export()['crawl_logs']
    if old is not None: os.environ['CRAWL_MAX_PAGES_PER_SOURCE']=old
    if old2 is not None: os.environ['CRAWL_SAME_DOMAIN_ONLY']=old2
    return {"safe_completed":True,"quality_report":out.get('quality_report',{}),"compliance_log":comp}



def review_list():
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        rows=c.execute("SELECT id,entity_record_id,title,confidence_score,missing_fields,decision FROM review_queue ORDER BY id DESC").fetchall()
    return [dict(id=r[0],entity_record_id=r[1],title=r[2],confidence_score=r[3],missing_fields=json.loads(r[4] or '[]'),decision=r[5]) for r in rows]

def review_show(i):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        r=c.execute("SELECT * FROM review_queue WHERE id=?",(i,)).fetchone();
    return r

def review_decide(i,decision,reviewed_by,notes=''):
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        r=c.execute("SELECT entity_record_id FROM review_queue WHERE id=?",(i,)).fetchone()
        if not r: raise RuntimeError('review not found')
        c.execute("UPDATE review_queue SET reviewed_at=?,reviewed_by=?,decision=?,notes=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),reviewed_by,decision,notes,i))
        pr=c.execute("SELECT payload,source_id FROM crawler_records WHERE id=?",(r[0],)).fetchone(); rec=json.loads(pr[0]); src_id=pr[1]; rec['lifecycle_state']='approved' if decision=='approved' else 'rejected'
        c.execute("UPDATE crawler_records SET payload=? WHERE id=?",(json.dumps(rec),r[0])); c.commit()
    return {'ok':True,'decision':decision}

def publish_entity(i,idempotency_key=None):
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        pr=c.execute("SELECT payload,source_id FROM crawler_records WHERE id=?",(i,)).fetchone();
        if not pr: raise RuntimeError('record not found')
        rec=json.loads(pr[0]); src_id=pr[1]
        if rec.get('lifecycle_state')!='approved':
            raise RuntimeError(f"invalid state: {rec.get('lifecycle_state')}; next_action=record:approve")
        if idempotency_key:
            ex=c.execute("SELECT version FROM published_records WHERE entity_record_id=? AND idempotency_key=? ORDER BY version DESC LIMIT 1",(i,idempotency_key)).fetchone()
            if ex: return {'entity_id':i,'version':ex[0],'status':'published','idempotent':True}
        v=(c.execute("SELECT COALESCE(MAX(version),0) FROM published_records WHERE entity_record_id=?",(i,)).fetchone()[0])+1
        rec['lifecycle_state']='published'
        c.execute("UPDATE crawler_records SET payload=? WHERE id=?",(json.dumps(rec),i))
        c.execute("INSERT INTO published_records(entity_record_id,source_id,version,payload,published_at,idempotency_key) VALUES(?,?,?,?,?,?)",(i,src_id,v,json.dumps(rec),datetime.now(timezone.utc).isoformat(),idempotency_key)); c.commit()
    _log_event('publish_completed',entity_id=i,version=v)
    return {'entity_id':i,'version':v,'status':'published'}

def publish_list():
    with sqlite3.connect(Repo(_cfg().database_url).path) as c: rows=c.execute("SELECT entity_record_id,version,published_at FROM published_records ORDER BY id DESC").fetchall()
    return [dict(entity_id=r[0],version=r[1],published_at=r[2]) for r in rows]

def chatbot_sync(eid,idempotency_key=None):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        r=c.execute("SELECT version,payload,source_id FROM published_records WHERE entity_record_id=? ORDER BY version DESC LIMIT 1",(eid,)).fetchone()
        if not r: raise RuntimeError('not published; next_action=publish:entity')
        if idempotency_key:
            ex=c.execute("SELECT id,published_version,status FROM chatbot_sync_logs WHERE entity_record_id=? AND idempotency_key=? ORDER BY id DESC LIMIT 1",(eid,idempotency_key)).fetchone()
            if ex: return {'entity_id':eid,'published_version':ex[1],'status':ex[2],'idempotent':True}
        rec=json.loads(r[1]); src_id=r[2]; fields=list(rec.get('fields',{}).keys())
        c.execute("INSERT INTO chatbot_sync_logs(entity_record_id,source_id,title,published_version,fields_synced,status,created_at,idempotency_key) VALUES(?,?,?,?,?,?,?,?)",(eid,src_id,rec.get('title'),r[0],json.dumps(fields),'queued',datetime.now(timezone.utc).isoformat(),idempotency_key)); c.commit()
    _log_event('chatbot_sync_queued',entity_id=eid,version=r[0])
    return {'entity_id':eid,'published_version':r[0],'fields_synced':fields,'status':'queued'}



def record_list(state=None):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        rows=c.execute('SELECT id,payload FROM crawler_records').fetchall()
    out=[]
    for i,p in rows:
        rec=json.loads(p); st=rec.get('lifecycle_state','')
        if state and st!=state: continue
        out.append({'id':i,'title':rec.get('title'),'state':st,'confidence_score':rec.get('confidence_score')})
    return out

def record_show(i):
    return export_entity(i)

def _audit(i,action,notes,reviewed_by):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        c.execute('INSERT INTO audit_logs(entity_record_id,action,notes,reviewed_by,created_at) VALUES(?,?,?,?,?)',(i,action,notes,reviewed_by,datetime.now(timezone.utc).isoformat())); c.commit()

def review_seed(entity_id):
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        pr=c.execute('SELECT payload,entity_type,title,confidence_score,missing_fields FROM crawler_records WHERE id=?',(entity_id,)).fetchone()
        if not pr: raise RuntimeError('record not found')
        rec=json.loads(pr[0]); src_id=pr[1]; st=rec.get('lifecycle_state')
        if st not in {'draft','needs_review','approved'}: raise RuntimeError('seed only for draft/needs_review')
        ex=c.execute('SELECT id FROM review_queue WHERE entity_record_id=?',(entity_id,)).fetchone()
        if not ex:
            c.execute('INSERT INTO review_queue(entity_record_id,entity_type,title,confidence_score,missing_fields,quality_gate_status,suggested_action,created_at) VALUES(?,?,?,?,?,?,?,?)',(entity_id,pr[1],pr[2],pr[3],pr[4],'manual','review',datetime.now(timezone.utc).isoformat())); c.commit()
    return {'ok':True,'entity_id':entity_id}

def record_approve(entity_id,reviewed_by,force=False):
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        pr=c.execute('SELECT payload,source_id FROM crawler_records WHERE id=?',(entity_id,)).fetchone()
        if not pr: raise RuntimeError('record not found')
        rec=json.loads(pr[0]); src_id=pr[1]; st=rec.get('lifecycle_state')
        if st=='rejected' and not force: raise RuntimeError('rejected requires --force')
        if st=='quarantine': raise RuntimeError('quarantine cannot be approved')
        rec['lifecycle_state']='approved'
        c.execute('UPDATE crawler_records SET payload=? WHERE id=?',(json.dumps(rec),entity_id)); c.commit()
    review_seed(entity_id)
    _audit(entity_id,'record_approve','',reviewed_by)
    return {'ok':True,'entity_id':entity_id,'state':'approved'}

def record_reject(entity_id,reviewed_by,notes=''):
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        pr=c.execute('SELECT payload,source_id FROM crawler_records WHERE id=?',(entity_id,)).fetchone()
        if not pr: raise RuntimeError('record not found')
        rec=json.loads(pr[0]); src_id=pr[1]; st=rec.get('lifecycle_state')
        if st=='published': raise RuntimeError('published cannot be rejected directly')
        rec['lifecycle_state']='rejected'
        c.execute('UPDATE crawler_records SET payload=? WHERE id=?',(json.dumps(rec),entity_id)); c.commit()
    _audit(entity_id,'record_reject',notes,reviewed_by)
    return {'ok':True,'entity_id':entity_id,'state':'rejected'}



class MemoryQueueBackend:
    def enqueue(self, job_id): return True

class RedisQueueBackend:
    def __init__(self):
        if redis is None: raise RuntimeError('redis dependency missing')
        self.r=redis.from_url(os.getenv('REDIS_URL','redis://localhost:6379/0'))
    def enqueue(self, job_id):
        self.r.rpush('crawl_jobs', job_id); return True

def _queue_backend():
    b=os.getenv('QUEUE_BACKEND','memory')
    return RedisQueueBackend() if b=='redis' else MemoryQueueBackend()

def enqueue_job(source_id,job_type='crawl',dry_run=False,priority=5,payload=None,idempotency_key=None):
    repo=Repo(_cfg().database_url); repo.init()
    if not isinstance(idempotency_key,str):
        idempotency_key=None
    with sqlite3.connect(repo.path) as c:
        if idempotency_key:
            ex=c.execute('SELECT id,status FROM crawl_jobs WHERE idempotency_key=? ORDER BY id DESC LIMIT 1',(idempotency_key,)).fetchone()
            if ex: return {'job_id':ex[0],'status':ex[1],'idempotent':True}
        c.execute('INSERT INTO crawl_jobs(source_id,job_type,status,dry_run,priority,payload_json,idempotency_key,created_at) VALUES(?,?,?,?,?,?,?,?)',(source_id,job_type,'queued',1 if dry_run else 0,priority,json.dumps(payload or {}),idempotency_key,datetime.now(timezone.utc).isoformat()))
        jid=c.execute('select last_insert_rowid()').fetchone()[0]; c.commit()
    _queue_backend().enqueue(jid)
    return {'job_id':jid,'status':'queued'}

def job_get(i):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        r=c.execute('SELECT id,source_id,job_type,status,dry_run,priority,payload_json,result_json,error_message,created_at,started_at,completed_at FROM crawl_jobs WHERE id=?',(i,)).fetchone()
    if not r: return None
    return {'id':r[0],'source_id':r[1],'job_type':r[2],'status':r[3],'dry_run':bool(r[4]),'priority':r[5],'payload_json':json.loads(r[6] or '{}'),'result_json':json.loads(r[7] or '{}') if r[7] else None,'error_message':r[8],'created_at':r[9],'started_at':r[10],'completed_at':r[11]}

def jobs_list():
    with sqlite3.connect(Repo(_cfg().database_url).path) as c: rows=c.execute('SELECT id,source_id,job_type,status,created_at FROM crawl_jobs ORDER BY id DESC').fetchall()
    return [dict(id=r[0],source_id=r[1],job_type=r[2],status=r[3],created_at=r[4]) for r in rows]

def jobs_cancel(i):
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        c.execute("UPDATE crawl_jobs SET status='cancelled',completed_at=? WHERE id=? AND status in ('queued','running')",(datetime.now(timezone.utc).isoformat(),i))
        c.execute("INSERT INTO audit_logs(entity_record_id,action,notes,reviewed_by,created_at) VALUES(?,?,?,?,?)",(i,'job_cancel','cancelled via jobs:cancel','system',datetime.now(timezone.utc).isoformat()))
        c.commit()
    return {'ok':True,'job_id':i}

def worker_once():
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        r=c.execute("SELECT id,source_id,dry_run,retry_count FROM crawl_jobs WHERE status='queued' AND (next_retry_at IS NULL OR next_retry_at<=?) ORDER BY priority DESC,id ASC LIMIT 1",(datetime.now(timezone.utc).isoformat(),)).fetchone()
        if not r: return {'processed':0}
        jid,sid,dry,retry_count=r
        c.execute("UPDATE crawl_jobs SET status='running',started_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),jid)); c.commit()
    try:
        result=crawl_source(sid,bool(dry))
        with sqlite3.connect(repo.path) as c:
            c.execute("UPDATE crawl_jobs SET status='completed',result_json=?,completed_at=? WHERE id=?",(json.dumps(result),datetime.now(timezone.utc).isoformat(),jid)); c.commit()
        _log_event('crawl_job_completed',job_id=jid,source_id=sid)
        return {'processed':1,'job_id':jid,'status':'completed'}
    except Exception as e:
        with sqlite3.connect(repo.path) as c:
            maxr=2
            if retry_count<maxr:
                back=2**retry_count
                nr=(datetime.now(timezone.utc)+timedelta(minutes=back)).isoformat()
                c.execute("UPDATE crawl_jobs SET status='queued',retry_count=retry_count+1,last_error=?,next_retry_at=? WHERE id=?",(str(e),nr,jid))
                c.commit(); _log_event('crawl_job_failed',job_id=jid,retry_count=retry_count+1,error=str(e)); return {'processed':1,'job_id':jid,'status':'retry_queued','retry_count':retry_count+1}
            c.execute("UPDATE crawl_jobs SET status='failed',error_message=?,last_error=?,completed_at=? WHERE id=?",(str(e),str(e),datetime.now(timezone.utc).isoformat(),jid)); c.commit()
        return {'processed':1,'job_id':jid,'status':'failed'}

def worker_run():
    out=[]
    while True:
        r=worker_once(); out.append(r)
        if r.get('processed',0)==0: break
    return {'runs':out}



def _env_int(k,d):
    try:return int(os.getenv(k,str(d)))
    except:return d

def _job_priority(job_type):
    return {'registration':100,'missing_fields':90,'admissions':80,'jobs':80,'news':80,'refresh':60}.get(job_type,50)

def _recover_stale_jobs():
    stale=_env_int('JOB_STALE_MINUTES',30)
    now=datetime.now(timezone.utc)
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        rows=c.execute("SELECT id,started_at,retry_count FROM crawl_jobs WHERE status='running' AND started_at IS NOT NULL").fetchall()
        for jid,st,rc in rows:
            try: stt=datetime.fromisoformat(st)
            except: continue
            if (now-stt).total_seconds()>stale*60:
                if rc<2:
                    c.execute("UPDATE crawl_jobs SET status='queued',retry_count=retry_count+1,started_at=NULL,last_error='stale_requeue' WHERE id=?",(jid,))
                else:
                    c.execute("UPDATE crawl_jobs SET status='failed',last_error='stale_failed',completed_at=? WHERE id=?",(now.isoformat(),jid))
        c.commit()

def scheduler_run_once():
    _recover_stale_jobs()
    repo=Repo(_cfg().database_url); repo.init(); enq=[]
    daily=_env_int('DAILY_MAX_JOBS',200); per_domain=_env_int('DAILY_MAX_JOBS_PER_DOMAIN',20); max_failed=_env_int('MAX_FAILED_JOBS_PER_SOURCE',3); cooldown=_env_int('CRAWL_COOLDOWN_HOURS_AFTER_FAILURE',24)
    report={'sources_checked':0,'jobs_enqueued':0,'skipped_not_due':0,'skipped_budget':0,'skipped_cooldown':0,'failed_sources_blocked':0}
    now=datetime.now(timezone.utc)
    with sqlite3.connect(repo.path) as c:
        src=c.execute('SELECT id,official_url,last_crawled_at,COALESCE(crawl_frequency_days,7) FROM source_registry WHERE is_active=1').fetchall()
        jobs_today=c.execute("SELECT count(*) FROM crawl_jobs WHERE date(created_at)=date('now')").fetchone()[0]
        domain_counts={r[0]:r[1] for r in c.execute("SELECT substr(json_extract(payload_json,'$.domain'),1),count(*) FROM crawl_jobs WHERE date(created_at)=date('now') GROUP BY 1").fetchall() if r[0]}
        for sid,url,last,days in src:
            report['sources_checked']+=1
            if jobs_today>=daily: report['skipped_budget']+=1; continue
            domain=urlparse(url).netloc
            if domain and domain_counts.get(domain,0)>=per_domain: report['skipped_budget']+=1; continue
            due=False
            if not last: due=True
            else:
                try: due=(now-datetime.fromisoformat(last)).total_seconds()>=int(days)*86400
                except: due=True
            if not due: report['skipped_not_due']+=1; continue
            f=c.execute("SELECT count(*),max(completed_at) FROM crawl_jobs WHERE source_id=? AND status='failed'",(sid,)).fetchone()
            if f[0]>=max_failed: report['failed_sources_blocked']+=1; continue
            if f[1]:
                try:
                    if (now-datetime.fromisoformat(f[1])).total_seconds()<cooldown*3600: report['skipped_cooldown']+=1; continue
                except: pass
            j=enqueue_job(sid,'refresh',False,_job_priority('refresh'),{'domain':domain})
            enq.append(j); jobs_today+=1; domain_counts[domain]=domain_counts.get(domain,0)+1; report['jobs_enqueued']+=1
    report['enqueued']=enq
    return report



def _log_event(event, **data):
    if os.getenv('LOG_FORMAT','').lower()=='json':
        print(json.dumps({'event':event, **data, 'ts':datetime.now(timezone.utc).isoformat()}))

def metrics_summary():
    repo=Repo(_cfg().database_url); repo.init()
    with sqlite3.connect(repo.path) as c:
        total_sources=c.execute('select count(*) from source_registry').fetchone()[0]
        active_sources=c.execute('select count(*) from source_registry where is_active=1').fetchone()[0]
        jobs={k:c.execute(f"select count(*) from crawl_jobs where status='{k}'").fetchone()[0] for k in ['queued','running','completed','failed','cancelled']}
        retry_queued=c.execute("select count(*) from crawl_jobs where status='queued' and retry_count>0").fetchone()[0]
        quarantine=c.execute('select count(*) from quarantine_records').fetchone()[0]
        review=c.execute('select count(*) from review_queue').fetchone()[0]
        published=c.execute('select count(*) from published_records').fetchone()[0]
        recs=[json.loads(r[0]) for r in c.execute('select payload from crawler_records').fetchall()]
    due=len([r for r in sources_freshness() if r['freshness_status'] in {'due','never_crawled'}])
    conf=[r.get('confidence_score',0) for r in recs] or [0]
    fresh=sum(1 for r in recs if r.get('missing_fields')==[])
    incomplete=sum(1 for r in recs if r.get('missing_fields'))
    stale=max(0,total_sources-fresh-incomplete)
    return {'total_sources':total_sources,'active_sources':active_sources,'due_sources':due,'total_jobs':sum(jobs.values()),'queued_jobs':jobs['queued'],'running_jobs':jobs['running'],'completed_jobs':jobs['completed'],'failed_jobs':jobs['failed'],'cancelled_jobs':jobs['cancelled'],'retry_queued_jobs':retry_queued,'quarantine_count':quarantine,'review_queue_count':review,'published_count':published,'avg_confidence_score':round(sum(conf)/len(conf),3),'fresh_records':fresh,'incomplete_records':incomplete,'stale_records':stale}

def sources_freshness():
    out=[]; now=datetime.now(timezone.utc)
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        rows=c.execute('select id,entity_name,entity_type,official_url,last_crawled_at,coalesce(crawl_frequency_days,7) from source_registry').fetchall()
        for r in rows:
            last=None if not r[4] else datetime.fromisoformat(r[4])
            next_due=None if not last else (last+timedelta(days=int(r[5])))
            status='never_crawled' if last is None else ('due' if next_due<=now else 'fresh')
            lj=c.execute('select status,error_message from crawl_jobs where source_id=? order by id desc limit 1',(r[0],)).fetchone()
            out.append({'id':r[0],'entity_name':r[1],'entity_type':r[2],'official_url':r[3],'last_crawled_at':r[4],'crawl_frequency_days':r[5],'next_due_at':next_due.isoformat() if next_due else None,'freshness_status':status,'last_job_status':lj[0] if lj else None,'last_error':lj[1] if lj else None})
    return out

def jobs_failures():
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        rows=c.execute("select source_id,count(*),max(last_error),max(retry_count),max(next_retry_at) from crawl_jobs where status='failed' or (status='queued' and retry_count>0) group by source_id").fetchall()
    return [{'source_id':r[0],'failed_jobs':r[1],'last_error':r[2],'retry_count':r[3] or 0,'next_retry_at':r[4],'suggested_action':'inspect source or increase timeout'} for r in rows]

def quality_report_summary():
    with sqlite3.connect(Repo(_cfg().database_url).path) as c:
        recs=[json.loads(r[0]) for r in c.execute('select payload from crawler_records').fetchall()]
    by_state={}; by_fresh={}; low=[]; missing=[]; top={}
    for r in recs:
        st=r.get('lifecycle_state','unknown'); by_state[st]=by_state.get(st,0)+1
        fr='fresh' if not r.get('missing_fields') else 'incomplete'; by_fresh[fr]=by_fresh.get(fr,0)+1
        if r.get('confidence_score',1)<0.75: low.append(r.get('title'))
        if r.get('missing_fields'): missing.append({'title':r.get('title'),'missing_fields':r.get('missing_fields')})
        et=r.get('entity_type','unknown'); top.setdefault(et,{})
        for f in r.get('missing_fields',[]): top[et][f]=top[et].get(f,0)+1
    return {'records_by_lifecycle_state':by_state,'records_by_freshness_status':by_fresh,'low_confidence_records':low,'records_with_missing_required_fields':missing,'top_missing_fields_by_entity_type':top}



def integrity_check():
    repo=Repo(_cfg().database_url); repo.init()
    out={}
    with sqlite3.connect(repo.path) as c:
        out['records_without_source_id']=c.execute('select count(*) from crawler_records where source_id is null').fetchone()[0]
        out['published_without_entity']=c.execute('select count(*) from published_records p left join crawler_records c on p.entity_record_id=c.id where c.id is null').fetchone()[0]
        out['sync_without_published']=c.execute('select count(*) from chatbot_sync_logs s left join published_records p on s.entity_record_id=p.entity_record_id and s.published_version=p.version where p.id is null').fetchone()[0]
        out['duplicate_active_sources_by_url']=c.execute('select count(*) from (select official_url,count(*) c from source_registry where is_active=1 group by official_url having c>1)').fetchone()[0]
        out['source_last_crawled_mismatch']=c.execute("select count(*) from source_registry s where s.last_crawled_at is null and exists(select 1 from crawl_jobs j where j.source_id=s.id and j.status='completed')").fetchone()[0]
    return out

def integrity_repair(apply=False):
    repo=Repo(_cfg().database_url); repo.init(); changes=[]
    with sqlite3.connect(repo.path) as c:
        rows=c.execute('select id,source_url from crawler_records where source_id is null').fetchall()
        for rid,url in rows:
            src=c.execute('select id from source_registry where official_url=? order by id limit 1',(url,)).fetchone()
            if src:
                changes.append({'record_id':rid,'source_id':src[0]})
                if apply: c.execute('update crawler_records set source_id=? where id=?',(src[0],rid))
        rows=c.execute("select s.id,max(j.completed_at) from source_registry s join crawl_jobs j on s.id=j.source_id and j.status='completed' group by s.id").fetchall()
        for sid,last in rows:
            cur=c.execute('select last_crawled_at from source_registry where id=?',(sid,)).fetchone()[0]
            if (not cur) and last:
                changes.append({'source_id':sid,'last_crawled_at':last})
                if apply: c.execute('update source_registry set last_crawled_at=?,updated_at=? where id=?',(last,datetime.now(timezone.utc).isoformat(),sid))
        dups=c.execute('select official_url from source_registry where is_active=1 group by official_url having count(*)>1').fetchall()
        for (u,) in dups:
            ids=[r[0] for r in c.execute('select id from source_registry where official_url=? and is_active=1 order by id',(u,)).fetchall()][1:]
            for sid in ids:
                changes.append({'deactivate_source_id':sid})
                if apply: c.execute('update source_registry set is_active=0 where id=?',(sid,))
        if apply: c.commit()
    return {'dry_run':not apply,'changes':changes}

def main():
    pa=argparse.ArgumentParser(); sub=pa.add_subparsers(dest='cmd',required=True)
    sub.add_parser('init-db')
    sub.add_parser('db:migrate')
    sub.add_parser('db:status')
    sub.add_parser('worker:run')
    sub.add_parser('worker:once')
    sub.add_parser('jobs:list')
    jsh=sub.add_parser('jobs:show'); jsh.add_argument('--id',type=int,required=True)
    jca=sub.add_parser('jobs:cancel'); jca.add_argument('--id',type=int,required=True)
    sca=sub.add_parser('source:crawl-async'); sca.add_argument('--id',type=int,required=True); sca.add_argument('--dry-run',action='store_true'); sca.add_argument('--idempotency-key',default=None)
    sub.add_parser('scheduler:run-once')
    sub.add_parser('metrics:summary')
    sub.add_parser('sources:freshness')
    sub.add_parser('jobs:failures')
    sub.add_parser('quality:report')
    sub.add_parser('integrity:check')
    ir=sub.add_parser('integrity:repair'); ir.add_argument('--dry-run',action='store_true'); ir.add_argument('--apply',action='store_true')
    a=sub.add_parser('source:add'); a.add_argument('--entity-type',required=True); a.add_argument('--entity-name',required=True); a.add_argument('--url',required=True); a.add_argument('--trust-tier',default='official')
    sub.add_parser('source:list')
    p=sub.add_parser('source:preview'); p.add_argument('--id',type=int,required=True)
    c=sub.add_parser('source:crawl'); c.add_argument('--id',type=int,required=True); c.add_argument('--dry-run',action='store_true')
    e=sub.add_parser('export:entity'); e.add_argument('--id',type=int,required=True); e.add_argument('--format',default='json')
    t=sub.add_parser('extract:test'); t.add_argument('--url',required=True)
    d=sub.add_parser('extract:debug'); d.add_argument('--url',required=True)
    pc=sub.add_parser('pilot:college'); pc.add_argument('--name',required=True); pc.add_argument('--url',required=True); pc.add_argument('--dry-run',action='store_true'); pc.add_argument('--save',action='store_true')
    ph=sub.add_parser('pilot:http-smoke'); ph.add_argument('--url',required=True); ph.add_argument('--name',required=True)
    ev=sub.add_parser('export:validate'); ev.add_argument('--id',type=int,required=True)
    sub.add_parser('readiness:check')
    ae=sub.add_parser('audit:export'); ae.add_argument('--format',default='json')
    sub.add_parser('review:list')
    rs=sub.add_parser('review:show'); rs.add_argument('--id',type=int,required=True)
    ra=sub.add_parser('review:approve'); ra.add_argument('--id',type=int,required=True); ra.add_argument('--reviewed-by',required=True)
    rr=sub.add_parser('review:reject'); rr.add_argument('--id',type=int,required=True); rr.add_argument('--reviewed-by',required=True); rr.add_argument('--notes',default='')
    pe=sub.add_parser('publish:entity'); pe.add_argument('--id',type=int,required=True); pe.add_argument('--idempotency-key',default=None)
    sub.add_parser('publish:list')
    cs=sub.add_parser('chatbot:sync'); cs.add_argument('--entity-id',type=int,required=True); cs.add_argument('--idempotency-key',default=None)
    rl=sub.add_parser('record:list'); rl.add_argument('--state',default=None)
    rsh=sub.add_parser('record:show'); rsh.add_argument('--id',type=int,required=True)
    rap=sub.add_parser('record:approve'); rap.add_argument('--id',type=int,required=True); rap.add_argument('--reviewed-by',required=True); rap.add_argument('--force',action='store_true')
    rrj=sub.add_parser('record:reject'); rrj.add_argument('--id',type=int,required=True); rrj.add_argument('--reviewed-by',required=True); rrj.add_argument('--notes',default='')
    rsd=sub.add_parser('review:seed'); rsd.add_argument('--entity-id',type=int,required=True)
    args=pa.parse_args(); repo=Repo(_cfg().database_url); repo.init()
    if args.cmd=='init-db': print('initialized')
    elif args.cmd=='db:migrate': print(json.dumps(db_migrate(),indent=2))
    elif args.cmd=='db:status': print(json.dumps(db_status(),indent=2))
    elif args.cmd=='worker:run': print(json.dumps(worker_run(),indent=2))
    elif args.cmd=='worker:once': print(json.dumps(worker_once(),indent=2))
    elif args.cmd=='jobs:list': print(json.dumps(jobs_list(),indent=2))
    elif args.cmd=='jobs:show': print(json.dumps(job_get(args.id),indent=2))
    elif args.cmd=='jobs:cancel': print(json.dumps(jobs_cancel(args.id),indent=2))
    elif args.cmd=='source:crawl-async': print(json.dumps(enqueue_job(args.id,'crawl',args.dry_run,5,None,args.idempotency_key),indent=2))
    elif args.cmd=='scheduler:run-once': print(json.dumps(scheduler_run_once(),indent=2))
    elif args.cmd=='metrics:summary': print(json.dumps(metrics_summary(),indent=2))
    elif args.cmd=='sources:freshness': print(json.dumps(sources_freshness(),indent=2))
    elif args.cmd=='jobs:failures': print(json.dumps(jobs_failures(),indent=2))
    elif args.cmd=='quality:report': print(json.dumps(quality_report_summary(),indent=2))
    elif args.cmd=='integrity:check': print(json.dumps(integrity_check(),indent=2))
    elif args.cmd=='integrity:repair': print(json.dumps(integrity_repair(apply=args.apply and not args.dry_run),indent=2))
    elif args.cmd=='source:add': print('added', repo.add_source(vars(args)))
    elif args.cmd=='source:list': print(json.dumps([{"id":r[0],"entity_type":r[1],"entity_name":r[2],"official_url":r[3],"trust_tier":r[4],"is_active":r[5]} for r in repo.list_sources()],indent=2))
    elif args.cmd=='source:preview': s=repo.get_source(args.id); plan=discover(s[3],_cfg(),repo); print(json.dumps({"source_id":args.id,"estimated_page_count":len(plan),"urls":plan,"quality_report":{"pages_discovered":len(plan)}},indent=2))
    elif args.cmd=='source:crawl': print(json.dumps(crawl_source(args.id,args.dry_run),indent=2))
    elif args.cmd=='export:entity': print(json.dumps(export_entity(args.id),indent=2))
    elif args.cmd=='extract:test': print(json.dumps(extract_fallback(args.url),indent=2))
    elif args.cmd=='extract:debug': ex=extract_fallback(args.url); rec=merge_pages('college','debug',args.url,[{"url":args.url,"extract":ex,"page_type":"homepage"}],'official'); print(json.dumps({"detected_sections":ex.get('sections',{}),"extracted_fields":ex.get('field_details',{}),"missing_fields":rec['missing_fields'],"final_normalized_record":rec},indent=2))
    elif args.cmd=='pilot:college': print(json.dumps(pilot_college(args.name,args.url,args.dry_run,args.save),indent=2))
    elif args.cmd=='pilot:http-smoke':
        try:
            print(json.dumps(pilot_http_smoke(args.url,args.name),indent=2))
        except Exception as e:
            print(json.dumps({'safe_completed':False,'error':str(e)})); raise
    elif args.cmd=='export:validate': print(json.dumps(export_validate(args.id),indent=2))
    elif args.cmd=='readiness:check': print(json.dumps(readiness_check(),indent=2))
    elif args.cmd=='audit:export': print(json.dumps(audit_export(),indent=2))
    elif args.cmd=='review:list': print(json.dumps(review_list(),indent=2))
    elif args.cmd=='review:show': print(json.dumps(review_show(args.id),indent=2))
    elif args.cmd=='review:approve': print(json.dumps(review_decide(args.id,'approved',args.reviewed_by),indent=2))
    elif args.cmd=='review:reject': print(json.dumps(review_decide(args.id,'rejected',args.reviewed_by,args.notes),indent=2))
    elif args.cmd=='publish:entity': print(json.dumps(publish_entity(args.id,args.idempotency_key),indent=2))
    elif args.cmd=='publish:list': print(json.dumps(publish_list(),indent=2))
    elif args.cmd=='chatbot:sync': print(json.dumps(chatbot_sync(args.entity_id,args.idempotency_key),indent=2))
    elif args.cmd=='record:list': print(json.dumps(record_list(args.state),indent=2))
    elif args.cmd=='record:show': print(json.dumps(record_show(args.id),indent=2))
    elif args.cmd=='record:approve': print(json.dumps(record_approve(args.id,args.reviewed_by,args.force),indent=2))
    elif args.cmd=='record:reject': print(json.dumps(record_reject(args.id,args.reviewed_by,args.notes),indent=2))
    elif args.cmd=='review:seed': print(json.dumps(review_seed(args.entity_id),indent=2))

if __name__=='__main__': main()

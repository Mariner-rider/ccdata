from __future__ import annotations
import argparse, hashlib, json, os, sqlite3, time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from html.parser import HTMLParser

from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback

RAW_RETENTION_DAYS = int(os.getenv("RAW_HTML_RETENTION_DAYS", "7"))
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "./artifacts")
TRUST = {"official":1.0,"government/regulator":0.95,"recognized news":0.75,"aggregator":0.60,"user/review":0.40}
REQUIRED = {
"college":["name","location","official_website","courses","fees","admission_link","placement","faculty","hostel"],
"admission":["institution_name","program","application_start_date","application_deadline","apply_link","eligibility"],
"job":["title","organization","location","deadline","apply_link","eligibility"],
"scholarship":["scholarship_name","provider","eligibility","amount","deadline","apply_link"],
"institute":["name","location","courses","fees","contact","reviews"],
"news":["title","published_date","source","category","summary","url"],
"education_loan":["bank_name","loan_type","interest_rate","eligibility","max_amount","apply_link"],
}
KEYWORDS=["admission","courses","fees","placement","faculty","hostel","scholarship","career","jobs","news"]

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__(); self.links=[]
    def handle_starttag(self, tag, attrs):
        if tag=="a":
            h=dict(attrs).get("href")
            if h: self.links.append(h)

@dataclass
class RuntimeConfig:
    database_url:str; max_pages:int; max_depth:int; rate:float; timeout:float; same_domain:bool

class Repo:
    def __init__(self, db_url:str): self.path=db_url.replace("sqlite:///","")
    def init_db(self):
        with sqlite3.connect(self.path) as c:
            c.execute("""CREATE TABLE IF NOT EXISTS crawler_records(id INTEGER PRIMARY KEY, entity_type TEXT, title TEXT, source_url TEXT, official_url TEXT, summary TEXT, extracted_fields TEXT, missing_fields TEXT, confidence_score REAL, trust_tier TEXT, freshness_status TEXT, content_hash TEXT UNIQUE, last_crawled_at TEXT, updated_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS source_registry(id INTEGER PRIMARY KEY, entity_type TEXT, entity_name TEXT, official_url TEXT, country TEXT, trust_tier TEXT, crawl_frequency_days INTEGER, last_crawled_at TEXT, is_active INTEGER, created_at TEXT, updated_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS crawl_logs(id INTEGER PRIMARY KEY, source_id INTEGER, url TEXT, status TEXT, detail TEXT, event_ts TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS crawl_tasks(id INTEGER PRIMARY KEY, source_id INTEGER, url TEXT, reason TEXT, created_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY, record_id INTEGER, old_content_hash TEXT, new_content_hash TEXT, changed_at TEXT, old_payload TEXT)""")
            c.commit()
    def add_source(self,e):
        now=datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.path) as c:
            c.execute("INSERT INTO source_registry(entity_type,entity_name,official_url,country,trust_tier,crawl_frequency_days,last_crawled_at,is_active,created_at,updated_at) VALUES(?,?,?,?,?,?,NULL,1,?,?)",(e['entity_type'],e['entity_name'],e.get('official_url') or e.get('url'),e.get('country',''),e.get('trust_tier','official'),e.get('crawl_frequency_days',7),now,now)); c.commit()
    def list_sources(self):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,trust_tier,is_active FROM source_registry ORDER BY id").fetchall()
    def get_source(self,id):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,country,trust_tier,crawl_frequency_days,is_active FROM source_registry WHERE id=?",(id,)).fetchone()
    def active_sources(self):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id FROM source_registry WHERE is_active=1").fetchall()
    def upsert_record(self, rec):
        with sqlite3.connect(self.path) as c:
            row=c.execute("SELECT id,content_hash,extracted_fields FROM crawler_records WHERE source_url=? AND entity_type=?",(rec['source_url'],rec['entity_type'])).fetchone()
            now=datetime.now(timezone.utc).isoformat()
            if row:
                if row[1]==rec['content_hash']: return "unchanged"
                c.execute("INSERT INTO audit_log(record_id,old_content_hash,new_content_hash,changed_at,old_payload) VALUES(?,?,?,?,?)",(row[0],row[1],rec['content_hash'],now,row[2]))
                c.execute("UPDATE crawler_records SET title=?,official_url=?,summary=?,extracted_fields=?,missing_fields=?,confidence_score=?,trust_tier=?,freshness_status=?,content_hash=?,last_crawled_at=?,updated_at=? WHERE id=?",(rec['title'],rec['official_url'],rec['summary'],json.dumps(rec['extracted_fields']),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['trust_tier'],rec['freshness_status'],rec['content_hash'],rec['last_crawled_at'],now,row[0]))
                c.commit(); return "updated"
            c.execute("INSERT INTO crawler_records(entity_type,title,source_url,official_url,summary,extracted_fields,missing_fields,confidence_score,trust_tier,freshness_status,content_hash,last_crawled_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(rec['entity_type'],rec['title'],rec['source_url'],rec['official_url'],rec['summary'],json.dumps(rec['extracted_fields']),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['trust_tier'],rec['freshness_status'],rec['content_hash'],rec['last_crawled_at'],now)); c.commit(); return "created"
    def add_task(self,source_id,url,reason):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO crawl_tasks(source_id,url,reason,created_at) VALUES(?,?,?,?)",(source_id,url,reason,datetime.now(timezone.utc).isoformat())); c.commit()
    def log(self,source_id,url,status,detail):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO crawl_logs(source_id,url,status,detail,event_ts) VALUES(?,?,?,?,?)",(source_id,url,status,detail[:2000],datetime.now(timezone.utc).isoformat())); c.commit()


def _load_config():
    return RuntimeConfig(os.getenv("DATABASE_URL","sqlite:///./collegecue_local.db"),int(os.getenv("CRAWL_MAX_PAGES_PER_SOURCE","25")),int(os.getenv("CRAWL_MAX_DEPTH","2")),float(os.getenv("CRAWL_RATE_LIMIT_SECONDS","2")),float(os.getenv("CRAWL_TIMEOUT_SECONDS","15")),os.getenv("CRAWL_SAME_DOMAIN_ONLY","true").lower()=="true")

def _robots_allowed(url):
    if url.startswith("file://"): return True
    p=urlparse(url); rp=robotparser.RobotFileParser(); rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
    try: rp.read()
    except Exception: return True
    return rp.can_fetch("ccdata-lite-bot",url)

def _canonical(url):
    p=urlparse(url); return f"{p.scheme}://{p.netloc}{p.path}" if p.scheme else url

def discover_urls(seed,cfg):
    urls=[seed];
    if seed.startswith("file://"):
        txt=Path((urlparse(seed).netloc+urlparse(seed).path)).read_text(encoding="utf-8")
    else:
        txt=extract_fallback(seed, timeout=cfg.timeout)["meta"].get("raw_html","") if False else ""
    p=LinkParser(); p.feed(txt)
    base=urlparse(seed)
    scored=[]
    for l in p.links:
        u=_canonical(urljoin(seed,l))
        if cfg.same_domain and not u.startswith("file://") and urlparse(u).netloc!=base.netloc: continue
        if any(u.lower().endswith(ext) for ext in [".pdf",".zip",".jpg",".png",".gif"]): continue
        score=sum(1 for k in KEYWORDS if k in u.lower())
        scored.append((score,u))
    scored=sorted(set(scored), reverse=True)
    urls.extend([u for _,u in scored][: cfg.max_pages-1])
    return urls[:cfg.max_pages]

def map_record(entity_type, entity_name, source_url, extracted, trust_tier):
    details=extracted.get('field_details',{})
    def missing_field(f):
        v=extracted.get(f)
        has_detail=f in details
        c=details.get(f,{}).get('confidence',1.0 if not has_detail else 0)
        empty = v in (None,'',[],{})
        return empty or c < 0.6
    missing=[f for f in REQUIRED[entity_type] if missing_field(f)]
    completeness=1-(len(missing)/max(1,len(REQUIRED[entity_type])))
    recency=1.0
    structured=1.0 if extracted.get('field_details') else 0.6
    avg_field_conf = (sum(v.get('confidence',0) for v in details.values())/len(details)) if details else 1.0
    confidence=round((completeness*0.45)+(TRUST.get(trust_tier,0.6)*0.25)+(recency*0.1)+(structured*0.1)+(avg_field_conf*0.1),3)
    now=datetime.now(timezone.utc).isoformat()
    return {"entity_type":entity_type,"title":extracted.get("name") or extracted.get("title") or entity_name,"source_url":source_url,"official_url":source_url,"summary":(extracted.get("meta",{}).get("meta_description") or "")[:300],"extracted_fields":extracted,"missing_fields":missing,"confidence_score":confidence,"trust_tier":trust_tier,"freshness_status":"incomplete" if missing else "fresh","content_hash":hashlib.sha256(json.dumps(extracted,sort_keys=True).encode()).hexdigest(),"last_crawled_at":now}

def crawl_source(source_id:int):
    cfg=_load_config(); repo=Repo(cfg.database_url); repo.init_db(); s=repo.get_source(source_id)
    if not s: raise RuntimeError("source not found")
    sid,etype,ename,url,_,trust,_,_=s
    if not _robots_allowed(url): repo.log(sid,url,"blocked","robots"); return {"status":"blocked"}
    crawled=[]
    for u in discover_urls(url,cfg):
        try:
            extracted=extract_fallback(u, timeout=cfg.timeout)
            rec=map_record("education_loan" if etype=="loan" else etype,ename,u,extracted,trust)
            status=repo.upsert_record(rec)
            if rec['missing_fields']:
                candidates=discover_urls(url,cfg)
                added=False
                for ku in candidates:
                    if any(k in ku.lower() for k in KEYWORDS):
                        repo.add_task(sid,ku,"missing_fields"); added=True
                if not added:
                    repo.add_task(sid,url,"missing_fields")
            repo.log(sid,u,status,rec['freshness_status'])
            crawled.append({"url":u,"status":status,"missing":rec['missing_fields']})
            time.sleep(cfg.rate if u.startswith('http') else 0)
        except Exception as e:
            repo.log(sid,u,"error",str(e))
    return {"source_id":sid,"pages":len(crawled),"results":crawled}

def main():
    p=argparse.ArgumentParser(); sub=p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('init-db')
    sa=sub.add_parser('source:add'); sa.add_argument('--entity-type',required=True); sa.add_argument('--entity-name',required=True); sa.add_argument('--url',required=True); sa.add_argument('--country',default=''); sa.add_argument('--trust-tier',default='official'); sa.add_argument('--crawl-frequency-days',type=int,default=7)
    sub.add_parser('source:list')
    sc=sub.add_parser('source:crawl'); sc.add_argument('--id',type=int,required=True)
    sub.add_parser('source:crawl-active')
    s1=sub.add_parser('crawl:single'); s1.add_argument('--url',required=True)
    s2=sub.add_parser('extract:test'); s2.add_argument('--url',required=True)
    s3=sub.add_parser('extract:debug'); s3.add_argument('--url',required=True)
    args=p.parse_args(); cfg=_load_config(); repo=Repo(cfg.database_url); repo.init_db()
    if args.cmd=='init-db': print('initialized')
    elif args.cmd=='source:add': repo.add_source(vars(args)); print('added')
    elif args.cmd=='source:list': print(json.dumps([{"id":r[0],"entity_type":r[1],"entity_name":r[2],"official_url":r[3],"trust_tier":r[4],"is_active":r[5]} for r in repo.list_sources()],indent=2))
    elif args.cmd=='source:crawl': print(json.dumps(crawl_source(args.id),indent=2))
    elif args.cmd=='source:crawl-active': print(json.dumps([crawl_source(r[0]) for r in repo.active_sources()],indent=2))
    elif args.cmd=='extract:test': print(json.dumps(extract_fallback(args.url),indent=2))
    elif args.cmd=='extract:debug':
        ex=extract_fallback(args.url)
        rec=map_record('college','debug',args.url,ex,'official')
        out={'detected_sections':ex.get('sections',{}),'extracted_fields':ex.get('field_details',{}),'missing_fields':rec['missing_fields'],'final_normalized_record':rec}
        print(json.dumps(out,indent=2))
    elif args.cmd=='crawl:single':
        repo.add_source({'entity_type':'college','entity_name':'single','official_url':args.url,'country':'','trust_tier':'official','crawl_frequency_days':7})
        sid=repo.list_sources()[-1][0]
        print(json.dumps(crawl_source(sid),indent=2))

if __name__=='__main__': main()

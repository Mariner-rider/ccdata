from __future__ import annotations
import argparse, hashlib, json, os, sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib import robotparser
from services.extraction.webclaw_adapter.fallback_extractor import extract_fallback

TRUST={"official":1.0,"government/regulator":0.95,"recognized news":0.75,"aggregator":0.60,"user/review":0.40}
REQ_COLLEGE=["name","location","official_website","courses","fees","admission_link","placement","faculty","hostel"]
KEYWORDS=["admission","courses","fees","placement","faculty","hostel","infrastructure","scholarship","contact","about"]

class LinkParser(HTMLParser):
    def __init__(self): super().__init__(); self.links=[]
    def handle_starttag(self, tag, attrs):
        if tag=="a":
            h=dict(attrs).get("href")
            if h: self.links.append(h)

@dataclass
class Cfg: database_url:str; max_pages:int; timeout:float; same_domain:bool

def _cfg(): return Cfg(os.getenv("DATABASE_URL","sqlite:///./collegecue_local.db"),int(os.getenv("CRAWL_MAX_PAGES_PER_SOURCE","25")),float(os.getenv("CRAWL_TIMEOUT_SECONDS","15")),os.getenv("CRAWL_SAME_DOMAIN_ONLY","true").lower()=="true")

def _canon(u): p=urlparse(u); return f"{p.scheme}://{p.netloc}{p.path}" if p.scheme else u

def _robots(url):
    if url.startswith("file://"): return True
    p=urlparse(url); rp=robotparser.RobotFileParser(); rp.set_url(f"{p.scheme}://{p.netloc}/robots.txt")
    try: rp.read(); return rp.can_fetch("ccdata-lite-bot",url)
    except Exception: return True

def _ptype(u):
    lu=u.lower()
    for t,k in [("admission","admission"),("courses_fees","course"),("courses_fees","fees"),("faculty","faculty"),("placement","placement"),("hostel","hostel"),("gallery","gallery"),("contact","contact")]:
        if k in lu: return t
    if lu.endswith("index.html") or lu.rstrip('/').endswith(('.edu','.ac.in')): return "homepage"
    return "unknown"

class Repo:
    def __init__(self,db): self.path=db.replace("sqlite:///","")
    def init(self):
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TABLE IF NOT EXISTS source_registry(id INTEGER PRIMARY KEY,entity_type TEXT,entity_name TEXT,official_url TEXT,trust_tier TEXT,is_active INTEGER DEFAULT 1)")
            c.execute("CREATE TABLE IF NOT EXISTS crawler_records(id INTEGER PRIMARY KEY,entity_type TEXT,title TEXT,source_url TEXT UNIQUE,official_url TEXT,payload TEXT,missing_fields TEXT,confidence_score REAL,trust_tier TEXT,content_hash TEXT,last_crawled_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS quarantine_records(id INTEGER PRIMARY KEY,source_url TEXT,payload TEXT,reason TEXT,created_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY,entity_id INTEGER,field TEXT,alt_value TEXT,created_at TEXT)")
            c.commit()
    def add_source(self,e):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO source_registry(entity_type,entity_name,official_url,trust_tier,is_active) VALUES(?,?,?,?,1)",(e['entity_type'],e['entity_name'],e.get('url') or e.get('official_url'),e.get('trust_tier','official'))); c.commit()
    def get_source(self,id):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,trust_tier FROM source_registry WHERE id=?",(id,)).fetchone()
    def list_sources(self):
        with sqlite3.connect(self.path) as c: return c.execute("SELECT id,entity_type,entity_name,official_url,trust_tier,is_active FROM source_registry").fetchall()
    def save_entity(self, rec):
        with sqlite3.connect(self.path) as c:
            row=c.execute("SELECT id,payload,content_hash FROM crawler_records WHERE source_url=?",(rec['source_url'],)).fetchone()
            if row and row[2]==rec['content_hash']: return 'unchanged'
            if row:
                old=json.loads(row[1]);
                for k,v in rec['fields'].items():
                    if old['fields'].get(k)!=v: c.execute("INSERT INTO audit_log(entity_id,field,alt_value,created_at) VALUES(?,?,?,?)",(row[0],k,json.dumps(old['fields'].get(k)),datetime.now(timezone.utc).isoformat()))
                c.execute("UPDATE crawler_records SET payload=?,missing_fields=?,confidence_score=?,content_hash=?,last_crawled_at=? WHERE id=?",(json.dumps(rec),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['content_hash'],rec['last_crawled_at'],row[0]))
                c.commit(); return 'updated'
            c.execute("INSERT INTO crawler_records(entity_type,title,source_url,official_url,payload,missing_fields,confidence_score,trust_tier,content_hash,last_crawled_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(rec['entity_type'],rec['title'],rec['source_url'],rec['official_url'],json.dumps(rec),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['trust_tier'],rec['content_hash'],rec['last_crawled_at'])); c.commit(); return 'created'
    def save_quarantine(self,source_url,payload,reason):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO quarantine_records(source_url,payload,reason,created_at) VALUES(?,?,?,?)",(source_url,json.dumps(payload),reason,datetime.now(timezone.utc).isoformat())); c.commit()
    def get_entity(self,id):
        with sqlite3.connect(self.path) as c: r=c.execute("SELECT id,payload FROM crawler_records WHERE id=?",(id,)).fetchone(); return json.loads(r[1]) if r else None

def _clean_list(vals, headings):
    out=[]
    for v in vals or []:
        s=" ".join(str(v).split()).strip()
        if not s or s.lower() in headings: continue
        if s not in out: out.append(s)
    return out

def discover(seed,cfg):
    txt=Path((urlparse(seed).netloc+urlparse(seed).path)).read_text(encoding='utf-8') if seed.startswith('file://') else ''
    p=LinkParser(); p.feed(txt)
    base=urlparse(seed)
    cand=[]
    for h in p.links:
        u=_canon(urljoin(seed,h)); lu=u.lower(); score=sum(2 for k in KEYWORDS if k in lu)
        if cfg.same_domain and not u.startswith('file://') and urlparse(u).netloc!=base.netloc: continue
        cand.append((score,u))
    ded=[]; seen=set([seed])
    for s,u in sorted(cand, reverse=True):
        if u not in seen: ded.append({"url":u,"page_type":_ptype(u),"priority":s,"reason":"keyword_match" if s else "internal_link","robots_allowed":_robots(u)}); seen.add(u)
    return [{"url":seed,"page_type":"homepage","priority":99,"reason":"seed","robots_allowed":_robots(seed)}]+ded[:cfg.max_pages-1]

def merge_pages(entity_type,name,official_url,pages,trust):
    headings={"courses","placement","faculty","hostel","fees","admission","contact","gallery"}
    merged={"name":name,"official_website":official_url,"location":"","courses":[],"fees":[],"admission_link":[],"placement":[],"faculty":[],"hostel":[],"gallery":[],"contact":[]}
    field_src={}
    for page in pages:
        ex=page['extract'];
        for f in ["location","courses","fees","admission_link","placement","faculty","hostel","gallery","contact"]:
            val=ex.get(f)
            conf=ex.get('field_details',{}).get(f,{}).get('confidence',0.5)
            if isinstance(val,list): val=_clean_list(val, headings)
            else: val=" ".join(str(val or '').split()).strip()
            if not val: continue
            existing=field_src.get(f,{"confidence":-1})
            if conf>existing['confidence']:
                merged[f]=val; field_src[f]={"source_url":page['url'],"confidence":conf,"method":ex.get('field_details',{}).get(f,{}).get('extraction_method','heuristic')}
    missing=[f for f in REQ_COLLEGE if not merged.get(f)]
    completeness=1-len(missing)/len(REQ_COLLEGE)
    conf=round(completeness*0.6 + TRUST.get(trust,0.6)*0.25 + 0.15,3)
    rec={"entity_type":entity_type,"title":merged.get('name') or name,"source_url":official_url,"official_url":official_url,"info":{"name":merged.get('name'),"location":merged.get('location'),"contact":merged.get('contact')},"courses_and_fees":{"courses":merged.get('courses'),"fees":merged.get('fees'),"admission_link":merged.get('admission_link')},"gallery":merged.get('gallery'),"faculty":merged.get('faculty'),"hostel":merged.get('hostel'),"placement":merged.get('placement'),"reviews":[],"metadata":{"field_sources":field_src,"page_count":len(pages)},"fields":merged,"missing_fields":missing,"confidence_score":conf,"trust_tier":trust,"content_hash":hashlib.sha256(json.dumps(merged,sort_keys=True).encode()).hexdigest(),"last_crawled_at":datetime.now(timezone.utc).isoformat()}
    return rec

def crawl_source(id,dry=False):
    cfg=_cfg(); repo=Repo(cfg.database_url); repo.init(); s=repo.get_source(id)
    sid,etype,name,url,trust=s
    pv=discover(url,cfg); pages=[]
    for p in pv:
        if not p['robots_allowed']: continue
        ex=extract_fallback(p['url'], timeout=cfg.timeout); pages.append({"url":p['url'],"extract":ex,"page_type":p['page_type']})
    rec=merge_pages(etype,name,url,pages,trust)
    if dry: return {"dry_run":True,"pages":len(pages),"record":rec}
    valid=rec['confidence_score']>=0.65 and (1-len(rec['missing_fields'])/len(REQ_COLLEGE))>=0.7 and rec['trust_tier'] in TRUST and rec['content_hash']
    if valid: st=repo.save_entity(rec)
    else: repo.save_quarantine(url,rec,"quality_gate_failed"); st='quarantined'
    return {"source_id":sid,"status":st,"pages":len(pages),"missing":rec['missing_fields']}

def export_entity(eid, fmt='json'):
    repo=Repo(_cfg().database_url); repo.init(); rec=repo.get_entity(eid)
    return rec

def main():
    pa=argparse.ArgumentParser(); sub=pa.add_subparsers(dest='cmd',required=True)
    sub.add_parser('init-db')
    a=sub.add_parser('source:add'); a.add_argument('--entity-type',required=True); a.add_argument('--entity-name',required=True); a.add_argument('--url',required=True); a.add_argument('--trust-tier',default='official')
    sub.add_parser('source:list')
    p=sub.add_parser('source:preview'); p.add_argument('--id',type=int,required=True)
    c=sub.add_parser('source:crawl'); c.add_argument('--id',type=int,required=True); c.add_argument('--dry-run',action='store_true')
    e=sub.add_parser('export:entity'); e.add_argument('--id',type=int,required=True); e.add_argument('--format',default='json')
    t=sub.add_parser('extract:test'); t.add_argument('--url',required=True)
    d=sub.add_parser('extract:debug'); d.add_argument('--url',required=True)
    args=pa.parse_args(); repo=Repo(_cfg().database_url); repo.init()
    if args.cmd=='init-db': print('initialized')
    elif args.cmd=='source:add': repo.add_source(vars(args)); print('added')
    elif args.cmd=='source:list': print(json.dumps([{"id":r[0],"entity_type":r[1],"entity_name":r[2],"official_url":r[3],"trust_tier":r[4],"is_active":r[5]} for r in repo.list_sources()],indent=2))
    elif args.cmd=='source:preview': s=repo.get_source(args.id); print(json.dumps({"source_id":args.id,"estimated_page_count":len(discover(s[3],_cfg())),"urls":discover(s[3],_cfg())},indent=2))
    elif args.cmd=='source:crawl': print(json.dumps(crawl_source(args.id,args.dry_run),indent=2))
    elif args.cmd=='export:entity': print(json.dumps(export_entity(args.id,args.format),indent=2))
    elif args.cmd=='extract:test': print(json.dumps(extract_fallback(args.url),indent=2))
    elif args.cmd=='extract:debug': ex=extract_fallback(args.url); rec=merge_pages('college','debug',args.url,[{"url":args.url,"extract":ex,"page_type":"homepage"}],'official'); print(json.dumps({"detected_sections":ex.get('sections',{}),"extracted_fields":ex.get('field_details',{}),"missing_fields":rec['missing_fields'],"final_normalized_record":rec},indent=2))

if __name__=='__main__': main()

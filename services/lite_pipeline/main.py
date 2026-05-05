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
            c.execute("CREATE TABLE IF NOT EXISTS source_registry(id INTEGER PRIMARY KEY,entity_type TEXT,entity_name TEXT,official_url TEXT,trust_tier TEXT,is_active INTEGER DEFAULT 1)")
            c.execute("CREATE TABLE IF NOT EXISTS crawler_records(id INTEGER PRIMARY KEY,entity_type TEXT,title TEXT,source_url TEXT UNIQUE,official_url TEXT,payload TEXT,missing_fields TEXT,confidence_score REAL,trust_tier TEXT,content_hash TEXT,last_crawled_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS quarantine_records(id INTEGER PRIMARY KEY,source_url TEXT,payload TEXT,reason TEXT,created_at TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS crawl_logs(id INTEGER PRIMARY KEY,source_url TEXT,status TEXT,detail TEXT,event_ts TEXT)")
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
            c.execute("INSERT INTO crawler_records(entity_type,title,source_url,official_url,payload,missing_fields,confidence_score,trust_tier,content_hash,last_crawled_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(rec['entity_type'],rec['title'],rec['source_url'],rec['official_url'],json.dumps(rec),json.dumps(rec['missing_fields']),rec['confidence_score'],rec['trust_tier'],rec['content_hash'],rec['last_crawled_at'])); c.commit(); return 'created'
    def save_quarantine(self,source_url,payload,reason):
        with sqlite3.connect(self.path) as c: c.execute("INSERT INTO quarantine_records(source_url,payload,reason,created_at) VALUES(?,?,?,?)",(source_url,json.dumps(payload),reason,datetime.now(timezone.utc).isoformat())); c.commit()
    def get_entity(self,id):
        with sqlite3.connect(self.path) as c: r=c.execute("SELECT payload FROM crawler_records WHERE id=?",(id,)).fetchone(); return json.loads(r[0]) if r else None

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
    rec=merge_pages(etype,name,url,pages,trust)
    valid=rec['confidence_score']>=0.65 and (1-len(rec['missing_fields'])/len(REQ_COLLEGE))>=0.7 and rec['trust_tier'] in TRUST and rec['content_hash']
    qr=quality_report(plan,pages,rec,'pass' if valid else 'fail','quality_gate_failed' if not valid else '')
    if dry: return {"dry_run":True,"quality_report":qr,"record":rec}
    st=repo.save_entity(rec) if valid else (repo.save_quarantine(url,rec,'quality_gate_failed') or 'quarantined')
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
    pc=sub.add_parser('pilot:college'); pc.add_argument('--name',required=True); pc.add_argument('--url',required=True); pc.add_argument('--dry-run',action='store_true'); pc.add_argument('--save',action='store_true')
    ph=sub.add_parser('pilot:http-smoke'); ph.add_argument('--url',required=True); ph.add_argument('--name',required=True)
    ev=sub.add_parser('export:validate'); ev.add_argument('--id',type=int,required=True)
    sub.add_parser('readiness:check')
    ae=sub.add_parser('audit:export'); ae.add_argument('--format',default='json')
    args=pa.parse_args(); repo=Repo(_cfg().database_url); repo.init()
    if args.cmd=='init-db': print('initialized')
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

if __name__=='__main__': main()

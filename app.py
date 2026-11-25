"""UBERVZLA demo. Synthetic data only; single instance SQLite."""
import os, sqlite3, time, secrets, hashlib, hmac, math, threading, json
from datetime import date, datetime, timezone
from contextlib import contextmanager, asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from pydantic import BaseModel, Field

BASE=Path(__file__).parent
DB=os.getenv('RUTIVA_DB',str(BASE/'rutiva.db'))
TTL=int(os.getenv('OFFER_SECONDS','25'))
ACTIVE=('accepted','arrived','in_progress')
GPS_TTL=45
GPS_RADIUS_KM=30
SERVICES={'car':'car','moto':'moto','delivery':'any','parcel':'any'}
PRICES={'car':4.0,'moto':2.5,'delivery':3.5,'parcel':3.0}
POINTS={'El Viñedo':(10.2115,-68.0068),'La Trigaleña':(10.2222,-67.9903),'Prebo':(10.2128,-68.0230),'San José':(10.1984,-68.0071),'Guaparo':(10.2372,-68.0105)}

def connect():
 c=sqlite3.connect(DB,timeout=15); c.row_factory=sqlite3.Row; c.execute('PRAGMA foreign_keys=ON'); return c
@contextmanager
def tx():
 c=connect()
 try:
  c.execute('BEGIN IMMEDIATE'); yield c; c.commit()
 except: c.rollback(); raise
 finally: c.close()
def init():
 Path(DB).parent.mkdir(parents=True,exist_ok=True)
 with connect() as c:
  c.executescript('''PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT, login TEXT UNIQUE, pw TEXT, role TEXT, vehicle TEXT, zone TEXT, online INTEGER DEFAULT 0, last_seen REAL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY, uid INTEGER REFERENCES users(id), expiry REAL);
 CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY, customer INTEGER REFERENCES users(id), service TEXT, pickup TEXT, destination TEXT, price REAL, status TEXT, driver INTEGER REFERENCES users(id), deadline REAL, created REAL, updated REAL);
 CREATE TABLE IF NOT EXISTS attempts(order_id INTEGER REFERENCES orders(id), driver INTEGER REFERENCES users(id), result TEXT, PRIMARY KEY(order_id,driver));
 CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id), event TEXT, ts REAL);
 CREATE TABLE IF NOT EXISTS shops(uid INTEGER PRIMARY KEY REFERENCES users(id), category TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY, merchant INTEGER REFERENCES shops(uid), name TEXT NOT NULL, price_cents INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS shop_orders(order_id INTEGER PRIMARY KEY REFERENCES orders(id), merchant INTEGER REFERENCES shops(uid), product_name TEXT, product_cents INTEGER);
 CREATE TABLE IF NOT EXISTS driver_reviews(uid INTEGER PRIMARY KEY REFERENCES users(id), status TEXT NOT NULL DEFAULT 'pending', submitted INTEGER NOT NULL DEFAULT 0, checks TEXT NOT NULL DEFAULT '{}', expires REAL NOT NULL DEFAULT 0, updated REAL NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS review_audit(id INTEGER PRIMARY KEY, uid INTEGER REFERENCES users(id), decision TEXT NOT NULL, checks TEXT NOT NULL, ts REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS demo_face_challenges(token TEXT PRIMARY KEY, uid INTEGER REFERENCES users(id), purpose TEXT, expiry REAL, binding TEXT, cents INTEGER DEFAULT 0);
 CREATE TABLE IF NOT EXISTS demo_face_sessions(token TEXT PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS demo_withdrawals(id INTEGER PRIMARY KEY, uid INTEGER REFERENCES users(id), cents INTEGER, ts REAL);
 CREATE INDEX IF NOT EXISTS orders_status ON orders(status,created);
 CREATE TABLE IF NOT EXISTS driver_gps(uid INTEGER PRIMARY KEY REFERENCES users(id), lat REAL, lon REAL, accuracy REAL, seen REAL);
 CREATE TABLE IF NOT EXISTS order_gps(oid INTEGER PRIMARY KEY REFERENCES orders(id), plat REAL, plon REAL, dlat REAL, dlon REAL);

 ''')
  for table in ('users','orders'):
   if 'gps_mode' not in {r['name'] for r in c.execute(f'PRAGMA table_info({table})')}:
    c.execute(f"ALTER TABLE {table} ADD COLUMN gps_mode INTEGER NOT NULL DEFAULT 0")
   if 'gps_room' not in {r['name'] for r in c.execute(f'PRAGMA table_info({table})')}:
    c.execute(f"ALTER TABLE {table} ADD COLUMN gps_room TEXT NOT NULL DEFAULT ''")
  c.execute("INSERT OR IGNORE INTO driver_reviews(uid) SELECT id FROM users WHERE role='driver'")
  c.execute("UPDATE users SET online=0 WHERE role='driver' AND id NOT IN (SELECT uid FROM driver_reviews WHERE status='approved' AND expires>?)",(time.time(),))
CHECKS={'identity':'Cédula de identidad','selfie':'Selfie y prueba de vida','phone':'Teléfono +58','email':'Correo electrónico','license':'Licencia de conducir','medical':'Certificado médico vial','title':'Título o certificado del vehículo','circulation':'Carnet de circulación','insurance':'Seguro RCV','background':'Antecedentes penales','vehicle':'Inspección del carro o moto'}
EXPIRING=('license','medical','insurance')
def qualified(c,uid):
 return bool(c.execute("SELECT 1 FROM driver_reviews WHERE uid=? AND status='approved' AND expires>?",(uid,time.time())).fetchone())
def review_info(c,uid):
 row=c.execute('SELECT * FROM driver_reviews WHERE uid=?',(uid,)).fetchone()
 if not row: return {'status':'pending','submitted':False,'checks':{},'eligible':False}
 status='expired' if row['status']=='approved' and row['expires']<=time.time() else row['status']
 return {'status':status,'submitted':bool(row['submitted']),'checks':json.loads(row['checks']),'eligible':qualified(c,uid),'expires':row['expires']}
def event(c,i,e): c.execute('INSERT INTO events(order_id,event,ts) VALUES(?,?,?)',(i,e,time.time()))
def dist(a,b):
 la,lo=POINTS[a]; lb,ob=POINTS[b]; return math.hypot((la-lb)*111,(lo-ob)*109)
def km(a,b):
 la,lo=map(math.radians,a); lb,ob=map(math.radians,b)
 h=math.sin((lb-la)/2)**2+math.cos(la)*math.cos(lb)*math.sin((ob-lo)/2)**2
 return 6371*2*math.asin(math.sqrt(min(1,max(0,h))))
def tick(c):
 now=time.time()
 c.execute("UPDATE users SET online=0 WHERE role='driver' AND id NOT IN (SELECT uid FROM driver_reviews WHERE status='approved' AND expires>?)",(now,))
 c.execute("DELETE FROM driver_gps WHERE uid IN (SELECT id FROM users WHERE online=0) AND uid NOT IN (SELECT driver FROM orders WHERE status IN ('accepted','arrived','in_progress') AND driver IS NOT NULL)")
 c.execute('DELETE FROM driver_gps WHERE seen<?',(now-GPS_TTL,))
 c.execute("DELETE FROM order_gps WHERE oid IN (SELECT id FROM orders WHERE status IN ('completed','cancelled'))")
 # Expire old GPS orders if never accepted. Do not cancel active rides automatically.
 c.execute("UPDATE orders SET status='cancelled',driver=NULL,deadline=NULL,updated=? WHERE gps_mode=1 AND status IN ('pending','offered') AND created<?",(now,now-1800))
 c.execute("DELETE FROM order_gps WHERE oid IN (SELECT id FROM orders WHERE status='cancelled' OR (gps_mode=1 AND created<?))",(now-14400,))
 for o in c.execute("SELECT * FROM orders WHERE status='offered' AND (deadline<=? OR driver IN (SELECT id FROM users WHERE online=0 OR last_seen<?) OR (gps_mode=1 AND driver NOT IN (SELECT uid FROM driver_gps)))",(now,now-60)).fetchall():
  c.execute("UPDATE attempts SET result='expired' WHERE order_id=? AND driver=?",(o['id'],o['driver']))
  c.execute("UPDATE orders SET status='pending',driver=NULL,deadline=NULL,updated=? WHERE id=?",(now,o['id'])); event(c,o['id'],'offer_expired')
 for o in c.execute("SELECT * FROM orders WHERE status='pending' ORDER BY created,id").fetchall():
  eligible=c.execute("""SELECT u.* FROM users u WHERE role='driver' AND online=1 AND EXISTS(SELECT 1 FROM driver_reviews v WHERE v.uid=u.id AND v.status='approved' AND v.expires>?) AND last_seen>=? AND (vehicle=? OR ?='any') AND u.gps_mode=? AND (u.gps_mode=0 OR u.gps_room=?) AND NOT EXISTS(SELECT 1 FROM orders x WHERE x.driver=u.id AND x.status IN ('offered','accepted','arrived','in_progress')) AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.order_id=? AND a.driver=u.id)""",(now,now-60,SERVICES[o['service']],SERVICES[o['service']],o['gps_mode'],o['gps_room'],o['id'])).fetchall()
  if not eligible: continue
  if o['gps_mode']:
   g=c.execute('SELECT * FROM order_gps WHERE oid=?',(o['id'],)).fetchone()
   if not g: continue
   candidates=[]
   for u in eligible:
    fix=c.execute('SELECT * FROM driver_gps WHERE uid=? AND seen>=?',(u['id'],now-GPS_TTL)).fetchone()
    if fix:
     distance=km((fix['lat'],fix['lon']),(g['plat'],g['plon']))
     if distance<=GPS_RADIUS_KM: candidates.append((distance,u['id'],u))
   if not candidates: continue
   d=min(candidates,key=lambda x:(x[0],x[1]))[2]
  else: d=min(eligible,key=lambda u:(dist(u['zone'],o['pickup']),u['id']))
  c.execute("INSERT INTO attempts VALUES(?,?,'offered')",(o['id'],d['id']))
  c.execute("UPDATE orders SET status='offered',driver=?,deadline=?,updated=? WHERE id=?",(d['id'],now+TTL,now,o['id'])); event(c,o['id'],'offered')
def worker(stop):
 while not stop.wait(2):
  try:
   with tx() as c: tick(c)
  except Exception: import traceback; traceback.print_exc()
@asynccontextmanager
async def lifespan(app):
 init(); stop=threading.Event(); t=threading.Thread(target=worker,args=(stop,),daemon=True); t.start()
 yield
 stop.set(); t.join(timeout=3)
app=FastAPI(title='UBERVZLA MVP',lifespan=lifespan,docs_url=None,redoc_url=None)

def hashpw(p,s=None):
 s=s or secrets.token_hex(16); return s+':'+hashlib.pbkdf2_hmac('sha256',p.encode(),bytes.fromhex(s),210000).hex()
ROLES=('customer','driver','merchant')
def portal(r):
 role=r.headers.get('X-Portal','')
 if role not in ROLES: raise HTTPException(400,'Acceso de plataforma inválido')
 return role
def cookie_name(r): return 'ubervzla_'+portal(r)
def user(c,r):
 token=r.cookies.get(cookie_name(r),''); u=c.execute('SELECT u.* FROM users u JOIN sessions s ON s.uid=u.id WHERE s.token=? AND s.expiry>?',(hashlib.sha256(token.encode()).hexdigest(),time.time())).fetchone()
 if not u or u['role']!=portal(r): raise HTTPException(401,'Inicia sesión')
 if u['role']=='driver' and not c.execute('SELECT 1 FROM demo_face_sessions WHERE token=?',(hashlib.sha256(token.encode()).hexdigest(),)).fetchone(): raise HTTPException(401,'Reingresa y completa el paso facial demo')
 return u
def public(u): return {k:u[k] for k in ('id','name','login','role','vehicle','zone','online','gps_mode')}
def session(c,u):
 token=secrets.token_urlsafe(32); c.execute('DELETE FROM sessions WHERE expiry<?',(time.time(),)); c.execute('INSERT INTO sessions VALUES(?,?,?)',(hashlib.sha256(token.encode()).hexdigest(),u['id'],time.time()+86400))
 if u['role']=='driver': c.execute('INSERT INTO demo_face_sessions VALUES(?)',(hashlib.sha256(token.encode()).hexdigest(),))
 res=JSONResponse(public(u)); res.set_cookie('ubervzla_'+u['role'],token,max_age=86400,httponly=True,samesite='strict',secure=os.getenv('RENDER')=='true' or os.getenv('SECURE_COOKIES')=='1'); return res
class Register(BaseModel):
 name:str=Field(min_length=2,max_length=40); login:str=Field(min_length=3,max_length=32,pattern=r'^[a-zA-Z0-9_-]+$'); password:str=Field(min_length=8,max_length=100); role:str; vehicle:str=''; zone:str='El Viñedo'; category:str=''
class Login(BaseModel):
 login:str=Field(max_length=32); password:str=Field(max_length=100)
class Availability(BaseModel):
 online:bool; zone:str; gps_mode:bool=False; room:str=Field(default="",max_length=64)
class Fix(BaseModel):
 lat:float=Field(ge=-90,le=90,allow_inf_nan=False); lon:float=Field(ge=-180,le=180,allow_inf_nan=False); accuracy:float=Field(ge=0,le=1000,allow_inf_nan=False)
class Order(BaseModel):
 service:str; pickup:str; destination:str; pickup_gps:Fix|None=None; destination_gps:Fix|None=None; room:str=Field(default="",max_length=64)
class Action(BaseModel):
 action:str
@app.get('/')
def home():
 return HTMLResponse('<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>UBERVZLA | Venezuela en movimiento</title><style>*{box-sizing:border-box}body{margin:0;font:15px system-ui,sans-serif;color:#111827;background:#f6f7fb}header{display:flex;align-items:center;justify-content:space-between;gap:14px;max-width:1200px;margin:auto;padding:19px 24px}header strong{font-size:26px;font-style:italic;letter-spacing:-1.1px;white-space:nowrap}.mark{display:inline-block;width:30px;height:24px;background:linear-gradient(0deg,#ef343e 0 30%,#275aff 30% 60%,#ffdb32 60%);clip-path:polygon(0 30%,90% 0,100% 32%,61% 46%,95% 46%,85% 73%,40% 83%,67% 83%,55% 100%,0 75%,48% 59%,3% 61%,49% 44%);vertical-align:middle;margin-right:5px}nav{display:flex;gap:24px}a{color:inherit;text-decoration:none}nav a{font-size:14px;font-weight:650}.cta{display:inline-flex;align-items:center;justify-content:center;background:#5236e9;color:white;border-radius:10px;padding:12px 19px;font-weight:750}.cta.secondary{border:1px solid #c5c9d5;color:#241d57;background:white}main{max-width:1200px;margin:auto;padding:0 24px}.hero{position:relative;overflow:hidden;background:#101827;color:white;border-radius:22px;display:grid;grid-template-columns:1.03fr .97fr;align-items:center;min-height:410px}.copy{padding:42px;position:relative;z-index:1}.eyebrow{color:#c3b8ff;font-size:11px;font-weight:800;letter-spacing:1.6px;text-transform:uppercase}.hero h1{font-size:clamp(34px,4.9vw,60px);letter-spacing:-2.2px;line-height:1.04;margin:16px 0}.hero p{font-size:16px;line-height:1.55;color:#d8dfed;max-width:420px}.hero .cta{margin-top:12px}.heroart{width:100%;height:100%;min-height:260px;object-fit:cover}.services{display:flex;gap:18px;margin-top:28px;font-weight:700;color:#dbe2f1;font-size:12px}.services span{display:grid;gap:6px}.services svg{width:28px;height:28px}.sectionhead{display:flex;justify-content:space-between;align-items:end;margin:28px 0 12px}.sectionhead h2{margin:0;font-size:24px;letter-spacing:-.7px}.sectionhead small{color:#647084}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:14px}.card{padding:21px;background:white;border:1px solid #e5e8ee;border-radius:17px;box-shadow:0 5px 15px #15213b08}.card .icon{display:grid;place-items:center;width:48px;height:48px;border-radius:13px;background:#f0edff;color:#4930e8}.card .icon svg{width:28px;height:28px}.card:nth-child(2) .icon{background:#e0fae8;color:#078b36}.card:nth-child(3) .icon{background:#fff1db;color:#94600a}.card h3{font-size:21px;letter-spacing:-.4px;margin:12px 0 7px}.card p{color:#647084;line-height:1.5;min-height:45px}.links{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.links a{font-size:13px;font-weight:750;color:#4930e8}.links .cta{color:white}.demo{border:1px solid #e3ddfa;background:#f0edff;color:#51487e;border-radius:12px;padding:12px;font-size:12px;margin:20px 0}footer{padding:14px 20px 24px;text-align:center;font-size:12px;color:#647084}@media(max-width:760px){header{padding:15px 13px}header strong{font-size:21px}header nav{display:none}header .cta{font-size:12px;padding:10px 12px}main{padding:0 11px}.hero{grid-template-columns:1fr;min-height:0;border-radius:18px}.copy{padding:24px 20px 10px}.hero h1{font-size:36px;letter-spacing:-1.5px}.hero p{font-size:14px}.heroart{max-height:245px;min-height:0}.services{margin-top:17px}.cards{grid-template-columns:1fr;gap:10px}.sectionhead{margin:20px 0 10px}.sectionhead h2{font-size:21px}.sectionhead small{display:none}.card{padding:16px}.card p{min-height:0}.card h3{font-size:19px}} </style></head><body><header><strong><i class="mark"></i>UBERVZLA</strong><nav><a href="#accesos">Servicios</a><a href="/driver">Conductores</a><a href="/comercio">Comercios</a></nav><a class="cta" href="#accesos">Iniciar sesión</a></header><main><section class="hero"><div class="copy"><span class="eyebrow">Venezuela en movimiento</span><h1>Transporte, compras y entregas en Venezuela.</h1><p>Una sola plataforma para moverte, comprar y recibir lo que necesitas.</p><a class="cta" href="/cliente?registro=1">Comenzar ahora →</a><div class="services"><span>◉ Viajes</span><span>▣ Compras</span><span>◇ Entregas</span></div></div><svg class="heroart" viewBox="0 0 500 370" role="img" aria-label="Ilustración de ciudad venezolana y vehículo de movilidad"><defs><linearGradient id="sky" x1="0" y1="0" x2="0" y2="1"><stop stop-color="#395587"/><stop offset="1" stop-color="#a8bddb"/></linearGradient><linearGradient id="car" x1="0" y1="0" x2="0" y2="1"><stop stop-color="#fff"/><stop offset="1" stop-color="#aeb9c7"/></linearGradient></defs><rect width="500" height="370" fill="url(#sky)"/><circle cx="415" cy="72" r="34" fill="#f4df9c" opacity=".85"/><path d="M0 158 68 98 125 150 206 46 270 105 308 68 415 155 500 114V285H0Z" fill="#476a6e"/><path d="M0 200 80 137 160 185 218 126 297 190 385 121 500 200V290H0Z" fill="#315453"/><g fill="#c6b5a6"><path d="M22 191h39v98H22zM80 161h47v128H80zM152 205h44v84h-44zM210 178h42v111h-42zM280 150h52v139h-52zM357 193h46v96h-46zM430 174h48v115h-48z"/></g><g fill="#5e7283" opacity=".8"><rect x="28" y="199" width="7" height="10" rx="1"/><rect x="28" y="217" width="7" height="10" rx="1"/><rect x="28" y="235" width="7" height="10" rx="1"/><rect x="28" y="253" width="7" height="10" rx="1"/><rect x="28" y="271" width="7" height="10" rx="1"/><rect x="40" y="199" width="7" height="10" rx="1"/><rect x="40" y="217" width="7" height="10" rx="1"/><rect x="40" y="235" width="7" height="10" rx="1"/><rect x="40" y="253" width="7" height="10" rx="1"/><rect x="40" y="271" width="7" height="10" rx="1"/><rect x="52" y="199" width="7" height="10" rx="1"/><rect x="52" y="217" width="7" height="10" rx="1"/><rect x="52" y="235" width="7" height="10" rx="1"/><rect x="52" y="253" width="7" height="10" rx="1"/><rect x="52" y="271" width="7" height="10" rx="1"/><rect x="86" y="172" width="7" height="10" rx="1"/><rect x="86" y="190" width="7" height="10" rx="1"/><rect x="86" y="208" width="7" height="10" rx="1"/><rect x="86" y="226" width="7" height="10" rx="1"/><rect x="86" y="244" width="7" height="10" rx="1"/><rect x="86" y="262" width="7" height="10" rx="1"/><rect x="86" y="280" width="7" height="10" rx="1"/><rect x="98" y="172" width="7" height="10" rx="1"/><rect x="98" y="190" width="7" height="10" rx="1"/><rect x="98" y="208" width="7" height="10" rx="1"/><rect x="98" y="226" width="7" height="10" rx="1"/><rect x="98" y="244" width="7" height="10" rx="1"/><rect x="98" y="262" width="7" height="10" rx="1"/><rect x="98" y="280" width="7" height="10" rx="1"/><rect x="110" y="172" width="7" height="10" rx="1"/><rect x="110" y="190" width="7" height="10" rx="1"/><rect x="110" y="208" width="7" height="10" rx="1"/><rect x="110" y="226" width="7" height="10" rx="1"/><rect x="110" y="244" width="7" height="10" rx="1"/><rect x="110" y="262" width="7" height="10" rx="1"/><rect x="110" y="280" width="7" height="10" rx="1"/><rect x="158" y="214" width="7" height="10" rx="1"/><rect x="158" y="232" width="7" height="10" rx="1"/><rect x="158" y="250" width="7" height="10" rx="1"/><rect x="158" y="268" width="7" height="10" rx="1"/><rect x="170" y="214" width="7" height="10" rx="1"/><rect x="170" y="232" width="7" height="10" rx="1"/><rect x="170" y="250" width="7" height="10" rx="1"/><rect x="170" y="268" width="7" height="10" rx="1"/><rect x="182" y="214" width="7" height="10" rx="1"/><rect x="182" y="232" width="7" height="10" rx="1"/><rect x="182" y="250" width="7" height="10" rx="1"/><rect x="182" y="268" width="7" height="10" rx="1"/><rect x="216" y="187" width="7" height="10" rx="1"/><rect x="216" y="205" width="7" height="10" rx="1"/><rect x="216" y="223" width="7" height="10" rx="1"/><rect x="216" y="241" width="7" height="10" rx="1"/><rect x="216" y="259" width="7" height="10" rx="1"/><rect x="216" y="277" width="7" height="10" rx="1"/><rect x="228" y="187" width="7" height="10" rx="1"/><rect x="228" y="205" width="7" height="10" rx="1"/><rect x="228" y="223" width="7" height="10" rx="1"/><rect x="228" y="241" width="7" height="10" rx="1"/><rect x="228" y="259" width="7" height="10" rx="1"/><rect x="228" y="277" width="7" height="10" rx="1"/><rect x="240" y="187" width="7" height="10" rx="1"/><rect x="240" y="205" width="7" height="10" rx="1"/><rect x="240" y="223" width="7" height="10" rx="1"/><rect x="240" y="241" width="7" height="10" rx="1"/><rect x="240" y="259" width="7" height="10" rx="1"/><rect x="240" y="277" width="7" height="10" rx="1"/><rect x="286" y="162" width="7" height="10" rx="1"/><rect x="286" y="180" width="7" height="10" rx="1"/><rect x="286" y="198" width="7" height="10" rx="1"/><rect x="286" y="216" width="7" height="10" rx="1"/><rect x="286" y="234" width="7" height="10" rx="1"/><rect x="286" y="252" width="7" height="10" rx="1"/><rect x="286" y="270" width="7" height="10" rx="1"/><rect x="298" y="162" width="7" height="10" rx="1"/><rect x="298" y="180" width="7" height="10" rx="1"/><rect x="298" y="198" width="7" height="10" rx="1"/><rect x="298" y="216" width="7" height="10" rx="1"/><rect x="298" y="234" width="7" height="10" rx="1"/><rect x="298" y="252" width="7" height="10" rx="1"/><rect x="298" y="270" width="7" height="10" rx="1"/><rect x="310" y="162" width="7" height="10" rx="1"/><rect x="310" y="180" width="7" height="10" rx="1"/><rect x="310" y="198" width="7" height="10" rx="1"/><rect x="310" y="216" width="7" height="10" rx="1"/><rect x="310" y="234" width="7" height="10" rx="1"/><rect x="310" y="252" width="7" height="10" rx="1"/><rect x="310" y="270" width="7" height="10" rx="1"/><rect x="363" y="202" width="7" height="10" rx="1"/><rect x="363" y="220" width="7" height="10" rx="1"/><rect x="363" y="238" width="7" height="10" rx="1"/><rect x="363" y="256" width="7" height="10" rx="1"/><rect x="363" y="274" width="7" height="10" rx="1"/><rect x="375" y="202" width="7" height="10" rx="1"/><rect x="375" y="220" width="7" height="10" rx="1"/><rect x="375" y="238" width="7" height="10" rx="1"/><rect x="375" y="256" width="7" height="10" rx="1"/><rect x="375" y="274" width="7" height="10" rx="1"/><rect x="387" y="202" width="7" height="10" rx="1"/><rect x="387" y="220" width="7" height="10" rx="1"/><rect x="387" y="238" width="7" height="10" rx="1"/><rect x="387" y="256" width="7" height="10" rx="1"/><rect x="387" y="274" width="7" height="10" rx="1"/><rect x="436" y="185" width="7" height="10" rx="1"/><rect x="436" y="203" width="7" height="10" rx="1"/><rect x="436" y="221" width="7" height="10" rx="1"/><rect x="436" y="239" width="7" height="10" rx="1"/><rect x="436" y="257" width="7" height="10" rx="1"/><rect x="436" y="275" width="7" height="10" rx="1"/><rect x="448" y="185" width="7" height="10" rx="1"/><rect x="448" y="203" width="7" height="10" rx="1"/><rect x="448" y="221" width="7" height="10" rx="1"/><rect x="448" y="239" width="7" height="10" rx="1"/><rect x="448" y="257" width="7" height="10" rx="1"/><rect x="448" y="275" width="7" height="10" rx="1"/><rect x="460" y="185" width="7" height="10" rx="1"/><rect x="460" y="203" width="7" height="10" rx="1"/><rect x="460" y="221" width="7" height="10" rx="1"/><rect x="460" y="239" width="7" height="10" rx="1"/><rect x="460" y="257" width="7" height="10" rx="1"/><rect x="460" y="275" width="7" height="10" rx="1"/></g><path d="M0 287H500V370H0Z" fill="#343b49"/><path d="M0 346H500" stroke="#fff" stroke-width="3" stroke-dasharray="27 24" opacity=".6"/><ellipse cx="303" cy="337" rx="166" ry="17" fill="#111827" opacity=".4"/><g><path d="M127 293 169 280 208 236Q222 222 250 222h104q33 0 54 21l36 37 27 11q12 8 12 29v10H119v-21q0-11 8-16Z" fill="url(#car)" stroke="#8995a5" stroke-width="2"/><path d="M185 279 217 241q8-9 28-9h55v47Z M311 232h39q29 0 46 17l29 30H311Z" fill="#233347"/><path d="M122 307h359" stroke="#8995a5" stroke-width="2"/><path d="M151 325h288" stroke="#2e61f5" stroke-width="7"/><path d="M151 319h288" stroke="#ffd633" stroke-width="5"/><path d="M151 331h288" stroke="#ea3643" stroke-width="4"/><circle cx="188" cy="329" r="26" fill="#17202b"/><circle cx="188" cy="329" r="13" fill="#aab6c6"/><circle cx="423" cy="329" r="26" fill="#17202b"/><circle cx="423" cy="329" r="13" fill="#aab6c6"/><text x="267" y="302" font-size="16" font-weight="900" font-style="italic" font-family="system-ui" fill="#17243a">UBERVZLA</text></g></svg></section><div class="sectionhead" id="accesos"><h2>Elige tu plataforma</h2><small>Tres accesos independientes</small></div><section class="cards"><article class="card"><div class="icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m5 11 2-6h10l2 6M3 11h18v7H3zM5 18v3m14-3v3M7 14h1m8 0h1"/></svg></div><h3>Soy cliente</h3><p>Pide viajes, compra en comercios y envía paquetes.</p><div class="links"><a class="cta" href="/cliente?registro=1">Registrarme</a><a href="/cliente">Entrar →</a></div></article><article class="card"><div class="icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 12v9m-6-9 6 5 6-5"/></svg></div><h3>Quiero conducir</h3><p>Registra tu carro o moto y recibe ofertas de trabajo.</p><div class="links"><a class="cta" href="/driver?registro=1">Registrarme</a><a href="/driver">Entrar →</a></div></article><article class="card"><div class="icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9h18L19 3H5zM5 9v12h14V9M9 21v-7h6v7"/></svg></div><h3>Soy comerciante</h3><p>Publica productos y recibe pedidos de tus clientes.</p><div class="links"><a class="cta" href="/comercio?registro=1">Registrarme</a><a href="/comercio">Entrar →</a></div></article></section><div class="demo"><b>Modo demostración.</b> Solo datos ficticios, sin cobros reales. GPS opcional y privado en lugares públicos. La ilustración es conceptual.</div></main><footer>UBERVZLA · v0.5 · Prototipo independiente · Venezuela</footer></body></html>',headers={'Cache-Control':'no-store'})
@app.get('/cliente')
@app.get('/driver')
@app.get('/comercio')
def portal_page(r:Request):
 role={'/cliente':'customer','/driver':'driver','/comercio':'merchant'}[r.url.path]
 return HTMLResponse((BASE/('driver.html' if role=='driver' else 'index.html')).read_text(encoding='utf-8').replace('__PORTAL_ROLE__',role),headers={'Cache-Control':'no-store'})
@app.get('/health')
def health():
 with connect() as c: c.execute('SELECT 1 FROM users LIMIT 1')
 return {'status':'ok','version':'0.7.1-driver-approval-face-demo','mode':'TEST_ONLY'}
@app.post('/api/register')
def register(data:Register,r:Request):
 if data.role!=portal(r): raise HTTPException(403,'Registro no corresponde a esta plataforma')
 if data.role not in ('customer','driver','merchant') or data.zone not in POINTS or (data.role=='driver' and data.vehicle not in ('car','moto')): raise HTTPException(400,'Datos inválidos')
 if data.role=='merchant' and data.category not in ('Restaurante','Supermercado','Farmacia','Panadería','Tienda','Otro'): raise HTTPException(400,'Categoría inválida')
 with tx() as c:
  try: i=c.execute('INSERT INTO users(name,login,pw,role,vehicle,zone) VALUES(?,?,?,?,?,?)',(data.name.strip(),data.login.lower(),hashpw(data.password),data.role,data.vehicle if data.role=='driver' else '',data.zone)).lastrowid
  except sqlite3.IntegrityError: raise HTTPException(409,'Ese usuario ya existe')
  if data.role=='driver': c.execute('INSERT INTO driver_reviews(uid) VALUES(?)',(i,))
  if data.role=='merchant': c.execute('INSERT INTO shops VALUES(?,?)',(i,data.category))
  u=c.execute('SELECT * FROM users WHERE id=?',(i,)).fetchone()
  return face_challenge(c,u,'login') if u['role']=='driver' else session(c,u)
@app.post('/api/login')
def login(data:Login,r:Request):
 with tx() as c:
  u=c.execute('SELECT * FROM users WHERE login=?',(data.login.lower(),)).fetchone()
  if not u or u['role']!=portal(r) or not hmac.compare_digest(u['pw'],hashpw(data.password,u['pw'].split(':')[0])): raise HTTPException(401,'Usuario o clave incorrectos')
  return face_challenge(c,u,'login') if u['role']=='driver' else session(c,u)
@app.post('/api/logout')
def logout(r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']=='driver':
   c.execute('UPDATE users SET online=0 WHERE id=?',(u['id'],)); c.execute('DELETE FROM driver_gps WHERE uid=?',(u['id'],))
  c.execute('DELETE FROM sessions WHERE token=?',(hashlib.sha256(r.cookies.get(cookie_name(r),'').encode()).hexdigest(),)); tick(c)
 res=JSONResponse({'ok':True}); res.delete_cookie(cookie_name(r)); return res
@app.post('/api/availability')
def availability(data:Availability,r:Request):
 if data.zone not in POINTS: raise HTTPException(400,'Zona inválida')
 with tx() as c:
  u=user(c,r)
  if u['role']!='driver': raise HTTPException(403,'Solo conductores')
  if data.online and not qualified(c,u['id']): raise HTTPException(403,'Cuenta no aprobada o documentos vencidos (demo)')
  if data.gps_mode and data.online and len(data.room.strip())<8: raise HTTPException(400,'Código de prueba: mínimo 8 caracteres')
  room=hashlib.sha256(data.room.strip().encode()).hexdigest() if data.gps_mode and data.room.strip() else u['gps_room'] if data.gps_mode else ''
  if (bool(u['gps_mode'])!=data.gps_mode or u['gps_room']!=room) and c.execute("SELECT 1 FROM orders WHERE driver=? AND status IN ('offered','accepted','arrived','in_progress')",(u['id'],)).fetchone(): raise HTTPException(409,'Finaliza o rechaza la orden antes de cambiar modo')
  c.execute('UPDATE users SET online=?,zone=?,last_seen=?,gps_mode=?,gps_room=? WHERE id=?',(int(data.online),data.zone,time.time(),int(data.gps_mode),room,u['id']))
  if not data.online or not data.gps_mode: c.execute('DELETE FROM driver_gps WHERE uid=?',(u['id'],))
  tick(c)
  return {'ok':True}
@app.post('/api/gps')
def gps(data:Fix,r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='driver' or not u['online'] or not u['gps_mode']: raise HTTPException(403,'Activa el modo GPS y conéctate primero')
  c.execute('INSERT INTO driver_gps VALUES(?,?,?,?,?) ON CONFLICT(uid) DO UPDATE SET lat=excluded.lat,lon=excluded.lon,accuracy=excluded.accuracy,seen=excluded.seen',(u['id'],data.lat,data.lon,data.accuracy,time.time()))
  c.execute('UPDATE users SET last_seen=? WHERE id=?',(time.time(),u['id'])); tick(c)
  return {'ok':True}
@app.post('/api/gps/stop')
def gps_stop(r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='driver': raise HTTPException(403,'Solo conductores')
  c.execute('DELETE FROM driver_gps WHERE uid=?',(u['id'],)); c.execute('UPDATE users SET online=0 WHERE id=?',(u['id'],)); tick(c)
  return {'ok':True}
@app.post('/api/orders')
def new_order(data:Order,r:Request):
 real=bool(data.pickup_gps and data.destination_gps)
 if data.service not in SERVICES or (bool(data.pickup_gps)!=bool(data.destination_gps)) or (not real and (data.pickup not in POINTS or data.destination not in POINTS or data.pickup==data.destination)): raise HTTPException(400,'Selecciona servicio y dos puntos distintos')
 if real:
  if len(data.room.strip())<8: raise HTTPException(400,'Código de prueba: mínimo 8 caracteres')
  distance=km((data.pickup_gps.lat,data.pickup_gps.lon),(data.destination_gps.lat,data.destination_gps.lon))
  if not .02<=distance<=100: raise HTTPException(400,'Distancia de prueba permitida: 20 m a 100 km')
 else: distance=dist(data.pickup,data.destination)
 with tx() as c:
  u=user(c,r)
  if u['role']!='customer': raise HTTPException(403,'Solo clientes')
  if c.execute("SELECT 1 FROM orders WHERE customer=? AND status NOT IN ('completed','cancelled')",(u['id'],)).fetchone(): raise HTTPException(409,'Ya tienes una solicitud activa')
  now=time.time(); price=round(PRICES[data.service]+distance*.4,2)
  i=c.execute("INSERT INTO orders(customer,service,pickup,destination,price,status,created,updated,gps_mode,gps_room) VALUES(?,?,?,?,?,'pending',?,?,?,?)",(u['id'],data.service,'Punto GPS de recogida' if real else data.pickup,'Destino GPS' if real else data.destination,price,now,now,int(real),hashlib.sha256(data.room.strip().encode()).hexdigest() if real else "")).lastrowid
  if real: c.execute('INSERT INTO order_gps VALUES(?,?,?,?,?)',(i,data.pickup_gps.lat,data.pickup_gps.lon,data.destination_gps.lat,data.destination_gps.lon))
  event(c,i,'created'); tick(c); return {'id':i,'price':price}
@app.post('/api/orders/{oid}/action')
def action(oid:int,data:Action,r:Request):
 with tx() as c:
  u=user(c,r); tick(c); o=c.execute('SELECT * FROM orders WHERE id=?',(oid,)).fetchone()
  if not o: raise HTTPException(404,'Orden no encontrada')
  a=data.action; now=time.time(); state=o['status']
  if a=='cancel' and u['role']=='customer' and o['customer']==u['id'] and state in ('preparing','pending','offered','accepted','arrived'):
   c.execute("UPDATE orders SET status='cancelled',deadline=NULL,updated=? WHERE id=?",(now,oid)); event(c,oid,'cancelled')
  elif u['role']=='merchant' and a=='ready' and state=='preparing' and c.execute('SELECT 1 FROM shop_orders WHERE order_id=? AND merchant=?',(oid,u['id'])).fetchone():
   c.execute("UPDATE orders SET status='pending',updated=? WHERE id=?",(now,oid)); event(c,oid,'merchant_ready')
  elif u['role']=='driver' and o['driver']==u['id']:
   if a=='accept' and not qualified(c,u['id']): raise HTTPException(403,'Cuenta no autorizada')
   if a in ('accept','reject') and state=='offered' and o['deadline']>now:
    c.execute('UPDATE attempts SET result=? WHERE order_id=? AND driver=?',(a,oid,u['id']))
    if a=='accept': c.execute("UPDATE orders SET status='accepted',deadline=NULL,updated=? WHERE id=?",(now,oid))
    else: c.execute("UPDATE orders SET status='pending',driver=NULL,deadline=NULL,updated=? WHERE id=?",(now,oid))
    event(c,oid,a)
   elif (state,a) in {('accepted','arrive'),('arrived','start'),('in_progress','complete')}:
    nextstate={'arrive':'arrived','start':'in_progress','complete':'completed'}[a]
    c.execute('UPDATE orders SET status=?,updated=? WHERE id=?',(nextstate,now,oid)); event(c,oid,nextstate)
    if a=='complete' and not o['gps_mode']: c.execute('UPDATE users SET zone=? WHERE id=?',(o['destination'],u['id']))
   else: raise HTTPException(409,'Oferta vencida o acción inválida')
  else: raise HTTPException(403,'Orden no asignada a tu cuenta o acción no permitida')
  tick(c); return {'ok':True}
@app.get('/api/state')
def state(r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']=='driver': c.execute('UPDATE users SET last_seen=? WHERE id=?',(time.time(),u['id']))
  tick(c)
  query="SELECT o.*, d.name driver_name, d.zone driver_zone, so.product_name, so.product_cents FROM orders o LEFT JOIN users d ON d.id=o.driver LEFT JOIN shop_orders so ON so.order_id=o.id WHERE "
  query+= 'o.customer=?' if u['role']=='customer' else ('o.driver=?' if u['role']=='driver' else 'so.merchant=?')
  rows=c.execute(query+' ORDER BY o.id DESC LIMIT 30',(u['id'],)).fetchall()
  orders=[]
  for row in rows:
   o=dict(row); o.pop('gps_room',None); o['driver_pay']=round(o['price']*.85,2)
   if u['role']=='customer' and o['status']=='offered': o['driver_name']=None; o['driver_zone']=None; o['driver']=None
   if o['gps_mode'] and o['status'] not in ('completed','cancelled') and u['role'] in ('customer','driver'):
    g=c.execute('SELECT * FROM order_gps WHERE oid=?',(o['id'],)).fetchone()
    if g and (u['role']=='customer' or o['status'] in ACTIVE): o['gps_points']={'pickup':[g['plat'],g['plon']],'destination':[g['dlat'],g['dlon']]}
    if g and u['role']=='driver' and o['status']=='offered':
     fix=c.execute('SELECT * FROM driver_gps WHERE uid=?',(u['id'],)).fetchone()
     if fix: o['pickup_distance_km']=round(km((fix['lat'],fix['lon']),(g['plat'],g['plon'])),1)
     o['trip_distance_km']=round(km((g['plat'],g['plon']),(g['dlat'],g['dlon'])),1)
    if o['status'] in ACTIVE:
     fix=c.execute('SELECT lat,lon,accuracy,seen FROM driver_gps WHERE uid=? AND seen>=?',(o['driver'],time.time()-GPS_TTL)).fetchone()
     if fix: o['driver_gps']=dict(fix)
   orders.append(o)
  products=[dict(p) for p in c.execute('SELECT id,name,price_cents FROM products WHERE merchant=?',(u['id'],))] if u['role']=='merchant' else []
  return JSONResponse({'products':products,'me':public(c.execute('SELECT * FROM users WHERE id=?',(u['id'],)).fetchone()),'review':review_info(c,u['id']) if u['role']=='driver' else None,'wallet_demo_cents':wallet(c,u['id']) if u['role']=='driver' else 0,'orders':orders,'points':POINTS,'now':time.time(),'demo':True},headers={'Cache-Control':'no-store'})

class Product(BaseModel):
 name:str=Field(min_length=2,max_length=80); price_cents:int=Field(ge=1,le=1000000)
class Purchase(BaseModel):
 product_id:int; destination:str
@app.post('/api/products')
def add_product(data:Product,r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='merchant': raise HTTPException(403,'Solo comercios')
  i=c.execute('INSERT INTO products(merchant,name,price_cents) VALUES(?,?,?)',(u['id'],data.name.strip(),data.price_cents)).lastrowid
  return {'id':i}
@app.get('/api/catalog')
def catalog(r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='customer': raise HTTPException(403,'Solo clientes')
  return [dict(p) for p in c.execute('SELECT p.id,p.name,p.price_cents,u.name shop,u.zone,s.category FROM products p JOIN shops s ON s.uid=p.merchant JOIN users u ON u.id=s.uid ORDER BY p.id DESC LIMIT 100')]
@app.post('/api/purchase')
def purchase(data:Purchase,r:Request):
 if data.destination not in POINTS: raise HTTPException(400,'Zona inválida')
 with tx() as c:
  u=user(c,r)
  if u['role']!='customer': raise HTTPException(403,'Solo clientes')
  if c.execute("SELECT 1 FROM orders WHERE customer=? AND status NOT IN ('completed','cancelled')",(u['id'],)).fetchone(): raise HTTPException(409,'Ya tienes una solicitud activa')
  p=c.execute('SELECT p.*,u.zone FROM products p JOIN users u ON u.id=p.merchant WHERE p.id=?',(data.product_id,)).fetchone()
  if not p: raise HTTPException(404,'Producto no disponible')
  now=time.time(); price=round(PRICES['delivery']+dist(p['zone'],data.destination)*.4,2)
  i=c.execute("INSERT INTO orders(customer,service,pickup,destination,price,status,created,updated) VALUES(?,?,?,?,?,'preparing',?,?)",(u['id'],'delivery',p['zone'],data.destination,price,now,now)).lastrowid
  c.execute('INSERT INTO shop_orders VALUES(?,?,?,?)',(i,p['merchant'],p['name'],p['price_cents'])); event(c,i,'purchase_created'); return {'id':i,'price':price,'total':round(price+p['price_cents']/100,2)}


# DEMO ONLY: no personal documents, biometric processing or real checks.
class ReviewDecision(BaseModel):
 decision:str
 checks:dict[str,bool]=Field(default_factory=dict)
 dates:dict[str,str]=Field(default_factory=dict)

def admin_auth(r):
 key=os.getenv('UBERVZLA_ADMIN_KEY','')
 if len(key)<32: raise HTTPException(503,'Panel sin configurar. Define UBERVZLA_ADMIN_KEY de al menos 32 caracteres.')
 supplied=r.headers.get('Authorization','')
 if not hmac.compare_digest(supplied.encode(),('Bearer '+key).encode()): raise HTTPException(401,'Acceso administrativo inválido')
@app.get('/admin')
def admin_page():
 return HTMLResponse((BASE/'admin.html').read_text(encoding='utf-8'),headers={'Cache-Control':'no-store'})
@app.post('/api/review/submit')
def submit_review(r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='driver': raise HTTPException(403,'Solo conductor')
  current=review_info(c,u['id'])
  if current['status'] not in ('pending','correction','expired','rejected'): raise HTTPException(409,'No puedes reenviar esta solicitud')
  c.execute("UPDATE driver_reviews SET submitted=1,status='pending',checks='{}',expires=0,updated=? WHERE uid=?",(time.time(),u['id']))
  c.execute("INSERT INTO review_audit(uid,decision,checks,ts) VALUES(?,'submitted','{}',?)",(u['id'],time.time()))
  return review_info(c,u['id'])
@app.get('/api/admin/drivers')
def admin_drivers(r:Request):
 admin_auth(r)
 with tx() as c:
  return JSONResponse({'checks':CHECKS,'expiring':EXPIRING,'drivers':[{'id':u['id'],'name':u['name'],'login':u['login'],'vehicle':u['vehicle'],**review_info(c,u['id'])} for u in c.execute("SELECT * FROM users WHERE role='driver' ORDER BY id DESC")]},headers={'Cache-Control':'no-store'})
@app.post('/api/admin/drivers/{uid}')
def decide_review(uid:int,data:ReviewDecision,r:Request):
 admin_auth(r)
 if data.decision not in ('approved','correction','rejected','suspended'): raise HTTPException(400,'Decisión inválida')
 with tx() as c:
  row=c.execute('SELECT * FROM driver_reviews WHERE uid=?',(uid,)).fetchone()
  if not row: raise HTTPException(404,'Conductor no encontrado')
  expires=0
  checks={k:bool(data.checks.get(k,False)) for k in CHECKS}
  if data.decision=='approved':
   if not row['submitted'] or not all(checks.values()): raise HTTPException(400,'Solicitud y todos los controles demo son obligatorios')
   try: stamps=[datetime.fromisoformat(data.dates[k]).replace(tzinfo=timezone.utc).timestamp()+86400 for k in EXPIRING]
   except (ValueError,KeyError,OverflowError): raise HTTPException(400,'Indica las tres fechas de vigencia ficticias')
   expires=min(stamps)
   if expires<=time.time(): raise HTTPException(400,'Hay documentos de prueba vencidos')
  payload=json.dumps(checks)
  c.execute('UPDATE driver_reviews SET status=?,checks=?,expires=?,updated=? WHERE uid=?',(data.decision,payload,expires,time.time(),uid))
  c.execute('INSERT INTO review_audit(uid,decision,checks,ts) VALUES(?,?,?,?)',(uid,data.decision,json.dumps({'checks':checks,'dates':{k:data.dates.get(k) for k in EXPIRING}}),time.time()))
  tick(c)
  return review_info(c,uid)


# The following is a UX/state-machine simulation. No camera, biometric comparison,
# live payout or official identity verification is enabled by this code.
def wallet(c,uid):
 earned=sum(round(round(r['price']*.85,2)*100) for r in c.execute("SELECT price FROM orders WHERE driver=? AND status='completed'",(uid,)))
 paid=c.execute('SELECT COALESCE(SUM(cents),0) FROM demo_withdrawals WHERE uid=?',(uid,)).fetchone()[0]
 return max(0,earned-paid)
def face_challenge(c,u,purpose,binding='',cents=0):
 c.execute('DELETE FROM demo_face_challenges WHERE expiry<=?',(time.time(),))
 c.execute('DELETE FROM demo_face_challenges WHERE uid=? AND purpose=?',(u['id'],purpose))
 token=secrets.token_urlsafe(32)
 c.execute('INSERT INTO demo_face_challenges VALUES(?,?,?,?,?,?)',(hashlib.sha256(token.encode()).hexdigest(),u['id'],purpose,time.time()+120,binding,cents))
 return {'face_demo_required':True,'challenge':token,'purpose':purpose,'expires_in':120,'demo_only':True}
class FaceDemo(BaseModel):
 challenge:str=Field(min_length=20,max_length=128)
 confirm_demo:bool
class DemoWithdraw(BaseModel):
 cents:int=Field(gt=0,le=10000000)
@app.post('/api/face-demo/confirm')
def face_confirm(data:FaceDemo,r:Request):
 if portal(r)!='driver' or not data.confirm_demo: raise HTTPException(400,'Confirmación demo requerida')
 with tx() as c:
  h=hashlib.sha256(data.challenge.encode()).hexdigest()
  row=c.execute('SELECT * FROM demo_face_challenges WHERE token=? AND expiry>?',(h,time.time())).fetchone()
  if not row: raise HTTPException(400,'Prueba facial vencida o usada; vuelve a empezar')
  u=c.execute('SELECT * FROM users WHERE id=?',(row['uid'],)).fetchone()
  if row['purpose']=='login':
   c.execute('DELETE FROM demo_face_challenges WHERE token=?',(h,))
   return session(c,u)
  current=user(c,r)
  binding=hashlib.sha256(r.cookies.get(cookie_name(r),'').encode()).hexdigest()
  if current['id']!=u['id'] or row['binding']!=binding or row['purpose']!='withdraw': raise HTTPException(403,'Prueba no corresponde a esta sesión')
  if not qualified(c,u['id']) or wallet(c,u['id'])<row['cents']: raise HTTPException(409,'Cuenta no autorizada o saldo demo insuficiente')
  c.execute('DELETE FROM demo_face_challenges WHERE token=?',(h,))
  c.execute('INSERT INTO demo_withdrawals(uid,cents,ts) VALUES(?,?,?)',(u['id'],row['cents'],time.time()))
  return {'demo_only':True,'money_sent':False,'wallet_demo_cents':wallet(c,u['id'])}
@app.post('/api/withdraw-demo/start')
def withdraw_demo(data:DemoWithdraw,r:Request):
 with tx() as c:
  u=user(c,r)
  if u['role']!='driver' or not qualified(c,u['id']): raise HTTPException(403,'Cuenta no autorizada')
  if wallet(c,u['id'])<data.cents: raise HTTPException(400,'Saldo demo insuficiente')
  return face_challenge(c,u,'withdraw',hashlib.sha256(r.cookies.get(cookie_name(r),'').encode()).hexdigest(),data.cents)

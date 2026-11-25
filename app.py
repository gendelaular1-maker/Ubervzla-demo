"""UBERVZLA demo. Synthetic data only; single instance SQLite."""
import os, sqlite3, time, secrets, hashlib, hmac, math, threading
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
 CREATE INDEX IF NOT EXISTS orders_status ON orders(status,created);
 CREATE TABLE IF NOT EXISTS driver_gps(uid INTEGER PRIMARY KEY REFERENCES users(id), lat REAL, lon REAL, accuracy REAL, seen REAL);
 CREATE TABLE IF NOT EXISTS order_gps(oid INTEGER PRIMARY KEY REFERENCES orders(id), plat REAL, plon REAL, dlat REAL, dlon REAL);

 ''')
  for table in ('users','orders'):
   if 'gps_mode' not in {r['name'] for r in c.execute(f'PRAGMA table_info({table})')}:
    c.execute(f"ALTER TABLE {table} ADD COLUMN gps_mode INTEGER NOT NULL DEFAULT 0")
   if 'gps_room' not in {r['name'] for r in c.execute(f'PRAGMA table_info({table})')}:
    c.execute(f"ALTER TABLE {table} ADD COLUMN gps_room TEXT NOT NULL DEFAULT ''")
def event(c,i,e): c.execute('INSERT INTO events(order_id,event,ts) VALUES(?,?,?)',(i,e,time.time()))
def dist(a,b):
 la,lo=POINTS[a]; lb,ob=POINTS[b]; return math.hypot((la-lb)*111,(lo-ob)*109)
def km(a,b):
 la,lo=map(math.radians,a); lb,ob=map(math.radians,b)
 h=math.sin((lb-la)/2)**2+math.cos(la)*math.cos(lb)*math.sin((ob-lo)/2)**2
 return 6371*2*math.asin(math.sqrt(min(1,max(0,h))))
def tick(c):
 now=time.time()
 c.execute('DELETE FROM driver_gps WHERE seen<?',(now-GPS_TTL,))
 c.execute("DELETE FROM order_gps WHERE oid IN (SELECT id FROM orders WHERE status IN ('completed','cancelled'))")
 # Expire old GPS orders if never accepted. Do not cancel active rides automatically.
 c.execute("UPDATE orders SET status='cancelled',driver=NULL,deadline=NULL,updated=? WHERE gps_mode=1 AND status IN ('pending','offered') AND created<?",(now,now-1800))
 c.execute("DELETE FROM order_gps WHERE oid IN (SELECT id FROM orders WHERE status='cancelled' OR (gps_mode=1 AND created<?))",(now-14400,))
 for o in c.execute("SELECT * FROM orders WHERE status='offered' AND (deadline<=? OR driver IN (SELECT id FROM users WHERE online=0 OR last_seen<?) OR (gps_mode=1 AND driver NOT IN (SELECT uid FROM driver_gps)))",(now,now-60)).fetchall():
  c.execute("UPDATE attempts SET result='expired' WHERE order_id=? AND driver=?",(o['id'],o['driver']))
  c.execute("UPDATE orders SET status='pending',driver=NULL,deadline=NULL,updated=? WHERE id=?",(now,o['id'])); event(c,o['id'],'offer_expired')
 for o in c.execute("SELECT * FROM orders WHERE status='pending' ORDER BY created,id").fetchall():
  eligible=c.execute("""SELECT u.* FROM users u WHERE role='driver' AND online=1 AND last_seen>=? AND (vehicle=? OR ?='any') AND u.gps_mode=? AND (u.gps_mode=0 OR u.gps_room=?) AND NOT EXISTS(SELECT 1 FROM orders x WHERE x.driver=u.id AND x.status IN ('offered','accepted','arrived','in_progress')) AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.order_id=? AND a.driver=u.id)""",(now-60,SERVICES[o['service']],SERVICES[o['service']],o['gps_mode'],o['gps_room'],o['id'])).fetchall()
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
 return u
def public(u): return {k:u[k] for k in ('id','name','login','role','vehicle','zone','online','gps_mode')}
def session(c,u):
 token=secrets.token_urlsafe(32); c.execute('DELETE FROM sessions WHERE expiry<?',(time.time(),)); c.execute('INSERT INTO sessions VALUES(?,?,?)',(hashlib.sha256(token.encode()).hexdigest(),u['id'],time.time()+86400))
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
 return HTMLResponse('<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>UBERVZLA | Venezuela en movimiento</title><style>*{box-sizing:border-box}body{margin:0;font:16px system-ui;color:#162034;background:#f5f6fa}header{background:#fff;border-bottom:1px solid #e1e5ed;padding:16px max(20px,calc((100% - 1120px)/2));display:flex;align-items:center;justify-content:space-between;gap:12px}header b{font-size:24px;font-style:italic;letter-spacing:-1px;white-space:nowrap}.mark{display:inline-block;width:24px;height:18px;margin-right:7px;background:linear-gradient(165deg,#ffd32b 0 33%,#2758f7 33% 66%,#ef384b 66%);clip-path:polygon(0 45%,100% 0,65% 100%)}header a{font-size:13px;color:#4933d5;text-decoration:none;font-weight:750}main{max-width:1160px;padding:22px;margin:auto}.hero{position:relative;overflow:hidden;display:grid;grid-template-columns:1fr .8fr;align-items:center;gap:16px;background:linear-gradient(115deg,#111a2a,#29304e);color:white;padding:45px;border-radius:26px}.hero h1{font-size:clamp(34px,5vw,58px);letter-spacing:-2px;line-height:1.04;margin:14px 0}.hero p{line-height:1.6;color:#dbe1ef;margin:16px 0 25px;max-width:490px}.eyebrow{color:#c7bfff;text-transform:uppercase;font-size:12px;letter-spacing:2px;font-weight:800}.cta{display:inline-block;background:#5840e9;color:#fff;padding:14px 22px;border-radius:12px;text-decoration:none;font-weight:750}.art{width:100%;max-height:300px}.cards{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-top:18px}.card{display:block;text-decoration:none;color:inherit;background:white;border:1px solid #e4e7ee;border-radius:18px;padding:22px;box-shadow:0 5px 18px #17233a09}.card b{display:block;font-size:20px;margin:12px 0 6px}.card p{color:#657187;font-size:14px;line-height:1.5}.card span{color:#4a36d6;font-size:13px;font-weight:800}.icon{display:inline-grid;place-items:center;width:48px;height:48px;border-radius:13px;background:#eeebff;color:#4933d5;font-size:25px}.demo{background:#fff8e5;color:#725419;border:1px solid #f0e0b9;padding:12px;border-radius:12px;font-size:12px;margin-top:18px}footer{padding:25px;text-align:center;color:#657187;font-size:12px}@media(max-width:720px){main{padding:12px}.hero{grid-template-columns:1fr;padding:25px}.hero h1{letter-spacing:-1.5px}.art{max-height:180px}.cards{grid-template-columns:1fr;gap:9px}.card{padding:15px}header{padding:15px}header b{font-size:21px}}</style></head><body><header><b><i class="mark"></i>UBERVZLA</b><a href="#accesos">Acceder / Registrarse</a></header><main><section class="hero"><div><span class="eyebrow">Venezuela en movimiento</span><h1>Viaja, compra y envía.<br>Todo en un lugar.</h1><p>Una plataforma para conectar pasajeros, conductores y comercios. Tu ciudad, a un toque.</p><a class="cta" href="#accesos">Comenzar ahora</a></div><svg class="art" viewBox="0 0 410 300" role="img" aria-label="Ilustración de una ruta de movilidad"><rect x="12" y="14" width="386" height="272" rx="25" fill="#f0f2f4"/><g stroke="#fff" stroke-width="11" fill="none"><path d="M12 70 398 135M12 150 398 203M12 232 398 270M85 14 35 286M176 14 133 286M257 14 221 286M350 14 320 286"/></g><g fill="#d8e8d8"><rect x="190" y="38" width="48" height="45" rx="9"/><rect x="44" y="180" width="51" height="37" rx="9"/><rect x="270" y="213" width="30" height="41" rx="7"/></g><path d="M69 245 153 257 172 172 243 184 257 112 334 126 344 62" stroke="#513be3" stroke-width="7" fill="none" stroke-linecap="round" stroke-linejoin="round"/><circle cx="69" cy="245" r="12" fill="#513be3" stroke="white" stroke-width="4"/><path d="M344 82c-9-12-20-23-20-34a20 20 0 1 1 40 0c0 11-11 22-20 34z" fill="#172034"/><circle cx="344" cy="48" r="6" fill="white"/><rect x="111" y="119" width="106" height="41" rx="12" fill="#172034"/><text x="164" y="144" font-family="system-ui" font-size="13" font-weight="bold" fill="white" text-anchor="middle">Tu próximo viaje</text></svg></section><section class="cards" id="accesos"><a class="card" href="/cliente"><div class="icon">♙</div><b>Soy cliente</b><p>Solicita viajes, compra en comercios y envía paquetes.</p><span>Entrar a Cliente →</span></a><a class="card" href="/driver"><div class="icon">🚘</div><b>Quiero conducir</b><p>Registra tu carro o moto y recibe ofertas de servicios.</p><span>Entrar a Driver →</span></a><a class="card" href="/comercio"><div class="icon">▤</div><b>Quiero vender</b><p>Registra tu negocio, publica productos y gestiona pedidos.</p><span>Entrar a Comercios →</span></a></section><div class="demo"><b>Plataforma de prueba.</b> Sin cobros reales. Utiliza datos ficticios; GPS opcional en lugares públicos.</div></main><footer>UBERVZLA · v0.4 Diseño + GPS · Prototipo independiente</footer></body></html>',headers={'Cache-Control':'no-store'})
@app.get('/cliente')
@app.get('/driver')
@app.get('/comercio')
def portal_page(r:Request):
 role={'/cliente':'customer','/driver':'driver','/comercio':'merchant'}[r.url.path]
 return HTMLResponse((BASE/'index.html').read_text(encoding='utf-8').replace('__PORTAL_ROLE__',role),headers={'Cache-Control':'no-store'})
@app.get('/health')
def health():
 with connect() as c: c.execute('SELECT 1 FROM users LIMIT 1')
 return {'status':'ok','version':'0.4.0-design-gps-demo','mode':'TEST_ONLY'}
@app.post('/api/register')
def register(data:Register,r:Request):
 if data.role!=portal(r): raise HTTPException(403,'Registro no corresponde a esta plataforma')
 if data.role not in ('customer','driver','merchant') or data.zone not in POINTS or (data.role=='driver' and data.vehicle not in ('car','moto')): raise HTTPException(400,'Datos inválidos')
 if data.role=='merchant' and data.category not in ('Restaurante','Supermercado','Farmacia','Panadería','Tienda','Otro'): raise HTTPException(400,'Categoría inválida')
 with tx() as c:
  try: i=c.execute('INSERT INTO users(name,login,pw,role,vehicle,zone) VALUES(?,?,?,?,?,?)',(data.name.strip(),data.login.lower(),hashpw(data.password),data.role,data.vehicle if data.role=='driver' else '',data.zone)).lastrowid
  except sqlite3.IntegrityError: raise HTTPException(409,'Ese usuario ya existe')
  if data.role=='merchant': c.execute('INSERT INTO shops VALUES(?,?)',(i,data.category))
  return session(c,c.execute('SELECT * FROM users WHERE id=?',(i,)).fetchone())
@app.post('/api/login')
def login(data:Login,r:Request):
 with tx() as c:
  u=c.execute('SELECT * FROM users WHERE login=?',(data.login.lower(),)).fetchone()
  if not u or u['role']!=portal(r) or not hmac.compare_digest(u['pw'],hashpw(data.password,u['pw'].split(':')[0])): raise HTTPException(401,'Usuario o clave incorrectos')
  return session(c,u)
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
  return JSONResponse({'products':products,'me':public(c.execute('SELECT * FROM users WHERE id=?',(u['id'],)).fetchone()),'orders':orders,'points':POINTS,'now':time.time(),'demo':True},headers={'Cache-Control':'no-store'})

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

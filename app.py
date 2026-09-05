#!/usr/bin/env python3
import os, json, time, math, queue, sqlite3, threading, requests
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string
import websocket

VERSION = "1.0"
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8081"))
DB_PATH = os.getenv("DB_PATH", "mexc_routes.db")
MARKET_CACHE = os.getenv("MARKET_CACHE", "mexc_markets_cache.json")
TZ_NAME = os.getenv("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)

FEE = float(os.getenv("TAKER_FEE", "0.0005"))  # 0.05% per leg
STABLES = tuple(x.strip().upper() for x in os.getenv("STABLES", "USDT,USDC,USD1").split(",") if x.strip())
SIZES = tuple(float(x) for x in os.getenv("SIZES", "250,500,1000,2000").split(","))
MIN_NET = float(os.getenv("MIN_NET_PCT", "0.01")) / 100.0
MAX_BBO_AGE_MS = int(os.getenv("MAX_BBO_AGE_MS", "1500"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "1000"))
MAX_WS_SYMBOLS = int(os.getenv("MAX_WS_SYMBOLS", "600"))
MAX_BRIDGE_ASSETS = int(os.getenv("MAX_BRIDGE_ASSETS", "80"))
SIM_CAPITAL = float(os.getenv("SIM_CAPITAL", "2000"))
SIM_MIN_TRADE = float(os.getenv("SIM_MIN_TRADE", "50"))
MAX_ROUTES_PER_SYMBOL_SCAN = int(os.getenv("MAX_ROUTES_PER_SYMBOL_SCAN", "6000"))

REST = "https://api.mexc.com"
WS = "wss://wbs-api.mexc.com/ws"

app = Flask(__name__)
state_lock = threading.RLock()
event_lock = threading.RLock()
state = {
    "bbo": {}, "last_ws_ms": 0, "ws_connected": 0, "ws_expected": 0,
    "symbols": [], "routes": [], "route_by_id": {}, "routes_by_symbol": defaultdict(list),
    "errors": deque(maxlen=20), "started_ms": int(time.time()*1000),
    "market_source": "", "scan_updates": 0,
}
active_events = {}
dbq = queue.Queue(maxsize=20000)
scanq = queue.Queue(maxsize=20000)
queued_symbols = set()
queued_lock = threading.Lock()


def now_ms(): return int(time.time()*1000)

def logerr(msg):
    line = f"{datetime.now(TZ).isoformat(timespec='seconds')} {msg}"
    print(line)
    with state_lock: state["errors"].appendleft(line)

def local_day_bounds_ms(day_offset=0):
    now = datetime.now(TZ) + timedelta(days=day_offset)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return int(start.timestamp()*1000), int(end.timestamp()*1000), start.date().isoformat()

# ---------- DB ----------
def db_writer():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS opportunities(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      route_id TEXT NOT NULL,
      route_type INTEGER NOT NULL,
      path TEXT NOT NULL,
      start_asset TEXT NOT NULL,
      end_asset TEXT NOT NULL,
      start_ts_ms INTEGER NOT NULL,
      end_ts_ms INTEGER NOT NULL,
      duration_ms INTEGER NOT NULL,
      ticks INTEGER NOT NULL,
      entry_net REAL NOT NULL,
      peak_net REAL NOT NULL,
      avg_net REAL NOT NULL,
      best_size REAL NOT NULL,
      entry_profit REAL NOT NULL,
      peak_profit REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_opp_start ON opportunities(start_ts_ms);
    CREATE INDEX IF NOT EXISTS idx_opp_route ON opportunities(route_id,start_ts_ms);
    """)
    con.commit()
    while True:
        item = dbq.get()
        if item is None: break
        try:
            typ, payload = item
            if typ == "event":
                con.execute("""INSERT INTO opportunities(
                  route_id,route_type,path,start_asset,end_asset,start_ts_ms,end_ts_ms,duration_ms,
                  ticks,entry_net,peak_net,avg_net,best_size,entry_profit,peak_profit)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "meta":
                con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", payload)
            con.commit()
        except Exception as e:
            logerr(f"DB: {e}")
        finally:
            dbq.task_done()
    con.close()


def put_db(item):
    try: dbq.put_nowait(item)
    except queue.Full: logerr("DB queue full")

# ---------- MEXC market discovery ----------
def save_market_cache(markets):
    try:
        with open(MARKET_CACHE, "w", encoding="utf-8") as f: json.dump(markets, f)
    except Exception as e: logerr(f"market cache save: {e}")

def load_market_cache():
    try:
        with open(MARKET_CACHE, "r", encoding="utf-8") as f:
            x=json.load(f)
            if isinstance(x,list) and x: return x
    except Exception: pass
    return None

def discover_markets():
    headers={"User-Agent":"Mozilla/5.0 mexc-routes-scanner/1.0", "Accept":"application/json"}
    # V3 first
    for i in range(4):
        try:
            r=requests.get(REST+"/api/v3/exchangeInfo", headers=headers, timeout=15)
            r.raise_for_status(); data=r.json(); out=[]
            for s in data.get("symbols",[]):
                status=str(s.get("status","")).upper()
                if status not in ("1","ENABLED","TRADING",""): continue
                base=str(s.get("baseAsset","")).upper(); quote=str(s.get("quoteAsset","")).upper(); sym=str(s.get("symbol","")).upper()
                if base and quote and sym: out.append({"symbol":sym,"base":base,"quote":quote})
            if out:
                save_market_cache(out); state["market_source"]="MEXC v3 exchangeInfo"; return out
        except Exception as e:
            logerr(f"exchangeInfo try {i+1}: {e}"); time.sleep(2**i)
    # Old public endpoint is a useful fallback when WAF blocks v3 exchangeInfo.
    try:
        r=requests.get("https://www.mexc.com/open/api/v2/market/symbols",headers=headers,timeout=20)
        r.raise_for_status(); data=r.json(); out=[]
        for s in data.get("data",[]):
            raw=str(s.get("symbol","")).upper()
            status=str(s.get("state","")).upper()
            if status not in ("ENABLED","1",""): continue
            if "_" not in raw: continue
            base,quote=raw.split("_",1); sym=base+quote
            out.append({"symbol":sym,"base":base,"quote":quote})
        if out:
            save_market_cache(out); state["market_source"]="MEXC v2 symbols fallback"; return out
    except Exception as e: logerr(f"v2 symbols fallback: {e}")
    cached=load_market_cache()
    if cached:
        state["market_source"]="local market cache"; return cached
    raise RuntimeError("Impossible de charger la liste des marchés MEXC et aucun cache local n'existe.")

# ---------- Route graph ----------
def select_symbols_and_routes(markets):
    # Deduplicate symbols.
    bysym={m["symbol"]:m for m in markets if m["base"]!=m["quote"]}
    markets=list(bysym.values())
    stable_set=set(STABLES)
    stable_markets=[m for m in markets if m["base"] in stable_set or m["quote"] in stable_set]
    stable_assets=set()
    stable_degree=defaultdict(int)
    for m in stable_markets:
        other=m["quote"] if m["base"] in stable_set else m["base"]
        if other not in stable_set:
            stable_assets.add(other); stable_degree[other]+=1
    # Dynamically choose bridge assets by graph connectivity, not a hard-coded crypto list.
    cross_degree=defaultdict(int)
    for m in markets:
        if m["base"] in stable_assets and m["quote"] in stable_assets:
            cross_degree[m["base"]]+=1; cross_degree[m["quote"]]+=1
    ranked=sorted(stable_assets,key=lambda a:(stable_degree[a],cross_degree[a]),reverse=True)
    bridge=set(ranked[:MAX_BRIDGE_ASSETS])
    cross=[m for m in markets if m["base"] in bridge and m["quote"] in bridge]
    # Stable-linked markets are always first priority; cross markets fill remaining slots.
    chosen=[]; seen=set()
    cross.sort(key=lambda m: cross_degree[m["base"]]+cross_degree[m["quote"]], reverse=True)
    for m in stable_markets+cross:
        if m["symbol"] not in seen and len(chosen)<MAX_WS_SYMBOLS:
            chosen.append(m); seen.add(m["symbol"])
    # Build undirected conversion graph from selected symbols.
    graph=defaultdict(list); meta={m["symbol"]:m for m in chosen}
    for m in chosen:
        graph[m["base"]].append((m["quote"],m["symbol"]))
        graph[m["quote"]].append((m["base"],m["symbol"]))
    routes=[]; route_ids=set()
    # 2-leg: stable -> A -> different stable
    for s1 in STABLES:
        for a,sym1 in graph.get(s1,[]):
            if a in stable_set: continue
            for s2,sym2 in graph.get(a,[]):
                if s2 in stable_set and s2!=s1 and sym2!=sym1:
                    path=(s1,a,s2); rid=">".join(path)
                    if rid not in route_ids:
                        route_ids.add(rid); routes.append({"id":rid,"path":path,"symbols":tuple(dict.fromkeys((sym1,sym2))),"type":2})
    # 3-leg: stable -> A -> B -> stable (A/B discovered dynamically)
    for s1 in STABLES:
        for a,sym1 in graph.get(s1,[]):
            if a in stable_set: continue
            for b,sym2 in graph.get(a,[]):
                if b in stable_set or b==s1 or sym2==sym1: continue
                for s2,sym3 in graph.get(b,[]):
                    if s2 in stable_set and sym3 not in (sym1,sym2):
                        path=(s1,a,b,s2); rid=">".join(path)
                        if rid not in route_ids:
                            route_ids.add(rid); routes.append({"id":rid,"path":path,"symbols":tuple(dict.fromkeys((sym1,sym2,sym3))),"type":3})
    return chosen, routes, meta

# ---------- protobuf BBO ----------
def _pb_varint(data,pos):
    value=0; shift=0
    while pos<len(data):
        b=data[pos]; pos+=1; value|=(b&0x7f)<<shift
        if not (b&0x80): return value,pos
        shift+=7
    raise ValueError("truncated varint")

def _pb_fields(data):
    out=[]; pos=0
    while pos<len(data):
        key,pos=_pb_varint(data,pos); field=key>>3; wire=key&7
        if wire==0: val,pos=_pb_varint(data,pos)
        elif wire==1: val=data[pos:pos+8]; pos+=8
        elif wire==2:
            n,pos=_pb_varint(data,pos); val=data[pos:pos+n]; pos+=n
        elif wire==5: val=data[pos:pos+4]; pos+=4
        else: raise ValueError(f"wire {wire}")
        out.append((field,wire,val))
    return out

def decode_bookticker(payload):
    symbol=None; send=None; book=None
    for f,w,v in _pb_fields(payload):
        if f==3 and w==2: symbol=v.decode("utf-8","ignore")
        elif f==6 and w==0: send=v
        elif f==315 and w==2: book=v
    if not symbol or book is None: return None
    vals={}
    for f,w,v in _pb_fields(book):
        if w==2 and f in (1,2,3,4): vals[f]=v.decode("utf-8","ignore")
    if not all(k in vals for k in (1,2,3,4)): return None
    return {"symbol":symbol,"bid":float(vals[1]),"bidq":float(vals[2]),"ask":float(vals[3]),"askq":float(vals[4]),"send":int(send or now_ms())}

# ---------- Route math ----------
def conversion_leg(from_asset,to_asset,symbol,amount,books,meta):
    b=books.get(symbol); m=meta.get(symbol)
    if not b or not m or now_ms()-b["ts"]>MAX_BBO_AGE_MS: return None
    base,quote=m["base"],m["quote"]
    if from_asset==base and to_asset==quote:
        if amount>b["bidq"]+1e-12: return None
        return amount*b["bid"]*(1.0-FEE)
    if from_asset==quote and to_asset==base:
        max_quote=b["ask"]*b["askq"]
        if amount>max_quote+1e-9: return None
        return (amount/b["ask"])*(1.0-FEE)
    return None

def stable_usdt_value(asset,books,meta):
    if asset=="USDT": return 1.0
    # executable mark to USDT using direct book if available
    for sym,m in meta.items():
        if {m["base"],m["quote"]}=={asset,"USDT"}:
            b=books.get(sym)
            if not b or now_ms()-b["ts"]>MAX_BBO_AGE_MS: break
            if m["base"]==asset: return b["bid"]*(1.0-FEE)
            return (1.0/b["ask"])*(1.0-FEE)
    return 1.0  # fallback peg assumption, shown on dashboard

def route_calc(route,books,meta):
    # Net return is size-independent at top-of-book. We test the configured sizes to determine capacity.
    start,end=route["path"][0],route["path"][-1]
    sv=stable_usdt_value(start,books,meta); ev=stable_usdt_value(end,books,meta)
    best=0.0; ratio=None
    for usd in SIZES:
        amount=usd/max(sv,1e-12); cur=amount; ok=True
        for i,sym in enumerate(route["symbols"]):
            cur=conversion_leg(route["path"][i],route["path"][i+1],sym,cur,books,meta)
            if cur is None: ok=False; break
        if ok:
            best=usd; ratio=cur/amount
    if best<=0 or ratio is None: return None
    net=ratio*ev/sv-1.0
    return {"net":net,"best_size":best,"profit":best*net,"out_ratio":ratio}

# ---------- Event aggregation ----------
def close_event(key,ev,end_ts=None):
    end_ts=int(end_ts or ev["last_ts"])
    avg=ev["sum_net"]/max(ev["ticks"],1)
    payload=(ev["route_id"],ev["type"],ev["path"],ev["start_asset"],ev["end_asset"],
             ev["start_ts"],end_ts,max(0,end_ts-ev["start_ts"]),ev["ticks"],ev["entry_net"],
             ev["peak_net"],avg,ev["best_size"],ev["entry_profit"],ev["peak_profit"])
    put_db(("event",payload))

def process_route(route,ts):
    with state_lock:
        books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta)
    key=route["id"]
    with event_lock:
        ev=active_events.get(key)
        if x and x["net"]>=MIN_NET:
            if ev is None:
                active_events[key]={
                    "route_id":route["id"],"type":route["type"],"path":">".join(route["path"]),
                    "start_asset":route["path"][0],"end_asset":route["path"][-1],"start_ts":ts,"last_ts":ts,
                    "ticks":1,"entry_net":x["net"],"peak_net":x["net"],"sum_net":x["net"],
                    "best_size":x["best_size"],"entry_profit":x["profit"],"peak_profit":x["profit"]}
            else:
                ev["last_ts"]=ts; ev["ticks"]+=1; ev["sum_net"]+=x["net"]
                if x["net"]>ev["peak_net"]: ev["peak_net"]=x["net"]
                if x["profit"]>ev["peak_profit"]: ev["peak_profit"]=x["profit"]
                if x["best_size"]>ev["best_size"]: ev["best_size"]=x["best_size"]
        elif ev is not None:
            close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

def event_sweeper():
    while True:
        time.sleep(.5); ts=now_ms()
        with event_lock:
            for key,ev in list(active_events.items()):
                if ts-ev["last_ts"]>EVENT_CLOSE_GAP_MS:
                    close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

def enqueue_scan(symbol):
    with queued_lock:
        if symbol in queued_symbols: return
        queued_symbols.add(symbol)
    try: scanq.put_nowait(symbol)
    except queue.Full:
        with queued_lock: queued_symbols.discard(symbol)

def scan_worker():
    while True:
        sym=scanq.get()
        try:
            with state_lock: routes=list(state["routes_by_symbol"].get(sym,()))[:MAX_ROUTES_PER_SYMBOL_SCAN]
            ts=now_ms()
            for r in routes: process_route(r,ts)
            with state_lock: state["scan_updates"]+=1
        except Exception as e: logerr(f"scan {sym}: {e}")
        finally:
            with queued_lock: queued_symbols.discard(sym)
            scanq.task_done()

# ---------- WS ----------
def ws_worker(symbols,worker_id):
    while True:
        opened=False
        try:
            def on_open(ws):
                nonlocal opened; opened=True
                with state_lock: state["ws_connected"]+=1
                params=[f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{s}" for s in symbols]
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":params}))
                print(f"[Routes V{VERSION}] WS {worker_id}: {len(symbols)} symbols")
            def on_message(ws,msg):
                if isinstance(msg,str): return
                try:
                    x=decode_bookticker(msg)
                    if not x: return
                    ts=now_ms()
                    with state_lock:
                        state["bbo"][x["symbol"]]={"bid":x["bid"],"bidq":x["bidq"],"ask":x["ask"],"askq":x["askq"],"ts":ts,"lat":max(0,ts-x["send"])}
                        state["last_ws_ms"]=ts
                    enqueue_scan(x["symbol"])
                except Exception as e: logerr(f"decode WS {worker_id}: {e}")
            def on_error(ws,e): logerr(f"WS {worker_id}: {e}")
            def on_close(ws,code,msg):
                nonlocal opened
                if opened:
                    with state_lock: state["ws_connected"]=max(0,state["ws_connected"]-1)
                    opened=False
            w=websocket.WebSocketApp(WS,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
            w.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e: logerr(f"WS loop {worker_id}: {e}")
        time.sleep(2)

# ---------- Dashboard data ----------
def db_connect():
    con=sqlite3.connect(DB_PATH,timeout=10); con.row_factory=sqlite3.Row; return con

def daily_rows():
    start,end,label=local_day_bounds_ms(0); con=db_connect()
    rows=con.execute("""SELECT route_id,route_type,path,start_asset,end_asset,COUNT(*) n,
      AVG(avg_net) avg_net,MAX(peak_net) max_net,AVG(duration_ms) avg_dur,MAX(peak_profit) best_profit,
      SUM(CASE WHEN best_size>=250 THEN 1 ELSE 0 END) s250,
      SUM(CASE WHEN best_size>=500 THEN 1 ELSE 0 END) s500,
      SUM(CASE WHEN best_size>=1000 THEN 1 ELSE 0 END) s1000,
      SUM(CASE WHEN best_size>=2000 THEN 1 ELSE 0 END) s2000
      FROM opportunities WHERE start_ts_ms>=? AND start_ts_ms<? GROUP BY route_id ORDER BY n DESC""",(start,end)).fetchall()
    con.close(); d={r["route_id"]:dict(r) for r in rows}; out=[]; used=set()
    for rid,r in d.items():
        if rid in used: continue
        rev=">".join(reversed(rid.split(">"))); rr=d.get(rev)
        used.add(rid); used.add(rev)
        a=r["n"]; b=rr["n"] if rr else 0
        balance=(2*min(a,b)/(a+b)*100) if a+b else 0
        # show the more active direction first
        if rr and rr["n"]>r["n"]: r,rr=rr,r; a,b=b,a
        out.append({**r,"inverse":rr["route_id"] if rr else rev,"n_inverse":b,"balance":balance})
    out.sort(key=lambda x:x["n"]+x["n_inverse"],reverse=True)
    return out,label

def paper_simulation():
    start,end,label=local_day_bounds_ms(0); con=db_connect()
    rows=con.execute("""SELECT route_id,start_asset,end_asset,start_ts_ms,entry_net,best_size
      FROM opportunities WHERE start_ts_ms>=? AND start_ts_ms<? ORDER BY start_ts_ms,id""",(start,end)).fetchall(); con.close()
    if not STABLES: return {}
    balances={s:SIM_CAPITAL/len(STABLES) for s in STABLES}; trades=0; skipped=0; gross_profit=0.0
    for r in rows:
        s,e=r["start_asset"],r["end_asset"]
        if s not in balances or e not in balances: continue
        amount=min(balances[s],float(r["best_size"] or 0))
        if amount<SIM_MIN_TRADE: skipped+=1; continue
        result=amount*(1.0+float(r["entry_net"]))
        balances[s]-=amount; balances[e]+=result; gross_profit+=result-amount; trades+=1
    nav=sum(balances.values()); ret=(nav/SIM_CAPITAL-1)*100 if SIM_CAPITAL else 0
    return {"capital":SIM_CAPITAL,"nav":nav,"return_pct":ret,"profit":nav-SIM_CAPITAL,"trades":trades,"skipped":skipped,"balances":balances,"label":label}

@app.get("/api/status")
def api_status():
    rows,label=daily_rows(); sim=paper_simulation(); now=now_ms()
    with state_lock:
        age=now-state["last_ws_ms"] if state["last_ws_ms"] else None
        base={"version":VERSION,"symbols":len(state["symbols"]),"routes":len(state["routes"]),"ws":state["ws_connected"],"ws_expected":state["ws_expected"],"age_ms":age,"market_source":state["market_source"],"errors":list(state["errors"]),"scan_updates":state["scan_updates"]}
    base.update({"day":label,"rows":rows[:100],"sim":sim,"fee_pct":FEE*100,"stables":STABLES,"sizes":SIZES,"min_net_pct":MIN_NET*100})
    return jsonify(base)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEXC Spot Routes</title>
<style>body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}h2{font-size:18px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px;max-width:900px}.c,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.v{font-weight:800;font-size:20px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.bad{color:#ff6677}.panel{margin-top:12px;overflow:auto}table{border-collapse:collapse;width:100%;font-size:12px;min-width:1050px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right}th:first-child,td:first-child{text-align:left}.route{font-weight:700}.pill{padding:2px 6px;border-radius:8px;background:#1c2940}.warn{color:#ffcc66}</style></head><body>
<h2>MEXC — Scanner routes Spot V1.0</h2><div class="cards" id="cards"></div><div class="panel" id="sim"></div>
<div class="panel"><h3>Opportunités — aujourd'hui (00:00 → maintenant, Europe/Paris)</h3><div class="muted">Compteurs fixes par journée : ils repartent visuellement à 0 à minuit. La base conserve tout l'historique.</div><table><thead><tr><th>Route dominante</th><th>Legs</th><th>Opp.</th><th>Inverse</th><th>Opp. inverse</th><th>Équilibre</th><th>Net moy.</th><th>Net max</th><th>Durée moy.</th><th>Meilleur profit</th><th>Capacité 250/500/1k/2k</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="panel"><b>Modèle</b><div class="muted" id="model"></div></div>
<script>function pct(x){return (100*x).toFixed(4)+'%'}; async function refresh(){let d=await fetch('/api/status').then(r=>r.json());
let s=d.sim; document.getElementById('cards').innerHTML=`<div class=c><div class=v>${d.symbols}</div><div class=muted>marchés WS</div></div><div class=c><div class=v>${d.routes}</div><div class=muted>routes 2/3 legs</div></div><div class=c><div class=v>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div><div class=c><div class=v>${d.age_ms??'-'} ms</div><div class=muted>âge dernier BBO</div></div><div class=c><div class=v>${d.fee_pct.toFixed(3)}%</div><div class=muted>taker / leg</div></div>`;
document.getElementById('sim').innerHTML=`<h3>Simulation rendement — journée fixe</h3><div class=cards><div><div class='v good'>${s.return_pct.toFixed(3)}%</div><div class=muted>rendement paper</div></div><div><div class=v>${s.nav.toFixed(2)} $</div><div class=muted>NAV sur ${s.capital.toFixed(0)} $ initiaux</div></div><div><div class=v>${s.profit.toFixed(2)} $</div><div class=muted>profit théorique</div></div><div><div class=v>${s.trades}</div><div class=muted>opportunités simulées</div></div></div><div class=muted>Capital initial réparti entre les stablecoins. Une route à sens unique déplace réellement le capital du stablecoin de départ vers celui d'arrivée : quand le bucket source est vide, la simulation cesse de prendre ces opportunités jusqu'à ce qu'un sens inverse le réalimente. Soldes: ${Object.entries(s.balances).map(([k,v])=>k+' '+v.toFixed(2)).join(' · ')}</div>`;
let h=''; for(let r of d.rows){let n=r.n+r.n_inverse; h+=`<tr><td class=route>${r.route_id}</td><td>${r.route_type}</td><td>${r.n}</td><td>${r.inverse}</td><td>${r.n_inverse}</td><td>${r.balance.toFixed(1)}%</td><td class=good>${pct(r.avg_net)}</td><td class=good>${pct(r.max_net)}</td><td>${Math.round(r.avg_dur)} ms</td><td>${r.best_profit.toFixed(3)} $</td><td>${r.s250}/${r.s500}/${r.s1000}/${r.s2000}</td></tr>`} document.getElementById('rows').innerHTML=h;
document.getElementById('model').innerHTML=`Spot uniquement. Frais conservateurs: ${d.fee_pct}% taker à chaque leg. Seuls bid/ask exécutables au top-of-book sont utilisés; si la quantité BBO ne suffit pas, la taille est rejetée. Tailles testées: ${d.sizes.join(' / ')} $. Stables: ${d.stables.join(', ')}. Seuil d'enregistrement: +${d.min_net_pct.toFixed(3)}% net. La simulation est indicative et n'est pas un backtest d'ordres réellement exécutés.`}
refresh();setInterval(refresh,3000)</script></body></html>'''

@app.get("/")
def index(): return render_template_string(HTML)


def bootstrap():
    threading.Thread(target=db_writer,daemon=True).start()
    markets=discover_markets(); chosen,routes,meta=select_symbols_and_routes(markets)
    route_by_id={r["id"]:r for r in routes}; bysym=defaultdict(list)
    for r in routes:
        for s in r["symbols"]: bysym[s].append(r)
    symbols=[m["symbol"] for m in chosen]
    with state_lock:
        state["symbols"]=symbols; state["routes"]=routes; state["route_by_id"]=route_by_id; state["routes_by_symbol"]=bysym; state["market_meta"]=meta
    groups=[symbols[i:i+30] for i in range(0,len(symbols),30)]
    with state_lock: state["ws_expected"]=len(groups)
    print(f"[Routes V{VERSION}] market source={state['market_source']} selected={len(symbols)} routes={len(routes)} WS={len(groups)}")
    put_db(("meta",("version",VERSION))); put_db(("meta",("market_source",state["market_source"])))
    for i in range(4): threading.Thread(target=scan_worker,daemon=True).start()
    threading.Thread(target=event_sweeper,daemon=True).start()
    for i,g in enumerate(groups,1): threading.Thread(target=ws_worker,args=(g,i),daemon=True).start()

if __name__=="__main__":
    bootstrap(); app.run(host=HOST,port=PORT,threaded=True,use_reloader=False)

#!/usr/bin/env python3
import os, json, time, math, queue, sqlite3, threading, requests
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string
import websocket

VERSION = "2.0"
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8081"))
DB_PATH = os.getenv("DB_PATH", "mexc_routes_v2.db")
MARKET_CACHE = os.getenv("MARKET_CACHE", "mexc_markets_cache.json")
TZ_NAME = os.getenv("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)

# Trading assumptions
FEE = float(os.getenv("TAKER_FEE", "0.0005"))            # 0.05% taker / leg
STABLES = tuple(x.strip().upper() for x in os.getenv("STABLES", "USDT,USDC,USD1").split(",") if x.strip())
MIN_EXEC_USD = float(os.getenv("MIN_EXEC_USD", "10"))
MAX_EXEC_USD = float(os.getenv("MAX_EXEC_USD", "0"))     # 0 = no artificial cap
MIN_NET = float(os.getenv("MIN_NET_PCT", "0.01")) / 100.0
MAX_BBO_AGE_MS = int(os.getenv("MAX_BBO_AGE_MS", "750"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "750"))
MAX_WS_SYMBOLS = int(os.getenv("MAX_WS_SYMBOLS", "600"))
MAX_ROUTES_PER_SYMBOL_SCAN = int(os.getenv("MAX_ROUTES_PER_SYMBOL_SCAN", "10000"))

# Realistic paper-execution model
SIM_CAPITAL = float(os.getenv("SIM_CAPITAL", "2000"))
SIM_MIN_TRADE_USD = float(os.getenv("SIM_MIN_TRADE_USD", "10"))
SIM_EXEC_LATENCY_MS = int(os.getenv("SIM_EXEC_LATENCY_MS", "100"))
SIM_MIN_CONFIRM_TICKS = int(os.getenv("SIM_MIN_CONFIRM_TICKS", "2"))
SIM_DONOR_RESERVE_PCT = float(os.getenv("SIM_DONOR_RESERVE_PCT", "0.10"))
REBALANCE_MAX_PROFIT_SHARE = float(os.getenv("REBALANCE_MAX_PROFIT_SHARE", "0.80"))
REBALANCE_EDGE_MULT = float(os.getenv("REBALANCE_EDGE_MULT", "1.25"))

# Historical research data
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "2.0"))
SNAPSHOT_TOP_N = int(os.getenv("SNAPSHOT_TOP_N", "50"))
STABLE_QUOTE_INTERVAL_SEC = float(os.getenv("STABLE_QUOTE_INTERVAL_SEC", "1.0"))

REST = "https://api.mexc.com"
WS = "wss://wbs-api.mexc.com/ws"

app = Flask(__name__)
state_lock = threading.RLock()
event_lock = threading.RLock()
paper_lock = threading.RLock()
state = {
    "bbo": {}, "last_ws_ms": 0, "ws_connected": 0, "ws_expected": 0,
    "symbols": [], "routes": [], "routes_by_symbol": defaultdict(list), "market_meta": {},
    "errors": deque(maxlen=30), "started_ms": int(time.time()*1000), "market_source": "",
    "scan_updates": 0, "latest_routes": {}, "route_diag": {},
}
active_events = {}
dbq = queue.Queue(maxsize=50000)
scanq = queue.Queue(maxsize=30000)
queued_symbols = set()
queued_lock = threading.Lock()
paper = {}


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
      entry_size REAL NOT NULL,
      max_size REAL NOT NULL,
      entry_profit REAL NOT NULL,
      peak_profit REAL NOT NULL,
      max_bbo_age_ms INTEGER NOT NULL,
      confirmed INTEGER NOT NULL DEFAULT 0,
      confirm_ts_ms INTEGER,
      confirm_net REAL,
      confirm_size REAL,
      confirm_profit REAL,
      confirm_ticks INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_opp_start ON opportunities(start_ts_ms);
    CREATE INDEX IF NOT EXISTS idx_opp_route ON opportunities(route_id,start_ts_ms);

    CREATE TABLE IF NOT EXISTS paper_trades(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      ts_ms INTEGER NOT NULL,
      route_id TEXT NOT NULL,
      start_asset TEXT NOT NULL,
      end_asset TEXT NOT NULL,
      input_units REAL NOT NULL,
      input_usd REAL NOT NULL,
      output_units REAL NOT NULL,
      output_usd REAL NOT NULL,
      net_pct REAL NOT NULL,
      profit_usd REAL NOT NULL,
      event_start_ts_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_paper_day ON paper_trades(day,ts_ms);

    CREATE TABLE IF NOT EXISTS paper_rebalances(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      ts_ms INTEGER NOT NULL,
      from_asset TEXT NOT NULL,
      to_asset TEXT NOT NULL,
      input_units REAL NOT NULL,
      output_units REAL NOT NULL,
      input_usd REAL NOT NULL,
      output_usd REAL NOT NULL,
      cost_usd REAL NOT NULL,
      profit_bank_before REAL NOT NULL,
      unlocked_expected_profit REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_reb_day ON paper_rebalances(day,ts_ms);

    CREATE TABLE IF NOT EXISTS paper_state(
      day TEXT PRIMARY KEY,
      updated_ts_ms INTEGER NOT NULL,
      balances_json TEXT NOT NULL,
      profit_bank REAL NOT NULL,
      trades INTEGER NOT NULL,
      rebalances INTEGER NOT NULL,
      rebalance_cost REAL NOT NULL,
      skipped_flash INTEGER NOT NULL,
      skipped_balance INTEGER NOT NULL,
      skipped_rebalance INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS route_snapshots(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_ms INTEGER NOT NULL,
      route_id TEXT NOT NULL,
      route_type INTEGER NOT NULL,
      net REAL NOT NULL,
      executable_usd REAL NOT NULL,
      max_bbo_age_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_snap_ts ON route_snapshots(ts_ms);

    CREATE TABLE IF NOT EXISTS stable_quotes(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_ms INTEGER NOT NULL,
      from_asset TEXT NOT NULL,
      to_asset TEXT NOT NULL,
      rate_after_fee REAL NOT NULL,
      max_input_units REAL NOT NULL,
      mark_from_usdt REAL NOT NULL,
      mark_to_usdt REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_stable_ts ON stable_quotes(ts_ms);
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
                  ticks,entry_net,peak_net,avg_net,entry_size,max_size,entry_profit,peak_profit,max_bbo_age_ms,
                  confirmed,confirm_ts_ms,confirm_net,confirm_size,confirm_profit,confirm_ticks)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "meta":
                con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", payload)
            elif typ == "paper_trade":
                con.execute("""INSERT INTO paper_trades(day,ts_ms,route_id,start_asset,end_asset,input_units,input_usd,
                    output_units,output_usd,net_pct,profit_usd,event_start_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "paper_rebalance":
                con.execute("""INSERT INTO paper_rebalances(day,ts_ms,from_asset,to_asset,input_units,output_units,
                    input_usd,output_usd,cost_usd,profit_bank_before,unlocked_expected_profit) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "paper_state":
                con.execute("""INSERT OR REPLACE INTO paper_state(day,updated_ts_ms,balances_json,profit_bank,trades,rebalances,
                    rebalance_cost,skipped_flash,skipped_balance,skipped_rebalance) VALUES(?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "snapshot_many":
                con.executemany("INSERT INTO route_snapshots(ts_ms,route_id,route_type,net,executable_usd,max_bbo_age_ms) VALUES(?,?,?,?,?,?)", payload)
            elif typ == "stable_many":
                con.executemany("INSERT INTO stable_quotes(ts_ms,from_asset,to_asset,rate_after_fee,max_input_units,mark_from_usdt,mark_to_usdt) VALUES(?,?,?,?,?,?,?)", payload)
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
    headers={"User-Agent":"Mozilla/5.0 mexc-routes-scanner/2.0", "Accept":"application/json"}
    for i in range(4):
        try:
            r=requests.get(REST+"/api/v3/exchangeInfo", headers=headers, timeout=15)
            r.raise_for_status(); data=r.json(); out=[]
            for s in data.get("symbols",[]):
                status=str(s.get("status","")).upper()
                if status not in ("1","ENABLED","TRADING",""): continue
                base=str(s.get("baseAsset","")).upper(); quote=str(s.get("quoteAsset","")).upper(); sym=str(s.get("symbol","")).upper()
                if base and quote and sym and base!=quote: out.append({"symbol":sym,"base":base,"quote":quote})
            if out:
                save_market_cache(out); state["market_source"]="MEXC v3 exchangeInfo"; return out
        except Exception as e:
            logerr(f"exchangeInfo try {i+1}: {e}"); time.sleep(2**i)
    try:
        r=requests.get("https://www.mexc.com/open/api/v2/market/symbols",headers=headers,timeout=20)
        r.raise_for_status(); data=r.json(); out=[]
        for s in data.get("data",[]):
            raw=str(s.get("symbol","")).upper(); status=str(s.get("state","")).upper()
            if status not in ("ENABLED","1","") or "_" not in raw: continue
            base,quote=raw.split("_",1)
            if base and quote and base!=quote: out.append({"symbol":base+quote,"base":base,"quote":quote})
        if out:
            save_market_cache(out); state["market_source"]="MEXC v2 symbols fallback"; return out
    except Exception as e: logerr(f"v2 symbols fallback: {e}")
    cached=load_market_cache()
    if cached:
        state["market_source"]="local market cache"; return cached
    raise RuntimeError("Impossible de charger la liste des marchés MEXC et aucun cache local n'existe.")

# ---------- Route graph V2 ----------
def market_lookup(markets):
    bysym={m["symbol"]:m for m in markets if m.get("symbol") and m.get("base") and m.get("quote") and m["base"]!=m["quote"]}
    pair={}
    graph=defaultdict(list)
    for m in bysym.values():
        a,b=m["base"],m["quote"]
        pair[frozenset((a,b))]=m["symbol"]
        graph[a].append((b,m["symbol"])); graph[b].append((a,m["symbol"]))
    return bysym,pair,graph

def build_all_candidate_routes(markets):
    bysym,pair,graph=market_lookup(markets)
    stable_set=set(STABLES)
    routes={}

    # 2-leg routes: stable -> asset -> different stable.
    # Generate directly from each non-stable asset's stable neighbours.
    stable_neigh=defaultdict(list)
    for asset,edges in graph.items():
        if asset in stable_set: continue
        for n,sym in edges:
            if n in stable_set: stable_neigh[asset].append((n,sym))
    for a,edges in stable_neigh.items():
        for s1,sym1 in edges:
            for s2,sym2 in edges:
                if s1==s2 or sym1==sym2: continue
                path=(s1,a,s2); rid=">".join(path)
                routes[rid]={"id":rid,"path":path,"symbols":(sym1,sym2),"type":2}

    # 3-leg routes: stable -> A -> B -> stable, same or different stable allowed.
    # Build from every A/B cross market where both A and B have at least one stable market.
    for m in bysym.values():
        a,b=m["base"],m["quote"]
        if a in stable_set or b in stable_set: continue
        if not stable_neigh.get(a) or not stable_neigh.get(b): continue
        cross=m["symbol"]
        for s1,sym1 in stable_neigh[a]:
            for s2,sym3 in stable_neigh[b]:
                if len({sym1,cross,sym3})<3: continue
                p=(s1,a,b,s2); rid=">".join(p)
                routes[rid]={"id":rid,"path":p,"symbols":(sym1,cross,sym3),"type":3}
        for s1,sym1 in stable_neigh[b]:
            for s2,sym3 in stable_neigh[a]:
                if len({sym1,cross,sym3})<3: continue
                p=(s1,b,a,s2); rid=">".join(p)
                routes[rid]={"id":rid,"path":p,"symbols":(sym1,cross,sym3),"type":3}
    return list(routes.values()), bysym, graph

def select_routes_under_symbol_budget(markets):
    candidates,bysym,graph=build_all_candidate_routes(markets)
    r2=[r for r in candidates if r["type"]==2]
    r3=[r for r in candidates if r["type"]==3]

    selected=[]; selected_ids=set(); symbols=set()

    # Always include direct stable/stable markets if available: needed for marks and intelligent rebalancing.
    direct_stable=[]
    for m in bysym.values():
        if m["base"] in STABLES and m["quote"] in STABLES:
            direct_stable.append(m["symbol"])
    for s in direct_stable:
        if len(symbols)<MAX_WS_SYMBOLS: symbols.add(s)

    # V1.1's bug was market-first selection. V2 selects complete ROUTES.
    # First preserve every 2-leg route that fits. They are cheap (normally 2 symbols) and directly useful.
    # Prefer routes reusing already selected symbols.
    remaining=list(r2)
    while remaining:
        remaining.sort(key=lambda r:(len(set(r["symbols"])-symbols), r["id"]))
        r=remaining.pop(0)
        new=set(r["symbols"])-symbols
        if len(symbols)+len(new)>MAX_WS_SYMBOLS: continue
        selected.append(r); selected_ids.add(r["id"]); symbols.update(r["symbols"])

    # Then add 3-leg routes. Score once by graph reuse/connectivity so startup remains fast
    # even if the full MEXC graph produces tens of thousands of candidates.
    freq=defaultdict(int)
    for r in r3:
        for s in r["symbols"]: freq[s]+=1
    remaining=[r for r in r3 if r["id"] not in selected_ids]
    remaining.sort(key=lambda r:(
        sum(1 for s in r["symbols"] if s in symbols),
        1 if r["path"][0]==r["path"][-1] else 0,
        sum(freq[s] for s in r["symbols"])
    ), reverse=True)
    for r in remaining:
        if len(symbols)>=MAX_WS_SYMBOLS: break
        new=set(r["symbols"])-symbols
        if len(symbols)+len(new)>MAX_WS_SYMBOLS: continue
        selected.append(r); selected_ids.add(r["id"]); symbols.update(r["symbols"])

    chosen=[bysym[s] for s in symbols if s in bysym]
    selected2=sum(1 for r in selected if r["type"]==2)
    selected3=sum(1 for r in selected if r["type"]==3)
    diag={
        "all_markets":len(bysym),
        "candidate_routes":len(candidates),
        "candidate_2leg":len(r2),
        "candidate_3leg":len(r3),
        "selected_symbols":len(chosen),
        "selected_routes":len(selected),
        "selected_2leg":selected2,
        "selected_3leg":selected3,
        "dropped_routes":len(candidates)-len(selected),
        "direct_stable_symbols":len(direct_stable),
    }
    return chosen, selected, bysym, diag

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
def direct_pair(asset1,asset2,meta):
    for sym,m in meta.items():
        if {m["base"],m["quote"]}=={asset1,asset2}: return sym,m
    return None,None

def stable_mark_usdt(asset,books,meta):
    # Valuation mark only: NO trading fee. Use current mid; fallback peg only if no fresh direct USDT book.
    if asset=="USDT": return 1.0
    sym,m=direct_pair(asset,"USDT",meta)
    if sym:
        b=books.get(sym)
        if b and now_ms()-b["ts"]<=MAX_BBO_AGE_MS and b["bid"]>0 and b["ask"]>0:
            mid=(b["bid"]+b["ask"])/2.0
            return mid if m["base"]==asset else 1.0/mid
    return 1.0

def route_calc(route,books,meta):
    start,end=route["path"][0],route["path"][-1]
    sv=stable_mark_usdt(start,books,meta); ev=stable_mark_usdt(end,books,meta)
    if sv<=0 or ev<=0: return None
    multiplier=1.0; max_start=float("inf"); ts=now_ms(); max_age=0
    symbols=route["symbols"]
    if len(symbols)!=len(route["path"])-1: return None
    for i,sym in enumerate(symbols):
        b=books.get(sym); m=meta.get(sym)
        if not b or not m: return None
        age=ts-b["ts"]; max_age=max(max_age,age)
        if age>MAX_BBO_AGE_MS: return None
        frm,to=route["path"][i],route["path"][i+1]
        if frm==m["base"] and to==m["quote"]:
            cap_in=b["bidq"]; rate=b["bid"]*(1.0-FEE)
        elif frm==m["quote"] and to==m["base"]:
            cap_in=b["ask"]*b["askq"]; rate=(1.0/b["ask"])*(1.0-FEE)
        else: return None
        if cap_in<=0 or rate<=0 or multiplier<=0: return None
        max_start=min(max_start,cap_in/multiplier); multiplier*=rate
    if not math.isfinite(max_start) or max_start<=0: return None
    executable_usd=max_start*sv
    if MAX_EXEC_USD>0: executable_usd=min(executable_usd,MAX_EXEC_USD)
    if executable_usd<MIN_EXEC_USD: return None
    net=multiplier*ev/sv-1.0
    return {"net":net,"size":executable_usd,"profit":executable_usd*net,"out_ratio":multiplier,
            "start_mark":sv,"end_mark":ev,"max_age":int(max_age)}

def stable_conversion(frm,to,input_units,books,meta):
    if frm==to: return None
    sym,m=direct_pair(frm,to,meta)
    if not sym: return None
    b=books.get(sym)
    if not b or now_ms()-b["ts"]>MAX_BBO_AGE_MS: return None
    if frm==m["base"] and to==m["quote"]:
        max_input=b["bidq"]; rate=b["bid"]*(1.0-FEE)
    elif frm==m["quote"] and to==m["base"]:
        max_input=b["ask"]*b["askq"]; rate=(1.0/b["ask"])*(1.0-FEE)
    else: return None
    use=min(input_units,max_input)
    if use<=0: return None
    out=use*rate
    mf=stable_mark_usdt(frm,books,meta); mt=stable_mark_usdt(to,books,meta)
    in_usd=use*mf; out_usd=out*mt
    return {"symbol":sym,"input":use,"output":out,"rate":rate,"input_usd":in_usd,"output_usd":out_usd,
            "cost":max(0.0,in_usd-out_usd),"max_input":max_input,"mark_from":mf,"mark_to":mt}

# ---------- Paper engine ----------
def paper_default(day):
    each=SIM_CAPITAL/len(STABLES) if STABLES else 0.0
    return {"day":day,"balances":{s:each for s in STABLES},"profit_bank":0.0,"trades":0,"rebalances":0,
            "rebalance_cost":0.0,"skipped_flash":0,"skipped_balance":0,"skipped_rebalance":0}

def load_paper_state():
    global paper
    _,_,day=local_day_bounds_ms(0)
    try:
        con=sqlite3.connect(DB_PATH,timeout=10)
        row=con.execute("SELECT * FROM paper_state WHERE day=?",(day,)).fetchone(); con.close()
        if row:
            # sqlite default tuple order follows schema
            paper={"day":row[0],"balances":json.loads(row[2]),"profit_bank":float(row[3]),"trades":int(row[4]),
                   "rebalances":int(row[5]),"rebalance_cost":float(row[6]),"skipped_flash":int(row[7]),
                   "skipped_balance":int(row[8]),"skipped_rebalance":int(row[9])}
        else: paper=paper_default(day)
    except Exception:
        paper=paper_default(day)

def ensure_paper_day():
    global paper
    _,_,day=local_day_bounds_ms(0)
    if not paper or paper.get("day")!=day:
        paper=paper_default(day)
        persist_paper()

def persist_paper():
    put_db(("paper_state",(paper["day"],now_ms(),json.dumps(paper["balances"],sort_keys=True),paper["profit_bank"],
                           paper["trades"],paper["rebalances"],paper["rebalance_cost"],paper["skipped_flash"],
                           paper["skipped_balance"],paper["skipped_rebalance"])))

def choose_rebalance(target_asset,needed_units,event_x,books,meta):
    # Rebalancing is allowed only if already-earned profit since the previous rebalance pays for it.
    # It must also unlock enough profit on the CURRENT confirmed arbitrage to justify the cost.
    target_mark=stable_mark_usdt(target_asset,books,meta)
    if target_mark<=0 or needed_units<=0 or paper["profit_bank"]<=0: return None
    initial_each=SIM_CAPITAL/len(STABLES)
    candidates=[]
    for donor,bal in paper["balances"].items():
        if donor==target_asset: continue
        donor_mark=stable_mark_usdt(donor,books,meta)
        reserve_units=(initial_each*SIM_DONOR_RESERVE_PCT)/max(donor_mark,1e-12)
        available=max(0.0,bal-reserve_units)
        if available<=0: continue
        # rate first using tiny probe, then calculate input needed for requested target units.
        probe=stable_conversion(donor,target_asset,available,books,meta)
        if not probe or probe["rate"]<=0: continue
        input_need=needed_units/probe["rate"]
        conv=stable_conversion(donor,target_asset,min(available,input_need),books,meta)
        if not conv: continue
        delivered=conv["output"]
        unlocked_usd=delivered*target_mark
        expected=unlocked_usd*max(event_x["net"],0.0)
        budget=paper["profit_bank"]*REBALANCE_MAX_PROFIT_SHARE
        if conv["cost"]>budget+1e-12: continue
        if conv["cost"]>0 and expected < conv["cost"]*REBALANCE_EDGE_MULT: continue
        candidates.append((expected-conv["cost"],donor,conv,expected))
    if not candidates: return None
    candidates.sort(reverse=True,key=lambda x:x[0])
    _,donor,conv,expected=candidates[0]
    return donor,conv,expected

def paper_execute_confirmed(route,event_x,event_start_ts):
    with paper_lock:
        ensure_paper_day()
        with state_lock:
            books=dict(state["bbo"]); meta=state["market_meta"]
        s,e=route["path"][0],route["path"][-1]
        if s not in paper["balances"] or e not in paper["balances"]: return False
        sm=event_x["start_mark"]; em=event_x["end_mark"]
        max_input_units=event_x["size"]/max(sm,1e-12)
        have=paper["balances"].get(s,0.0)
        desired=max_input_units
        shortage=max(0.0,desired-have)

        if shortage>0:
            rb=choose_rebalance(s,shortage,event_x,books,meta)
            if rb:
                donor,conv,expected=rb
                bank_before=paper["profit_bank"]
                paper["balances"][donor]-=conv["input"]
                paper["balances"][s]+=conv["output"]
                paper["rebalances"]+=1; paper["rebalance_cost"]+=conv["cost"]
                # Conservative rule requested: every rebalance must be financed by profits earned since the previous one.
                # After using that interval's profit bank, reset it; next rebalance needs fresh profits.
                paper["profit_bank"]=0.0
                put_db(("paper_rebalance",(paper["day"],now_ms(),donor,s,conv["input"],conv["output"],conv["input_usd"],
                    conv["output_usd"],conv["cost"],bank_before,expected)))
                have=paper["balances"].get(s,0.0)
            else:
                paper["skipped_rebalance"]+=1

        input_units=min(have,max_input_units)
        input_usd=input_units*sm
        if input_usd<SIM_MIN_TRADE_USD:
            paper["skipped_balance"]+=1; persist_paper(); return False
        output_units=input_units*event_x["out_ratio"]
        output_usd=output_units*em
        profit=output_usd-input_usd
        paper["balances"][s]-=input_units; paper["balances"][e]+=output_units
        paper["trades"]+=1; paper["profit_bank"]+=profit
        put_db(("paper_trade",(paper["day"],now_ms(),route["id"],s,e,input_units,input_usd,output_units,output_usd,
                                event_x["net"]*100.0,profit,event_start_ts)))
        persist_paper(); return True

# ---------- Event aggregation ----------
def close_event(key,ev,end_ts=None):
    end_ts=int(end_ts or ev["last_ts"]); avg=ev["sum_net"]/max(ev["ticks"],1)
    payload=(ev["route_id"],ev["type"],ev["path"],ev["start_asset"],ev["end_asset"],ev["start_ts"],end_ts,
             max(0,end_ts-ev["start_ts"]),ev["ticks"],ev["entry_net"],ev["peak_net"],avg,ev["entry_size"],ev["max_size"],
             ev["entry_profit"],ev["peak_profit"],ev["max_bbo_age"],1 if ev.get("confirmed") else 0,
             ev.get("confirm_ts"),ev.get("confirm_net"),ev.get("confirm_size"),ev.get("confirm_profit"),ev.get("confirm_ticks"))
    put_db(("event",payload))

def process_route(route,ts):
    with state_lock:
        books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta)
    with state_lock:
        if x: state["latest_routes"][route["id"]]={"ts":ts,"net":x["net"],"size":x["size"],"type":route["type"],"max_age":x["max_age"]}
    key=route["id"]
    with event_lock:
        ev=active_events.get(key)
        if x and x["net"]>=MIN_NET:
            if ev is None:
                ev={"route_id":route["id"],"type":route["type"],"path":">".join(route["path"]),"start_asset":route["path"][0],
                    "end_asset":route["path"][-1],"start_ts":ts,"last_ts":ts,"ticks":1,"entry_net":x["net"],"peak_net":x["net"],
                    "sum_net":x["net"],"entry_size":x["size"],"max_size":x["size"],"entry_profit":x["profit"],"peak_profit":x["profit"],
                    "max_bbo_age":x["max_age"],"confirmed":False}
                active_events[key]=ev
            else:
                ev["last_ts"]=ts; ev["ticks"]+=1; ev["sum_net"]+=x["net"]; ev["max_bbo_age"]=max(ev["max_bbo_age"],x["max_age"])
                ev["peak_net"]=max(ev["peak_net"],x["net"]); ev["max_size"]=max(ev["max_size"],x["size"]); ev["peak_profit"]=max(ev["peak_profit"],x["profit"])

            if (not ev["confirmed"] and ev["ticks"]>=SIM_MIN_CONFIRM_TICKS and ts-ev["start_ts"]>=SIM_EXEC_LATENCY_MS):
                ev["confirmed"]=True; ev["confirm_ts"]=ts; ev["confirm_net"]=x["net"]; ev["confirm_size"]=x["size"]
                ev["confirm_profit"]=x["profit"]; ev["confirm_ticks"]=ev["ticks"]
                paper_execute_confirmed(route,x,ev["start_ts"])
        elif ev is not None:
            if not ev.get("confirmed"):
                with paper_lock:
                    ensure_paper_day(); paper["skipped_flash"]+=1; persist_paper()
            close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

def event_sweeper():
    while True:
        time.sleep(.25); ts=now_ms()
        with event_lock:
            for key,ev in list(active_events.items()):
                if ts-ev["last_ts"]>EVENT_CLOSE_GAP_MS:
                    if not ev.get("confirmed"):
                        with paper_lock:
                            ensure_paper_day(); paper["skipped_flash"]+=1; persist_paper()
                    close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

# ---------- Historical snapshots ----------
def snapshot_worker():
    while True:
        time.sleep(SNAPSHOT_INTERVAL_SEC)
        ts=now_ms()
        with state_lock:
            vals=[(rid,x) for rid,x in state["latest_routes"].items() if ts-x["ts"]<=3000]
        vals.sort(key=lambda kv:kv[1]["net"],reverse=True)
        rows=[(ts,rid,x["type"],x["net"],x["size"],x["max_age"]) for rid,x in vals[:SNAPSHOT_TOP_N]]
        if rows: put_db(("snapshot_many",rows))

def stable_quote_worker():
    while True:
        time.sleep(STABLE_QUOTE_INTERVAL_SEC); ts=now_ms()
        with state_lock:
            books=dict(state["bbo"]); meta=state["market_meta"]
        rows=[]
        for a in STABLES:
            for b in STABLES:
                if a==b: continue
                conv=stable_conversion(a,b,1e18,books,meta)
                if not conv: continue
                rows.append((ts,a,b,conv["rate"],conv["max_input"],conv["mark_from"],conv["mark_to"]))
        if rows: put_db(("stable_many",rows))

# ---------- Scan queue ----------
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

# ---------- Dashboard ----------
def db_connect():
    con=sqlite3.connect(DB_PATH,timeout=10); con.row_factory=sqlite3.Row; return con

def daily_rows():
    start,end,label=local_day_bounds_ms(0); con=db_connect()
    rows=con.execute("""SELECT route_id,route_type,path,start_asset,end_asset,COUNT(*) n,
      SUM(CASE WHEN confirmed=1 THEN 1 ELSE 0 END) confirmed_n,
      SUM(CASE WHEN ticks=1 THEN 1 ELSE 0 END) flash_n,
      AVG(avg_net) avg_net,MAX(peak_net) max_net,AVG(duration_ms) avg_dur,MAX(peak_profit) best_profit,
      AVG(max_size) avg_size,MAX(max_size) max_size,AVG(max_bbo_age_ms) avg_age
      FROM opportunities WHERE start_ts_ms>=? AND start_ts_ms<? GROUP BY route_id ORDER BY n DESC""",(start,end)).fetchall()
    con.close(); d={r["route_id"]:dict(r) for r in rows}; out=[]; used=set()
    for rid,r in d.items():
        if rid in used: continue
        rev=">".join(reversed(rid.split(">"))); rr=d.get(rev); used.add(rid); used.add(rev)
        a=r["n"]; b=rr["n"] if rr else 0; balance=(2*min(a,b)/(a+b)*100) if a+b else 0
        if rr and rr["n"]>r["n"]: r,rr=rr,r; a,b=b,a
        out.append({**r,"inverse":rr["route_id"] if rr else rev,"n_inverse":b,"balance":balance})
    out.sort(key=lambda x:x["n"]+x["n_inverse"],reverse=True)
    return out,label

def paper_status():
    with paper_lock:
        ensure_paper_day(); p=json.loads(json.dumps(paper))
    with state_lock:
        books=dict(state["bbo"]); meta=state["market_meta"]
    nav=0.0
    for s,v in p["balances"].items(): nav+=v*stable_mark_usdt(s,books,meta)
    p["nav"]=nav; p["capital"]=SIM_CAPITAL; p["profit"]=nav-SIM_CAPITAL; p["return_pct"]=(nav/SIM_CAPITAL-1)*100 if SIM_CAPITAL else 0
    return p

@app.get("/api/status")
def api_status():
    rows,label=daily_rows(); sim=paper_status(); now=now_ms()
    with state_lock:
        age=now-state["last_ws_ms"] if state["last_ws_ms"] else None
        base={"version":VERSION,"symbols":len(state["symbols"]),"routes":len(state["routes"]),"ws":state["ws_connected"],
              "ws_expected":state["ws_expected"],"age_ms":age,"market_source":state["market_source"],"errors":list(state["errors"]),
              "scan_updates":state["scan_updates"],"diag":state["route_diag"]}
    base.update({"day":label,"rows":rows[:100],"sim":sim,"fee_pct":FEE*100,"stables":STABLES,"min_exec_usd":MIN_EXEC_USD,
                 "max_exec_usd":MAX_EXEC_USD,"min_net_pct":MIN_NET*100,"latency_ms":SIM_EXEC_LATENCY_MS,"confirm_ticks":SIM_MIN_CONFIRM_TICKS,
                 "rebalance_share":REBALANCE_MAX_PROFIT_SHARE*100,"rebalance_edge_mult":REBALANCE_EDGE_MULT})
    return jsonify(base)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEXC Spot Routes V2</title>
<style>body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}h2{font-size:18px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.c,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.v{font-weight:800;font-size:20px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.warn{color:#ffcc66}.bad{color:#ff6677}.panel{margin-top:12px;overflow:auto}table{border-collapse:collapse;width:100%;font-size:12px;min-width:1300px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right}th:first-child,td:first-child{text-align:left}.route{font-weight:700}.tag{padding:2px 6px;border-radius:7px;background:#1c2940}</style></head><body>
<h2>MEXC — Scanner routes Spot V2.0</h2><div class="cards" id="cards"></div><div class="panel" id="sim"></div><div class="panel" id="diag"></div>
<div class="panel"><h3>Opportunités — aujourd'hui (00:00 → maintenant, Europe/Paris)</h3><div class="muted">Journée fixe. La base n'est jamais remise à zéro. La simulation réaliste n'exécute qu'une opportunité confirmée après la latence configurée.</div><table><thead><tr><th>Route dominante</th><th>Legs</th><th>Opp.</th><th>Inverse</th><th>Opp. inv.</th><th>Équilibre</th><th>Confirmées</th><th>Flash 1 tick</th><th>Net moy.</th><th>Net max</th><th>Durée moy.</th><th>Meilleur profit</th><th>Taille moy.</th><th>Taille max</th><th>Âge BBO moy.</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="panel"><b>Modèle V2</b><div class="muted" id="model"></div></div>
<script>function pct(x){return (100*x).toFixed(4)+'%'}; async function refresh(){let d=await fetch('/api/status').then(r=>r.json()); let s=d.sim, g=d.diag;
document.getElementById('cards').innerHTML=`<div class=c><div class=v>${d.symbols}</div><div class=muted>marchés WS</div></div><div class=c><div class=v>${d.routes}</div><div class=muted>routes sélectionnées</div></div><div class=c><div class=v>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div><div class=c><div class=v>${d.age_ms??'-'} ms</div><div class=muted>âge dernier BBO</div></div><div class=c><div class=v>${d.latency_ms} ms</div><div class=muted>latence paper</div></div>`;
document.getElementById('sim').innerHTML=`<h3>Simulation nette — journée fixe</h3><div class=cards><div><div class='v ${s.return_pct>=0?'good':'bad'}'>${s.return_pct.toFixed(3)}%</div><div class=muted>rendement paper net</div></div><div><div class=v>${s.nav.toFixed(2)} $</div><div class=muted>NAV / ${s.capital.toFixed(0)} $ initiaux</div></div><div><div class=v>${s.profit.toFixed(2)} $</div><div class=muted>profit net simulé</div></div><div><div class=v>${s.trades}</div><div class=muted>trades confirmés</div></div><div><div class=v>${s.rebalances}</div><div class=muted>rééquilibrages</div></div><div><div class=v>${s.rebalance_cost.toFixed(3)} $</div><div class=muted>coût rééquilibrage</div></div></div><div class=muted>Soldes: ${Object.entries(s.balances).map(([k,v])=>k+' '+v.toFixed(2)).join(' · ')} · Profit-bank depuis dernier rééquilibrage: ${s.profit_bank.toFixed(3)} $ · Flashs rejetés: ${s.skipped_flash} · Manque de solde: ${s.skipped_balance} · Rééquilibrage refusé: ${s.skipped_rebalance}</div>`;
document.getElementById('diag').innerHTML=`<h3>Univers & diagnostic routes</h3><div class=cards><div><div class=v>${g.all_markets}</div><div class=muted>marchés découverts</div></div><div><div class=v>${g.candidate_routes}</div><div class=muted>routes candidates totales</div></div><div><div class=v>${g.candidate_2leg}</div><div class=muted>candidates 2-leg</div></div><div><div class=v>${g.candidate_3leg}</div><div class=muted>candidates 3-leg</div></div><div><div class=v>${g.selected_2leg}</div><div class=muted>2-leg suivies</div></div><div><div class=v>${g.selected_3leg}</div><div class=muted>3-leg suivies</div></div></div><div class=muted>V2 sélectionne des routes complètes avant les symboles. Cela corrige la V1.1 qui remplissait d'abord le quota WS avec des marchés liés aux stablecoins et laissait presque aucune place aux marchés A/B nécessaires aux triangles 3-leg.</div>`;
let h=''; for(let r of d.rows){h+=`<tr><td class=route>${r.route_id}</td><td>${r.route_type}</td><td>${r.n}</td><td>${r.inverse}</td><td>${r.n_inverse}</td><td>${r.balance.toFixed(1)}%</td><td>${r.confirmed_n}</td><td>${r.flash_n}</td><td class=good>${pct(r.avg_net)}</td><td class=good>${pct(r.max_net)}</td><td>${Math.round(r.avg_dur)} ms</td><td>${r.best_profit.toFixed(3)} $</td><td>${r.avg_size.toFixed(2)} $</td><td>${r.max_size.toFixed(2)} $</td><td>${Math.round(r.avg_age)} ms</td></tr>`} document.getElementById('rows').innerHTML=h;
document.getElementById('model').innerHTML=`Spot uniquement · ${d.fee_pct.toFixed(3)}% taker/leg · taille dynamique au BBO · minimum ${d.min_exec_usd.toFixed(0)} $ · confirmation paper après ${d.latency_ms} ms et au moins ${d.confirm_ticks} ticks. Les événements 0 ms/1 tick restent enregistrés mais ne sont pas crédités comme exécutables. Rééquilibrage direct entre stablecoins uniquement si son coût réel au BBO est <= ${d.rebalance_share.toFixed(0)}% du profit déjà gagné depuis le précédent rééquilibrage ET si le profit de l'opportunité actuellement débloquée couvre au moins ${d.rebalance_edge_mult.toFixed(2)}x ce coût. Après un rééquilibrage, le profit-bank repart à zéro. Les marks USDT/USDC/USD1 utilisent le mid sans ajouter de frais fictifs.`}
refresh();setInterval(refresh,3000)</script></body></html>'''

@app.get("/")
def index(): return render_template_string(HTML)


def bootstrap():
    threading.Thread(target=db_writer,daemon=True).start()
    # Give DB schema thread a moment before state restore.
    time.sleep(.15); load_paper_state()
    markets=discover_markets(); chosen,routes,meta,diag=select_routes_under_symbol_budget(markets)
    bysym=defaultdict(list)
    for r in routes:
        for s in r["symbols"]: bysym[s].append(r)
    symbols=[m["symbol"] for m in chosen]
    with state_lock:
        state["symbols"]=symbols; state["routes"]=routes; state["routes_by_symbol"]=bysym; state["market_meta"]=meta; state["route_diag"]=diag
    groups=[symbols[i:i+30] for i in range(0,len(symbols),30)]
    with state_lock: state["ws_expected"]=len(groups)
    print(f"[Routes V{VERSION}] source={state['market_source']} markets={diag['all_markets']} candidates={diag['candidate_routes']} "
          f"(2L={diag['candidate_2leg']},3L={diag['candidate_3leg']}) selected_symbols={len(symbols)} selected_routes={len(routes)} "
          f"(2L={diag['selected_2leg']},3L={diag['selected_3leg']}) WS={len(groups)}")
    put_db(("meta",("version",VERSION))); put_db(("meta",("market_source",state["market_source"])))
    put_db(("meta",("route_diag",json.dumps(diag,sort_keys=True))))
    for _ in range(6): threading.Thread(target=scan_worker,daemon=True).start()
    threading.Thread(target=event_sweeper,daemon=True).start()
    threading.Thread(target=snapshot_worker,daemon=True).start()
    threading.Thread(target=stable_quote_worker,daemon=True).start()
    for i,g in enumerate(groups,1): threading.Thread(target=ws_worker,args=(g,i),daemon=True).start()

if __name__=="__main__":
    bootstrap(); app.run(host=HOST,port=PORT,threaded=True,use_reloader=False)

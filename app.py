#!/usr/bin/env python3
import os, json, time, math, queue, sqlite3, threading, requests
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string
import websocket

VERSION = "2.3-depth-strict"
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8081"))
DB_PATH = os.getenv("DB_PATH", "mexc_routes_v23.db")
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
# V2.1: no artificial confirmation wait. Decision is immediate at T0.
# Primary paper portfolio: depth-walk sequential execution; total latency split leg1/leg2.
PRIMARY_BBO_AGE_MS = int(os.getenv("PRIMARY_BBO_AGE_MS", "150"))
PRIMARY_EXEC_LATENCY_MS = int(os.getenv("PRIMARY_EXEC_LATENCY_MS", "150"))
PRIMARY_MAX_SKEW_MS = int(os.getenv("PRIMARY_MAX_SKEW_MS", "50"))
RESEARCH_BBO_AGES_MS = tuple(int(x) for x in os.getenv("RESEARCH_BBO_AGES_MS", "50,100,150,200,250,300").split(","))
RESEARCH_LATENCIES_MS = tuple(int(x) for x in os.getenv("RESEARCH_LATENCIES_MS", "10,25,50,75,100,125,150,175,200,250,300").split(","))
EXEC_OBSERVE_MAX_AGE_MS = int(os.getenv("EXEC_OBSERVE_MAX_AGE_MS", "1000"))
TRACE_WINDOW_MS = int(os.getenv("TRACE_WINDOW_MS", "300"))
TRACE_MIN_STEP_MS = int(os.getenv("TRACE_MIN_STEP_MS", "2"))
DECISION_MAX_RECV_AGE_MS = int(os.getenv("DECISION_MAX_RECV_AGE_MS", "300"))
DECISION_MAX_SKEW_MS = int(os.getenv("DECISION_MAX_SKEW_MS", "250"))
# Economic re-arm: after a paper entry, the route must be neutral (<= 0%) continuously before another entry.
PAPER_REARM_NEUTRAL_MS = int(os.getenv("PAPER_REARM_NEUTRAL_MS", "1000"))
PAPER_REARM_NET = float(os.getenv("PAPER_REARM_NET_PCT", "0.0")) / 100.0
PAPER_HARD_COOLDOWN_MS = int(os.getenv("PAPER_HARD_COOLDOWN_MS", "5000"))
WS_LATENCY_SAMPLE_MS = int(os.getenv("WS_LATENCY_SAMPLE_MS", "10000"))
REBALANCE_COVER_MULT = float(os.getenv("REBALANCE_COVER_MULT", "1.50"))
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
    "depth": {}, "depth_ready": 0,
}
active_events = {}
dbq = queue.Queue(maxsize=50000)
scanq = queue.Queue(maxsize=30000)
queued_symbols = set()
queued_lock = threading.Lock()
paper = {}
paper_reserved = defaultdict(float)
paper_consumed_events = set()
paper_route_armed = defaultdict(lambda: True)
paper_route_neutral_since = {}
paper_route_last_trade = defaultdict(int)
active_traces = {}
trace_lock = threading.RLock()
ws_latency_last_sample = {}
depth_lock = threading.RLock()
depth_buffers = defaultdict(lambda: deque(maxlen=5000))


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

    CREATE TABLE IF NOT EXISTS paper_execution_attempts(
      id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT NOT NULL, ts_ms INTEGER NOT NULL, decision_id TEXT NOT NULL,
      route_id TEXT NOT NULL, event_start_ts_ms INTEGER NOT NULL, status TEXT NOT NULL, input_usd REAL NOT NULL,
      leg1_input_units REAL NOT NULL, leg1_mid_units REAL NOT NULL, leg2_mid_used REAL NOT NULL, leg2_output_units REAL NOT NULL,
      unwind_mid_used REAL NOT NULL, unwind_start_units REAL NOT NULL, stranded_mid_units REAL NOT NULL,
      output_usd_total REAL NOT NULL, profit_usd REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_paper_attempt_day ON paper_execution_attempts(day,ts_ms);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_attempt_decision ON paper_execution_attempts(decision_id);

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

    CREATE TABLE IF NOT EXISTS decisions_v22(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL, path TEXT NOT NULL,
      ts_ms INTEGER NOT NULL, decision_net REAL NOT NULL, decision_size_usd REAL NOT NULL,
      recv_age_max_ms INTEGER NOT NULL, exchange_age_max_ms INTEGER NOT NULL,
      recv_skew_ms INTEGER NOT NULL, exchange_skew_ms INTEGER NOT NULL,
      leg1_symbol TEXT NOT NULL, leg2_symbol TEXT NOT NULL,
      leg1_send_ts_ms INTEGER, leg1_recv_ts_ms INTEGER, leg2_send_ts_ms INTEGER, leg2_recv_ts_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_dec_v22_day ON decisions_v22(day,ts_ms);

    CREATE TABLE IF NOT EXISTS decision_trace_v22(
      id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT NOT NULL, route_id TEXT NOT NULL,
      ts_ms INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL, net REAL, executable_usd REAL,
      recv_age_max_ms INTEGER, exchange_age_max_ms INTEGER, recv_skew_ms INTEGER, exchange_skew_ms INTEGER,
      leg1_bid REAL, leg1_ask REAL, leg1_bidq REAL, leg1_askq REAL, leg1_send_ts_ms INTEGER, leg1_recv_ts_ms INTEGER,
      leg2_bid REAL, leg2_ask REAL, leg2_bidq REAL, leg2_askq REAL, leg2_send_ts_ms INTEGER, leg2_recv_ts_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_trace_v22_dec ON decision_trace_v22(decision_id,elapsed_ms);
    CREATE INDEX IF NOT EXISTS idx_trace_v22_ts ON decision_trace_v22(ts_ms);

    CREATE TABLE IF NOT EXISTS ws_latency_v22(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, symbol TEXT NOT NULL, send_ts_ms INTEGER NOT NULL,
      recv_ts_ms INTEGER NOT NULL, transport_ms INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_ws_lat_v22_ts ON ws_latency_v22(ts_ms);

    CREATE TABLE IF NOT EXISTS execution_trials(
      id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT NOT NULL, decision_id TEXT NOT NULL, route_id TEXT NOT NULL,
      decision_ts_ms INTEGER NOT NULL, exec_ts_ms INTEGER NOT NULL, bbo_age_limit_ms INTEGER NOT NULL, latency_ms INTEGER NOT NULL,
      decision_net REAL NOT NULL, decision_size_usd REAL NOT NULL, decision_max_bbo_age_ms INTEGER NOT NULL, decision_bbo_skew_ms INTEGER NOT NULL,
      exec_net REAL, exec_size_usd REAL, exec_max_bbo_age_ms INTEGER, exec_bbo_skew_ms INTEGER,
      pnl_usd REAL, status TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_trials_day ON execution_trials(day,decision_ts_ms);
    CREATE INDEX IF NOT EXISTS idx_trials_matrix ON execution_trials(day,bbo_age_limit_ms,latency_ms);
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
            elif typ == "paper_attempt":
                con.execute("""INSERT OR IGNORE INTO paper_execution_attempts(day,ts_ms,decision_id,route_id,event_start_ts_ms,status,input_usd,
                    leg1_input_units,leg1_mid_units,leg2_mid_used,leg2_output_units,unwind_mid_used,unwind_start_units,stranded_mid_units,
                    output_usd_total,profit_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
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
            elif typ == "decision_v22":
                con.execute("""INSERT OR IGNORE INTO decisions_v22(decision_id,day,route_id,path,ts_ms,decision_net,decision_size_usd,
                    recv_age_max_ms,exchange_age_max_ms,recv_skew_ms,exchange_skew_ms,leg1_symbol,leg2_symbol,
                    leg1_send_ts_ms,leg1_recv_ts_ms,leg2_send_ts_ms,leg2_recv_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "trace_v22":
                con.execute("""INSERT INTO decision_trace_v22(decision_id,route_id,ts_ms,elapsed_ms,net,executable_usd,
                    recv_age_max_ms,exchange_age_max_ms,recv_skew_ms,exchange_skew_ms,
                    leg1_bid,leg1_ask,leg1_bidq,leg1_askq,leg1_send_ts_ms,leg1_recv_ts_ms,
                    leg2_bid,leg2_ask,leg2_bidq,leg2_askq,leg2_send_ts_ms,leg2_recv_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "ws_lat_v22":
                con.execute("INSERT INTO ws_latency_v22(ts_ms,symbol,send_ts_ms,recv_ts_ms,transport_ms) VALUES(?,?,?,?,?)", payload)
            elif typ == "trial":
                con.execute("""INSERT INTO execution_trials(day,decision_id,route_id,decision_ts_ms,exec_ts_ms,bbo_age_limit_ms,latency_ms,
                    decision_net,decision_size_usd,decision_max_bbo_age_ms,decision_bbo_skew_ms,exec_net,exec_size_usd,exec_max_bbo_age_ms,
                    exec_bbo_skew_ms,pnl_usd,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
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
    # V2.1 is deliberately 2-leg only. 3-leg candidates are counted for diagnostics
    # but never subscribed/scanned; they will live in a separate scanner later.
    selected=[]; symbols=set()
    direct_stable=[m["symbol"] for m in bysym.values() if m["base"] in STABLES and m["quote"] in STABLES]
    symbols.update(direct_stable[:MAX_WS_SYMBOLS])
    remaining=list(r2)
    while remaining:
        remaining.sort(key=lambda r:(len(set(r["symbols"])-symbols), r["id"]))
        r=remaining.pop(0); new=set(r["symbols"])-symbols
        if len(symbols)+len(new)>MAX_WS_SYMBOLS: continue
        selected.append(r); symbols.update(r["symbols"])
    chosen=[bysym[x] for x in symbols if x in bysym]
    diag={"all_markets":len(bysym),"candidate_routes":len(candidates),"candidate_2leg":len(r2),
          "candidate_3leg":len(r3),"selected_symbols":len(chosen),"selected_routes":len(selected),
          "selected_2leg":len(selected),"selected_3leg":0,"dropped_routes":len(candidates)-len(selected),
          "direct_stable_symbols":len(direct_stable),"mode":"2-leg only"}
    return chosen,selected,bysym,diag

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


def _decode_level(blob):
    vals={}
    for f,w,v in _pb_fields(blob):
        if w==2 and f in (1,2): vals[f]=v.decode("utf-8","ignore")
    try: return float(vals[1]),float(vals[2])
    except Exception: return None

def decode_depth(payload):
    symbol=None; send=None; body=None
    for f,w,v in _pb_fields(payload):
        if f==3 and w==2: symbol=v.decode("utf-8","ignore")
        elif f==6 and w==0: send=v
        elif f==313 and w==2: body=v
    if not symbol or body is None: return None
    asks=[]; bids=[]; fv=None; tv=None
    for f,w,v in _pb_fields(body):
        if w==2 and f==1:
            z=_decode_level(v); asks.append(z) if z else None
        elif w==2 and f==2:
            z=_decode_level(v); bids.append(z) if z else None
        elif w==2 and f==4:
            try: fv=int(v.decode())
            except: pass
        elif w==2 and f==5:
            try: tv=int(v.decode())
            except: pass
    return {"symbol":symbol,"send":int(send or now_ms()),"asks":asks,"bids":bids,"from":fv,"to":tv}

def _publish_depth_bbo(sym, send_ts):
    with depth_lock:
        ob=state["depth"].get(sym)
        if not ob or not ob.get("ready") or not ob["bids"] or not ob["asks"]: return
        bp=max(ob["bids"]); ap=min(ob["asks"]); bq=ob["bids"][bp]; aq=ob["asks"][ap]
    ts=now_ms()
    with state_lock:
        state["bbo"][sym]={"bid":bp,"bidq":bq,"ask":ap,"askq":aq,"ts":ts,"send_ts":send_ts,"lat":max(0,ts-send_ts)}
        state["last_ws_ms"]=ts
    enqueue_scan(sym)

def apply_depth_update(x):
    sym=x["symbol"]
    with depth_lock:
        ob=state["depth"].get(sym)
        if not ob or not ob.get("ready"):
            depth_buffers[sym].append(x); return False
        prev=ob.get("version")
        fv=x.get("from"); tv=x.get("to")
        if tv is not None and prev is not None and tv<=prev: return False
        if fv is not None and prev is not None and fv>prev+1:
            ob["ready"]=False; depth_buffers[sym].append(x)
            threading.Thread(target=init_depth_symbol,args=(sym,),daemon=True).start(); return False
        for side in ("asks","bids"):
            book=ob[side]
            for price,qty in x[side]:
                if qty<=0: book.pop(price,None)
                else: book[price]=qty
        if tv is not None: ob["version"]=tv
        ob["send_ts"]=x["send"]; ob["ts"]=now_ms()
    _publish_depth_bbo(sym,x["send"]); return True

def init_depth_symbol(sym):
    try:
        r=requests.get(REST+"/api/v3/depth",params={"symbol":sym,"limit":100},timeout=8); r.raise_for_status(); j=r.json()
        bids={float(a):float(b) for a,b in j.get("bids",[]) if float(b)>0}; asks={float(a):float(b) for a,b in j.get("asks",[]) if float(b)>0}
        ver=int(j.get("lastUpdateId",0)); send=now_ms()
        with depth_lock:
            ob={"bids":bids,"asks":asks,"version":ver,"ready":True,"ts":send,"send_ts":send}
            state["depth"][sym]=ob
            buf=list(depth_buffers.pop(sym,[]))
            for x in buf:
                tv=x.get("to")
                if tv is not None and tv<=ob["version"]: continue
                fv=x.get("from")
                if fv is not None and fv>ob["version"]+1:
                    ob["ready"]=False; break
                for side in ("asks","bids"):
                    for price,qty in x[side]:
                        if qty<=0: ob[side].pop(price,None)
                        else: ob[side][price]=qty
                if tv is not None: ob["version"]=tv
                send=x["send"]
            if ob["ready"]: state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
        if ob.get("ready"): _publish_depth_bbo(sym,send)
    except Exception as e:
        logerr(f"depth snapshot {sym}: {e}")

def depth_bootstrap_worker(symbols):
    # WS buffers deltas while REST snapshots are built, matching MEXC's documented local-book procedure.
    for sym in symbols:
        with depth_lock:
            if sym not in state["depth"]: state["depth"][sym]={"bids":{},"asks":{},"version":None,"ready":False}
        init_depth_symbol(sym)
        time.sleep(.03)

def depth_walk(symbol, frm, to, input_units, meta):
    m=meta.get(symbol)
    if not m or input_units<=0: return None
    with depth_lock:
        ob=state["depth"].get(symbol)
        if not ob or not ob.get("ready"): return None
        bids=sorted(ob["bids"].items(), reverse=True)
        asks=sorted(ob["asks"].items())
        book_ts=ob.get("ts",0); send_ts=ob.get("send_ts",book_ts)
    if now_ms()-book_ts > PRIMARY_BBO_AGE_MS: return None
    remain=float(input_units); out=0.0; used=0.0; notional=0.0
    if frm==m["base"] and to==m["quote"]:
        for price,qty in bids:
            take=min(remain,qty);
            if take<=0: continue
            used+=take; out+=take*price; notional+=take*price; remain-=take
            if remain<=1e-12: break
        out*=1.0-FEE
    elif frm==m["quote"] and to==m["base"]:
        for price,qty in asks:
            max_quote=price*qty; takeq=min(remain,max_quote)
            if takeq<=0: continue
            base=takeq/price; used+=takeq; out+=base; notional+=takeq; remain-=takeq
            if remain<=1e-12: break
        out*=1.0-FEE
    else: return None
    if used<=0: return None
    return {"input_used":used,"output":out,"fill_ratio":used/input_units,"book_ts":book_ts,"send_ts":send_ts}

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

def route_calc(route,books,meta, age_limit_ms=None):
    start,end=route["path"][0],route["path"][-1]
    sv=stable_mark_usdt(start,books,meta); ev=stable_mark_usdt(end,books,meta)
    if sv<=0 or ev<=0: return None
    multiplier=1.0; max_start=float("inf"); ts=now_ms(); max_age=0; max_exchange_age=0; leg_ts=[]; leg_send_ts=[]; leg_books=[]
    age_limit_ms = MAX_BBO_AGE_MS if age_limit_ms is None else age_limit_ms
    symbols=route["symbols"]
    if len(symbols)!=len(route["path"])-1: return None
    for i,sym in enumerate(symbols):
        b=books.get(sym); m=meta.get(sym)
        if not b or not m: return None
        age=ts-b["ts"]; exch_age=max(0,ts-int(b.get("send_ts",b["ts"])))
        max_age=max(max_age,age); max_exchange_age=max(max_exchange_age,exch_age); leg_ts.append(b["ts"]); leg_send_ts.append(int(b.get("send_ts",b["ts"]))); leg_books.append(dict(b))
        if age>age_limit_ms: return None
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
            "start_mark":sv,"end_mark":ev,"max_age":int(max_age), "exchange_max_age":int(max_exchange_age),
            "bbo_skew":int(max(leg_ts)-min(leg_ts)) if leg_ts else 0,
            "exchange_skew":int(max(leg_send_ts)-min(leg_send_ts)) if leg_send_ts else 0,
            "leg_books":leg_books}

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

def reserve_paper_trade(route,event_x,event_start_ts,decision_id,decision_ts):
    """Reserve real portfolio capital at T0. Profit already sits in balances, so it compounds automatically."""
    with paper_lock:
        ensure_paper_day()
        with state_lock:
            books=dict(state["bbo"]); meta=state["market_meta"]
        s,e=route["path"][0],route["path"][-1]
        if s not in paper["balances"] or e not in paper["balances"]: return None
        sm=event_x["start_mark"]
        max_input_units=event_x["size"]/max(sm,1e-12)
        free=max(0.0,paper["balances"].get(s,0.0)-paper_reserved.get(s,0.0))
        desired=max_input_units
        shortage=max(0.0,desired-free)

        # Try an economically-financed rebalance before giving up on size.
        if shortage>0:
            rb=choose_rebalance(s,shortage,event_x,books,meta)
            if rb:
                donor,conv,expected=rb
                bank_before=paper["profit_bank"]
                # Do not spend donor capital already reserved by another in-flight trade.
                donor_free=max(0.0,paper["balances"].get(donor,0.0)-paper_reserved.get(donor,0.0))
                if conv["input"]<=donor_free+1e-12:
                    paper["balances"][donor]-=conv["input"]
                    paper["balances"][s]+=conv["output"]
                    paper["rebalances"]+=1; paper["rebalance_cost"]+=conv["cost"]
                    # profit_bank is only a financing ledger; economic profit remains in balances/NAV and compounds.
                    paper["profit_bank"]=max(0.0,paper["profit_bank"]-conv["cost"]*REBALANCE_COVER_MULT)
                    put_db(("paper_rebalance",(paper["day"],decision_ts,donor,s,conv["input"],conv["output"],conv["input_usd"],
                        conv["output_usd"],conv["cost"],bank_before,expected)))
                    free=max(0.0,paper["balances"].get(s,0.0)-paper_reserved.get(s,0.0))
                else:
                    paper["skipped_rebalance"]+=1
            else:
                paper["skipped_rebalance"]+=1

        input_units=min(free,max_input_units)
        input_usd=input_units*sm
        if input_usd<SIM_MIN_TRADE_USD:
            paper["skipped_balance"]+=1; persist_paper(); return None
        paper_reserved[s]+=input_units
        persist_paper()
        return {"decision_id":decision_id,"route":route,"event_start_ts":event_start_ts,"t0":decision_ts,
                "x0":dict(event_x),"start_asset":s,"end_asset":e,"reserved_units":input_units,
                "reserved_usd":input_usd,"leg1_due":decision_ts+max(1,PRIMARY_EXEC_LATENCY_MS//2),"leg2_due":decision_ts+PRIMARY_EXEC_LATENCY_MS,"stage":1}


def paper_leg1(q,ts):
    r=q["route"]; sym=r["symbols"][0]; frm,to=r["path"][0],r["path"][1]
    with state_lock: meta=state["market_meta"]
    fill=depth_walk(sym,frm,to,q["reserved_units"],meta)
    if not fill or fill["input_used"]*q["x0"]["start_mark"]<SIM_MIN_TRADE_USD:
        return False
    q["leg1"]=fill; q["stage"]=2
    return True

def settle_paper_trade(q,exec_ts):
    """Irreversible paper execution. Once leg 1 fills, every outcome is booked.
    If leg 2 is missing/partial, remaining intermediate inventory is emergency-unwound
    through leg 1 in reverse. Any still-stranded intermediate units are valued at zero
    (deliberately conservative) so a failed second leg can never disappear from PnL.
    """
    with paper_lock:
        ensure_paper_day(); s,e=q["start_asset"],q["end_asset"]; reserved=q["reserved_units"]
        r=q["route"]; sym1=r["symbols"][0]; sym2=r["symbols"][1]; mid=r["path"][1]
        with state_lock: meta=state["market_meta"]; books=dict(state["bbo"])
        leg1=q["leg1"]; mid_total=leg1["output"]; input_units=min(leg1["input_used"],paper["balances"].get(s,0.0))
        input_usd=input_units*q["x0"]["start_mark"]
        fill2=depth_walk(sym2,mid,e,mid_total,meta)
        leg2_used=fill2["input_used"] if fill2 else 0.0
        end_out=fill2["output"] if fill2 else 0.0
        remaining=max(0.0,mid_total-leg2_used)
        unwind=depth_walk(sym1,mid,s,remaining,meta) if remaining>1e-12 else None
        unwind_used=unwind["input_used"] if unwind else 0.0
        start_back=unwind["output"] if unwind else 0.0
        stranded=max(0.0,remaining-unwind_used)
        paper_reserved[s]=max(0.0,paper_reserved.get(s,0.0)-reserved)

        # Book the economic result unconditionally after leg 1.
        paper["balances"][s]-=input_units
        paper["balances"][s]+=start_back
        paper["balances"][e]+=end_out
        sm=stable_mark_usdt(s,books,meta); em=stable_mark_usdt(e,books,meta)
        output_usd=start_back*sm + end_out*em  # stranded mid deliberately worth zero
        profit=output_usd-input_usd
        status="completed" if remaining<=1e-12 else ("forced_unwind" if stranded<=1e-12 else "forced_unwind_stranded")
        paper["trades"]+=1; paper["profit_bank"]+=profit
        put_db(("paper_attempt",(paper["day"],exec_ts,q["decision_id"],r["id"],q["event_start_ts"],status,input_usd,
            input_units,mid_total,leg2_used,end_out,unwind_used,start_back,stranded,output_usd,profit)))
        # Keep the legacy completed-trade table only for fully completed 2-leg routes.
        if status=="completed":
            put_db(("paper_trade",(paper["day"],exec_ts,r["id"],s,e,input_units,input_usd,end_out,end_out*em,
                ((end_out*em/input_usd)-1.0)*100.0 if input_usd else 0.0,profit,q["event_start_ts"])))
        else:
            paper["skipped_flash"]+=1
        persist_paper(); return True

# ---------- V2.1 immediate-decision / delayed-execution research ----------
pending_trials=[]
pending_paper_trials=[]
pending_lock=threading.RLock()
last_decision_ms={}
DECISION_COOLDOWN_MS=int(os.getenv("DECISION_COOLDOWN_MS","250"))

def _trace_payload(did,route,ts,x):
    lbs=x.get("leg_books") or []
    if len(lbs)<2: return None
    a,b=lbs[0],lbs[1]
    return (did,route["id"],ts,max(0,ts-int(did.rsplit("@",1)[1])),x.get("net"),x.get("size"),
            x.get("max_age"),x.get("exchange_max_age"),x.get("bbo_skew"),x.get("exchange_skew"),
            a.get("bid"),a.get("ask"),a.get("bidq"),a.get("askq"),a.get("send_ts"),a.get("ts"),
            b.get("bid"),b.get("ask"),b.get("bidq"),b.get("askq"),b.get("send_ts"),b.get("ts"))

def record_active_trace(route,ts,x):
    with trace_lock:
        q=active_traces.get(route["id"],[])
        if not q: return
        keep=[]
        for tr in q:
            if ts-tr["t0"]>TRACE_WINDOW_MS: continue
            if ts-tr.get("last_sample",0)<TRACE_MIN_STEP_MS: keep.append(tr); continue
            tr["last_sample"]=ts; keep.append(tr)
            if x:
                payload=_trace_payload(tr["did"],route,ts,x)
                if payload: put_db(("trace_v22",payload))
        if keep: active_traces[route["id"]]=keep
        else: active_traces.pop(route["id"],None)

def schedule_decision(route,x,ts,event_start_ts):
    # V2.2: T0 is immediate, based on 10 ms WebSocket BBO. Broad research limits preserve data for offline replay.
    if ts-last_decision_ms.get(route["id"],0)<DECISION_COOLDOWN_MS: return
    if x.get("max_age",999999)>DECISION_MAX_RECV_AGE_MS: return
    if x.get("bbo_skew",999999)>DECISION_MAX_SKEW_MS: return
    last_decision_ms[route["id"]]=ts
    _,_,day=local_day_bounds_ms(0); did=f"{route['id']}@{ts}"; lbs=x.get("leg_books") or []
    if len(lbs)>=2:
        put_db(("decision_v22",(did,day,route["id"],">".join(route["path"]),ts,x["net"],x["size"],x["max_age"],
            x.get("exchange_max_age",0),x.get("bbo_skew",0),x.get("exchange_skew",0),route["symbols"][0],route["symbols"][1],
            lbs[0].get("send_ts"),lbs[0].get("ts"),lbs[1].get("send_ts"),lbs[1].get("ts"))))
        p=_trace_payload(did,route,ts,x)
        if p: put_db(("trace_v22",p))
    with trace_lock:
        active_traces.setdefault(route["id"],[]).append({"did":did,"t0":ts,"last_sample":ts})
    with pending_lock:
        for age_lim in RESEARCH_BBO_AGES_MS:
            if x["max_age"]>age_lim: continue
            for lat in RESEARCH_LATENCIES_MS:
                pending_trials.append({"due":ts+lat,"day":day,"did":did,"route":route,"t0":ts,
                    "age_lim":age_lim,"lat":lat,"x0":dict(x)})

    # Separate $2k portfolio simulation: one executable entry per arbitrage event, real capital reserved at T0.
    event_key=(route["id"],int(event_start_ts))
    rid=route["id"]
    paper_ok = (paper_route_armed[rid] and ts-paper_route_last_trade[rid]>=PAPER_HARD_COOLDOWN_MS)
    if paper_ok and event_key not in paper_consumed_events and x.get("max_age",999999)<=PRIMARY_BBO_AGE_MS and x.get("bbo_skew",999999)<=PRIMARY_MAX_SKEW_MS:
        q=reserve_paper_trade(route,x,event_start_ts,did,ts)
        if q:
            paper_consumed_events.add(event_key); paper_route_armed[rid]=False; paper_route_last_trade[rid]=ts; paper_route_neutral_since.pop(rid,None)
            with pending_lock: pending_paper_trials.append(q)

def execution_trial_worker():
    while True:
        time.sleep(.005); ts=now_ms(); due=[]; paper_due=[]
        with pending_lock:
            keep=[]
            for q in pending_trials:
                (due if q["due"]<=ts else keep).append(q)
            pending_trials[:] = keep
            pkeep=[]
            for q in pending_paper_trials:
                (paper_due if (q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"])<=ts else pkeep).append(q)
            pending_paper_trials[:] = pkeep
        if not due and not paper_due: continue
        with state_lock:
            books=dict(state["bbo"]); meta=state["market_meta"]
        for q in due:
            # IMPORTANT: once T0 fired, the result is recorded even when negative.
            # We observe the current BBO after the requested latency; no positivity filter here.
            xe=route_calc(q["route"],books,meta,age_limit_ms=EXEC_OBSERVE_MAX_AGE_MS)
            x0=q["x0"]; status="executed" if xe else "unresolved_book"
            pnl=None
            if xe:
                # Raw research has no portfolio cap; it remains the comparison reference requested by the user.
                fill_usd=min(x0["size"],xe["size"])
                pnl=fill_usd*xe["net"]
            else: fill_usd=None
            put_db(("trial",(q["day"],q["did"],q["route"]["id"],q["t0"],ts,q["age_lim"],q["lat"],
                x0["net"],x0["size"],x0["max_age"],x0.get("bbo_skew",0),
                xe["net"] if xe else None,fill_usd,xe["max_age"] if xe else None,xe.get("bbo_skew",0) if xe else None,pnl,status)))
        for q in paper_due:
            if q.get("stage",1)==1:
                if paper_leg1(q,ts):
                    with pending_lock: pending_paper_trials.append(q)
                else:
                    with paper_lock:
                        paper_reserved[q["start_asset"]]=max(0.0,paper_reserved.get(q["start_asset"],0.0)-q["reserved_units"])
                        paper["skipped_flash"]+=1; persist_paper()
            else:
                settle_paper_trade(q,ts)

# ---------- Event aggregation ----------
def close_event(key,ev,end_ts=None):
    end_ts=int(end_ts or ev["last_ts"]); avg=ev["sum_net"]/max(ev["ticks"],1)
    payload=(ev["route_id"],ev["type"],ev["path"],ev["start_asset"],ev["end_asset"],ev["start_ts"],end_ts,
             max(0,end_ts-ev["start_ts"]),ev["ticks"],ev["entry_net"],ev["peak_net"],avg,ev["entry_size"],ev["max_size"],
             ev["entry_profit"],ev["peak_profit"],ev["max_bbo_age"],1 if ev.get("confirmed") else 0,
             ev.get("confirm_ts"),ev.get("confirm_net"),ev.get("confirm_size"),ev.get("confirm_profit"),ev.get("confirm_ticks"))
    put_db(("event",payload))
    paper_consumed_events.discard((ev["route_id"],int(ev["start_ts"])))

def process_route(route,ts):
    with state_lock:
        books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta)
    with state_lock:
        if x: state["latest_routes"][route["id"]]={"ts":ts,"net":x["net"],"size":x["size"],"type":route["type"],"max_age":x["max_age"]}
    key=route["id"]
    record_active_trace(route,ts,x)
    # Re-arm only after a genuine neutral market state has persisted; a micro-gap is not a new trade.
    if not paper_route_armed[key]:
        if x is not None and x["net"]<=PAPER_REARM_NET:
            since=paper_route_neutral_since.get(key)
            if since is None: paper_route_neutral_since[key]=ts
            elif ts-since>=PAPER_REARM_NEUTRAL_MS:
                paper_route_armed[key]=True; paper_route_neutral_since.pop(key,None)
        else:
            paper_route_neutral_since.pop(key,None)
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

            # Decision is immediate at T0. The $2k simulation can enter once per event when its stricter BBO/skew rules are met.
            schedule_decision(route,x,ts,ev["start_ts"])

            # No confirmation gate. Event duration remains descriptive only.
            if not ev["confirmed"]:
                ev["confirmed"]=True; ev["confirm_ts"]=ev["start_ts"]; ev["confirm_net"]=ev["entry_net"]; ev["confirm_size"]=ev["entry_size"]
                ev["confirm_profit"]=ev["entry_profit"]; ev["confirm_ticks"]=1
        elif ev is not None:
            close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

def event_sweeper():
    while True:
        time.sleep(.25); ts=now_ms()
        with event_lock:
            for key,ev in list(active_events.items()):
                if ts-ev["last_ts"]>EVENT_CLOSE_GAP_MS:
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
                params=[f"spot@public.aggre.depth.v3.api.pb@10ms@{s}" for s in symbols]
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":params}))
                print(f"[Routes V{VERSION}] WS {worker_id}: {len(symbols)} symbols")
            def on_message(ws,msg):
                if isinstance(msg,str): return
                try:
                    x=decode_depth(msg)
                    if not x: return
                    ts=now_ms()
                    last_lat=ws_latency_last_sample.get(x["symbol"],0)
                    if ts-last_lat>=WS_LATENCY_SAMPLE_MS:
                        ws_latency_last_sample[x["symbol"]]=ts
                        put_db(("ws_lat_v22",(ts,x["symbol"],x["send"],ts,max(0,ts-x["send"]))))
                    apply_depth_update(x)
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
    p["reserved"]={s:paper_reserved.get(s,0.0) for s in STABLES}
    p["free_balances"]={s:max(0.0,p["balances"].get(s,0.0)-paper_reserved.get(s,0.0)) for s in STABLES}
    return p

def raw_primary_status():
    """Raw BBO PnL for the exact same primary exchange-age/skew/latency conditions, without a capital base."""
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    r=con.execute("""SELECT COUNT(*) n,
      SUM(CASE WHEN t.status='executed' THEN 1 ELSE 0 END) executed,
      SUM(CASE WHEN t.pnl_usd>0 THEN 1 ELSE 0 END) wins,
      SUM(CASE WHEN t.pnl_usd<0 THEN 1 ELSE 0 END) losses,
      COALESCE(SUM(t.pnl_usd),0) pnl, AVG(t.exec_net) avg_exec_net
      FROM execution_trials t JOIN decisions_v22 d ON d.decision_id=t.decision_id
      WHERE t.decision_ts_ms>=? AND t.decision_ts_ms<? AND t.latency_ms=? AND t.bbo_age_limit_ms=300
        AND d.exchange_age_max_ms<=? AND d.exchange_skew_ms<=?""",
        (start,end,PRIMARY_EXEC_LATENCY_MS,PRIMARY_BBO_AGE_MS,PRIMARY_MAX_SKEW_MS)).fetchone()
    con.close()
    return {"n":r[0] or 0,"executed":r[1] or 0,"wins":r[2] or 0,"losses":r[3] or 0,"pnl":r[4] or 0.0,"avg_exec_net":r[5]}

def execution_matrix():
    start,end,label=local_day_bounds_ms(0); con=db_connect()
    rows=con.execute("""SELECT bbo_age_limit_ms age,latency_ms lat,COUNT(*) n,
      SUM(CASE WHEN status='executed' THEN 1 ELSE 0 END) executed,
      SUM(CASE WHEN pnl_usd>0 THEN 1 ELSE 0 END) wins, SUM(CASE WHEN pnl_usd<0 THEN 1 ELSE 0 END) losses,
      COALESCE(SUM(pnl_usd),0) pnl, AVG(exec_net) avg_exec_net
      FROM execution_trials WHERE decision_ts_ms>=? AND decision_ts_ms<? GROUP BY age,lat ORDER BY age,lat""",(start,end)).fetchall()
    con.close(); return [dict(r) for r in rows]

def recent_v22_decisions(limit=30):
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    rows=con.execute("""SELECT decision_id,route_id,ts_ms,decision_net,decision_size_usd,recv_age_max_ms,exchange_age_max_ms,
        recv_skew_ms,exchange_skew_ms FROM decisions_v22 WHERE ts_ms>=? AND ts_ms<? ORDER BY ts_ms DESC LIMIT ?""",(start,end,limit)).fetchall()
    con.close(); return [dict(r) for r in rows]

def v22_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    d=con.execute("SELECT COUNT(*) n, COUNT(DISTINCT route_id) routes, AVG(exchange_age_max_ms) age, AVG(exchange_skew_ms) skew FROM decisions_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    w=con.execute("SELECT AVG(transport_ms) av, MAX(transport_ms) mx FROM ws_latency_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    tr=con.execute("SELECT COUNT(*) n FROM decision_trace_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    con.close(); return {"decisions":d[0] or 0,"routes":d[1] or 0,"avg_exchange_age_ms":d[2],"avg_exchange_skew_ms":d[3],"avg_ws_transport_ms":w[0],"max_ws_transport_ms":w[1],"trace_points":tr[0] or 0}

@app.get("/api/status")
def api_status():
    rows,label=daily_rows(); sim=paper_status(); now=now_ms()
    with state_lock:
        age=now-state["last_ws_ms"] if state["last_ws_ms"] else None
        base={"version":VERSION,"symbols":len(state["symbols"]),"routes":len(state["routes"]),"ws":state["ws_connected"],
              "ws_expected":state["ws_expected"],"age_ms":age,"market_source":state["market_source"],"errors":list(state["errors"]),
              "scan_updates":state["scan_updates"],"diag":state["route_diag"],"depth_ready":state.get("depth_ready",0)}
    base.update({"day":label,"rows":rows[:100],"sim":sim,"fee_pct":FEE*100,"stables":STABLES,"min_exec_usd":MIN_EXEC_USD,
                 "max_exec_usd":MAX_EXEC_USD,"min_net_pct":MIN_NET*100,"primary_age_ms":PRIMARY_BBO_AGE_MS,"primary_latency_ms":PRIMARY_EXEC_LATENCY_MS,
                 "research_ages":RESEARCH_BBO_AGES_MS,"research_latencies":RESEARCH_LATENCIES_MS,
                 "matrix":execution_matrix(),"raw_primary":raw_primary_status(),"rebalance_cover_mult":REBALANCE_COVER_MULT,"v22":v22_stats(),
                 "trace_window_ms":TRACE_WINDOW_MS,"decision_max_recv_age_ms":DECISION_MAX_RECV_AGE_MS,"decision_max_skew_ms":DECISION_MAX_SKEW_MS,"primary_max_skew_ms":PRIMARY_MAX_SKEW_MS,"paper_rearm_neutral_ms":PAPER_REARM_NEUTRAL_MS,"paper_hard_cooldown_ms":PAPER_HARD_COOLDOWN_MS,
                 "recent_decisions":recent_v22_decisions()})
    return jsonify(base)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEXC 2-Leg V2.3 Depth Strict</title>
<style>body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.c,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.compare{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px;margin-top:12px}.v{font-weight:800;font-size:20px}.big{font-size:28px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.bad{color:#ff6677}.panel{margin-top:12px;overflow:auto}.compare .panel{margin-top:0}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right}th:first-child,td:first-child{text-align:left}.tag{display:inline-block;border:1px solid #334155;border-radius:999px;padding:3px 7px;font-size:11px;color:#bac6d8}</style></head><body>
<h2>MEXC — Scanner Spot 2-leg V2.2 Depth</h2><div class=cards id=cards></div>
<div class=compare><div class=panel id=paper></div><div class=panel id=raw></div></div>
<div class=panel><h3>Décisions récentes — trace 0→300 ms</h3><table><thead><tr><th>Heure</th><th>Route</th><th>Edge T0</th><th>Taille BBO</th><th>Âge échange</th><th>Skew échange</th></tr></thead><tbody id=recent></tbody></table></div>
<div class=panel><h3>Matrice brute exécution — journée fixe</h3><div class=muted>Sans capital de base : mesure du signal et de la latence. Carnet local depth 10 ms pour le flux marché; décision immédiate à T0, gains et pertes conservés. Le PnL brut peut utiliser une taille BBO supérieure à 2 000 $.</div><table><thead><tr><th>Âge réception max</th><th>Latence</th><th>Décisions</th><th>Résolues</th><th>Gagnantes</th><th>Perdantes</th><th>PnL brut</th><th>Net exec moy.</th></tr></thead><tbody id=matrix></tbody></table></div><div class=panel id=diag></div>
<script>async function refresh(){let d=await fetch('/api/status').then(r=>r.json()),g=d.diag,s=d.sim,r=d.raw_primary||{};document.getElementById('cards').innerHTML=`<div class=c><div class=v>${d.symbols}</div><div class=muted>marchés WS</div></div><div class=c><div class=v>${d.routes}</div><div class=muted>routes 2-leg</div></div><div class=c><div class=v>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div><div class=c><div class=v>${d.age_ms??'-'} ms</div><div class=muted>dernier BBO reçu</div></div><div class=c><div class=v>${d.depth_ready||0}/${d.symbols}</div><div class=muted>carnets depth prêts</div></div><div class=c><div class=v>${d.v22?.decisions||0}</div><div class=muted>décisions tracées</div></div><div class=c><div class=v>${d.v22?.trace_points||0}</div><div class=muted>points trajectoire</div></div><div class=c><div class=v>${d.v22?.avg_ws_transport_ms==null?'-':d.v22.avg_ws_transport_ms.toFixed(1)+' ms'}</div><div class=muted>MEXC → VPS brut (horloges non corrigées)</div></div>`;
let bals=Object.entries(s.balances||{}).map(([k,v])=>`${k}: ${v.toFixed(2)}`).join(' · ');document.getElementById('paper').innerHTML=`<div class=tag>SIMULATION CAPITAL RÉEL</div><h3>Paper bot — capital initial ${s.capital.toFixed(0)} $</h3><div class='v big ${s.profit>=0?'good':'bad'}'>${s.profit>=0?'+':''}${s.profit.toFixed(2)} $</div><div class='v ${s.return_pct>=0?'good':'bad'}'>${s.return_pct>=0?'+':''}${s.return_pct.toFixed(3)} %</div><div class=muted>NAV actuelle : ${s.nav.toFixed(2)} $ · profits/pertes réalloués automatiquement au capital</div><div class=cards style='margin-top:12px'><div><div class=v>${s.trades}</div><div class=muted>trades exécutés</div></div><div><div class=v>${s.rebalances}</div><div class=muted>rebalances</div></div><div><div class=v>${s.rebalance_cost.toFixed(3)} $</div><div class=muted>coût rebalance</div></div><div><div class=v>${s.skipped_balance}</div><div class=muted>refus capital</div></div></div><div class=muted style='margin-top:10px'>Buckets : ${bals}<br>Règles live : âge local ≤${d.primary_age_ms} ms · skew ≤${d.primary_max_skew_ms} ms · depth multi-niveaux · leg 1 vers +${Math.floor(d.primary_latency_ms/2)} ms · leg 2 vers +${d.primary_latency_ms} ms · capital réservé à T0 · 1 entrée max par événement.</div>`;
document.getElementById('raw').innerHTML=`<div class=tag>DONNÉES BRUTES — SANS CAPITAL</div><h3>Même filtre bot, taille BBO brute</h3><div class='v big ${(r.pnl||0)>=0?'good':'bad'}'>${(r.pnl||0)>=0?'+':''}${(r.pnl||0).toFixed(2)} $</div><div class=muted>Pas de capital initial, pas de contrainte de buckets : ce PnL sert uniquement à comparer le signal brut à ce que les 2 000 $ peuvent réellement exploiter.</div><div class=cards style='margin-top:12px'><div><div class=v>${r.executed||0}</div><div class=muted>résolues</div></div><div><div class=v>${r.wins||0}</div><div class=muted>gagnantes</div></div><div><div class=v>${r.losses||0}</div><div class=muted>perdantes</div></div><div><div class=v>${r.avg_exec_net==null?'-':(100*r.avg_exec_net).toFixed(4)+'%'}</div><div class=muted>net exec moyen</div></div></div><div class=muted style='margin-top:10px'>Même condition : âge échange ≤${d.primary_age_ms} ms · skew ≤${d.primary_max_skew_ms} ms · +${d.primary_latency_ms} ms.</div>`;
let m=d.matrix||[],h='';for(let x of m){h+=`<tr><td>${x.age} ms</td><td>${x.lat} ms</td><td>${x.n}</td><td>${x.executed}</td><td>${x.wins||0}</td><td>${x.losses||0}</td><td class='${x.pnl>=0?'good':'bad'}'>${x.pnl.toFixed(3)} $</td><td>${x.avg_exec_net==null?'-':(100*x.avg_exec_net).toFixed(4)+'%'}</td></tr>`}document.getElementById('matrix').innerHTML=h;let rh='';for(let q of (d.recent_decisions||[])){let t=new Date(q.ts_ms);rh+=`<tr><td>${t.toLocaleTimeString()}</td><td>${q.route_id}</td><td class='good'>${(100*q.decision_net).toFixed(4)}%</td><td>${q.decision_size_usd.toFixed(2)} $</td><td>${q.exchange_age_max_ms} ms</td><td>${q.exchange_skew_ms} ms</td></tr>`}document.getElementById('recent').innerHTML=rh;document.getElementById('diag').innerHTML=`<h3>Univers</h3><div class=cards><div><div class=v>${g.all_markets}</div><div class=muted>marchés découverts</div></div><div><div class=v>${g.candidate_2leg}</div><div class=muted>2-leg candidates</div></div><div><div class=v>${g.selected_2leg}</div><div class=muted>2-leg suivies</div></div><div><div class=v>${g.candidate_3leg}</div><div class=muted>3-leg détectées mais ignorées</div></div></div>`}refresh();setInterval(refresh,3000)</script></body></html>'''



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
    put_db(("meta",("version",VERSION))); put_db(("meta",("paper_rearm_neutral_ms",str(PAPER_REARM_NEUTRAL_MS)))); put_db(("meta",("paper_hard_cooldown_ms",str(PAPER_HARD_COOLDOWN_MS)))); put_db(("meta",("paper_failed_leg_policy","reverse_leg1_then_zero_value_stranded"))); put_db(("meta",("ws_depth_interval","10ms"))); put_db(("meta",("depth_snapshot_levels","100"))); put_db(("meta",("trace_window_ms",str(TRACE_WINDOW_MS)))); put_db(("meta",("market_source",state["market_source"])))
    put_db(("meta",("route_diag",json.dumps(diag,sort_keys=True))))
    for _ in range(6): threading.Thread(target=scan_worker,daemon=True).start()
    threading.Thread(target=event_sweeper,daemon=True).start()
    threading.Thread(target=execution_trial_worker,daemon=True).start()
    threading.Thread(target=snapshot_worker,daemon=True).start()
    threading.Thread(target=stable_quote_worker,daemon=True).start()
    for i,g in enumerate(groups,1): threading.Thread(target=ws_worker,args=(g,i),daemon=True).start()
    time.sleep(1.0)
    threading.Thread(target=depth_bootstrap_worker,args=(symbols,),daemon=True).start()

if __name__=="__main__":
    bootstrap(); app.run(host=HOST,port=PORT,threaded=True,use_reloader=False)

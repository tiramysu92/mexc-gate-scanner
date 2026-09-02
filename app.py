import os, time, json, math, sqlite3, threading, queue, statistics
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string, request

# ============================================================
# MEXC <-> Gate spot arbitrage scanner V3.3
# Public market data only. No API keys and no order execution.
#
# Architecture:
# - Gate BBO: WebSocket, update speed offered by Gate: 10 ms
# - MEXC BBO: native protobuf WebSocket bookTicker, 100 ms channels
#   split across up to 5 connections (30 subscriptions/connection)
# - Exact order-book depth is fetched only when the BBO suggests a
#   potentially interesting cross-CEX spread.
# - Positive signals are grouped into OPPORTUNITY EVENTS so 50 ticks
#   during one 3-second spread count as 1 opportunity, not 50.
# ============================================================

APP_PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "scanner_v3.db")

SIZES = [250.0, 500.0, 1000.0, 2000.0]
MAX_PAIRS = int(os.getenv("MAX_PAIRS", "150"))

# Focus on tradable but not ultra-thin markets.
MIN_QUOTE_VOL_24H = float(os.getenv("MIN_QUOTE_VOL_24H", "100000"))     # each CEX
MAX_QUOTE_VOL_24H = float(os.getenv("MAX_QUOTE_VOL_24H", "75000000"))  # soft cap
MEXC_BBO_INTERVAL = float(os.getenv("MEXC_BBO_INTERVAL", "0.25"))       # safer default: 4 req/s
MEXC_BACKOFF_INITIAL = float(os.getenv("MEXC_BACKOFF_INITIAL", "2.0"))
MEXC_BACKOFF_MAX = float(os.getenv("MEXC_BACKOFF_MAX", "60.0"))

# BBO prefilter: exact depth fetch only if gross spread reaches this.
# Negative threshold deliberately keeps near-misses so we can study them.
PREFILTER_GROSS = float(os.getenv("PREFILTER_GROSS", "0.0004"))  # +0.04%
VERIFY_COOLDOWN_MS = int(os.getenv("VERIFY_COOLDOWN_MS", "250"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "1000"))

# Current account assumptions discussed:
# MEXC usual spot taker 0.05%; XRP override 0%.
# Gate VIP0 spot taker 0.10%.
MEXC_DEFAULT_FEE = float(os.getenv("MEXC_TAKER_FEE", "0.0005"))
GATE_DEFAULT_FEE = float(os.getenv("GATE_TAKER_FEE", "0.0010"))
MEXC_FEE_OVERRIDES = {"XRPUSDT": 0.0}

# Pairs discovered in V2 that deserve guaranteed inclusion.
PINNED = {
    "ARBUSDT", "FETUSDT", "SEIUSDT", "NEARUSDT", "XRPUSDT",
    "OPUSDT", "SUIUSDT", "DOGEUSDT", "AAVEUSDT", "RENDERUSDT",
    "PEPEUSDT", "ADAUSDT"
}

# Remove common stable/stable, wrapped, leveraged and obvious index-like markets.
EXCLUDED_BASES = {
    "USDC","USDE","FDUSD","TUSD","DAI","EUR","EURT","USD1","USDP","PYUSD",
    "WBTC","WETH","STETH","WSTETH"
}
EXCLUDED_SUFFIXES = ("3L","3S","5L","5S","UP","DOWN","BULL","BEAR")

MEXC = "https://api.mexc.com"
GATE = "https://api.gateio.ws/api/v4"
GATE_WS = "wss://api.gateio.ws/ws/v4/"
MEXC_WS = "wss://wbs-api.mexc.com/ws"

app = Flask(__name__)
session = requests.Session()
session.headers.update({"User-Agent": "mexc-gate-scanner-v3/1.0"})

state_lock = threading.RLock()
state = {
    "started_ms": int(time.time()*1000),
    "pairs": [],
    "universe_meta": {},
    "mexc": {},
    "gate": {},
    "live": {},
    "last_mexc_ms": 0,
    "last_gate_ms": 0,
    "mexc_cycle_ms": None,
    "mexc_latency_ms": None,
    "mexc_ws_connected": 0,
    "mexc_ws_total": 0,
    "gate_connected": False,
    "verify_queue": 0,
    "errors": deque(maxlen=30),
}
last_verify = {}
active_events = {}   # (pair,direction) -> in-memory aggregate
event_lock = threading.RLock()
verify_pool = ThreadPoolExecutor(max_workers=10)
dbq = queue.Queue(maxsize=10000)

def now_ms():
    return int(time.time()*1000)

def err(msg):
    with state_lock:
        state["errors"].appendleft(f"{datetime.now().isoformat(timespec='seconds')} {msg}")

def fee_mexc(pair):
    return MEXC_FEE_OVERRIDES.get(pair, MEXC_DEFAULT_FEE)

def net_return(buy_avg, sell_avg, buy_fee, sell_fee):
    # Quote received after sell fee / quote spent including buy fee.
    return (sell_avg * (1.0 - sell_fee)) / (buy_avg * (1.0 + buy_fee)) - 1.0

def vwap_buy(asks, quote_usdt):
    remain = quote_usdt
    base = 0.0
    spent = 0.0
    for p, q in asks:
        p, q = float(p), float(q)
        level_quote = p*q
        take_quote = min(remain, level_quote)
        if take_quote <= 0: break
        base += take_quote/p
        spent += take_quote
        remain -= take_quote
        if remain <= 1e-9: break
    if remain > max(0.01, quote_usdt*1e-6) or base <= 0:
        return None
    return spent/base, base

def vwap_sell(bids, base_qty):
    remain = base_qty
    got = 0.0
    sold = 0.0
    for p, q in bids:
        p, q = float(p), float(q)
        take = min(remain, q)
        if take <= 0: break
        got += take*p
        sold += take
        remain -= take
        if remain <= 1e-12: break
    if remain > max(1e-12, base_qty*1e-6) or sold <= 0:
        return None
    return got/sold, got

def depth_result(pair, direction, size, mexc_book, gate_book):
    if direction == "MEXC->GATE":
        buy_book, sell_book = mexc_book, gate_book
        buy_fee, sell_fee = fee_mexc(pair), GATE_DEFAULT_FEE
    else:
        buy_book, sell_book = gate_book, mexc_book
        buy_fee, sell_fee = GATE_DEFAULT_FEE, fee_mexc(pair)

    buy = vwap_buy(buy_book["asks"], size)
    if not buy:
        return None
    buy_avg, base_qty = buy
    sell = vwap_sell(sell_book["bids"], base_qty)
    if not sell:
        return None
    sell_avg, gross_received = sell
    gross = sell_avg/buy_avg - 1.0
    net = net_return(buy_avg, sell_avg, buy_fee, sell_fee)
    # Exact quote profit for this model:
    quote_spent = size * (1.0 + buy_fee)
    quote_received = gross_received * (1.0 - sell_fee)
    profit = quote_received - quote_spent
    return {
        "size": size, "buy_avg": buy_avg, "sell_avg": sell_avg,
        "gross": gross, "net": net, "profit": profit,
        "base_qty": base_qty
    }

def db_writer():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript("""
    CREATE TABLE IF NOT EXISTS meta(
      key TEXT PRIMARY KEY, value TEXT
    );
    CREATE TABLE IF NOT EXISTS verified_checks(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_ms INTEGER NOT NULL,
      pair TEXT NOT NULL,
      direction TEXT NOT NULL,
      size REAL NOT NULL,
      gross REAL,
      net REAL,
      profit REAL,
      buy_avg REAL,
      sell_avg REAL,
      positive INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_checks_pair_ts ON verified_checks(pair, ts_ms);
    CREATE INDEX IF NOT EXISTS idx_checks_positive ON verified_checks(positive, ts_ms);

    CREATE TABLE IF NOT EXISTS opportunities(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pair TEXT NOT NULL,
      direction TEXT NOT NULL,
      start_ms INTEGER NOT NULL,
      end_ms INTEGER NOT NULL,
      duration_ms INTEGER NOT NULL,
      ticks INTEGER NOT NULL,
      peak_net REAL NOT NULL,
      avg_net REAL NOT NULL,
      peak_gross REAL NOT NULL,
      peak_profit REAL NOT NULL,
      best_size REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_opp_pair_start ON opportunities(pair, start_ms);

    CREATE TABLE IF NOT EXISTS universe_snapshots(
      ts_ms INTEGER NOT NULL,
      pair TEXT NOT NULL,
      mexc_quote_vol REAL,
      gate_quote_vol REAL,
      volatility_score REAL,
      selection_score REAL
    );
    """)
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('version','3')")
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('sizes',?)", (json.dumps(SIZES),))
    con.commit()
    buf=[]
    last=time.time()
    while True:
        try:
            item=dbq.get(timeout=.5)
            if item[0]=="check":
                _, r = item
                buf.append(("check",r))
            elif item[0]=="event":
                _, r = item
                buf.append(("event",r))
            elif item[0]=="universe":
                _, rows = item
                buf.extend(("universe",r) for r in rows)
        except queue.Empty:
            pass
        if buf and (len(buf)>=100 or time.time()-last>.75):
            try:
                for typ,r in buf:
                    if typ=="check":
                        con.execute("""INSERT INTO verified_checks
                        (ts_ms,pair,direction,size,gross,net,profit,buy_avg,sell_avg,positive)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (r["ts_ms"],r["pair"],r["direction"],r["size"],r["gross"],r["net"],
                         r["profit"],r["buy_avg"],r["sell_avg"],int(r["net"]>0)))
                    elif typ=="event":
                        con.execute("""INSERT INTO opportunities
                        (pair,direction,start_ms,end_ms,duration_ms,ticks,peak_net,avg_net,
                         peak_gross,peak_profit,best_size)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (r["pair"],r["direction"],r["start_ms"],r["end_ms"],r["duration_ms"],
                         r["ticks"],r["peak_net"],r["avg_net"],r["peak_gross"],
                         r["peak_profit"],r["best_size"]))
                    else:
                        con.execute("""INSERT INTO universe_snapshots
                        (ts_ms,pair,mexc_quote_vol,gate_quote_vol,volatility_score,selection_score)
                        VALUES(?,?,?,?,?,?)""",
                        (r["ts_ms"],r["pair"],r["mexc_quote_vol"],r["gate_quote_vol"],
                         r["volatility_score"],r["selection_score"]))
                con.commit()
                buf.clear()
                last=time.time()
            except Exception as e:
                con.rollback(); err(f"DB: {e}"); buf.clear()

def put_db(item):
    try: dbq.put_nowait(item)
    except queue.Full: err("DB queue full")

def fetch_json(url, params=None, timeout=4):
    r=session.get(url,params=params,timeout=timeout)
    r.raise_for_status()
    return r.json()

def load_cached_universe():
    """Load the most recent saved universe from scanner_v3.db without any exchange REST call."""
    if not os.path.exists(DB_PATH):
        return []
    con=None
    try:
        con=sqlite3.connect(DB_PATH,timeout=3)
        row=con.execute("SELECT MAX(ts_ms) FROM universe_snapshots").fetchone()
        if not row or row[0] is None:
            return []
        ts=row[0]
        rows=con.execute("""
            SELECT pair,mexc_quote_vol,gate_quote_vol,volatility_score,selection_score
            FROM universe_snapshots WHERE ts_ms=? ORDER BY selection_score DESC
        """,(ts,)).fetchall()
        # Preserve unique pairs; old snapshots may contain duplicates only if a prior run was interrupted.
        seen=set(); pairs=[]; meta={}
        for pair,mv,gv,vol,score in rows:
            if pair in seen:
                continue
            seen.add(pair); pairs.append(pair)
            meta[pair]={
                "pair":pair,"mexc_quote_vol":mv,"gate_quote_vol":gv,
                "volatility_score":vol,"selection_score":score,
                "pinned":pair in PINNED
            }
        pairs=pairs[:MAX_PAIRS]
        meta={p:meta[p] for p in pairs}
        if pairs:
            with state_lock:
                state["pairs"]=pairs
                state["universe_meta"]=meta
            print(f"[V3.3] Cached universe: {len(pairs)} pairs loaded from {DB_PATH}")
        return pairs
    except Exception as e:
        err(f"Cached universe: {e}")
        return []
    finally:
        if con is not None:
            con.close()

def discover_universe():
    mexc_info = fetch_json(MEXC+"/api/v3/exchangeInfo")
    gate_pairs = fetch_json(GATE+"/spot/currency_pairs")
    mexc_24 = fetch_json(MEXC+"/api/v3/ticker/24hr")
    gate_24 = fetch_json(GATE+"/spot/tickers")

    mexc_ok={}
    for s in mexc_info.get("symbols",[]):
        sym=s.get("symbol","")
        if sym.endswith("USDT") and s.get("status") in ("1","ENABLED",1):
            mexc_ok[sym]=s
    gate_ok={}
    for x in gate_pairs:
        if x.get("quote")=="USDT" and x.get("trade_status")=="tradable" and not x.get("st_tag",False):
            gate_ok[x["id"].replace("_","")]=x

    m24={x.get("symbol"):x for x in mexc_24 if isinstance(x,dict)}
    g24={x.get("currency_pair","").replace("_",""):x for x in gate_24 if isinstance(x,dict)}

    rows=[]
    for sym in set(mexc_ok)&set(gate_ok)&set(m24)&set(g24):
        base=sym[:-4]
        if base in EXCLUDED_BASES or any(base.endswith(z) for z in EXCLUDED_SUFFIXES):
            continue
        try:
            mv=float(m24[sym].get("quoteVolume") or 0)
            gv=float(g24[sym].get("quote_volume") or 0)
            # 24h % changes; Gate change_percentage already percentage points.
            mc=abs(float(m24[sym].get("priceChangePercent") or 0))
            gc=abs(float(g24[sym].get("change_percentage") or 0))
        except Exception:
            continue
        minv=min(mv,gv)
        maxv=max(mv,gv)
        if minv < MIN_QUOTE_VOL_24H and sym not in PINNED:
            continue
        volscore=(mc+gc)/2.0
        # Prefer mid/low liquidity + movement. Don't hard-exclude high volume; softly penalize.
        liquidity_penalty=max(1.0, math.log10(max(minv,10.0)))
        high_penalty=1.0 if maxv<=MAX_QUOTE_VOL_24H else 1.8
        score=(volscore+1.0)/(liquidity_penalty*high_penalty)
        rows.append({
            "pair":sym,"mexc_quote_vol":mv,"gate_quote_vol":gv,
            "volatility_score":volscore,"selection_score":score,
            "pinned": sym in PINNED
        })
    # Guaranteed pinned first, then highest inefficiency-potential score.
    pinned=[r for r in rows if r["pinned"]]
    others=sorted([r for r in rows if not r["pinned"]], key=lambda x:x["selection_score"], reverse=True)
    selected=(pinned+others)[:MAX_PAIRS]
    pairs=[r["pair"] for r in selected]
    meta={r["pair"]:r for r in selected}
    with state_lock:
        state["pairs"]=pairs
        state["universe_meta"]=meta
    ts=now_ms()
    put_db(("universe",[{**r,"ts_ms":ts} for r in selected]))
    print(f"[V3] Universe: {len(pairs)} common USDT pairs")
    return pairs

def _pb_varint(data, pos):
    value=0; shift=0
    while pos < len(data):
        b=data[pos]; pos+=1
        value |= (b & 0x7f) << shift
        if not (b & 0x80):
            return value,pos
        shift += 7
        if shift > 70:
            raise ValueError("protobuf varint too long")
    raise ValueError("truncated protobuf varint")

def _pb_fields(data):
    """Minimal protobuf wire decoder; returns [(field_no, wire_type, value)]."""
    out=[]; pos=0
    while pos < len(data):
        key,pos=_pb_varint(data,pos)
        field=key >> 3; wire=key & 7
        if wire == 0:
            val,pos=_pb_varint(data,pos)
        elif wire == 1:
            val=data[pos:pos+8]; pos+=8
        elif wire == 2:
            n,pos=_pb_varint(data,pos)
            val=data[pos:pos+n]; pos+=n
        elif wire == 5:
            val=data[pos:pos+4]; pos+=4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")
        out.append((field,wire,val))
    return out

def decode_mexc_bookticker(payload):
    """
    Decode the fields used from MEXC PushDataV3ApiWrapper:
      symbol=3, sendTime=6, publicAggreBookTicker=315.
    PublicAggreBookTicker fields: bidPrice=1, bidQuantity=2,
    askPrice=3, askQuantity=4.
    """
    symbol=None; send_time=None; book=None
    for field,wire,val in _pb_fields(payload):
        if field == 3 and wire == 2:
            symbol=val.decode("utf-8","ignore")
        elif field == 6 and wire == 0:
            send_time=val
        elif field == 315 and wire == 2:
            book=val
    if not symbol or book is None:
        return None
    vals={}
    for field,wire,val in _pb_fields(book):
        if wire == 2 and field in (1,2,3,4):
            vals[field]=val.decode("utf-8","ignore")
    if not all(k in vals for k in (1,2,3,4)):
        return None
    return {
        "symbol":symbol,
        "bid":float(vals[1]), "bidq":float(vals[2]),
        "ask":float(vals[3]), "askq":float(vals[4]),
        "ts":int(send_time or now_ms())
    }

def mexc_ws_worker(pairs, worker_id):
    # MEXC allows at most 30 subscriptions per WebSocket connection.
    while True:
        opened=False
        try:
            def on_open(ws):
                nonlocal opened
                opened=True
                with state_lock:
                    state["mexc_ws_connected"] += 1
                params=[
                    f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{sym}"
                    for sym in pairs
                ]
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":params}))
                print(f"[V3.2] MEXC WS {worker_id}: connected ({len(params)} pairs)")

            def on_message(ws, message):
                # Subscription/PONG responses are JSON text; market pushes are protobuf bytes.
                if isinstance(message, str):
                    return
                try:
                    x=decode_mexc_bookticker(message)
                    if not x:
                        return
                    ts=now_ms()
                    with state_lock:
                        prev=state["last_mexc_ms"]
                        state["mexc"][x["symbol"]]={
                            "bid":x["bid"],"bidq":x["bidq"],
                            "ask":x["ask"],"askq":x["askq"],"ts":ts
                        }
                        state["last_mexc_ms"]=ts
                        state["mexc_cycle_ms"]=(ts-prev) if prev else None
                        state["mexc_latency_ms"]=max(0, ts-x["ts"]) if x["ts"] else None
                    scan_candidates(ts)
                except Exception as e:
                    err(f"MEXC WS decode {worker_id}: {e}")

            def on_error(ws, error):
                err(f"MEXC WS {worker_id}: {error}")

            def on_close(ws, code, msg):
                nonlocal opened
                if opened:
                    with state_lock:
                        state["mexc_ws_connected"]=max(0,state["mexc_ws_connected"]-1)
                    opened=False
                err(f"MEXC WS {worker_id} closed: {code} {msg}")

            ws=websocket.WebSocketApp(
                MEXC_WS,on_open=on_open,on_message=on_message,
                on_error=on_error,on_close=on_close
            )
            # Protocol-level ping plus MEXC application PING via a small helper.
            stop_ping=threading.Event()
            def app_ping():
                while not stop_ping.wait(20):
                    try:
                        if ws.sock and ws.sock.connected:
                            ws.send(json.dumps({"method":"PING"}))
                    except Exception:
                        pass
            threading.Thread(target=app_ping,daemon=True).start()
            ws.run_forever(ping_interval=25,ping_timeout=10)
            stop_ping.set()
        except Exception as e:
            err(f"MEXC WS worker {worker_id}: {e}")
        time.sleep(2)

def start_mexc_ws():
    with state_lock:
        pairs=list(state["pairs"])
    chunks=[pairs[i:i+30] for i in range(0,len(pairs),30)]
    with state_lock:
        state["mexc_ws_total"]=len(chunks)
    for idx,chunk in enumerate(chunks,1):
        threading.Thread(target=mexc_ws_worker,args=(chunk,idx),daemon=True).start()

def gate_ws_loop():
    while True:
        try:
            pairs=[]
            with state_lock: pairs=list(state["pairs"])
            def on_open(ws):
                with state_lock: state["gate_connected"]=True
                # Gate accepts multiple pairs in one payload.
                ws.send(json.dumps({
                    "time":int(time.time()),"channel":"spot.book_ticker","event":"subscribe",
                    "payload":[p[:-4]+"_USDT" for p in pairs]
                }))
            def on_message(ws,msg):
                try:
                    j=json.loads(msg)
                    if j.get("channel")!="spot.book_ticker" or j.get("event")!="update": return
                    r=j["result"]; sym=r["s"].replace("_",""); ts=int(r.get("t") or now_ms())
                    with state_lock:
                        state["gate"][sym]={
                            "bid":float(r["b"]),"bidq":float(r["B"]),
                            "ask":float(r["a"]),"askq":float(r["A"]),"ts":ts
                        }
                        state["last_gate_ms"]=now_ms()
                except Exception as e: err(f"Gate msg: {e}")
            def on_error(ws,e): err(f"Gate WS: {e}")
            def on_close(ws,a,b):
                with state_lock: state["gate_connected"]=False
            ws=websocket.WebSocketApp(GATE_WS,on_open=on_open,on_message=on_message,
                                      on_error=on_error,on_close=on_close)
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as e:
            err(f"Gate loop: {e}")
        with state_lock: state["gate_connected"]=False
        time.sleep(1)

def scan_candidates(ts):
    with state_lock:
        pairs=list(state["pairs"])
        m=dict(state["mexc"]); g=dict(state["gate"])
    for pair in pairs:
        a=m.get(pair); b=g.get(pair)
        if not a or not b or min(a["bid"],a["ask"],b["bid"],b["ask"])<=0: continue
        # Ignore stale cross-CEX quote.
        if ts-a["ts"]>1500 or ts-b["ts"]>1500: continue
        gross_mg=b["bid"]/a["ask"]-1.0
        gross_gm=a["bid"]/b["ask"]-1.0
        with state_lock:
            state["live"][pair]={
                "pair":pair,"mexc_to_gate_gross":gross_mg,"gate_to_mexc_gross":gross_gm,
                "mexc_ts":a["ts"],"gate_ts":b["ts"],"ts":ts
            }
        if gross_mg>=PREFILTER_GROSS:
            schedule_verify(pair,"MEXC->GATE",ts)
        if gross_gm>=PREFILTER_GROSS:
            schedule_verify(pair,"GATE->MEXC",ts)
    close_stale_events(ts)

def schedule_verify(pair,direction,ts):
    key=(pair,direction)
    prev=last_verify.get(key,0)
    if ts-prev<VERIFY_COOLDOWN_MS: return
    last_verify[key]=ts
    verify_pool.submit(verify_depth,pair,direction,ts)

def verify_depth(pair,direction,trigger_ts):
    try:
        gp=pair[:-4]+"_USDT"
        # Parallel enough via worker pool; two HTTP snapshots are fetched only on candidates.
        mr=session.get(MEXC+"/api/v3/depth",params={"symbol":pair,"limit":100},timeout=2.5)
        gr=session.get(GATE+"/spot/order_book",params={"currency_pair":gp,"limit":100},timeout=2.5)
        mr.raise_for_status(); gr.raise_for_status()
        mj,gj=mr.json(),gr.json()
        mb={"bids":mj["bids"],"asks":mj["asks"]}
        gb={"bids":gj["bids"],"asks":gj["asks"]}
        ts=now_ms()
        results=[]
        for size in SIZES:
            r=depth_result(pair,direction,size,mb,gb)
            if r:
                row={**r,"ts_ms":ts,"pair":pair,"direction":direction}
                results.append(row); put_db(("check",row))
        positives=[r for r in results if r["net"]>0]
        with state_lock:
            state["verify_queue"]=getattr(verify_pool,"_work_queue",queue.Queue()).qsize()
            live=state["live"].setdefault(pair,{"pair":pair})
            live[direction]={
                "verified_ms":ts,
                "results":results,
                "positive":bool(positives)
            }
        update_event(pair,direction,ts,positives)
    except Exception as e:
        err(f"Verify {pair} {direction}: {e}")

def update_event(pair,direction,ts,positives):
    key=(pair,direction)
    with event_lock:
        ev=active_events.get(key)
        if not positives:
            if ev and ts-ev["last_positive_ms"]>=EVENT_CLOSE_GAP_MS:
                finalize_event(key,ev)
            return
        # One event tick = the best executable size at this instant.
        best=max(positives,key=lambda r:r["profit"])
        if ev is None:
            ev={
                "pair":pair,"direction":direction,"start_ms":ts,"last_positive_ms":ts,
                "ticks":0,"net_sum":0.0,"peak_net":-999.0,"peak_gross":-999.0,
                "peak_profit":-1e99,"best_size":best["size"]
            }
            active_events[key]=ev
        ev["last_positive_ms"]=ts
        ev["ticks"]+=1
        ev["net_sum"]+=best["net"]
        if best["net"]>ev["peak_net"]: ev["peak_net"]=best["net"]
        if best["gross"]>ev["peak_gross"]: ev["peak_gross"]=best["gross"]
        if best["profit"]>ev["peak_profit"]:
            ev["peak_profit"]=best["profit"]; ev["best_size"]=best["size"]

def close_stale_events(ts):
    with event_lock:
        for key,ev in list(active_events.items()):
            if ts-ev["last_positive_ms"]>=EVENT_CLOSE_GAP_MS:
                finalize_event(key,ev)

def finalize_event(key,ev):
    active_events.pop(key,None)
    end=ev["last_positive_ms"]
    row={
        "pair":ev["pair"],"direction":ev["direction"],
        "start_ms":ev["start_ms"],"end_ms":end,
        "duration_ms":max(0,end-ev["start_ms"]),
        "ticks":ev["ticks"],"peak_net":ev["peak_net"],
        "avg_net":ev["net_sum"]/max(1,ev["ticks"]),
        "peak_gross":ev["peak_gross"],"peak_profit":ev["peak_profit"],
        "best_size":ev["best_size"]
    }
    put_db(("event",row))

def stats_24h():
    cutoff=now_ms()-86400000
    con=sqlite3.connect(DB_PATH,timeout=5)
    con.row_factory=sqlite3.Row
    rows=con.execute("""
    SELECT pair,
           COUNT(*) AS opportunities,
           ROUND(AVG(peak_net)*100,4) AS avg_net_pct,
           ROUND(MAX(peak_net)*100,4) AS max_net_pct,
           ROUND(AVG(duration_ms),0) AS avg_duration_ms,
           ROUND(MAX(peak_profit),4) AS max_profit_usdt,
           ROUND(SUM(peak_profit),4) AS sum_peak_profit_usdt
    FROM opportunities
    WHERE start_ms>=?
    GROUP BY pair
    ORDER BY opportunities DESC, max_net_pct DESC
    """,(cutoff,)).fetchall()
    con.close()
    return [dict(r) for r in rows]

HTML = r"""
<!doctype html><html lang="fr"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="2">
<title>MEXC ↔ Gate Scanner V3</title>
<style>
body{font-family:system-ui;background:#0b0e14;color:#e7eaf0;margin:0;padding:16px}
.wrap{max-width:1280px;margin:auto}.top{display:flex;gap:12px;flex-wrap:wrap}
.card{background:#131824;border:1px solid #253047;border-radius:12px;padding:12px;margin:8px 0}
.kpi{min-width:150px}.green{color:#37d67a}.red{color:#ff6262}.muted{color:#9ba7bc}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:8px;border-bottom:1px solid #222b3a;text-align:right}
th:first-child,td:first-child{text-align:left}h1{font-size:20px;margin:0 0 8px}.badge{padding:2px 7px;border-radius:12px;background:#222b3a}
</style></head><body><div class="wrap">
<h1>MEXC ↔ Gate — Scanner V3.3</h1>
<div class="top">
 <div class="card kpi"><b>{{pairs}}</b><div class="muted">paires scannées</div></div>
 <div class="card kpi"><b>{{mexc_age}} ms</b><div class="muted">âge MEXC</div></div>
 <div class="card kpi"><b>{{gate_age}} ms</b><div class="muted">âge Gate</div></div>
 <div class="card kpi"><b>{{mexc_ws}}</b><div class="muted">MEXC WS</div></div>
 <div class="card kpi"><b>{{latency}} ms</b><div class="muted">latence MEXC</div></div>
 <div class="card kpi"><b>{{opps}}</b><div class="muted">opportunités 24 h</div></div>
</div>
<div class="card"><b>Tailles :</b> 250 / 500 / 1 000 / 2 000 USDT ·
<b>Frais :</b> MEXC 0,05 % (XRP 0 %) + Gate 0,10 % ·
<span class="{{'green' if gate_connected else 'red'}}">Gate WS {{'connecté' if gate_connected else 'déconnecté'}}</span>
</div>
<div class="card"><h3>Classement des opportunités — 24 h</h3>
<table><tr><th>Token</th><th>Opportunités</th><th>Net moyen</th><th>Net max</th><th>Durée moy.</th><th>Meilleur profit</th><th>Somme pics*</th></tr>
{% for r in stats %}
<tr><td><b>{{r.pair}}</b></td><td>{{r.opportunities}}</td><td class="green">{{r.avg_net_pct}}%</td>
<td class="green">{{r.max_net_pct}}%</td><td>{{r.avg_duration_ms}} ms</td>
<td>{{r.max_profit_usdt}} USDT</td><td>{{r.sum_peak_profit_usdt}} USDT</td></tr>{% endfor %}
</table><div class="muted">* Somme des meilleurs profits théoriques par événement, pas un backtest d'inventaire.</div></div>
<div class="card"><h3>Meilleurs spreads BBO actuels</h3>
<table><tr><th>Token</th><th>MEXC → Gate brut</th><th>Gate → MEXC brut</th><th>Dernière mesure</th></tr>
{% for r in live %}
<tr><td>{{r.pair}}</td><td class="{{'green' if r.mg>0 else 'red'}}">{{r.mg}}%</td>
<td class="{{'green' if r.gm>0 else 'red'}}">{{r.gm}}%</td><td>{{r.age}} ms</td></tr>{% endfor %}
</table></div>
</div></body></html>
"""

@app.route("/")
def home():
    ts=now_ms()
    with state_lock:
        pairs=len(state["pairs"]); lm=state["last_mexc_ms"]; lg=state["last_gate_ms"]
        cycle=state["mexc_cycle_ms"]; latency=state["mexc_latency_ms"]; mwc=state["mexc_ws_connected"]; mwt=state["mexc_ws_total"]; gc=state["gate_connected"]
        live0=list(state["live"].values())
    stats=stats_24h()
    live=[]
    for x in live0:
        if "mexc_to_gate_gross" not in x: continue
        live.append({
            "pair":x["pair"],"mg":round(x["mexc_to_gate_gross"]*100,4),
            "gm":round(x["gate_to_mexc_gross"]*100,4),
            "age":ts-x.get("ts",ts)
        })
    live=sorted(live,key=lambda x:max(x["mg"],x["gm"]),reverse=True)[:40]
    return render_template_string(HTML,pairs=pairs,mexc_age=(ts-lm if lm else "-"),
        gate_age=(ts-lg if lg else "-"),cycle=(cycle if cycle is not None else "-"),mexc_ws=f"{mwc}/{mwt}",latency=(latency if latency is not None else "-"),opps=sum(x["opportunities"] for x in stats),
        gate_connected=gc,stats=stats,live=live)

@app.route("/api/state")
def api_state():
    ts=now_ms()
    with state_lock:
        return jsonify({
            "version":"3.3","pairs":state["pairs"],"pair_count":len(state["pairs"]),
            "mexc_age_ms":ts-state["last_mexc_ms"] if state["last_mexc_ms"] else None,
            "gate_age_ms":ts-state["last_gate_ms"] if state["last_gate_ms"] else None,
            "mexc_cycle_ms":state["mexc_cycle_ms"],"mexc_latency_ms":state["mexc_latency_ms"],"mexc_ws_connected":state["mexc_ws_connected"],"mexc_ws_total":state["mexc_ws_total"],"gate_connected":state["gate_connected"],
            "sizes":SIZES,"errors":list(state["errors"])
        })

@app.route("/api/opportunities")
def api_opps():
    return jsonify(stats_24h())

@app.route("/api/universe")
def api_universe():
    with state_lock: return jsonify(list(state["universe_meta"].values()))

def main():
    threading.Thread(target=db_writer,daemon=True).start()

    # V3.3: never block startup on MEXC REST. Reuse the last universe saved in SQLite.
    pairs=load_cached_universe()
    if not pairs:
        # First-ever launch fallback only. Existing V3 installations should not enter here.
        print("[V3.3] No cached universe found; trying live discovery once...")
        while True:
            try:
                discover_universe(); break
            except Exception as e:
                err(f"Universe: {e}"); print("Universe error:",e); time.sleep(10)

    threading.Thread(target=gate_ws_loop,daemon=True).start()
    start_mexc_ws()
    print(f"[V3.3] Dashboard http://0.0.0.0:{APP_PORT}")
    app.run(host="0.0.0.0",port=APP_PORT,threaded=True,use_reloader=False)

if __name__=="__main__":
    main()

#!/usr/bin/env python3
import os, json, time, math, queue, sqlite3, threading, requests
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string
import websocket

VERSION = "2.3.5-depth-quality"
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8081"))
DB_PATH = os.getenv("DB_PATH", "mexc_routes_v235.db")
MARKET_CACHE = os.getenv("MARKET_CACHE", "mexc_markets_cache.json")
TZ_NAME = os.getenv("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)

# Trading assumptions
FEE = float(os.getenv("TAKER_FEE", "0.0005"))            # 0.05% taker / leg
STABLES = tuple(x.strip().upper() for x in os.getenv("STABLES", "USDT,USDC,USD1").split(",") if x.strip())
MIN_EXEC_USD = float(os.getenv("MIN_EXEC_USD", "10"))
MAX_EXEC_USD = 0.0                                        # V2.3.5: no artificial trade cap
MIN_NET = float(os.getenv("MIN_NET_PCT", "0.01")) / 100.0
MAX_BBO_AGE_MS = int(os.getenv("MAX_BBO_AGE_MS", "750"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "750"))
MAX_WS_SYMBOLS = int(os.getenv("MAX_WS_SYMBOLS", "600"))
MAX_ROUTES_PER_SYMBOL_SCAN = int(os.getenv("MAX_ROUTES_PER_SYMBOL_SCAN", "10000"))

# Realistic paper-execution model
SIM_CAPITAL = float(os.getenv("SIM_CAPITAL", "1000"))
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

# Shadow-live execution profiles: same real MEXC depth feed, no real orders.
SHADOW_PROFILES = {
    "FAST": (15, 30),
    "TARGET": (25, 50),
    "DEGRADED": (50, 100),
}
SHADOW_BBO_AGE_MS = int(os.getenv("SHADOW_BBO_AGE_MS", "150"))
SHADOW_MAX_SKEW_MS = int(os.getenv("SHADOW_MAX_SKEW_MS", "50"))
# V2.3.5 Depth Quality policy. The signal is classified once at T0 and the exact same
# selected size is then reserved independently by FAST / TARGET / DEGRADED.
SHADOW_MIN_EDGE = float(os.getenv("SHADOW_MIN_EDGE_PCT", "0.35")) / 100.0
DEPTH_QUALITY_A_FRACTION = float(os.getenv("DEPTH_QUALITY_A_FRACTION", "0.50"))
DEPTH_QUALITY_B_FRACTION = float(os.getenv("DEPTH_QUALITY_B_FRACTION", "0.33"))
DEPTH_QUALITY_C_FRACTION = float(os.getenv("DEPTH_QUALITY_C_FRACTION", "0.25"))
DEPTH_QUALITY_BPLUS_HALF_EDGE = float(os.getenv("DEPTH_QUALITY_BPLUS_HALF_EDGE_PCT", "0.75")) / 100.0
DEPTH_QUALITY_BPLUS_DROP_FLOOR = float(os.getenv("DEPTH_QUALITY_BPLUS_DROP_FLOOR_PCT", "-2.0")) / 100.0
DEPTH_QUALITY_COVERAGE3_MIN = float(os.getenv("DEPTH_QUALITY_COVERAGE3_MIN", "3.0"))
DEPTH_QUALITY_LEVELS = int(os.getenv("DEPTH_QUALITY_LEVELS", "25"))
SHADOW_TARGET_WEIGHTS = {"USDT":0.40, "USDC":0.10, "USD1":0.50}
SHADOW_REBALANCE_CHECK_SEC = int(os.getenv("SHADOW_REBALANCE_CHECK_SEC", "1800"))
SHADOW_REBALANCE_MAX_SEC = int(os.getenv("SHADOW_REBALANCE_MAX_SEC", "14400"))
SHADOW_REBALANCE_EARLY_DEV = float(os.getenv("SHADOW_REBALANCE_EARLY_DEV_PCT", "15")) / 100.0
SHADOW_REBALANCE_MIN_DEV = float(os.getenv("SHADOW_REBALANCE_MIN_DEV_PCT", "3")) / 100.0
POST_RESYNC_GRACE_MS = int(os.getenv("POST_RESYNC_GRACE_MS", "2000"))
ENABLE_LEGACY_PAPER = os.getenv("ENABLE_LEGACY_PAPER", "0") == "1"

# Replay-grade capture. Every broad T0 decision gets multi-level books at these future offsets,
# even if the live Shadow policy rejects it. This is what lets the next export replay balance,
# edge, persistence and latency policies chronologically.
DEPTH_CAPTURE_OFFSETS_MS = tuple(int(x) for x in os.getenv(
    "DEPTH_CAPTURE_OFFSETS_MS", "0,10,15,20,25,30,40,50,60,75,100,125,150,200,250,300,500,750,1000,1500,2000,3000,5000").split(","))
DEPTH_CAPTURE_LEVELS = int(os.getenv("DEPTH_CAPTURE_LEVELS", "25"))
STABLE_DEPTH_INTERVAL_SEC = float(os.getenv("STABLE_DEPTH_INTERVAL_SEC", "10"))

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
paper_open_exposures = {}

shadow_lock = threading.RLock()
shadow_states = {}
shadow_reserved = defaultdict(lambda: defaultdict(float))
shadow_open_exposures = defaultdict(dict)
shadow_consumed_events = set()
shadow_route_armed = defaultdict(lambda: True)
shadow_route_neutral_since = {}
shadow_route_last_trade = defaultdict(int)
active_traces = {}
trace_lock = threading.RLock()
ws_latency_last_sample = {}
pending_depth_captures = []
shadow_last_rebalance_ms = {}
shadow_last_rebalance_check_ms = {}
depth_lock = threading.RLock()
depth_buffers = defaultdict(lambda: deque(maxlen=5000))
# V2.3.2: serialized depth resync. A symbol can be queued only once.
depth_resync_q = queue.Queue()
depth_resync_pending = set()
depth_resync_lock = threading.Lock()
depth_resync_diag = defaultdict(lambda: {"drops":0,"resync_ok":0,"resync_fail":0,"not_ready_since":None,"last_reason":"","last_resync_ms":None,"last_ready_ts_ms":None})
depth_quality_compute_us = deque(maxlen=10000)


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

    CREATE TABLE IF NOT EXISTS paper_open_exposures(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL, opened_ts_ms INTEGER NOT NULL, updated_ts_ms INTEGER NOT NULL,
      symbol TEXT NOT NULL, mid_asset TEXT NOT NULL, start_asset TEXT NOT NULL, units REAL NOT NULL,
      recovered_start_units REAL NOT NULL DEFAULT 0, input_usd REAL NOT NULL DEFAULT 0, cash_output_usd REAL NOT NULL DEFAULT 0,
      recovered_usd REAL NOT NULL DEFAULT 0, last_mark_usd REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'open'
    );
    CREATE INDEX IF NOT EXISTS idx_open_exposure_day ON paper_open_exposures(day,updated_ts_ms);

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

    CREATE TABLE IF NOT EXISTS shadow_state(
      profile TEXT NOT NULL, day TEXT NOT NULL, updated_ts_ms INTEGER NOT NULL, balances_json TEXT NOT NULL,
      profit_bank REAL NOT NULL, trades INTEGER NOT NULL, wins INTEGER NOT NULL, losses INTEGER NOT NULL,
      rebalances INTEGER NOT NULL, rebalance_cost REAL NOT NULL, skipped_flash INTEGER NOT NULL,
      skipped_balance INTEGER NOT NULL, skipped_rebalance INTEGER NOT NULL,
      PRIMARY KEY(profile,day)
    );

    CREATE TABLE IF NOT EXISTS shadow_attempts(
      id INTEGER PRIMARY KEY AUTOINCREMENT, profile TEXT NOT NULL, day TEXT NOT NULL, ts_ms INTEGER NOT NULL,
      decision_id TEXT NOT NULL, route_id TEXT NOT NULL, event_start_ts_ms INTEGER NOT NULL, status TEXT NOT NULL,
      input_usd REAL NOT NULL, leg1_input_units REAL NOT NULL, leg1_mid_units REAL NOT NULL,
      leg2_mid_used REAL NOT NULL, leg2_output_units REAL NOT NULL, unwind_mid_used REAL NOT NULL,
      unwind_start_units REAL NOT NULL, stranded_mid_units REAL NOT NULL, output_usd_total REAL NOT NULL, profit_usd REAL NOT NULL,
      UNIQUE(profile,decision_id)
    );
    CREATE INDEX IF NOT EXISTS idx_shadow_attempt_day ON shadow_attempts(profile,day,ts_ms);

    CREATE TABLE IF NOT EXISTS shadow_open_exposures(
      profile TEXT NOT NULL, decision_id TEXT NOT NULL, day TEXT NOT NULL, route_id TEXT NOT NULL,
      opened_ts_ms INTEGER NOT NULL, updated_ts_ms INTEGER NOT NULL, symbol TEXT NOT NULL, mid_asset TEXT NOT NULL,
      start_asset TEXT NOT NULL, units REAL NOT NULL, recovered_start_units REAL NOT NULL DEFAULT 0,
      input_usd REAL NOT NULL DEFAULT 0, cash_output_usd REAL NOT NULL DEFAULT 0, recovered_usd REAL NOT NULL DEFAULT 0,
      last_mark_usd REAL NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'open', PRIMARY KEY(profile,decision_id)
    );

    CREATE TABLE IF NOT EXISTS shadow_rebalances_v234(
      id INTEGER PRIMARY KEY AUTOINCREMENT, profile TEXT NOT NULL, day TEXT NOT NULL, ts_ms INTEGER NOT NULL,
      reason TEXT NOT NULL, from_asset TEXT NOT NULL, to_asset TEXT NOT NULL, input_units REAL NOT NULL,
      output_units REAL NOT NULL, input_usd REAL NOT NULL, output_usd REAL NOT NULL, cost_usd REAL NOT NULL,
      balances_before_json TEXT NOT NULL, balances_after_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_shadow_reb_v234 ON shadow_rebalances_v234(profile,day,ts_ms);

    CREATE TABLE IF NOT EXISTS decision_context_v234(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL, event_start_ts_ms INTEGER NOT NULL,
      ts_ms INTEGER NOT NULL, path TEXT NOT NULL, start_asset TEXT NOT NULL, end_asset TEXT NOT NULL,
      decision_net REAL NOT NULL, decision_size_usd REAL NOT NULL, start_mark REAL, end_mark REAL,
      leg1_last_ready_age_ms INTEGER, leg2_last_ready_age_ms INTEGER, shadow_policy_eligible INTEGER NOT NULL, policy_reason TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_dec_ctx_v234_ts ON decision_context_v234(ts_ms,route_id);

    CREATE TABLE IF NOT EXISTS decision_quality_v235(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL, ts_ms INTEGER NOT NULL,
      grade TEXT NOT NULL, eligible INTEGER NOT NULL, reason TEXT NOT NULL,
      capacity_usd REAL NOT NULL, size_fraction REAL NOT NULL, selected_usd REAL NOT NULL,
      normal_edge REAL, half_bbo_edge REAL, drop_l1_edge REAL, coverage3 REAL,
      compute_us REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_quality_v235_ts ON decision_quality_v235(ts_ms,grade,eligible);

    CREATE TABLE IF NOT EXISTS decision_depth_samples_v234(
      id INTEGER PRIMARY KEY AUTOINCREMENT, decision_id TEXT NOT NULL, route_id TEXT NOT NULL, decision_ts_ms INTEGER NOT NULL,
      sample_ts_ms INTEGER NOT NULL, elapsed_ms INTEGER NOT NULL, leg INTEGER NOT NULL, symbol TEXT NOT NULL,
      path_from TEXT NOT NULL, path_to TEXT NOT NULL, ready INTEGER NOT NULL, version INTEGER, book_ts_ms INTEGER,
      send_ts_ms INTEGER, recv_age_ms INTEGER, last_ready_ts_ms INTEGER, bids_json TEXT, asks_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_depth_sample_v234_dec ON decision_depth_samples_v234(decision_id,elapsed_ms,leg);
    CREATE INDEX IF NOT EXISTS idx_depth_sample_v234_ts ON decision_depth_samples_v234(sample_ts_ms);

    CREATE TABLE IF NOT EXISTS shadow_execution_details_v234(
      profile TEXT NOT NULL, decision_id TEXT NOT NULL, route_id TEXT NOT NULL, decision_ts_ms INTEGER NOT NULL,
      leg1_ts_ms INTEGER, leg2_ts_ms INTEGER, decision_edge REAL NOT NULL,
      leg1_levels INTEGER, leg1_vwap REAL, leg1_fill_ratio REAL, leg1_book_ts_ms INTEGER,
      leg2_levels INTEGER, leg2_vwap REAL, leg2_fill_ratio REAL, leg2_book_ts_ms INTEGER,
      unwind_levels INTEGER, unwind_vwap REAL, unwind_fill_ratio REAL, unwind_book_ts_ms INTEGER,
      PRIMARY KEY(profile,decision_id)
    );

    CREATE TABLE IF NOT EXISTS stable_depth_snapshots_v234(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, symbol TEXT NOT NULL, base_asset TEXT NOT NULL, quote_asset TEXT NOT NULL,
      ready INTEGER NOT NULL, version INTEGER, book_ts_ms INTEGER, bids_json TEXT, asks_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_stable_depth_v234_ts ON stable_depth_snapshots_v234(ts_ms,symbol);

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

    CREATE TABLE IF NOT EXISTS depth_sync_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL, symbol TEXT NOT NULL,
      event TEXT NOT NULL, reason TEXT, duration_ms INTEGER, attempt INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_depth_sync_symbol_ts ON depth_sync_events(symbol,ts_ms);

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
            elif typ == "open_exposure":
                con.execute("""INSERT OR REPLACE INTO paper_open_exposures(decision_id,day,route_id,opened_ts_ms,updated_ts_ms,symbol,mid_asset,start_asset,units,recovered_start_units,input_usd,cash_output_usd,recovered_usd,last_mark_usd,status)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "close_exposure":
                con.execute("UPDATE paper_open_exposures SET updated_ts_ms=?,units=0,recovered_start_units=?,recovered_usd=?,last_mark_usd=0,status='closed' WHERE decision_id=?", payload[:4])
                con.execute("UPDATE paper_execution_attempts SET status='forced_unwind_later',output_usd_total=?,profit_usd=? WHERE decision_id=?", (payload[4],payload[5],payload[6]))
            elif typ == "paper_rebalance":
                con.execute("""INSERT INTO paper_rebalances(day,ts_ms,from_asset,to_asset,input_units,output_units,
                    input_usd,output_usd,cost_usd,profit_bank_before,unlocked_expected_profit) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "paper_state":
                con.execute("""INSERT OR REPLACE INTO paper_state(day,updated_ts_ms,balances_json,profit_bank,trades,rebalances,
                    rebalance_cost,skipped_flash,skipped_balance,skipped_rebalance) VALUES(?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "shadow_state":
                con.execute("""INSERT OR REPLACE INTO shadow_state(profile,day,updated_ts_ms,balances_json,profit_bank,trades,wins,losses,rebalances,rebalance_cost,skipped_flash,skipped_balance,skipped_rebalance) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "shadow_attempt":
                con.execute("""INSERT OR IGNORE INTO shadow_attempts(profile,day,ts_ms,decision_id,route_id,event_start_ts_ms,status,input_usd,leg1_input_units,leg1_mid_units,leg2_mid_used,leg2_output_units,unwind_mid_used,unwind_start_units,stranded_mid_units,output_usd_total,profit_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "shadow_open":
                con.execute("""INSERT OR REPLACE INTO shadow_open_exposures(profile,decision_id,day,route_id,opened_ts_ms,updated_ts_ms,symbol,mid_asset,start_asset,units,recovered_start_units,input_usd,cash_output_usd,recovered_usd,last_mark_usd,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "shadow_close":
                con.execute("UPDATE shadow_open_exposures SET updated_ts_ms=?,units=0,recovered_start_units=?,recovered_usd=?,last_mark_usd=0,status='closed' WHERE profile=? AND decision_id=?", payload[:5])
                con.execute("UPDATE shadow_attempts SET status='forced_unwind_later',output_usd_total=?,profit_usd=? WHERE profile=? AND decision_id=?", payload[5:])
            elif typ == "shadow_rebalance_v234":
                con.execute("""INSERT INTO shadow_rebalances_v234(profile,day,ts_ms,reason,from_asset,to_asset,input_units,output_units,input_usd,output_usd,cost_usd,balances_before_json,balances_after_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "decision_context_v234":
                con.execute("""INSERT OR REPLACE INTO decision_context_v234(decision_id,day,route_id,event_start_ts_ms,ts_ms,path,start_asset,end_asset,decision_net,decision_size_usd,start_mark,end_mark,leg1_last_ready_age_ms,leg2_last_ready_age_ms,shadow_policy_eligible,policy_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "decision_quality_v235":
                con.execute("""INSERT OR REPLACE INTO decision_quality_v235(decision_id,day,route_id,ts_ms,grade,eligible,reason,capacity_usd,size_fraction,selected_usd,normal_edge,half_bbo_edge,drop_l1_edge,coverage3,compute_us) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "depth_sample_v234":
                con.execute("""INSERT INTO decision_depth_samples_v234(decision_id,route_id,decision_ts_ms,sample_ts_ms,elapsed_ms,leg,symbol,path_from,path_to,ready,version,book_ts_ms,send_ts_ms,recv_age_ms,last_ready_ts_ms,bids_json,asks_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "shadow_exec_detail_v234":
                con.execute("""INSERT OR REPLACE INTO shadow_execution_details_v234(profile,decision_id,route_id,decision_ts_ms,leg1_ts_ms,leg2_ts_ms,decision_edge,leg1_levels,leg1_vwap,leg1_fill_ratio,leg1_book_ts_ms,leg2_levels,leg2_vwap,leg2_fill_ratio,leg2_book_ts_ms,unwind_levels,unwind_vwap,unwind_fill_ratio,unwind_book_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "stable_depth_many_v234":
                con.executemany("""INSERT INTO stable_depth_snapshots_v234(ts_ms,symbol,base_asset,quote_asset,ready,version,book_ts_ms,bids_json,asks_json) VALUES(?,?,?,?,?,?,?,?,?)""", payload)
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
            elif typ == "depth_sync":
                con.execute("INSERT INTO depth_sync_events(ts_ms,symbol,event,reason,duration_ms,attempt) VALUES(?,?,?,?,?,?)", payload)
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

def queue_depth_resync(sym, reason="gap"):
    """Queue one serialized REST resync per symbol; never spawn resync threads from WS callbacks."""
    ts=now_ms()
    with depth_resync_lock:
        d=depth_resync_diag[sym]
        d["last_reason"]=reason
        if d["not_ready_since"] is None:
            d["not_ready_since"]=ts; d["drops"]+=1
            put_db(("depth_sync",(ts,sym,"NOT_READY",reason,None,None)))
        if sym in depth_resync_pending:
            return False
        depth_resync_pending.add(sym)
        depth_resync_q.put(sym)
    return True


def apply_depth_update(x):
    sym=x["symbol"]
    need_resync=False
    with depth_lock:
        ob=state["depth"].get(sym)
        if not ob or not ob.get("ready"):
            depth_buffers[sym].append(x)
            # Startup symbols are queued by bootstrap; a dropped live symbol is already pending.
            return False
        prev=ob.get("version")
        fv=x.get("from"); tv=x.get("to")
        if tv is not None and prev is not None and tv<=prev: return False
        if fv is not None and prev is not None and fv>prev+1:
            ob["ready"]=False; depth_buffers[sym].append(x); need_resync=True
            state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
        else:
            for side in ("asks","bids"):
                book=ob[side]
                for price,qty in x[side]:
                    if qty<=0: book.pop(price,None)
                    else: book[price]=qty
            if tv is not None: ob["version"]=tv
            ob["send_ts"]=x["send"]; ob["ts"]=now_ms()
    if need_resync:
        queue_depth_resync(sym,"version_gap")
        return False
    _publish_depth_bbo(sym,x["send"]); return True


def init_depth_symbol(sym):
    """Build one coherent local book. Return True only if snapshot + buffered deltas reconcile."""
    try:
        r=requests.get(REST+"/api/v3/depth",params={"symbol":sym,"limit":100},timeout=8); r.raise_for_status(); j=r.json()
        bids={float(a):float(b) for a,b in j.get("bids",[]) if float(b)>0}; asks={float(a):float(b) for a,b in j.get("asks",[]) if float(b)>0}
        ver=int(j.get("lastUpdateId",0)); send=now_ms(); ready=True
        with depth_lock:
            ob={"bids":bids,"asks":asks,"version":ver,"ready":True,"ts":send,"send_ts":send}
            state["depth"][sym]=ob
            buf=list(depth_buffers.pop(sym,[]))
            for x in buf:
                tv=x.get("to")
                if tv is not None and tv<=ob["version"]: continue
                fv=x.get("from")
                if fv is not None and fv>ob["version"]+1:
                    ob["ready"]=False; ready=False
                    # Keep the offending delta and later deltas for the next snapshot attempt.
                    depth_buffers[sym].append(x)
                    continue
                if not ready:
                    depth_buffers[sym].append(x); continue
                for side in ("asks","bids"):
                    for price,qty in x[side]:
                        if qty<=0: ob[side].pop(price,None)
                        else: ob[side][price]=qty
                if tv is not None: ob["version"]=tv
                send=x["send"]
            state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
        if ready:
            _publish_depth_bbo(sym,send)
        return ready
    except Exception as e:
        logerr(f"depth snapshot {sym}: {e}")
        return False


def depth_resync_worker():
    """Single rate-limited REST worker with retry/backoff; prevents 429 resync storms."""
    while True:
        sym=depth_resync_q.get(); attempt=0
        try:
            while True:
                attempt+=1
                ok=init_depth_symbol(sym)
                if ok:
                    ts=now_ms()
                    with depth_resync_lock:
                        d=depth_resync_diag[sym]; since=d.get("not_ready_since")
                        dur=max(0,ts-since) if since is not None else None
                        d["resync_ok"]+=1; d["not_ready_since"]=None; d["last_resync_ms"]=dur; d["last_ready_ts_ms"]=ts
                    put_db(("depth_sync",(ts,sym,"READY",depth_resync_diag[sym].get("last_reason",""),dur,attempt)))
                    break
                with depth_resync_lock: depth_resync_diag[sym]["resync_fail"]+=1
                put_db(("depth_sync",(now_ms(),sym,"RETRY",depth_resync_diag[sym].get("last_reason",""),None,attempt)))
                # 0.5, 1, 2, 4, 8s; capped. One worker means no REST burst.
                time.sleep(min(8.0,0.5*(2**min(attempt-1,4))))
            # Keep successful snapshot requests under ~4/s even during a queue backlog.
            time.sleep(0.25)
        finally:
            with depth_resync_lock: depth_resync_pending.discard(sym)
            depth_resync_q.task_done()


def depth_bootstrap_worker(symbols):
    # All initial snapshots use the same serialized/rate-limited resync path.
    for sym in symbols:
        with depth_lock:
            if sym not in state["depth"]: state["depth"][sym]={"bids":{},"asks":{},"version":None,"ready":False}
        queue_depth_resync(sym,"bootstrap")


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
    remain=float(input_units); out=0.0; used=0.0; notional=0.0; levels_used=0; gross_out=0.0; side=None
    if frm==m["base"] and to==m["quote"]:
        side="sell_base"
        for price,qty in bids:
            take=min(remain,qty)
            if take<=0: continue
            levels_used+=1; used+=take; gross_out+=take*price; notional+=take*price; remain-=take
            if remain<=1e-12: break
        out=gross_out*(1.0-FEE)
        vwap=(notional/used) if used>0 else None
    elif frm==m["quote"] and to==m["base"]:
        side="buy_base"; base_gross=0.0
        for price,qty in asks:
            max_quote=price*qty; takeq=min(remain,max_quote)
            if takeq<=0: continue
            levels_used+=1; base=takeq/price; used+=takeq; base_gross+=base; notional+=takeq; remain-=takeq
            if remain<=1e-12: break
        gross_out=base_gross; out=base_gross*(1.0-FEE)
        vwap=(notional/base_gross) if base_gross>0 else None
    else: return None
    if used<=0: return None
    return {"input_requested":input_units,"input_used":used,"output":out,"gross_output":gross_out,"fill_ratio":used/input_units,
            "book_ts":book_ts,"send_ts":send_ts,"levels_used":levels_used,"vwap":vwap,"side":side}


def _quality_books(route):
    """Atomically copy the two local Depth books used by the T0 quality decision."""
    ts=now_ms(); out={}
    with depth_lock:
        for sym in route["symbols"]:
            ob=state["depth"].get(sym)
            if not ob or not ob.get("ready") or ts-ob.get("ts",0)>SHADOW_BBO_AGE_MS:
                return None
            out[sym]={
                "bids":sorted(ob.get("bids",{}).items(),reverse=True)[:DEPTH_QUALITY_LEVELS],
                "asks":sorted(ob.get("asks",{}).items())[:DEPTH_QUALITY_LEVELS],
                "book_ts":ob.get("ts",0),
            }
    if len(out)!=len(route["symbols"]): return None
    if max(x["book_ts"] for x in out.values())-min(x["book_ts"] for x in out.values())>SHADOW_MAX_SKEW_MS:
        return None
    return out


def _quality_walk(book, market, frm, to, input_units, stress="normal"):
    """Pure in-memory Depth walk. Stress changes only the side actually consumed."""
    if input_units<=0: return None
    if frm==market.get("base") and to==market.get("quote"):
        levels=list(book["bids"]); buy_base=False
    elif frm==market.get("quote") and to==market.get("base"):
        levels=list(book["asks"]); buy_base=True
    else: return None
    if stress=="drop_l1": levels=levels[1:]
    elif stress=="half_bbo" and levels:
        levels=[(levels[0][0],levels[0][1]*0.50)]+levels[1:]
    remain=float(input_units); used=0.0; gross_out=0.0; levels_used=0
    for price,qty in levels:
        price=float(price); qty=float(qty)
        if price<=0 or qty<=0: continue
        if buy_base:
            take=min(remain,price*qty); gross_out+=take/price
        else:
            take=min(remain,qty); gross_out+=take*price
        if take<=0: continue
        used+=take; remain-=take; levels_used+=1
        if remain<=max(1e-12,input_units*1e-10): break
    if used<=0: return None
    return {"input_used":used,"output":gross_out*(1.0-FEE),"fill_ratio":min(1.0,used/input_units),"levels_used":levels_used}


def _quality_roundtrip(route, books, meta, input_usd, start_mark, end_mark, stress="normal"):
    if input_usd<=0 or start_mark<=0 or end_mark<=0: return None
    units=input_usd/start_mark; fills=[]
    for i,sym in enumerate(route["symbols"]):
        fill=_quality_walk(books[sym],meta.get(sym,{}),route["path"][i],route["path"][i+1],units,stress)
        if not fill or fill["fill_ratio"]<0.999999: return None
        fills.append(fill); units=fill["output"]
    output_usd=units*end_mark
    return {"edge":output_usd/input_usd-1.0,"output_usd":output_usd,"fills":fills}


def _quality_input_capacity(book, market, frm, to, levels=3):
    if frm==market.get("base") and to==market.get("quote"):
        return sum(float(q) for _,q in book["bids"][:levels])
    if frm==market.get("quote") and to==market.get("base"):
        return sum(float(p)*float(q) for p,q in book["asks"][:levels])
    return 0.0


def _quality_coverage3(route, books, meta, input_usd, start_mark):
    start_units=input_usd/max(start_mark,1e-12)
    first=_quality_walk(books[route["symbols"][0]],meta.get(route["symbols"][0],{}),route["path"][0],route["path"][1],start_units,"normal")
    if not first or first["fill_ratio"]<0.999999 or first["output"]<=0: return 0.0
    cap1=_quality_input_capacity(books[route["symbols"][0]],meta.get(route["symbols"][0],{}),route["path"][0],route["path"][1],3)
    cap2=_quality_input_capacity(books[route["symbols"][1]],meta.get(route["symbols"][1],{}),route["path"][1],route["path"][2],3)
    return min(cap1/start_units,cap2/first["output"])


def depth_quality_decision(route, event_x):
    """Classify A/B+/B-/C/D at T0 without waiting or making a REST call."""
    started=time.perf_counter_ns(); capacity=max(0.0,float(event_x.get("size",0.0)))
    result={"grade":"D","eligible":False,"reason":"depth_unavailable","capacity_usd":capacity,
            "size_fraction":0.0,"selected_usd":0.0,"normal_edge":None,"half_bbo_edge":None,
            "drop_l1_edge":None,"coverage3":None,"compute_us":0.0}
    try:
        with state_lock: meta=state["market_meta"]
        books=_quality_books(route)
        if not books: return result
        sm=float(event_x.get("start_mark") or 0.0); em=float(event_x.get("end_mark") or 0.0)

        # A: 50% of BBO capacity, still >= base edge after both level-1 quotes disappear.
        a_usd=capacity*DEPTH_QUALITY_A_FRACTION
        a_normal=_quality_roundtrip(route,books,meta,a_usd,sm,em,"normal")
        a_half=_quality_roundtrip(route,books,meta,a_usd,sm,em,"half_bbo")
        a_drop=_quality_roundtrip(route,books,meta,a_usd,sm,em,"drop_l1")
        if a_drop and a_drop["edge"]>=SHADOW_MIN_EDGE:
            result.update({"grade":"A","eligible":a_usd>=SIM_MIN_TRADE_USD,"reason":"eligible_A" if a_usd>=SIM_MIN_TRADE_USD else "below_min_trade",
                "size_fraction":DEPTH_QUALITY_A_FRACTION,"selected_usd":a_usd,
                "normal_edge":a_normal["edge"] if a_normal else None,"half_bbo_edge":a_half["edge"] if a_half else None,
                "drop_l1_edge":a_drop["edge"],"coverage3":_quality_coverage3(route,books,meta,a_usd,sm)})
            return result

        # B: 33% capacity must first survive -50% BBO at the base edge. Only B+ executes.
        b_usd=capacity*DEPTH_QUALITY_B_FRACTION
        b_normal=_quality_roundtrip(route,books,meta,b_usd,sm,em,"normal")
        b_half=_quality_roundtrip(route,books,meta,b_usd,sm,em,"half_bbo")
        b_drop=_quality_roundtrip(route,books,meta,b_usd,sm,em,"drop_l1")
        coverage=_quality_coverage3(route,books,meta,b_usd,sm)
        normal_edge=b_normal["edge"] if b_normal else None
        half_edge=b_half["edge"] if b_half else None
        drop_edge=b_drop["edge"] if b_drop else None
        result.update({"size_fraction":DEPTH_QUALITY_B_FRACTION,"selected_usd":b_usd,
            "normal_edge":normal_edge,"half_bbo_edge":half_edge,"drop_l1_edge":drop_edge,"coverage3":coverage})
        if b_half and half_edge>=SHADOW_MIN_EDGE:
            is_bplus=(half_edge>=DEPTH_QUALITY_BPLUS_HALF_EDGE and b_drop is not None and
                      drop_edge>=DEPTH_QUALITY_BPLUS_DROP_FLOOR and coverage>=DEPTH_QUALITY_COVERAGE3_MIN)
            result.update({"grade":"B+" if is_bplus else "B-",
                "eligible":bool(is_bplus and b_usd>=SIM_MIN_TRADE_USD),
                "reason":("eligible_B+" if b_usd>=SIM_MIN_TRADE_USD else "below_min_trade") if is_bplus else "rejected_B-"})
            return result

        # C is logged for counterfactual replay but is never admitted by V2.3.5.
        c_usd=capacity*DEPTH_QUALITY_C_FRACTION
        c_normal=_quality_roundtrip(route,books,meta,c_usd,sm,em,"normal")
        result.update({"grade":"C" if c_normal and c_normal["edge"]>=SHADOW_MIN_EDGE else "D",
            "eligible":False,"reason":"rejected_C" if c_normal and c_normal["edge"]>=SHADOW_MIN_EDGE else "rejected_D",
            "size_fraction":DEPTH_QUALITY_C_FRACTION,"selected_usd":c_usd,
            "normal_edge":c_normal["edge"] if c_normal else None})
        return result
    finally:
        result["compute_us"]=(time.perf_counter_ns()-started)/1000.0
        depth_quality_compute_us.append(result["compute_us"])

def _depth_book_payload(symbol):
    """Small replay snapshot of the local multi-level book. Kept only around decisions/stable rebalance pairs."""
    ts=now_ms()
    with depth_lock:
        ob=state["depth"].get(symbol)
        if not ob:
            return {"ready":0,"version":None,"book_ts":None,"send_ts":None,"recv_age":None,"bids":[],"asks":[]}
        ready=1 if ob.get("ready") else 0
        bids=sorted(ob.get("bids",{}).items(), reverse=True)[:DEPTH_CAPTURE_LEVELS]
        asks=sorted(ob.get("asks",{}).items())[:DEPTH_CAPTURE_LEVELS]
        book_ts=ob.get("ts"); send_ts=ob.get("send_ts"); version=ob.get("version")
    return {"ready":ready,"version":version,"book_ts":book_ts,"send_ts":send_ts,
            "recv_age":(ts-book_ts if book_ts else None),"bids":bids,"asks":asks}


def capture_decision_depth(q, sample_ts):
    route=q["route"]; did=q["did"]; t0=q["t0"]
    for leg,sym in enumerate(route["symbols"],1):
        b=_depth_book_payload(sym)
        with depth_resync_lock:
            ready_ts=depth_resync_diag[sym].get("last_ready_ts_ms")
        frm,to=route["path"][leg-1],route["path"][leg]
        put_db(("depth_sample_v234",(did,route["id"],t0,sample_ts,max(0,sample_ts-t0),leg,sym,frm,to,
            b["ready"],b["version"],b["book_ts"],b["send_ts"],b["recv_age"],ready_ts,
            json.dumps(b["bids"],separators=(",",":")),json.dumps(b["asks"],separators=(",",":")))))


def symbols_post_resync_safe(route, ts):
    """Never admit a Shadow trade immediately after one of its own books has been rebuilt."""
    with depth_resync_lock:
        for sym in route["symbols"]:
            rt=depth_resync_diag[sym].get("last_ready_ts_ms")
            if rt is not None and ts-rt < POST_RESYNC_GRACE_MS:
                return False
    return True

def exposure_mark_usd(exp, books, meta):
    """Mark an open intermediate exposure without ever forcing it to zero."""
    units=max(0.0,float(exp.get("units",0.0)))
    if units<=0: return 0.0
    sym=exp.get("symbol"); mid=exp.get("mid_asset"); start=exp.get("start_asset")
    m=meta.get(sym); b=books.get(sym); start_mark=stable_mark_usdt(start,books,meta) if start else 1.0
    if m and b and now_ms()-b.get("ts",0)<=MAX_BBO_AGE_MS:
        if mid==m["base"] and start==m["quote"] and b.get("bid",0)>0:
            return units*b["bid"]*(1.0-FEE)*start_mark
        if mid==m["quote"] and start==m["base"] and b.get("ask",0)>0:
            return (units/b["ask"])*(1.0-FEE)*start_mark
    return max(0.0,float(exp.get("last_mark_usd",0.0)))

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
        row=con.execute("SELECT * FROM paper_state WHERE day=?",(day,)).fetchone()
        if row:
            paper={"day":row[0],"balances":json.loads(row[2]),"profit_bank":float(row[3]),"trades":int(row[4]),
                   "rebalances":int(row[5]),"rebalance_cost":float(row[6]),"skipped_flash":int(row[7]),
                   "skipped_balance":int(row[8]),"skipped_rebalance":int(row[9])}
        else:
            paper=paper_default(day)
        paper_open_exposures.clear()
        for r in con.execute("SELECT decision_id,day,route_id,opened_ts_ms,updated_ts_ms,symbol,mid_asset,start_asset,units,recovered_start_units,input_usd,cash_output_usd,recovered_usd,last_mark_usd,status FROM paper_open_exposures WHERE day=? AND status='open' AND units>0",(day,)).fetchall():
            paper_open_exposures[r[0]]={"decision_id":r[0],"day":r[1],"route_id":r[2],"opened_ts_ms":r[3],"updated_ts_ms":r[4],"symbol":r[5],"mid_asset":r[6],"start_asset":r[7],"units":float(r[8]),"recovered_start_units":float(r[9]),"input_usd":float(r[10]),"cash_output_usd":float(r[11]),"recovered_usd":float(r[12]),"last_mark_usd":float(r[13]),"status":r[14]}
        con.close()
    except Exception as e:
        logerr(f"paper restore: {e}")
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
    """Irreversible paper execution. Once leg 1 fills, every outcome remains in NAV/PnL.
    If leg 2 is partial, immediately unwind what the current reverse depth can absorb.
    Any remainder becomes a real open paper exposure and is retried until liquidated; it is never valued at zero.
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
        open_units=max(0.0,remaining-unwind_used)
        paper_reserved[s]=max(0.0,paper_reserved.get(s,0.0)-reserved)

        paper["balances"][s]-=input_units
        paper["balances"][s]+=start_back
        paper["balances"][e]+=end_out
        sm=stable_mark_usdt(s,books,meta); em=stable_mark_usdt(e,books,meta)
        cash_output_usd=start_back*sm + end_out*em
        realized_delta=cash_output_usd-input_usd
        paper["trades"]+=1
        paper["profit_bank"]+=realized_delta

        status="completed" if remaining<=1e-12 else ("forced_unwind" if open_units<=1e-12 else "open_exposure")
        mark_usd=0.0
        if open_units>1e-12:
            exp={"decision_id":q["decision_id"],"day":paper["day"],"route_id":r["id"],"opened_ts_ms":exec_ts,"updated_ts_ms":exec_ts,
                 "symbol":sym1,"mid_asset":mid,"start_asset":s,"units":open_units,"recovered_start_units":0.0,"input_usd":input_usd,"cash_output_usd":cash_output_usd,"recovered_usd":0.0,"last_mark_usd":0.0,"status":"open"}
            mark_usd=exposure_mark_usd(exp,books,meta)
            exp["last_mark_usd"]=mark_usd
            paper_open_exposures[q["decision_id"]]=exp
            put_db(("open_exposure",(exp["decision_id"],exp["day"],exp["route_id"],exp["opened_ts_ms"],exp["updated_ts_ms"],exp["symbol"],exp["mid_asset"],exp["start_asset"],exp["units"],0.0,input_usd,cash_output_usd,0.0,mark_usd,"open")))

        economic_output_usd=cash_output_usd+mark_usd
        economic_profit=economic_output_usd-input_usd
        put_db(("paper_attempt",(paper["day"],exec_ts,q["decision_id"],r["id"],q["event_start_ts"],status,input_usd,
            input_units,mid_total,leg2_used,end_out,unwind_used,start_back,open_units,economic_output_usd,economic_profit)))
        if status=="completed":
            put_db(("paper_trade",(paper["day"],exec_ts,r["id"],s,e,input_units,input_usd,end_out,end_out*em,
                ((end_out*em/input_usd)-1.0)*100.0 if input_usd else 0.0,economic_profit,q["event_start_ts"])))
        persist_paper(); return True

def open_exposure_worker():
    """Continuously liquidate residual intermediate inventory through the reverse first leg."""
    while True:
        time.sleep(0.10)
        with paper_lock:
            if not paper_open_exposures: continue
            with state_lock:
                meta=state["market_meta"]; books=dict(state["bbo"])
            for did,exp in list(paper_open_exposures.items()):
                units=max(0.0,float(exp.get("units",0.0)))
                if units<=1e-12:
                    paper_open_exposures.pop(did,None); continue
                fill=depth_walk(exp["symbol"],exp["mid_asset"],exp["start_asset"],units,meta)
                if not fill: 
                    exp["last_mark_usd"]=exposure_mark_usd(exp,books,meta)
                    continue
                used=fill["input_used"]; recovered=fill["output"]
                if used<=0: continue
                exp["units"]=max(0.0,units-used)
                exp["recovered_start_units"]+=recovered
                exp["updated_ts_ms"]=now_ms()
                paper["balances"][exp["start_asset"]]=paper["balances"].get(exp["start_asset"],0.0)+recovered
                recovered_usd=recovered*stable_mark_usdt(exp["start_asset"],books,meta)
                paper["profit_bank"]+=recovered_usd
                exp["recovered_usd"]+=recovered_usd
                exp["last_mark_usd"]=exposure_mark_usd(exp,books,meta) if exp["units"]>1e-12 else 0.0
                if exp["units"]<=1e-12:
                    final_output=exp["cash_output_usd"]+exp["recovered_usd"]
                    final_profit=final_output-exp["input_usd"]
                    put_db(("close_exposure",(exp["updated_ts_ms"],exp["recovered_start_units"],exp["recovered_usd"],did,final_output,final_profit,did)))
                    paper_open_exposures.pop(did,None)
                else:
                    put_db(("open_exposure",(did,exp["day"],exp["route_id"],exp["opened_ts_ms"],exp["updated_ts_ms"],exp["symbol"],exp["mid_asset"],exp["start_asset"],exp["units"],exp["recovered_start_units"],exp["input_usd"],exp["cash_output_usd"],exp["recovered_usd"],exp["last_mark_usd"],"open")))
                persist_paper()


# ---------- Shadow-live engine (no real orders) ----------
def shadow_default(day):
    weights={a:SHADOW_TARGET_WEIGHTS.get(a,0.0) for a in STABLES}; sw=sum(weights.values())
    if sw<=0: weights={a:1.0/max(1,len(STABLES)) for a in STABLES}
    else: weights={a:w/sw for a,w in weights.items()}
    return {"day":day,"balances":{a:SIM_CAPITAL*weights[a] for a in STABLES},"profit_bank":0.0,"trades":0,"wins":0,"losses":0,
            "rebalances":0,"rebalance_cost":0.0,"skipped_flash":0,"skipped_balance":0,"skipped_rebalance":0}

def load_shadow_state():
    _,_,day=local_day_bounds_ms(0)
    with shadow_lock:
        for profile in SHADOW_PROFILES:
            shadow_states[profile]=shadow_default(day)
            shadow_reserved[profile].clear(); shadow_open_exposures[profile].clear()
    try:
        con=sqlite3.connect(DB_PATH,timeout=10)
        for profile in SHADOW_PROFILES:
            r=con.execute("SELECT day,balances_json,profit_bank,trades,wins,losses,rebalances,rebalance_cost,skipped_flash,skipped_balance,skipped_rebalance FROM shadow_state WHERE profile=? AND day=?",(profile,day)).fetchone()
            if r:
                shadow_states[profile]={"day":r[0],"balances":json.loads(r[1]),"profit_bank":float(r[2]),"trades":int(r[3]),"wins":int(r[4]),"losses":int(r[5]),"rebalances":int(r[6]),"rebalance_cost":float(r[7]),"skipped_flash":int(r[8]),"skipped_balance":int(r[9]),"skipped_rebalance":int(r[10])}
            for x in con.execute("SELECT decision_id,day,route_id,opened_ts_ms,updated_ts_ms,symbol,mid_asset,start_asset,units,recovered_start_units,input_usd,cash_output_usd,recovered_usd,last_mark_usd,status FROM shadow_open_exposures WHERE profile=? AND day=? AND status='open' AND units>0",(profile,day)).fetchall():
                shadow_open_exposures[profile][x[0]]={"decision_id":x[0],"day":x[1],"route_id":x[2],"opened_ts_ms":x[3],"updated_ts_ms":x[4],"symbol":x[5],"mid_asset":x[6],"start_asset":x[7],"units":float(x[8]),"recovered_start_units":float(x[9]),"input_usd":float(x[10]),"cash_output_usd":float(x[11]),"recovered_usd":float(x[12]),"last_mark_usd":float(x[13]),"status":x[14]}
        con.close()
    except Exception as e: logerr(f"shadow restore: {e}")

def ensure_shadow_day(profile):
    _,_,day=local_day_bounds_ms(0)
    st=shadow_states.get(profile)
    if not st or st.get("day")!=day:
        shadow_states[profile]=shadow_default(day); shadow_reserved[profile].clear(); shadow_open_exposures[profile].clear(); persist_shadow(profile)
    return shadow_states[profile]

def persist_shadow(profile):
    st=shadow_states[profile]
    put_db(("shadow_state",(profile,st["day"],now_ms(),json.dumps(st["balances"],sort_keys=True),st["profit_bank"],st["trades"],st["wins"],st["losses"],st["rebalances"],st["rebalance_cost"],st["skipped_flash"],st["skipped_balance"],st["skipped_rebalance"])))

def shadow_choose_rebalance(profile,target_asset,needed_units,event_x,books,meta):
    st=shadow_states[profile]; target_mark=stable_mark_usdt(target_asset,books,meta)
    if target_mark<=0 or needed_units<=0 or st["profit_bank"]<=0: return None
    initial_each=SIM_CAPITAL/len(STABLES); candidates=[]
    for donor,bal in st["balances"].items():
        if donor==target_asset: continue
        donor_mark=stable_mark_usdt(donor,books,meta); reserve_units=(initial_each*SIM_DONOR_RESERVE_PCT)/max(donor_mark,1e-12)
        available=max(0.0,bal-shadow_reserved[profile].get(donor,0.0)-reserve_units)
        if available<=0: continue
        probe=stable_conversion(donor,target_asset,available,books,meta)
        if not probe or probe["rate"]<=0: continue
        conv=stable_conversion(donor,target_asset,min(available,needed_units/probe["rate"]),books,meta)
        if not conv: continue
        expected=conv["output"]*target_mark*max(event_x["net"],0.0); budget=st["profit_bank"]*REBALANCE_MAX_PROFIT_SHARE
        if conv["cost"]>budget+1e-12: continue
        if conv["cost"]>0 and expected<conv["cost"]*REBALANCE_EDGE_MULT: continue
        candidates.append((expected-conv["cost"],donor,conv))
    if not candidates: return None
    candidates.sort(reverse=True,key=lambda x:x[0]); return candidates[0][1],candidates[0][2]

def reserve_shadow_trade(profile,route,event_x,event_start_ts,decision_id,decision_ts):
    with shadow_lock:
        st=ensure_shadow_day(profile)
        with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
        s,e=route["path"][0],route["path"][-1]
        if s not in st["balances"] or e not in st["balances"]: return None
        sm=event_x["start_mark"]; desired=event_x["size"]/max(sm,1e-12)
        free=max(0.0,st["balances"].get(s,0.0)-shadow_reserved[profile].get(s,0.0)); shortage=max(0.0,desired-free)
        if shortage>0:
            rb=shadow_choose_rebalance(profile,s,shortage,event_x,books,meta)
            if rb:
                donor,conv=rb; st["balances"][donor]-=conv["input"]; st["balances"][s]+=conv["output"]; st["rebalances"]+=1; st["rebalance_cost"]+=conv["cost"]; st["profit_bank"]=max(0.0,st["profit_bank"]-conv["cost"]*REBALANCE_COVER_MULT)
                free=max(0.0,st["balances"].get(s,0.0)-shadow_reserved[profile].get(s,0.0))
            else: st["skipped_rebalance"]+=1
        input_units=min(free,desired); input_usd=input_units*sm
        if input_usd<SIM_MIN_TRADE_USD: st["skipped_balance"]+=1; persist_shadow(profile); return None
        shadow_reserved[profile][s]+=input_units; persist_shadow(profile)
        l1,l2=SHADOW_PROFILES[profile]
        return {"profile":profile,"decision_id":decision_id,"route":route,"event_start_ts":event_start_ts,"t0":decision_ts,"x0":dict(event_x),"start_asset":s,"end_asset":e,"reserved_units":input_units,"reserved_usd":input_usd,"leg1_due":decision_ts+l1,"leg2_due":decision_ts+l2,"stage":1}

def shadow_leg1(q):
    r=q["route"]; sym=r["symbols"][0]; frm,to=r["path"][0],r["path"][1]
    with state_lock: meta=state["market_meta"]
    fill=depth_walk(sym,frm,to,q["reserved_units"],meta)
    if not fill or fill["input_used"]*q["x0"]["start_mark"]<SIM_MIN_TRADE_USD: return False
    q["leg1"]=fill; q["leg1_ts_ms"]=now_ms(); q["stage"]=2; return True

def settle_shadow_trade(q,exec_ts):
    profile=q["profile"]
    with shadow_lock:
        st=ensure_shadow_day(profile); s,e=q["start_asset"],q["end_asset"]; reserved=q["reserved_units"]; r=q["route"]; mid=r["path"][1]; sym1,sym2=r["symbols"]
        with state_lock: meta=state["market_meta"]; books=dict(state["bbo"])
        leg1=q["leg1"]; mid_total=leg1["output"]; input_units=min(leg1["input_used"],st["balances"].get(s,0.0)); input_usd=input_units*q["x0"]["start_mark"]
        fill2=depth_walk(sym2,mid,e,mid_total,meta); leg2_used=fill2["input_used"] if fill2 else 0.0; end_out=fill2["output"] if fill2 else 0.0
        remaining=max(0.0,mid_total-leg2_used); unwind=depth_walk(sym1,mid,s,remaining,meta) if remaining>1e-12 else None
        unwind_used=unwind["input_used"] if unwind else 0.0; start_back=unwind["output"] if unwind else 0.0; open_units=max(0.0,remaining-unwind_used)
        shadow_reserved[profile][s]=max(0.0,shadow_reserved[profile].get(s,0.0)-reserved)
        st["balances"][s]-=input_units; st["balances"][s]+=start_back; st["balances"][e]+=end_out
        sm=stable_mark_usdt(s,books,meta); em=stable_mark_usdt(e,books,meta); cash_output=start_back*sm+end_out*em
        mark_usd=0.0; status="completed" if remaining<=1e-12 else ("forced_unwind" if open_units<=1e-12 else "open_exposure")
        if open_units>1e-12:
            exp={"decision_id":q["decision_id"],"day":st["day"],"route_id":r["id"],"opened_ts_ms":exec_ts,"updated_ts_ms":exec_ts,"symbol":sym1,"mid_asset":mid,"start_asset":s,"units":open_units,"recovered_start_units":0.0,"input_usd":input_usd,"cash_output_usd":cash_output,"recovered_usd":0.0,"last_mark_usd":0.0,"status":"open"}
            mark_usd=exposure_mark_usd(exp,books,meta); exp["last_mark_usd"]=mark_usd; shadow_open_exposures[profile][q["decision_id"]]=exp
            put_db(("shadow_open",(profile,exp["decision_id"],exp["day"],exp["route_id"],exp["opened_ts_ms"],exp["updated_ts_ms"],exp["symbol"],exp["mid_asset"],exp["start_asset"],exp["units"],0.0,input_usd,cash_output,0.0,mark_usd,"open")))
        economic_output=cash_output+mark_usd; profit=economic_output-input_usd; st["trades"]+=1; st["profit_bank"]+=(cash_output-input_usd)
        if profit>0: st["wins"]+=1
        elif profit<0: st["losses"]+=1
        put_db(("shadow_attempt",(profile,st["day"],exec_ts,q["decision_id"],r["id"],q["event_start_ts"],status,input_usd,input_units,mid_total,leg2_used,end_out,unwind_used,start_back,open_units,economic_output,profit)))
        put_db(("shadow_exec_detail_v234",(profile,q["decision_id"],r["id"],q["t0"],q.get("leg1_ts_ms"),exec_ts,q["x0"].get("net",0.0),
            leg1.get("levels_used"),leg1.get("vwap"),leg1.get("fill_ratio"),leg1.get("book_ts"),
            fill2.get("levels_used") if fill2 else None,fill2.get("vwap") if fill2 else None,fill2.get("fill_ratio") if fill2 else None,fill2.get("book_ts") if fill2 else None,
            unwind.get("levels_used") if unwind else None,unwind.get("vwap") if unwind else None,unwind.get("fill_ratio") if unwind else None,unwind.get("book_ts") if unwind else None)))
        persist_shadow(profile)

def shadow_exposure_worker():
    while True:
        time.sleep(.10)
        with shadow_lock:
            with state_lock: meta=state["market_meta"]; books=dict(state["bbo"])
            for profile in SHADOW_PROFILES:
                st=ensure_shadow_day(profile)
                for did,exp in list(shadow_open_exposures[profile].items()):
                    units=max(0.0,float(exp.get("units",0.0)))
                    if units<=1e-12: shadow_open_exposures[profile].pop(did,None); continue
                    fill=depth_walk(exp["symbol"],exp["mid_asset"],exp["start_asset"],units,meta)
                    if not fill: exp["last_mark_usd"]=exposure_mark_usd(exp,books,meta); continue
                    used,recovered=fill["input_used"],fill["output"]
                    if used<=0: continue
                    exp["units"]=max(0.0,units-used); exp["recovered_start_units"]+=recovered; exp["updated_ts_ms"]=now_ms(); st["balances"][exp["start_asset"]]=st["balances"].get(exp["start_asset"],0.0)+recovered
                    recovered_usd=recovered*stable_mark_usdt(exp["start_asset"],books,meta); exp["recovered_usd"]+=recovered_usd; st["profit_bank"]+=recovered_usd; exp["last_mark_usd"]=exposure_mark_usd(exp,books,meta) if exp["units"]>1e-12 else 0.0
                    if exp["units"]<=1e-12:
                        final_output=exp["cash_output_usd"]+exp["recovered_usd"]; final_profit=final_output-exp["input_usd"]
                        put_db(("shadow_close",(exp["updated_ts_ms"],exp["recovered_start_units"],exp["recovered_usd"],profile,did,final_output,final_profit,profile,did)))
                        shadow_open_exposures[profile].pop(did,None)
                    else:
                        put_db(("shadow_open",(profile,did,exp["day"],exp["route_id"],exp["opened_ts_ms"],exp["updated_ts_ms"],exp["symbol"],exp["mid_asset"],exp["start_asset"],exp["units"],exp["recovered_start_units"],exp["input_usd"],exp["cash_output_usd"],exp["recovered_usd"],exp["last_mark_usd"],"open")))
                    persist_shadow(profile)

def _shadow_weight_snapshot(profile, books, meta):
    st=ensure_shadow_day(profile)
    vals={a:max(0.0,st["balances"].get(a,0.0)-shadow_reserved[profile].get(a,0.0))*stable_mark_usdt(a,books,meta) for a in STABLES}
    total=sum(vals.values())
    weights={a:(vals[a]/total if total>0 else 0.0) for a in STABLES}
    targets={a:SHADOW_TARGET_WEIGHTS.get(a,0.0) for a in STABLES}; sw=sum(targets.values()) or 1.0
    targets={a:v/sw for a,v in targets.items()}
    return vals,total,weights,targets


def shadow_target_rebalance(profile, reason):
    """Virtual periodic rebalance through real local stablecoin Depth. Never touches reserved funds or open exposures."""
    with shadow_lock:
        st=ensure_shadow_day(profile)
        if any(v>1e-9 for v in shadow_reserved[profile].values()) or shadow_open_exposures[profile]:
            return False
        with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
        vals,total,weights,targets=_shadow_weight_snapshot(profile,books,meta)
        if total<=0: return False
        before=json.dumps(st["balances"],sort_keys=True)
        changed=False
        # At most a few direct stable conversions are required for three buckets.
        for _ in range(6):
            vals,total,weights,targets=_shadow_weight_snapshot(profile,books,meta)
            deficits=sorted([(targets[a]*total-vals[a],a) for a in STABLES if targets[a]*total-vals[a]>0.50],reverse=True)
            excess=sorted([(vals[a]-targets[a]*total,a) for a in STABLES if vals[a]-targets[a]*total>0.50],reverse=True)
            if not deficits or not excess: break
            need_usd,to_asset=deficits[0]; give_usd,from_asset=excess[0]
            sym,m=direct_pair(from_asset,to_asset,meta)
            if not sym: break
            from_mark=stable_mark_usdt(from_asset,books,meta); to_mark=stable_mark_usdt(to_asset,books,meta)
            if from_mark<=0 or to_mark<=0: break
            free_units=max(0.0,st["balances"].get(from_asset,0.0)-shadow_reserved[profile].get(from_asset,0.0))
            input_units=min(free_units,min(give_usd,need_usd)/from_mark)
            if input_units<=1e-9: break
            fill=depth_walk(sym,from_asset,to_asset,input_units,meta)
            if not fill or fill.get("input_used",0)<=0: break
            used=fill["input_used"]; out=fill["output"]
            in_usd=used*from_mark; out_usd=out*to_mark; cost=max(0.0,in_usd-out_usd)
            bbefore=json.dumps(st["balances"],sort_keys=True)
            st["balances"][from_asset]=max(0.0,st["balances"].get(from_asset,0.0)-used)
            st["balances"][to_asset]=st["balances"].get(to_asset,0.0)+out
            st["rebalances"]+=1; st["rebalance_cost"]+=cost; st["profit_bank"]=max(0.0,st["profit_bank"]-cost)
            bafter=json.dumps(st["balances"],sort_keys=True)
            put_db(("shadow_rebalance_v234",(profile,st["day"],now_ms(),reason,from_asset,to_asset,used,out,in_usd,out_usd,cost,bbefore,bafter)))
            changed=True
        if changed: persist_shadow(profile)
        return changed


def shadow_periodic_rebalance_worker():
    while True:
        time.sleep(5.0); ts=now_ms()
        for profile in SHADOW_PROFILES:
            last_check=shadow_last_rebalance_check_ms.get(profile,0)
            if ts-last_check < SHADOW_REBALANCE_CHECK_SEC*1000: continue
            shadow_last_rebalance_check_ms[profile]=ts
            with shadow_lock:
                with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
                vals,total,weights,targets=_shadow_weight_snapshot(profile,books,meta)
                max_dev=max((abs(weights.get(a,0)-targets.get(a,0)) for a in STABLES),default=0.0)
            last=shadow_last_rebalance_ms.get(profile,ts)
            reason=None
            if max_dev>=SHADOW_REBALANCE_EARLY_DEV: reason="early_imbalance"
            elif ts-last>=SHADOW_REBALANCE_MAX_SEC*1000 and max_dev>=SHADOW_REBALANCE_MIN_DEV: reason="periodic_4h"
            if reason and shadow_target_rebalance(profile,reason): shadow_last_rebalance_ms[profile]=ts


def stable_depth_capture_worker():
    while True:
        time.sleep(STABLE_DEPTH_INTERVAL_SEC); ts=now_ms(); rows=[]
        with state_lock: meta=dict(state["market_meta"])
        seen=set()
        for a in STABLES:
            for b in STABLES:
                if a>=b: continue
                sym,m=direct_pair(a,b,meta)
                if not sym or sym in seen: continue
                seen.add(sym); z=_depth_book_payload(sym)
                rows.append((ts,sym,m["base"],m["quote"],z["ready"],z["version"],z["book_ts"],json.dumps(z["bids"],separators=(",",":")),json.dumps(z["asks"],separators=(",",":"))))
        if rows: put_db(("stable_depth_many_v234",rows))

def shadow_status():
    out={}
    with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
    with shadow_lock:
        for profile,(l1,l2) in SHADOW_PROFILES.items():
            st=dict(ensure_shadow_day(profile)); st["balances"]=dict(st["balances"])
            nav=sum(v*stable_mark_usdt(a,books,meta) for a,v in st["balances"].items())
            exposures=[dict(x) for x in shadow_open_exposures[profile].values()]; exp_nav=sum(exposure_mark_usd(x,books,meta) for x in exposures); nav+=exp_nav
            st.update({"profile":profile,"leg1_ms":l1,"leg2_ms":l2,"capital":SIM_CAPITAL,"nav":nav,"profit":nav-SIM_CAPITAL,"return_pct":(nav/SIM_CAPITAL-1)*100 if SIM_CAPITAL else 0.0,"open_exposures":len(exposures),"open_exposure_nav":exp_nav,"reserved":dict(shadow_reserved[profile])})
            out[profile]=st
    # Recompute W/L and average admitted edge from immutable attempts so delayed unwind updates are reflected.
    start,end,_=local_day_bounds_ms(0)
    try:
        con=db_connect()
        for profile in out:
            r=con.execute("""SELECT COUNT(*),SUM(CASE WHEN a.profit_usd>0 THEN 1 ELSE 0 END),SUM(CASE WHEN a.profit_usd<0 THEN 1 ELSE 0 END),AVG(d.decision_net),COALESCE(SUM(a.profit_usd),0) FROM shadow_attempts a LEFT JOIN decisions_v22 d ON d.decision_id=a.decision_id WHERE a.profile=? AND a.ts_ms>=? AND a.ts_ms<?""",(profile,start,end)).fetchone()
            out[profile]["trades"]=r[0] or 0; out[profile]["wins"]=r[1] or 0; out[profile]["losses"]=r[2] or 0; out[profile]["avg_edge"]=r[3]; out[profile]["attempt_pnl"]=r[4] or 0.0
        con.close()
    except Exception as e: logerr(f"shadow status stats: {e}")
    return out

# ---------- V2.1 immediate-decision / delayed-execution research ----------
pending_trials=[]
pending_paper_trials=[]
pending_shadow_trials=[]
pending_lock=threading.RLock()
last_decision_ms={}
last_quality_decision_ms={}
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
    # Broad research is sampled every 250 ms, but a fresh crossing of the 0.35% live-policy
    # threshold must not wait behind a sub-threshold research sample.
    rid=route["id"]
    broad_due=ts-last_decision_ms.get(rid,0)>=DECISION_COOLDOWN_MS
    quality_due=x.get("net",-1.0)>=SHADOW_MIN_EDGE and ts-last_quality_decision_ms.get(rid,0)>=DECISION_COOLDOWN_MS
    if not broad_due and not quality_due: return
    if x.get("max_age",999999)>DECISION_MAX_RECV_AGE_MS: return
    if x.get("bbo_skew",999999)>DECISION_MAX_SKEW_MS: return
    if broad_due: last_decision_ms[rid]=ts
    if quality_due: last_quality_decision_ms[rid]=ts
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
        for off in DEPTH_CAPTURE_OFFSETS_MS:
            if off>0: pending_depth_captures.append({"due":ts+off,"did":did,"route":route,"t0":ts})
    if 0 in DEPTH_CAPTURE_OFFSETS_MS:
        capture_decision_depth({"did":did,"route":route,"t0":ts},ts)

    # Separate research portfolio: one executable entry per arbitrage event, real capital reserved at T0.
    event_key=(route["id"],int(event_start_ts))
    with depth_resync_lock:
        ready_ages=[]
        for _sym in route["symbols"]:
            _rt=depth_resync_diag[_sym].get("last_ready_ts_ms")
            ready_ages.append((ts-_rt) if _rt is not None else None)
    _policy_reason="base_eligible"
    if x.get("net",-1.0)<SHADOW_MIN_EDGE: _policy_reason="edge_below_min"
    elif x.get("max_age",999999)>SHADOW_BBO_AGE_MS: _policy_reason="book_age"
    elif x.get("bbo_skew",999999)>SHADOW_MAX_SKEW_MS: _policy_reason="book_skew"
    elif not symbols_post_resync_safe(route,ts): _policy_reason="post_resync_grace"
    quality={"grade":"D","eligible":False,"reason":_policy_reason,"capacity_usd":x.get("size",0.0),
             "size_fraction":0.0,"selected_usd":0.0,"normal_edge":x.get("net"),"half_bbo_edge":None,
             "drop_l1_edge":None,"coverage3":None,"compute_us":0.0}
    if _policy_reason=="base_eligible":
        quality=depth_quality_decision(route,x)
        _policy_reason=quality["reason"]
    put_db(("decision_context_v234",(did,day,route["id"],int(event_start_ts),ts,">".join(route["path"]),route["path"][0],route["path"][-1],x["net"],x["size"],x.get("start_mark"),x.get("end_mark"),ready_ages[0] if len(ready_ages)>0 else None,ready_ages[1] if len(ready_ages)>1 else None,1 if quality["eligible"] else 0,_policy_reason)))
    put_db(("decision_quality_v235",(did,day,route["id"],ts,quality["grade"],1 if quality["eligible"] else 0,quality["reason"],
        quality["capacity_usd"],quality["size_fraction"],quality["selected_usd"],quality.get("normal_edge"),
        quality.get("half_bbo_edge"),quality.get("drop_l1_edge"),quality.get("coverage3"),quality["compute_us"])))
    paper_ok = ENABLE_LEGACY_PAPER and (paper_route_armed[rid] and ts-paper_route_last_trade[rid]>=PAPER_HARD_COOLDOWN_MS)
    if paper_ok and event_key not in paper_consumed_events and x.get("max_age",999999)<=PRIMARY_BBO_AGE_MS and x.get("bbo_skew",999999)<=PRIMARY_MAX_SKEW_MS:
        q=reserve_paper_trade(route,x,event_start_ts,did,ts)
        if q:
            paper_consumed_events.add(event_key); paper_route_armed[rid]=False; paper_route_last_trade[rid]=ts; paper_route_neutral_since.pop(rid,None)
            with pending_lock: pending_paper_trials.append(q)

    # Shadow-live: same event, three independent portfolios and measured depth at profile latencies.
    shadow_ok=(shadow_route_armed[rid] and ts-shadow_route_last_trade[rid]>=PAPER_HARD_COOLDOWN_MS)
    if shadow_ok and event_key not in shadow_consumed_events and quality["eligible"]:
        shadow_x=dict(x)
        shadow_x.update({"size":quality["selected_usd"],"quality_grade":quality["grade"],
            "quality_fraction":quality["size_fraction"],"quality_normal_edge":quality.get("normal_edge"),
            "quality_half_bbo_edge":quality.get("half_bbo_edge"),"quality_drop_l1_edge":quality.get("drop_l1_edge"),
            "quality_coverage3":quality.get("coverage3"),"quality_compute_us":quality["compute_us"]})
        qs=[]
        for profile in SHADOW_PROFILES:
            sq=reserve_shadow_trade(profile,route,shadow_x,event_start_ts,did,ts)
            if sq: qs.append(sq)
        if qs:
            shadow_consumed_events.add(event_key); shadow_route_armed[rid]=False; shadow_route_last_trade[rid]=ts; shadow_route_neutral_since.pop(rid,None)
            with pending_lock: pending_shadow_trials.extend(qs)

def execution_trial_worker():
    while True:
        time.sleep(.005); ts=now_ms(); due=[]; paper_due=[]; shadow_due=[]; depth_due=[]
        with pending_lock:
            keep=[]
            for q in pending_trials:
                (due if q["due"]<=ts else keep).append(q)
            pending_trials[:] = keep
            pkeep=[]
            for q in pending_paper_trials:
                (paper_due if (q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"])<=ts else pkeep).append(q)
            pending_paper_trials[:] = pkeep
            skeep=[]
            for q in pending_shadow_trials:
                (shadow_due if (q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"])<=ts else skeep).append(q)
            pending_shadow_trials[:] = skeep
            dkeep=[]
            for q in pending_depth_captures:
                (depth_due if q["due"]<=ts else dkeep).append(q)
            pending_depth_captures[:] = dkeep
        if not due and not paper_due and not shadow_due and not depth_due: continue
        with state_lock:
            books=dict(state["bbo"]); meta=state["market_meta"]
        for q in depth_due:
            capture_decision_depth(q,ts)
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
        for q in shadow_due:
            if q.get("stage",1)==1:
                if shadow_leg1(q):
                    with pending_lock: pending_shadow_trials.append(q)
                else:
                    with shadow_lock:
                        p=q["profile"]; shadow_reserved[p][q["start_asset"]]=max(0.0,shadow_reserved[p].get(q["start_asset"],0.0)-q["reserved_units"]); shadow_states[p]["skipped_flash"]+=1; persist_shadow(p)
            else:
                settle_shadow_trade(q,ts)

# ---------- Event aggregation ----------
def close_event(key,ev,end_ts=None):
    end_ts=int(end_ts or ev["last_ts"]); avg=ev["sum_net"]/max(ev["ticks"],1)
    payload=(ev["route_id"],ev["type"],ev["path"],ev["start_asset"],ev["end_asset"],ev["start_ts"],end_ts,
             max(0,end_ts-ev["start_ts"]),ev["ticks"],ev["entry_net"],ev["peak_net"],avg,ev["entry_size"],ev["max_size"],
             ev["entry_profit"],ev["peak_profit"],ev["max_bbo_age"],1 if ev.get("confirmed") else 0,
             ev.get("confirm_ts"),ev.get("confirm_net"),ev.get("confirm_size"),ev.get("confirm_profit"),ev.get("confirm_ticks"))
    put_db(("event",payload))
    paper_consumed_events.discard((ev["route_id"],int(ev["start_ts"])))
    shadow_consumed_events.discard((ev["route_id"],int(ev["start_ts"])))

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
    if not shadow_route_armed[key]:
        if x is not None and x["net"]<=PAPER_REARM_NET:
            since=shadow_route_neutral_since.get(key)
            if since is None: shadow_route_neutral_since[key]=ts
            elif ts-since>=PAPER_REARM_NEUTRAL_MS:
                shadow_route_armed[key]=True; shadow_route_neutral_since.pop(key,None)
        else:
            shadow_route_neutral_since.pop(key,None)
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

            # Decision is immediate at T0. The simulation can enter once per event when its stricter BBO/skew rules are met.
            schedule_decision(route,x,ts,ev["start_ts"])

            # No confirmation gate. Event duration remains descriptive only.
            if not ev["confirmed"]:
                ev["confirmed"]=True; ev["confirm_ts"]=ev["start_ts"]; ev["confirm_net"]=ev["entry_net"]; ev["confirm_size"]=ev["entry_size"]
                ev["confirm_profit"]=ev["entry_profit"]; ev["confirm_ticks"]=1
        elif ev is not None:
            close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)

def event_sweeper():
    while True:
        time.sleep(.03); ts=now_ms()
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
    with paper_lock:
        exposures=[dict(x) for x in paper_open_exposures.values()]
    exposure_nav=sum(exposure_mark_usd(x,books,meta) for x in exposures)
    nav+=exposure_nav
    p["open_exposures"]=len(exposures); p["open_exposure_nav"]=exposure_nav
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
    rows=con.execute("""SELECT d.decision_id,d.route_id,d.ts_ms,d.decision_net,d.decision_size_usd,d.recv_age_max_ms,d.exchange_age_max_ms,
        d.recv_skew_ms,d.exchange_skew_ms,q.grade,q.eligible,q.reason,q.size_fraction,q.selected_usd,
        q.normal_edge,q.half_bbo_edge,q.drop_l1_edge,q.coverage3,q.compute_us
        FROM decisions_v22 d LEFT JOIN decision_quality_v235 q ON q.decision_id=d.decision_id
        WHERE d.ts_ms>=? AND d.ts_ms<? ORDER BY d.ts_ms DESC LIMIT ?""",(start,end,limit)).fetchall()
    con.close(); return [dict(r) for r in rows]

def v22_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    d=con.execute("SELECT COUNT(*) n, COUNT(DISTINCT route_id) routes, AVG(exchange_age_max_ms) age, AVG(exchange_skew_ms) skew FROM decisions_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    w=con.execute("SELECT AVG(transport_ms) av, MAX(transport_ms) mx FROM ws_latency_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    tr=con.execute("SELECT COUNT(*) n FROM decision_trace_v22 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
    con.close(); return {"decisions":d[0] or 0,"routes":d[1] or 0,"avg_exchange_age_ms":d[2],"avg_exchange_skew_ms":d[3],"avg_ws_transport_ms":w[0],"max_ws_transport_ms":w[1],"trace_points":tr[0] or 0}

def v234_capture_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    try:
        a=con.execute("SELECT COUNT(*),COUNT(DISTINCT decision_id) FROM decision_depth_samples_v234 WHERE sample_ts_ms>=? AND sample_ts_ms<?",(start,end)).fetchone()
        b=con.execute("SELECT COUNT(*) FROM stable_depth_snapshots_v234 WHERE ts_ms>=? AND ts_ms<?",(start,end)).fetchone()
        c=con.execute("SELECT COUNT(*) FROM shadow_execution_details_v234 WHERE decision_ts_ms>=? AND decision_ts_ms<?",(start,end)).fetchone()
        return {"depth_samples":a[0] or 0,"depth_decisions":a[1] or 0,"stable_depth_samples":b[0] or 0,"exec_details":c[0] or 0}
    finally: con.close()

def v235_quality_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    try:
        rows=con.execute("""SELECT grade,eligible,reason,COUNT(*) n,AVG(selected_usd) avg_size,MAX(selected_usd) max_size,
            AVG(normal_edge) avg_normal_edge,AVG(half_bbo_edge) avg_half_edge,AVG(drop_l1_edge) avg_drop_edge,
            AVG(coverage3) avg_coverage3,AVG(compute_us) avg_compute_us
            FROM decision_quality_v235 WHERE ts_ms>=? AND ts_ms<? GROUP BY grade,eligible,reason ORDER BY n DESC""",(start,end)).fetchall()
        accepted=con.execute("SELECT COUNT(*),AVG(selected_usd),MAX(selected_usd) FROM decision_quality_v235 WHERE ts_ms>=? AND ts_ms<? AND eligible=1",(start,end)).fetchone()
    finally: con.close()
    perf=sorted(depth_quality_compute_us)
    def pct(q):
        if not perf: return None
        return perf[min(len(perf)-1,int((len(perf)-1)*q))]
    return {"rows":[dict(r) for r in rows],"accepted":accepted[0] or 0,"avg_selected_usd":accepted[1],"max_selected_usd":accepted[2],
            "compute":{"samples":len(perf),"p50_us":pct(.50),"p95_us":pct(.95),"p99_us":pct(.99),"max_us":max(perf) if perf else None}}

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
                 "recent_decisions":recent_v22_decisions(),"shadows":shadow_status(),"quality":v235_quality_stats(),
                 "shadow_profiles":{k:{"leg1_ms":v[0],"leg2_ms":v[1]} for k,v in SHADOW_PROFILES.items()},
                 "v235_policy":{"edge_min_pct":SHADOW_MIN_EDGE*100,"target_weights":SHADOW_TARGET_WEIGHTS,
                    "rebalance_check_min":SHADOW_REBALANCE_CHECK_SEC/60,"rebalance_max_h":SHADOW_REBALANCE_MAX_SEC/3600,
                    "early_dev_pct":SHADOW_REBALANCE_EARLY_DEV*100,"post_resync_grace_ms":POST_RESYNC_GRACE_MS,
                    "depth_capture_offsets_ms":DEPTH_CAPTURE_OFFSETS_MS,"depth_capture_levels":DEPTH_CAPTURE_LEVELS,
                    "a_fraction_pct":DEPTH_QUALITY_A_FRACTION*100,"b_fraction_pct":DEPTH_QUALITY_B_FRACTION*100,
                    "bplus_half_edge_pct":DEPTH_QUALITY_BPLUS_HALF_EDGE*100,"bplus_drop_floor_pct":DEPTH_QUALITY_BPLUS_DROP_FLOOR*100,
                    "coverage3_min":DEPTH_QUALITY_COVERAGE3_MIN,"absolute_cap_usd":0},
                 "v235_capture":v234_capture_stats()})
    return jsonify(base)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEXC 2-Leg V2.3.5</title>
<style>body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.c,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.v{font-weight:800;font-size:20px}.big{font-size:28px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.bad{color:#ff6677}.panel{margin-top:12px;overflow:auto}.latency{display:grid;grid-template-columns:repeat(3,minmax(250px,1fr));gap:10px}.latbox{background:#0c1421;border:1px solid #2a3850;border-radius:10px;padding:12px}.balance{margin-top:8px;line-height:1.7}.metricgrid{display:grid;grid-template-columns:repeat(2,minmax(95px,1fr));gap:7px;margin-top:10px}.metric{background:#111b2b;border-radius:8px;padding:8px}.tag{display:inline-block;border:1px solid #334155;border-radius:999px;padding:3px 7px;font-size:11px;color:#bac6d8}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right}th:first-child,td:first-child{text-align:left}@media(max-width:900px){.latency{grid-template-columns:1fr}}</style></head><body>
<h2>MEXC — Scanner Spot 2-leg V2.3.5 Depth Quality</h2><div class=cards id=cards></div>
<div class=panel id=latencies></div>
<div class=panel id=policy></div>
<div class=panel id=quality></div>
<div class=panel><h3>Décisions récentes — Depth Quality à T0</h3><table><thead><tr><th>Heure</th><th>Route</th><th>Edge T0</th><th>Classe</th><th>Taille retenue</th><th>Edge −50% BBO</th><th>Edge sans L1</th><th>Réserve L1–L3</th><th>Calcul</th></tr></thead><tbody id=recent></tbody></table></div>
<div class=panel id=capture></div><div class=panel id=diag></div>
<script>
async function refresh(){let d=await fetch('/api/status').then(r=>r.json()),g=d.diag,p=d.v235_policy||{},cap=d.v235_capture||{},ql=d.quality||{};
document.getElementById('cards').innerHTML=`<div class=c><div class=v>${d.symbols}</div><div class=muted>marchés WS</div></div><div class=c><div class=v>${d.routes}</div><div class=muted>routes 2-leg</div></div><div class=c><div class=v>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div><div class=c><div class=v>${d.age_ms??'-'} ms</div><div class=muted>dernier BBO reçu</div></div><div class=c><div class=v>${d.depth_ready||0}/${d.symbols}</div><div class=muted>carnets Depth prêts</div></div><div class=c><div class=v>${d.v22?.decisions||0}</div><div class=muted>décisions brutes tracées</div></div><div class=c><div class=v>${d.v22?.trace_points||0}</div><div class=muted>points trajectoire</div></div>`;
let sh=d.shadows||{},first=sh.FAST||{},h=`<div class=tag>LATENCE D'EXÉCUTION</div><h3>FAST / TARGET / DEGRADED — ${(first.capital||0).toFixed(0)} $ indépendants chacun</h3><div class=latency>`;
for(let n of ['FAST','TARGET','DEGRADED']){let x=sh[n];if(!x)continue;let bal=Object.entries(x.balances||{}).map(([a,v])=>`${a} <b>${v.toFixed(2)}</b>`).join(' · ');h+=`<div class=latbox><div class=muted>${n} · Leg1 +${x.leg1_ms} ms · Leg2 +${x.leg2_ms} ms</div><div class='v big ${x.profit>=0?'good':'bad'}'>${x.profit>=0?'+':''}${x.profit.toFixed(2)} $</div><div class=muted>NAV ${x.nav.toFixed(2)} $</div><div class=metricgrid><div class=metric><div class=v>${x.trades}</div><div class=muted>trades</div></div><div class=metric><div class=v>${x.wins} / ${x.losses}</div><div class=muted>gagnés / perdus</div></div><div class=metric><div class=v>${x.avg_edge==null?'-':(100*x.avg_edge).toFixed(3)+'%'}</div><div class=muted>edge T0 moyen</div></div><div class=metric><div class=v>${x.rebalances}</div><div class=muted>rebalances</div></div></div><div class='balance muted'>Balance : ${bal}<br>Coût rebalance ${x.rebalance_cost.toFixed(3)} $ · refus balance ${x.skipped_balance} · expositions ${x.open_exposures}</div></div>`}h+=`</div><div class=muted style='margin-top:10px'>Même signal et mêmes carnets MEXC réels. Seuls les délais virtuels diffèrent. Aucun ordre réel n'est envoyé.</div>`;document.getElementById('latencies').innerHTML=h;
let tw=Object.entries(p.target_weights||{}).map(([a,v])=>`${a} ${(100*v).toFixed(0)}%`).join(' · ');document.getElementById('policy').innerHTML=`<div class=tag>POLITIQUE V2.3.5</div><h3>Depth Quality adaptatif — aucun cap absolu</h3><div class=cards><div><div class=v>${(p.edge_min_pct??0).toFixed(2)}%</div><div class=muted>edge initial minimum</div></div><div><div class=v>A · ${(p.a_fraction_pct??0).toFixed(0)}%</div><div class=muted>capacité si suppression L1 ≥ edge min</div></div><div><div class=v>B+ · ${(p.b_fraction_pct??0).toFixed(0)}%</div><div class=muted>capacité si −50% BBO ≥ ${(p.bplus_half_edge_pct??0).toFixed(2)}%</div></div><div><div class=v>×${(p.coverage3_min??0).toFixed(0)}</div><div class=muted>réserve minimum niveaux 1–3</div></div><div><div class=v>${tw}</div><div class=muted>allocation cible</div></div><div><div class=v>${p.rebalance_check_min??'-'} min / ${p.rebalance_max_h??'-'} h</div><div class=muted>contrôle / rebalance maximum</div></div></div><div class=muted style='margin-top:10px'>A : 50% de la capacité robuste. B+ : 33%, edge après −50% BBO ≥0,75%, edge sans niveau 1 ≥−2%, réserve L1–L3 ≥×3. B−, C et D sont enregistrés mais refusés. Taille finale limitée uniquement par le solde disponible.</div>`;
let qc={};for(let r of (ql.rows||[])){qc[r.grade]=(qc[r.grade]||0)+r.n}let perf=ql.compute||{};document.getElementById('quality').innerHTML=`<div class=tag>QUALITÉ DEPTH AUJOURD'HUI</div><h3>A / B+ admis — B− / C / D observés</h3><div class=cards><div><div class=v good>${qc['A']||0}</div><div class=muted>classe A</div></div><div><div class=v good>${qc['B+']||0}</div><div class=muted>classe B+</div></div><div><div class=v>${qc['B-']||0}</div><div class=muted>B− refusés</div></div><div><div class=v>${(qc['C']||0)+(qc['D']||0)}</div><div class=muted>C / D refusés</div></div><div><div class=v>${ql.max_selected_usd==null?'-':ql.max_selected_usd.toFixed(2)+' $'}</div><div class=muted>taille Depth max admise</div></div><div><div class=v>${perf.p95_us==null?'-':perf.p95_us.toFixed(1)+' µs'}</div><div class=muted>temps calcul p95</div></div></div>`;
let fp=v=>v==null?'-':(100*v).toFixed(3)+'%',fn=v=>v==null?'-':Number(v).toFixed(2);let rh='';for(let q of (d.recent_decisions||[])){let t=new Date(q.ts_ms),ok=q.eligible===1;rh+=`<tr><td>${t.toLocaleTimeString()}</td><td>${q.route_id}</td><td>${fp(q.decision_net)}</td><td class='${ok?'good':'muted'}'>${q.grade||q.reason||'-'}</td><td>${q.selected_usd==null?'-':q.selected_usd.toFixed(2)+' $'}</td><td>${fp(q.half_bbo_edge)}</td><td>${fp(q.drop_l1_edge)}</td><td>${q.coverage3==null?'-':'×'+fn(q.coverage3)}</td><td>${q.compute_us==null?'-':fn(q.compute_us)+' µs'}</td></tr>`}document.getElementById('recent').innerHTML=rh;
document.getElementById('capture').innerHTML=`<div class=tag>DATASET REPLAY V2.3.5</div><h3>Collecte jusqu'à T0 + 5 secondes</h3><div class=cards><div><div class=v>${cap.depth_decisions||0}</div><div class=muted>décisions avec Depth</div></div><div><div class=v>${cap.depth_samples||0}</div><div class=muted>snapshots multi-level</div></div><div><div class=v>${cap.exec_details||0}</div><div class=muted>exécutions VWAP détaillées</div></div><div><div class=v>${cap.stable_depth_samples||0}</div><div class=muted>snapshots Depth stablecoins</div></div></div><div class=muted style='margin-top:10px'>Offsets Depth : ${(p.depth_capture_offsets_ms||[]).join(', ')} ms · top ${(p.depth_capture_levels||'-')} niveaux par côté. Les trois latences, tailles, stress BBO, rebalances, partial fills et expositions restent rejouables.</div>`;
document.getElementById('diag').innerHTML=`<h3>Univers</h3><div class=cards><div><div class=v>${g.all_markets}</div><div class=muted>marchés découverts</div></div><div><div class=v>${g.candidate_2leg}</div><div class=muted>2-leg candidates</div></div><div><div class=v>${g.selected_2leg}</div><div class=muted>2-leg suivies</div></div><div><div class=v>${g.candidate_3leg}</div><div class=muted>3-leg détectées mais ignorées</div></div></div>`}
refresh();setInterval(refresh,3000)
</script></body></html>
'''

@app.get("/")
def index(): return render_template_string(HTML)


def bootstrap():
    threading.Thread(target=db_writer,daemon=True).start()
    # Give DB schema thread a moment before state restore.
    time.sleep(.15); load_paper_state(); load_shadow_state()
    boot_ts=now_ms()
    for _p in SHADOW_PROFILES:
        shadow_last_rebalance_ms[_p]=boot_ts; shadow_last_rebalance_check_ms[_p]=boot_ts
    markets=discover_markets(); chosen,routes,meta,diag=select_routes_under_symbol_budget(markets)
    bysym=defaultdict(list)
    for r in routes:
        for s in r["symbols"]: bysym[s].append(r)
    symbols=[m["symbol"] for m in chosen]
    with state_lock:
        state["symbols"]=symbols; state["routes"]=routes; state["routes_by_symbol"]=bysym; state["market_meta"]=meta; state["route_diag"]=diag
    groups=[symbols[i:i+20] for i in range(0,len(symbols),20)]
    with state_lock: state["ws_expected"]=len(groups)
    print(f"[Routes V{VERSION}] source={state['market_source']} markets={diag['all_markets']} candidates={diag['candidate_routes']} "
          f"(2L={diag['candidate_2leg']},3L={diag['candidate_3leg']}) selected_symbols={len(symbols)} selected_routes={len(routes)} "
          f"(2L={diag['selected_2leg']},3L={diag['selected_3leg']}) WS={len(groups)}")
    put_db(("meta",("version",VERSION))); put_db(("meta",("paper_rearm_neutral_ms",str(PAPER_REARM_NEUTRAL_MS)))); put_db(("meta",("paper_hard_cooldown_ms",str(PAPER_HARD_COOLDOWN_MS)))); put_db(("meta",("paper_failed_leg_policy","reverse_leg1_then_persist_open_exposure_until_liquidated"))); put_db(("meta",("ws_depth_interval","10ms"))); put_db(("meta",("depth_snapshot_levels","100"))); put_db(("meta",("trace_window_ms",str(TRACE_WINDOW_MS)))); put_db(("meta",("market_source",state["market_source"])))
    put_db(("meta",("route_diag",json.dumps(diag,sort_keys=True))))
    put_db(("meta",("shadow_profiles",json.dumps(SHADOW_PROFILES,sort_keys=True))))
    put_db(("meta",("v235_policy",json.dumps({"edge_min":SHADOW_MIN_EDGE,"target_weights":SHADOW_TARGET_WEIGHTS,
        "a_fraction":DEPTH_QUALITY_A_FRACTION,"b_fraction":DEPTH_QUALITY_B_FRACTION,"c_fraction":DEPTH_QUALITY_C_FRACTION,
        "bplus_half_edge":DEPTH_QUALITY_BPLUS_HALF_EDGE,"bplus_drop_floor":DEPTH_QUALITY_BPLUS_DROP_FLOOR,
        "coverage3_min":DEPTH_QUALITY_COVERAGE3_MIN,"absolute_cap_usd":0,
        "rebalance_check_sec":SHADOW_REBALANCE_CHECK_SEC,"rebalance_max_sec":SHADOW_REBALANCE_MAX_SEC,
        "early_dev":SHADOW_REBALANCE_EARLY_DEV,"post_resync_grace_ms":POST_RESYNC_GRACE_MS,
        "depth_capture_offsets_ms":DEPTH_CAPTURE_OFFSETS_MS,"depth_capture_levels":DEPTH_CAPTURE_LEVELS},sort_keys=True))))
    for _ in range(6): threading.Thread(target=scan_worker,daemon=True).start()
    threading.Thread(target=event_sweeper,daemon=True).start()
    threading.Thread(target=execution_trial_worker,daemon=True).start()
    threading.Thread(target=open_exposure_worker,daemon=True).start()
    threading.Thread(target=shadow_exposure_worker,daemon=True).start()
    threading.Thread(target=snapshot_worker,daemon=True).start()
    threading.Thread(target=stable_quote_worker,daemon=True).start()
    threading.Thread(target=stable_depth_capture_worker,daemon=True).start()
    threading.Thread(target=shadow_periodic_rebalance_worker,daemon=True).start()
    for i,g in enumerate(groups,1): threading.Thread(target=ws_worker,args=(g,i),daemon=True).start()
    time.sleep(1.0)
    threading.Thread(target=depth_resync_worker,daemon=True).start()
    threading.Thread(target=depth_bootstrap_worker,args=(symbols,),daemon=True).start()

if __name__=="__main__":
    bootstrap(); app.run(host=HOST,port=PORT,threaded=True,use_reloader=False)

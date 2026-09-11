#!/usr/bin/env python3
"""MEXC spot 3-leg scanner.

Paper/research only: this process never authenticates and never sends an order.
It discovers triangular routes, maintains local Depth books, records T0 signals,
and evaluates the same signal through FAST/TARGET/DEGRADED sequential profiles.
"""

import json
import math
import os
import queue
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import websocket
from flask import Flask, jsonify, render_template_string


VERSION = "3.0.0-tri-depth-shadow"
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8083"))
DB_PATH = os.getenv("DB_PATH", "mexc_3leg_v300.db")
MARKET_CACHE = os.getenv("MARKET_CACHE", "mexc_3leg_markets_cache.json")
TZ_NAME = os.getenv("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)

# Economics. FEE is charged independently on every one of the three legs.
FEE = float(os.getenv("TAKER_FEE", "0.0005"))
STABLES = tuple(
    x.strip().upper()
    for x in os.getenv("STABLES", "USDT,USDC,USD1").split(",")
    if x.strip()
)
MIN_EXEC_USD = float(os.getenv("MIN_EXEC_USD", "10"))
MIN_EVENT_NET = float(os.getenv("MIN_EVENT_NET_PCT", "0.01")) / 100.0
MAX_BBO_AGE_MS = int(os.getenv("MAX_BBO_AGE_MS", "750"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "750"))

# Universe and MEXC stream health.
FOLLOW_ALL_3LEG = os.getenv("FOLLOW_ALL_3LEG", "1") == "1"
MAX_WS_SYMBOLS = int(os.getenv("MAX_WS_SYMBOLS", "900"))
WS_GROUP_SIZE = max(1, min(30, int(os.getenv("WS_GROUP_SIZE", "20"))))
MEXC_APP_PING_SEC = max(5.0, float(os.getenv("MEXC_APP_PING_SEC", "15")))
MEXC_APP_PONG_TIMEOUT_SEC = max(
    MEXC_APP_PING_SEC + 5.0,
    float(os.getenv("MEXC_APP_PONG_TIMEOUT_SEC", "45")),
)
MAX_ROUTES_PER_SYMBOL_SCAN = int(os.getenv("MAX_ROUTES_PER_SYMBOL_SCAN", "10000"))
SCAN_WORKERS = max(1, int(os.getenv("SCAN_WORKERS", "6")))
WS_LATENCY_SAMPLE_MS = int(os.getenv("WS_LATENCY_SAMPLE_MS", "10000"))

# Decision and replay capture. There is no artificial confirmation wait at T0.
DECISION_COOLDOWN_MS = int(os.getenv("DECISION_COOLDOWN_MS", "250"))
DECISION_MAX_RECV_AGE_MS = int(os.getenv("DECISION_MAX_RECV_AGE_MS", "300"))
DECISION_MAX_SKEW_MS = int(os.getenv("DECISION_MAX_SKEW_MS", "250"))
DEPTH_CAPTURE_OFFSETS_MS = tuple(
    int(x)
    for x in os.getenv(
        "DEPTH_CAPTURE_OFFSETS_MS",
        "0,10,15,20,25,30,40,45,50,60,75,100,125,150,200,250,300,500,750,1000,1500,2000,3000,5000",
    ).split(",")
)
DEPTH_CAPTURE_LEVELS = int(os.getenv("DEPTH_CAPTURE_LEVELS", "25"))
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "2"))
SNAPSHOT_TOP_N = int(os.getenv("SNAPSHOT_TOP_N", "50"))

# T0 quality policy inherited from the 2-leg Depth Quality scanner. Since the
# edge is already net of all three fees, the extra 0.05% is automatically paid.
SHADOW_BBO_AGE_MS = int(os.getenv("SHADOW_BBO_AGE_MS", "150"))
SHADOW_MAX_SKEW_MS = int(os.getenv("SHADOW_MAX_SKEW_MS", "50"))
SHADOW_MIN_EDGE = float(os.getenv("SHADOW_MIN_EDGE_PCT", "0.35")) / 100.0
DEPTH_QUALITY_A_FRACTION = float(os.getenv("DEPTH_QUALITY_A_FRACTION", "0.50"))
DEPTH_QUALITY_B_FRACTION = float(os.getenv("DEPTH_QUALITY_B_FRACTION", "0.33"))
DEPTH_QUALITY_C_FRACTION = float(os.getenv("DEPTH_QUALITY_C_FRACTION", "0.25"))
DEPTH_QUALITY_BPLUS_HALF_EDGE = (
    float(os.getenv("DEPTH_QUALITY_BPLUS_HALF_EDGE_PCT", "0.75")) / 100.0
)
DEPTH_QUALITY_BPLUS_DROP_FLOOR = (
    float(os.getenv("DEPTH_QUALITY_BPLUS_DROP_FLOOR_PCT", "-2.0")) / 100.0
)
DEPTH_QUALITY_COVERAGE3_MIN = float(os.getenv("DEPTH_QUALITY_COVERAGE3_MIN", "3"))
DEPTH_QUALITY_LEVELS = int(os.getenv("DEPTH_QUALITY_LEVELS", "25"))
POST_RESYNC_GRACE_MS = int(os.getenv("POST_RESYNC_GRACE_MS", "2000"))

# Sequential trade times: one more leg than the 2-leg profiles.
SHADOW_PROFILES = {
    "FAST": (15, 30, 45),
    "TARGET": (25, 50, 75),
    "DEGRADED": (50, 100, 150),
}
EXEC_BOOK_MAX_AGE_MS = int(os.getenv("EXEC_BOOK_MAX_AGE_MS", "1000"))
SIM_CAPITAL = float(os.getenv("SIM_CAPITAL", "1000"))
SIM_MIN_TRADE_USD = float(os.getenv("SIM_MIN_TRADE_USD", "10"))
PAPER_REARM_NEUTRAL_MS = int(os.getenv("PAPER_REARM_NEUTRAL_MS", "1000"))
PAPER_REARM_NET = float(os.getenv("PAPER_REARM_NET_PCT", "0")) / 100.0
PAPER_HARD_COOLDOWN_MS = int(os.getenv("PAPER_HARD_COOLDOWN_MS", "5000"))
SHADOW_TARGET_WEIGHTS = {"USDT": 0.40, "USDC": 0.10, "USD1": 0.50}
SIM_DONOR_RESERVE_PCT = float(os.getenv("SIM_DONOR_RESERVE_PCT", "0.10"))
REBALANCE_MAX_PROFIT_SHARE = float(os.getenv("REBALANCE_MAX_PROFIT_SHARE", "0.80"))
REBALANCE_EDGE_MULT = float(os.getenv("REBALANCE_EDGE_MULT", "1.25"))
SHADOW_REBALANCE_CHECK_SEC = int(os.getenv("SHADOW_REBALANCE_CHECK_SEC", "1800"))
SHADOW_REBALANCE_MAX_SEC = int(os.getenv("SHADOW_REBALANCE_MAX_SEC", "14400"))
SHADOW_REBALANCE_EARLY_DEV = (
    float(os.getenv("SHADOW_REBALANCE_EARLY_DEV_PCT", "15")) / 100.0
)
SHADOW_REBALANCE_MIN_DEV = (
    float(os.getenv("SHADOW_REBALANCE_MIN_DEV_PCT", "3")) / 100.0
)

REST = "https://api.mexc.com"
WS = "wss://wbs-api.mexc.com/ws"

app = Flask(__name__)
state_lock = threading.RLock()
depth_lock = threading.RLock()
event_lock = threading.RLock()
shadow_lock = threading.RLock()
pending_lock = threading.RLock()
queued_lock = threading.Lock()

state = {
    "bbo": {},
    "depth": {},
    "depth_ready": 0,
    "last_ws_ms": 0,
    "ws_connected": 0,
    "ws_expected": 0,
    "ws_workers": {},
    "symbols": [],
    "routes": [],
    "routes_by_symbol": defaultdict(list),
    "market_meta": {},
    "route_diag": {},
    "latest_routes": {},
    "errors": deque(maxlen=40),
    "started_ms": int(time.time() * 1000),
    "market_source": "",
    "scan_updates": 0,
    "scan_coalesced": 0,
    "scan_queue_drops": 0,
    "db_queue_drops": 0,
    "ws_disconnects": 0,
    "ws_reconnects": 0,
    "ws_app_pings": 0,
    "ws_app_pongs": 0,
    "ws_ping_errors": 0,
    "depth_gap_events": 0,
    "depth_resync_failures": 0,
    "depth_resync_recoveries": 0,
}

active_events = {}
last_decision_ms = {}
last_quality_decision_ms = {}
pending_shadow = []
pending_captures = []
shadow_states = {}
shadow_reserved = defaultdict(lambda: defaultdict(float))
shadow_consumed_events = set()
shadow_route_armed = defaultdict(lambda: True)
shadow_route_neutral_since = {}
shadow_route_last_trade = defaultdict(int)
shadow_last_rebalance_ms = {}
shadow_last_rebalance_check_ms = {}

dbq = queue.Queue(maxsize=50000)
scanq = queue.Queue(maxsize=30000)
queued_symbols = set()
depth_buffers = defaultdict(lambda: deque(maxlen=10000))
depth_resync_q = queue.Queue()
depth_resync_pending = set()
depth_resync_lock = threading.Lock()
depth_resync_diag = defaultdict(
    lambda: {
        "drops": 0,
        "resync_ok": 0,
        "resync_fail": 0,
        "not_ready_since": None,
        "last_reason": "",
        "last_resync_ms": None,
        "last_ready_ts_ms": None,
    }
)
ws_latency_last_sample = {}
depth_quality_compute_us = deque(maxlen=10000)
pair_index = {}


def now_ms():
    return int(time.time() * 1000)


def logerr(message):
    line = f"{datetime.now(TZ).isoformat(timespec='seconds')} {message}"
    print(line, flush=True)
    with state_lock:
        state["errors"].appendleft(line)


def local_day_bounds_ms(day_offset=0):
    local_now = datetime.now(TZ) + timedelta(days=day_offset)
    start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000), start.date().isoformat()


def percentile(values, p):
    if not values:
        return None
    xs = sorted(values)
    idx = min(len(xs) - 1, max(0, int(round((len(xs) - 1) * p))))
    return xs[idx]


# ---------------------------------------------------------------------------
# SQLite: generic JSON columns keep all three legs replayable without a wide,
# brittle two-leg schema.
# ---------------------------------------------------------------------------
def db_writer():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta(
          key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS opportunities_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          route_id TEXT NOT NULL, path TEXT NOT NULL,
          start_asset TEXT NOT NULL, end_asset TEXT NOT NULL,
          start_ts_ms INTEGER NOT NULL, end_ts_ms INTEGER NOT NULL,
          duration_ms INTEGER NOT NULL, ticks INTEGER NOT NULL,
          entry_net REAL NOT NULL, peak_net REAL NOT NULL, avg_net REAL NOT NULL,
          entry_size_usd REAL NOT NULL, max_size_usd REAL NOT NULL,
          entry_profit_usd REAL NOT NULL, peak_profit_usd REAL NOT NULL,
          max_bbo_age_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_opp3_start
          ON opportunities_3leg(start_ts_ms);
        CREATE INDEX IF NOT EXISTS idx_opp3_route
          ON opportunities_3leg(route_id,start_ts_ms);

        CREATE TABLE IF NOT EXISTS decisions_3leg(
          decision_id TEXT PRIMARY KEY, day TEXT NOT NULL,
          route_id TEXT NOT NULL, path TEXT NOT NULL,
          event_start_ts_ms INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
          decision_net REAL NOT NULL, capacity_usd REAL NOT NULL,
          selected_usd REAL NOT NULL, grade TEXT NOT NULL,
          eligible INTEGER NOT NULL, policy_reason TEXT NOT NULL,
          admission TEXT NOT NULL, profiles_reserved INTEGER NOT NULL,
          recv_age_max_ms INTEGER NOT NULL, exchange_age_max_ms INTEGER NOT NULL,
          recv_skew_ms INTEGER NOT NULL, exchange_skew_ms INTEGER NOT NULL,
          legs_json TEXT NOT NULL, compute_us REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_dec3_day
          ON decisions_3leg(day,ts_ms);

        CREATE TABLE IF NOT EXISTS decision_quality_3leg(
          decision_id TEXT PRIMARY KEY, day TEXT NOT NULL,
          route_id TEXT NOT NULL, ts_ms INTEGER NOT NULL,
          grade TEXT NOT NULL, eligible INTEGER NOT NULL, reason TEXT NOT NULL,
          capacity_usd REAL NOT NULL, size_fraction REAL NOT NULL,
          selected_usd REAL NOT NULL, normal_edge REAL,
          half_bbo_edge REAL, drop_l1_edge REAL, coverage_l1_l3 REAL,
          compute_us REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_quality3_day
          ON decision_quality_3leg(day,ts_ms,grade,eligible);

        CREATE TABLE IF NOT EXISTS decision_depth_samples_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          decision_id TEXT NOT NULL, route_id TEXT NOT NULL,
          decision_ts_ms INTEGER NOT NULL, sample_ts_ms INTEGER NOT NULL,
          elapsed_ms INTEGER NOT NULL, leg INTEGER NOT NULL,
          symbol TEXT NOT NULL, path_from TEXT NOT NULL, path_to TEXT NOT NULL,
          ready INTEGER NOT NULL, version INTEGER, book_ts_ms INTEGER,
          send_ts_ms INTEGER, recv_age_ms INTEGER, last_ready_ts_ms INTEGER,
          bids_json TEXT, asks_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_depth3_dec
          ON decision_depth_samples_3leg(decision_id,elapsed_ms,leg);
        CREATE INDEX IF NOT EXISTS idx_depth3_ts
          ON decision_depth_samples_3leg(sample_ts_ms);

        CREATE TABLE IF NOT EXISTS shadow_state_3leg(
          profile TEXT NOT NULL, day TEXT NOT NULL, updated_ts_ms INTEGER NOT NULL,
          balances_json TEXT NOT NULL, profit_bank REAL NOT NULL,
          completed INTEGER NOT NULL, wins INTEGER NOT NULL, losses INTEGER NOT NULL,
          failed_sequences INTEGER NOT NULL, rebalances INTEGER NOT NULL,
          rebalance_cost REAL NOT NULL, skipped_balance INTEGER NOT NULL,
          skipped_rebalance INTEGER NOT NULL,
          PRIMARY KEY(profile,day)
        );

        CREATE TABLE IF NOT EXISTS shadow_attempts_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL, day TEXT NOT NULL, decision_id TEXT NOT NULL,
          route_id TEXT NOT NULL, path TEXT NOT NULL,
          decision_ts_ms INTEGER NOT NULL, finished_ts_ms INTEGER NOT NULL,
          status TEXT NOT NULL, failed_leg INTEGER,
          input_units REAL NOT NULL, input_usd REAL NOT NULL,
          output_units REAL, output_usd REAL, profit_usd REAL,
          decision_net REAL NOT NULL, selected_usd REAL NOT NULL,
          legs_json TEXT NOT NULL,
          UNIQUE(profile,decision_id)
        );
        CREATE INDEX IF NOT EXISTS idx_attempt3_day
          ON shadow_attempts_3leg(profile,day,finished_ts_ms);

        CREATE TABLE IF NOT EXISTS shadow_rebalances_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          profile TEXT NOT NULL, day TEXT NOT NULL, ts_ms INTEGER NOT NULL,
          reason TEXT NOT NULL, from_asset TEXT NOT NULL, to_asset TEXT NOT NULL,
          input_units REAL NOT NULL, output_units REAL NOT NULL,
          input_usd REAL NOT NULL, output_usd REAL NOT NULL, cost_usd REAL NOT NULL,
          balances_before_json TEXT NOT NULL, balances_after_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_rebalance3_day
          ON shadow_rebalances_3leg(profile,day,ts_ms);

        CREATE TABLE IF NOT EXISTS route_snapshots_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
          route_id TEXT NOT NULL, net REAL NOT NULL,
          executable_usd REAL NOT NULL, max_bbo_age_ms INTEGER NOT NULL,
          bbo_skew_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_snap3_ts
          ON route_snapshots_3leg(ts_ms);

        CREATE TABLE IF NOT EXISTS ws_latency_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL, send_ts_ms INTEGER NOT NULL,
          recv_ts_ms INTEGER NOT NULL, transport_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_wslat3_ts
          ON ws_latency_3leg(ts_ms);

        CREATE TABLE IF NOT EXISTS depth_sync_events_3leg(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL, event TEXT NOT NULL, reason TEXT,
          duration_ms INTEGER, attempt INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_sync3_ts
          ON depth_sync_events_3leg(symbol,ts_ms);
        """
    )
    con.commit()
    while True:
        item = dbq.get()
        if item is None:
            break
        try:
            typ, payload = item
            if typ == "meta":
                con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", payload)
            elif typ == "event":
                con.execute(
                    """INSERT INTO opportunities_3leg(
                    route_id,path,start_asset,end_asset,start_ts_ms,end_ts_ms,
                    duration_ms,ticks,entry_net,peak_net,avg_net,entry_size_usd,
                    max_size_usd,entry_profit_usd,peak_profit_usd,max_bbo_age_ms)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "decision":
                con.execute(
                    """INSERT OR REPLACE INTO decisions_3leg(
                    decision_id,day,route_id,path,event_start_ts_ms,ts_ms,
                    decision_net,capacity_usd,selected_usd,grade,eligible,
                    policy_reason,admission,profiles_reserved,recv_age_max_ms,
                    exchange_age_max_ms,recv_skew_ms,exchange_skew_ms,legs_json,compute_us)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "quality":
                con.execute(
                    """INSERT OR REPLACE INTO decision_quality_3leg(
                    decision_id,day,route_id,ts_ms,grade,eligible,reason,
                    capacity_usd,size_fraction,selected_usd,normal_edge,
                    half_bbo_edge,drop_l1_edge,coverage_l1_l3,compute_us)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "depth_sample":
                con.execute(
                    """INSERT INTO decision_depth_samples_3leg(
                    decision_id,route_id,decision_ts_ms,sample_ts_ms,elapsed_ms,
                    leg,symbol,path_from,path_to,ready,version,book_ts_ms,
                    send_ts_ms,recv_age_ms,last_ready_ts_ms,bids_json,asks_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "shadow_state":
                con.execute(
                    """INSERT OR REPLACE INTO shadow_state_3leg(
                    profile,day,updated_ts_ms,balances_json,profit_bank,completed,
                    wins,losses,failed_sequences,rebalances,rebalance_cost,
                    skipped_balance,skipped_rebalance)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "attempt":
                con.execute(
                    """INSERT OR IGNORE INTO shadow_attempts_3leg(
                    profile,day,decision_id,route_id,path,decision_ts_ms,
                    finished_ts_ms,status,failed_leg,input_units,input_usd,
                    output_units,output_usd,profit_usd,decision_net,selected_usd,legs_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "rebalance":
                con.execute(
                    """INSERT INTO shadow_rebalances_3leg(
                    profile,day,ts_ms,reason,from_asset,to_asset,input_units,
                    output_units,input_usd,output_usd,cost_usd,
                    balances_before_json,balances_after_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "snapshot_many":
                con.executemany(
                    """INSERT INTO route_snapshots_3leg(
                    ts_ms,route_id,net,executable_usd,max_bbo_age_ms,bbo_skew_ms)
                    VALUES(?,?,?,?,?,?)""",
                    payload,
                )
            elif typ == "ws_latency":
                con.execute(
                    """INSERT INTO ws_latency_3leg(
                    ts_ms,symbol,send_ts_ms,recv_ts_ms,transport_ms)
                    VALUES(?,?,?,?,?)""",
                    payload,
                )
            elif typ == "depth_sync":
                con.execute(
                    """INSERT INTO depth_sync_events_3leg(
                    ts_ms,symbol,event,reason,duration_ms,attempt)
                    VALUES(?,?,?,?,?,?)""",
                    payload,
                )
            con.commit()
        except Exception as exc:
            logerr(f"DB: {exc}")
        finally:
            dbq.task_done()
    con.close()


def put_db(item):
    try:
        dbq.put_nowait(item)
    except queue.Full:
        with state_lock:
            state["db_queue_drops"] += 1
        logerr("DB queue full")


# ---------------------------------------------------------------------------
# Market discovery and complete 3-leg route selection.
# ---------------------------------------------------------------------------
def save_market_cache(markets):
    try:
        with open(MARKET_CACHE, "w", encoding="utf-8") as handle:
            json.dump(markets, handle)
    except Exception as exc:
        logerr(f"market cache save: {exc}")


def load_market_cache():
    try:
        with open(MARKET_CACHE, "r", encoding="utf-8") as handle:
            rows = json.load(handle)
            if isinstance(rows, list) and rows:
                return rows
    except Exception:
        pass
    return None


def discover_markets():
    headers = {
        "User-Agent": "Mozilla/5.0 mexc-3leg-scanner/3.0",
        "Accept": "application/json",
    }
    for attempt in range(4):
        try:
            response = requests.get(
                REST + "/api/v3/exchangeInfo", headers=headers, timeout=15
            )
            response.raise_for_status()
            rows = []
            for raw in response.json().get("symbols", []):
                status = str(raw.get("status", "")).upper()
                if status not in ("1", "ENABLED", "TRADING", ""):
                    continue
                base = str(raw.get("baseAsset", "")).upper()
                quote = str(raw.get("quoteAsset", "")).upper()
                symbol = str(raw.get("symbol", "")).upper()
                if base and quote and symbol and base != quote:
                    rows.append({"symbol": symbol, "base": base, "quote": quote})
            if rows:
                save_market_cache(rows)
                state["market_source"] = "MEXC v3 exchangeInfo"
                return rows
        except Exception as exc:
            logerr(f"exchangeInfo try {attempt + 1}: {exc}")
            time.sleep(2**attempt)

    cached = load_market_cache()
    if cached:
        state["market_source"] = "local market cache"
        return cached
    raise RuntimeError(
        "Impossible de charger les marchés MEXC et aucun cache local n'existe."
    )


def market_lookup(markets):
    by_symbol = {
        m["symbol"]: m
        for m in markets
        if m.get("symbol")
        and m.get("base")
        and m.get("quote")
        and m["base"] != m["quote"]
    }
    graph = defaultdict(list)
    for market in by_symbol.values():
        graph[market["base"]].append((market["quote"], market["symbol"]))
        graph[market["quote"]].append((market["base"], market["symbol"]))
    return by_symbol, graph


def build_all_3leg_routes(markets):
    """Generate stable -> A -> B -> stable in both directions.

    The starting and ending stablecoin may be identical (closed triangle) or
    different (inventory-balancing stable-to-stable route).
    """
    by_symbol, graph = market_lookup(markets)
    stable_set = set(STABLES)
    stable_neighbours = defaultdict(list)
    for asset, edges in graph.items():
        if asset in stable_set:
            continue
        for neighbour, symbol in edges:
            if neighbour in stable_set:
                stable_neighbours[asset].append((neighbour, symbol))

    routes = {}
    for market in by_symbol.values():
        asset_a, asset_b = market["base"], market["quote"]
        if asset_a in stable_set or asset_b in stable_set:
            continue
        if not stable_neighbours.get(asset_a) or not stable_neighbours.get(asset_b):
            continue
        cross = market["symbol"]
        for first, second in ((asset_a, asset_b), (asset_b, asset_a)):
            for start_stable, symbol_1 in stable_neighbours[first]:
                for end_stable, symbol_3 in stable_neighbours[second]:
                    symbols = (symbol_1, cross, symbol_3)
                    if len(set(symbols)) != 3:
                        continue
                    path = (start_stable, first, second, end_stable)
                    route_id = ">".join(path)
                    routes[route_id] = {
                        "id": route_id,
                        "path": path,
                        "symbols": symbols,
                        "type": 3,
                    }
    return list(routes.values()), by_symbol


def select_routes_under_symbol_budget(markets):
    candidates, by_symbol = build_all_3leg_routes(markets)
    direct_stables = sorted(
        m["symbol"]
        for m in by_symbol.values()
        if m["base"] in STABLES and m["quote"] in STABLES
    )

    if FOLLOW_ALL_3LEG:
        selected = sorted(candidates, key=lambda route: route["id"])
        symbols = set(direct_stables)
        for route in selected:
            symbols.update(route["symbols"])
    else:
        # Complete routes only: a route is never selected with one leg missing.
        selected = []
        symbols = set(direct_stables[:MAX_WS_SYMBOLS])
        remaining = list(candidates)
        while remaining:
            remaining.sort(
                key=lambda route: (len(set(route["symbols"]) - symbols), route["id"])
            )
            route = remaining.pop(0)
            new_symbols = set(route["symbols"]) - symbols
            if len(symbols) + len(new_symbols) > MAX_WS_SYMBOLS:
                continue
            selected.append(route)
            symbols.update(route["symbols"])

    chosen = [by_symbol[s] for s in sorted(symbols) if s in by_symbol]
    diag = {
        "all_markets": len(by_symbol),
        "candidate_3leg": len(candidates),
        "selected_3leg": len(selected),
        "dropped_3leg": len(candidates) - len(selected),
        "selected_symbols": len(chosen),
        "direct_stable_symbols": len(direct_stables),
        "follow_all_3leg": FOLLOW_ALL_3LEG,
        "symbol_budget": MAX_WS_SYMBOLS,
        "ws_group_size": WS_GROUP_SIZE,
        "mode": "3-leg only",
    }
    return chosen, selected, by_symbol, diag


def fetch_ws_activity_weights(symbols):
    try:
        response = requests.get(
            REST + "/api/v3/ticker/24hr",
            headers={
                "User-Agent": "Mozilla/5.0 mexc-3leg-scanner/3.0",
                "Accept": "application/json",
            },
            timeout=25,
        )
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            return {}, "ticker_unavailable"
        wanted = set(symbols)
        weights = {}
        for row in rows:
            symbol = str(row.get("symbol", "")).upper()
            if symbol not in wanted:
                continue
            try:
                weights[symbol] = max(0.0, float(row.get("quoteVolume") or 0.0))
            except (TypeError, ValueError):
                weights[symbol] = 0.0
        return weights, "mexc_quote_volume_24h"
    except Exception as exc:
        logerr(f"24h WS load weights: {exc}")
        return {}, "round_robin_fallback"


def build_balanced_ws_groups(symbols):
    symbols = sorted(set(symbols))
    if not symbols:
        return [], [], "empty"
    group_count = max(1, math.ceil(len(symbols) / WS_GROUP_SIZE))
    weights, mode = fetch_ws_activity_weights(symbols)
    groups = [[] for _ in range(group_count)]
    loads = [0.0 for _ in range(group_count)]
    ordered = (
        sorted(symbols, key=lambda symbol: (-weights.get(symbol, 0.0), symbol))
        if weights
        else symbols
    )
    for symbol in ordered:
        available = [i for i, group in enumerate(groups) if len(group) < WS_GROUP_SIZE]
        if weights:
            index = min(available, key=lambda i: (loads[i], len(groups[i]), i))
        else:
            index = min(available, key=lambda i: (len(groups[i]), i))
        groups[index].append(symbol)
        loads[index] += weights.get(symbol, 0.0)
    for group in groups:
        group.sort()
    return groups, loads, mode


# ---------------------------------------------------------------------------
# MEXC protobuf Depth stream and coherent local books.
# ---------------------------------------------------------------------------
def _pb_varint(data, position):
    value = 0
    shift = 0
    while position < len(data):
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, position
        shift += 7
    raise ValueError("truncated varint")


def _pb_fields(data):
    fields = []
    position = 0
    while position < len(data):
        key, position = _pb_varint(data, position)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, position = _pb_varint(data, position)
        elif wire == 1:
            value = data[position : position + 8]
            position += 8
        elif wire == 2:
            length, position = _pb_varint(data, position)
            value = data[position : position + length]
            position += length
        elif wire == 5:
            value = data[position : position + 4]
            position += 4
        else:
            raise ValueError(f"unsupported protobuf wire {wire}")
        fields.append((field, wire, value))
    return fields


def _decode_level(blob):
    values = {}
    for field, wire, value in _pb_fields(blob):
        if wire == 2 and field in (1, 2):
            values[field] = value.decode("utf-8", "ignore")
    try:
        return float(values[1]), float(values[2])
    except Exception:
        return None


def decode_depth(payload):
    symbol = None
    send_ts = None
    body = None
    for field, wire, value in _pb_fields(payload):
        if field == 3 and wire == 2:
            symbol = value.decode("utf-8", "ignore")
        elif field == 6 and wire == 0:
            send_ts = value
        elif field == 313 and wire == 2:
            body = value
    if not symbol or body is None:
        return None
    asks, bids, from_version, to_version = [], [], None, None
    for field, wire, value in _pb_fields(body):
        if wire == 2 and field == 1:
            level = _decode_level(value)
            if level:
                asks.append(level)
        elif wire == 2 and field == 2:
            level = _decode_level(value)
            if level:
                bids.append(level)
        elif wire == 2 and field == 4:
            try:
                from_version = int(value.decode())
            except Exception:
                pass
        elif wire == 2 and field == 5:
            try:
                to_version = int(value.decode())
            except Exception:
                pass
    return {
        "symbol": symbol,
        "send": int(send_ts or now_ms()),
        "asks": asks,
        "bids": bids,
        "from": from_version,
        "to": to_version,
    }


def enqueue_scan(symbol):
    with queued_lock:
        if symbol in queued_symbols:
            with state_lock:
                state["scan_coalesced"] += 1
            return
        queued_symbols.add(symbol)
    try:
        scanq.put_nowait(symbol)
    except queue.Full:
        with queued_lock:
            queued_symbols.discard(symbol)
        with state_lock:
            state["scan_queue_drops"] += 1


def _publish_depth_bbo(symbol, send_ts):
    with depth_lock:
        book = state["depth"].get(symbol)
        if (
            not book
            or not book.get("ready")
            or not book.get("bids")
            or not book.get("asks")
        ):
            return
        bid = max(book["bids"])
        ask = min(book["asks"])
        bid_qty = book["bids"][bid]
        ask_qty = book["asks"][ask]
    received_ts = now_ms()
    with state_lock:
        state["bbo"][symbol] = {
            "bid": bid,
            "bidq": bid_qty,
            "ask": ask,
            "askq": ask_qty,
            "ts": received_ts,
            "send_ts": send_ts,
            "lat": max(0, received_ts - send_ts),
        }
        state["last_ws_ms"] = received_ts
    enqueue_scan(symbol)


def queue_depth_resync(symbol, reason="gap"):
    timestamp = now_ms()
    new_gap = False
    with depth_resync_lock:
        diag = depth_resync_diag[symbol]
        diag["last_reason"] = reason
        if diag["not_ready_since"] is None:
            diag["not_ready_since"] = timestamp
            diag["drops"] += 1
            new_gap = reason != "bootstrap"
            put_db(("depth_sync", (timestamp, symbol, "NOT_READY", reason, None, None)))
        if symbol in depth_resync_pending:
            return False
        depth_resync_pending.add(symbol)
        depth_resync_q.put(symbol)
    if new_gap:
        with state_lock:
            state["depth_gap_events"] += 1
    return True


def apply_depth_update(update):
    symbol = update["symbol"]
    needs_resync = False
    with depth_lock:
        book = state["depth"].get(symbol)
        if not book or not book.get("ready"):
            depth_buffers[symbol].append(update)
            return False
        previous = book.get("version")
        from_version, to_version = update.get("from"), update.get("to")
        if to_version is not None and previous is not None and to_version <= previous:
            return False
        if from_version is not None and previous is not None and from_version > previous + 1:
            book["ready"] = False
            depth_buffers[symbol].append(update)
            needs_resync = True
            state["depth_ready"] = sum(
                1 for row in state["depth"].values() if row.get("ready")
            )
        else:
            for side in ("asks", "bids"):
                local_side = book[side]
                for price, quantity in update[side]:
                    if quantity <= 0:
                        local_side.pop(price, None)
                    else:
                        local_side[price] = quantity
            if to_version is not None:
                book["version"] = to_version
            book["send_ts"] = update["send"]
            book["ts"] = now_ms()
    if needs_resync:
        queue_depth_resync(symbol, "version_gap")
        return False
    _publish_depth_bbo(symbol, update["send"])
    return True


def init_depth_symbol(symbol):
    try:
        response = requests.get(
            REST + "/api/v3/depth",
            params={"symbol": symbol, "limit": 100},
            timeout=8,
        )
        response.raise_for_status()
        raw = response.json()
        bids = {float(p): float(q) for p, q in raw.get("bids", []) if float(q) > 0}
        asks = {float(p): float(q) for p, q in raw.get("asks", []) if float(q) > 0}
        version = int(raw.get("lastUpdateId", 0))
        received_ts = now_ms()
        ready = True
        with depth_lock:
            book = {
                "bids": bids,
                "asks": asks,
                "version": version,
                "ready": True,
                "ts": received_ts,
                "send_ts": received_ts,
            }
            state["depth"][symbol] = book
            buffered = list(depth_buffers.pop(symbol, []))
            for update in buffered:
                to_version = update.get("to")
                if to_version is not None and to_version <= book["version"]:
                    continue
                from_version = update.get("from")
                if from_version is not None and from_version > book["version"] + 1:
                    book["ready"] = False
                    ready = False
                    depth_buffers[symbol].append(update)
                    continue
                if not ready:
                    depth_buffers[symbol].append(update)
                    continue
                for side in ("asks", "bids"):
                    for price, quantity in update[side]:
                        if quantity <= 0:
                            book[side].pop(price, None)
                        else:
                            book[side][price] = quantity
                if to_version is not None:
                    book["version"] = to_version
                received_ts = update["send"]
            state["depth_ready"] = sum(
                1 for row in state["depth"].values() if row.get("ready")
            )
        if ready:
            _publish_depth_bbo(symbol, received_ts)
        return ready
    except Exception as exc:
        logerr(f"depth snapshot {symbol}: {exc}")
        return False


def depth_resync_worker():
    while True:
        symbol = depth_resync_q.get()
        attempt = 0
        try:
            while True:
                attempt += 1
                if init_depth_symbol(symbol):
                    timestamp = now_ms()
                    with depth_resync_lock:
                        diag = depth_resync_diag[symbol]
                        since = diag.get("not_ready_since")
                        duration = max(0, timestamp - since) if since is not None else None
                        reason = diag.get("last_reason", "")
                        diag["resync_ok"] += 1
                        diag["not_ready_since"] = None
                        diag["last_resync_ms"] = duration
                        diag["last_ready_ts_ms"] = timestamp
                    put_db(
                        (
                            "depth_sync",
                            (timestamp, symbol, "READY", reason, duration, attempt),
                        )
                    )
                    if reason != "bootstrap":
                        with state_lock:
                            state["depth_resync_recoveries"] += 1
                    break
                with depth_resync_lock:
                    depth_resync_diag[symbol]["resync_fail"] += 1
                with state_lock:
                    state["depth_resync_failures"] += 1
                put_db(
                    (
                        "depth_sync",
                        (
                            now_ms(),
                            symbol,
                            "RETRY",
                            depth_resync_diag[symbol].get("last_reason", ""),
                            None,
                            attempt,
                        ),
                    )
                )
                time.sleep(min(8.0, 0.5 * (2 ** min(attempt - 1, 4))))
            time.sleep(0.25)
        finally:
            with depth_resync_lock:
                depth_resync_pending.discard(symbol)
            depth_resync_q.task_done()


def depth_bootstrap_worker(symbols):
    for symbol in symbols:
        with depth_lock:
            state["depth"].setdefault(
                symbol,
                {"bids": {}, "asks": {}, "version": None, "ready": False},
            )
        queue_depth_resync(symbol, "bootstrap")


# ---------------------------------------------------------------------------
# Route math, multi-level execution and T0 depth-quality classification.
# ---------------------------------------------------------------------------
def direct_pair(asset_a, asset_b, market_meta):
    symbol = pair_index.get(frozenset((asset_a, asset_b)))
    if symbol:
        return symbol, market_meta.get(symbol)
    # Offline/unit-test fallback before bootstrap has built the immutable index.
    for candidate, market in market_meta.items():
        if {market["base"], market["quote"]} == {asset_a, asset_b}:
            return candidate, market
    return None, None


def stable_mark_usdt(asset, books, market_meta):
    if asset == "USDT":
        return 1.0
    symbol, market = direct_pair(asset, "USDT", market_meta)
    if symbol:
        book = books.get(symbol)
        if (
            book
            and now_ms() - book["ts"] <= MAX_BBO_AGE_MS
            and book["bid"] > 0
            and book["ask"] > 0
        ):
            mid = (book["bid"] + book["ask"]) / 2.0
            return mid if market["base"] == asset else 1.0 / mid
    # Stablecoin valuation fallback only; no trade is fabricated from this peg.
    return 1.0


def route_calc(route, books, market_meta, age_limit_ms=None):
    start_asset, end_asset = route["path"][0], route["path"][-1]
    start_mark = stable_mark_usdt(start_asset, books, market_meta)
    end_mark = stable_mark_usdt(end_asset, books, market_meta)
    if start_mark <= 0 or end_mark <= 0:
        return None
    age_limit = MAX_BBO_AGE_MS if age_limit_ms is None else age_limit_ms
    multiplier = 1.0
    max_start_units = float("inf")
    timestamp = now_ms()
    recv_times, send_times, leg_books = [], [], []
    for index, symbol in enumerate(route["symbols"]):
        book, market = books.get(symbol), market_meta.get(symbol)
        if not book or not market:
            return None
        recv_age = timestamp - book["ts"]
        if recv_age > age_limit:
            return None
        from_asset, to_asset = route["path"][index : index + 2]
        if from_asset == market["base"] and to_asset == market["quote"]:
            capacity_in = book["bidq"]
            rate = book["bid"] * (1.0 - FEE)
        elif from_asset == market["quote"] and to_asset == market["base"]:
            capacity_in = book["ask"] * book["askq"]
            rate = (1.0 / book["ask"]) * (1.0 - FEE)
        else:
            return None
        if capacity_in <= 0 or rate <= 0 or multiplier <= 0:
            return None
        max_start_units = min(max_start_units, capacity_in / multiplier)
        multiplier *= rate
        recv_times.append(int(book["ts"]))
        send_times.append(int(book.get("send_ts", book["ts"])))
        leg_books.append(dict(book))
    if not math.isfinite(max_start_units) or max_start_units <= 0:
        return None
    executable_usd = max_start_units * start_mark
    if executable_usd < MIN_EXEC_USD:
        return None
    net = multiplier * end_mark / start_mark - 1.0
    return {
        "net": net,
        "size": executable_usd,
        "profit": executable_usd * net,
        "out_ratio": multiplier,
        "start_mark": start_mark,
        "end_mark": end_mark,
        "max_age": max(timestamp - row for row in recv_times),
        "exchange_max_age": max(
            max(0, timestamp - row) for row in send_times
        ),
        "bbo_skew": max(recv_times) - min(recv_times),
        "exchange_skew": max(send_times) - min(send_times),
        "leg_books": leg_books,
    }


def depth_walk(symbol, from_asset, to_asset, input_units, market_meta, max_age_ms):
    market = market_meta.get(symbol)
    if not market or input_units <= 0:
        return None
    with depth_lock:
        local = state["depth"].get(symbol)
        if not local or not local.get("ready"):
            return None
        bids = sorted(local["bids"].items(), reverse=True)
        asks = sorted(local["asks"].items())
        book_ts = int(local.get("ts", 0))
        send_ts = int(local.get("send_ts", book_ts))
    if now_ms() - book_ts > max_age_ms:
        return None

    requested = float(input_units)
    remaining = requested
    input_used = 0.0
    gross_output = 0.0
    levels_used = 0
    notional = 0.0
    if from_asset == market["base"] and to_asset == market["quote"]:
        side = "sell_base"
        base_used = 0.0
        for price, quantity in bids:
            take = min(remaining, quantity)
            if take <= 0:
                continue
            levels_used += 1
            input_used += take
            base_used += take
            gross_output += take * price
            notional += take * price
            remaining -= take
            if remaining <= max(1e-12, requested * 1e-10):
                break
        vwap = notional / base_used if base_used > 0 else None
    elif from_asset == market["quote"] and to_asset == market["base"]:
        side = "buy_base"
        base_output = 0.0
        for price, quantity in asks:
            max_quote = price * quantity
            take_quote = min(remaining, max_quote)
            if take_quote <= 0:
                continue
            levels_used += 1
            input_used += take_quote
            bought = take_quote / price
            base_output += bought
            gross_output += bought
            notional += take_quote
            remaining -= take_quote
            if remaining <= max(1e-12, requested * 1e-10):
                break
        vwap = notional / base_output if base_output > 0 else None
    else:
        return None
    if input_used <= 0:
        return None
    return {
        "symbol": symbol,
        "from": from_asset,
        "to": to_asset,
        "input_requested": requested,
        "input_used": input_used,
        "output": gross_output * (1.0 - FEE),
        "gross_output": gross_output,
        "fill_ratio": min(1.0, input_used / requested),
        "book_ts": book_ts,
        "send_ts": send_ts,
        "recv_age_ms": max(0, now_ms() - book_ts),
        "levels_used": levels_used,
        "vwap": vwap,
        "side": side,
    }


def stable_conversion_depth(from_asset, to_asset, input_units, market_meta):
    symbol, _ = direct_pair(from_asset, to_asset, market_meta)
    if not symbol:
        return None
    fill = depth_walk(
        symbol,
        from_asset,
        to_asset,
        input_units,
        market_meta,
        EXEC_BOOK_MAX_AGE_MS,
    )
    if not fill:
        return None
    with state_lock:
        books = dict(state["bbo"])
    from_mark = stable_mark_usdt(from_asset, books, market_meta)
    to_mark = stable_mark_usdt(to_asset, books, market_meta)
    input_usd = fill["input_used"] * from_mark
    output_usd = fill["output"] * to_mark
    fill.update(
        {
            "input_usd": input_usd,
            "output_usd": output_usd,
            "cost_usd": max(0.0, input_usd - output_usd),
        }
    )
    return fill


def _quality_books(route):
    timestamp = now_ms()
    copied = {}
    with depth_lock:
        for symbol in route["symbols"]:
            book = state["depth"].get(symbol)
            if (
                not book
                or not book.get("ready")
                or timestamp - book.get("ts", 0) > SHADOW_BBO_AGE_MS
            ):
                return None
            copied[symbol] = {
                "bids": sorted(book.get("bids", {}).items(), reverse=True)[
                    :DEPTH_QUALITY_LEVELS
                ],
                "asks": sorted(book.get("asks", {}).items())[:DEPTH_QUALITY_LEVELS],
                "book_ts": book.get("ts", 0),
            }
    if max(row["book_ts"] for row in copied.values()) - min(
        row["book_ts"] for row in copied.values()
    ) > SHADOW_MAX_SKEW_MS:
        return None
    return copied


def _quality_walk(book, market, from_asset, to_asset, input_units, stress="normal"):
    if input_units <= 0:
        return None
    if from_asset == market.get("base") and to_asset == market.get("quote"):
        levels = list(book["bids"])
        buy_base = False
    elif from_asset == market.get("quote") and to_asset == market.get("base"):
        levels = list(book["asks"])
        buy_base = True
    else:
        return None
    if stress == "drop_l1":
        levels = levels[1:]
    elif stress == "half_bbo" and levels:
        levels = [(levels[0][0], levels[0][1] * 0.50)] + levels[1:]

    requested = float(input_units)
    remaining = requested
    used = 0.0
    gross_output = 0.0
    levels_used = 0
    for price, quantity in levels:
        price, quantity = float(price), float(quantity)
        if price <= 0 or quantity <= 0:
            continue
        if buy_base:
            take = min(remaining, price * quantity)
            gross_output += take / price
        else:
            take = min(remaining, quantity)
            gross_output += take * price
        if take <= 0:
            continue
        used += take
        remaining -= take
        levels_used += 1
        if remaining <= max(1e-12, requested * 1e-10):
            break
    if used <= 0:
        return None
    return {
        "input_used": used,
        "output": gross_output * (1.0 - FEE),
        "fill_ratio": min(1.0, used / requested),
        "levels_used": levels_used,
    }


def _quality_route(route, books, market_meta, input_usd, start_mark, end_mark, stress):
    if input_usd <= 0 or start_mark <= 0 or end_mark <= 0:
        return None
    units = input_usd / start_mark
    fills = []
    for index, symbol in enumerate(route["symbols"]):
        fill = _quality_walk(
            books[symbol],
            market_meta.get(symbol, {}),
            route["path"][index],
            route["path"][index + 1],
            units,
            stress,
        )
        if not fill or fill["fill_ratio"] < 0.999999:
            return None
        fills.append(fill)
        units = fill["output"]
    output_usd = units * end_mark
    return {
        "edge": output_usd / input_usd - 1.0,
        "output_usd": output_usd,
        "fills": fills,
    }


def _quality_input_capacity(book, market, from_asset, to_asset, levels=3):
    if from_asset == market.get("base") and to_asset == market.get("quote"):
        return sum(float(quantity) for _, quantity in book["bids"][:levels])
    if from_asset == market.get("quote") and to_asset == market.get("base"):
        return sum(
            float(price) * float(quantity)
            for price, quantity in book["asks"][:levels]
        )
    return 0.0


def _quality_coverage_l1_l3(route, books, market_meta, input_usd, start_mark):
    """Minimum top-three-level reserve ratio across all three sequential legs."""
    units = input_usd / max(start_mark, 1e-12)
    coverage = float("inf")
    for index, symbol in enumerate(route["symbols"]):
        market = market_meta.get(symbol, {})
        from_asset, to_asset = route["path"][index : index + 2]
        capacity = _quality_input_capacity(
            books[symbol], market, from_asset, to_asset, levels=3
        )
        coverage = min(coverage, capacity / max(units, 1e-12))
        fill = _quality_walk(
            books[symbol], market, from_asset, to_asset, units, "normal"
        )
        if not fill or fill["fill_ratio"] < 0.999999:
            return 0.0
        units = fill["output"]
    return 0.0 if not math.isfinite(coverage) else coverage


def depth_quality_decision(route, event_x):
    started = time.perf_counter_ns()
    capacity = max(0.0, float(event_x.get("size", 0.0)))
    result = {
        "grade": "D",
        "eligible": False,
        "reason": "depth_unavailable",
        "capacity_usd": capacity,
        "size_fraction": 0.0,
        "selected_usd": 0.0,
        "normal_edge": None,
        "half_bbo_edge": None,
        "drop_l1_edge": None,
        "coverage3": None,
        "compute_us": 0.0,
    }
    try:
        with state_lock:
            market_meta = state["market_meta"]
        books = _quality_books(route)
        if not books:
            return result
        start_mark = float(event_x.get("start_mark") or 0.0)
        end_mark = float(event_x.get("end_mark") or 0.0)

        a_usd = capacity * DEPTH_QUALITY_A_FRACTION
        a_normal = _quality_route(
            route, books, market_meta, a_usd, start_mark, end_mark, "normal"
        )
        a_half = _quality_route(
            route, books, market_meta, a_usd, start_mark, end_mark, "half_bbo"
        )
        a_drop = _quality_route(
            route, books, market_meta, a_usd, start_mark, end_mark, "drop_l1"
        )
        if a_drop and a_drop["edge"] >= SHADOW_MIN_EDGE:
            result.update(
                {
                    "grade": "A",
                    "eligible": a_usd >= SIM_MIN_TRADE_USD,
                    "reason": "eligible_A"
                    if a_usd >= SIM_MIN_TRADE_USD
                    else "below_min_trade",
                    "size_fraction": DEPTH_QUALITY_A_FRACTION,
                    "selected_usd": a_usd,
                    "normal_edge": a_normal["edge"] if a_normal else None,
                    "half_bbo_edge": a_half["edge"] if a_half else None,
                    "drop_l1_edge": a_drop["edge"],
                    "coverage3": _quality_coverage_l1_l3(
                        route, books, market_meta, a_usd, start_mark
                    ),
                }
            )
            return result

        b_usd = capacity * DEPTH_QUALITY_B_FRACTION
        b_normal = _quality_route(
            route, books, market_meta, b_usd, start_mark, end_mark, "normal"
        )
        b_half = _quality_route(
            route, books, market_meta, b_usd, start_mark, end_mark, "half_bbo"
        )
        b_drop = _quality_route(
            route, books, market_meta, b_usd, start_mark, end_mark, "drop_l1"
        )
        coverage = _quality_coverage_l1_l3(
            route, books, market_meta, b_usd, start_mark
        )
        normal_edge = b_normal["edge"] if b_normal else None
        half_edge = b_half["edge"] if b_half else None
        drop_edge = b_drop["edge"] if b_drop else None
        result.update(
            {
                "size_fraction": DEPTH_QUALITY_B_FRACTION,
                "selected_usd": b_usd,
                "normal_edge": normal_edge,
                "half_bbo_edge": half_edge,
                "drop_l1_edge": drop_edge,
                "coverage3": coverage,
            }
        )
        if b_half and half_edge >= SHADOW_MIN_EDGE:
            is_bplus = (
                half_edge >= DEPTH_QUALITY_BPLUS_HALF_EDGE
                and b_drop is not None
                and drop_edge >= DEPTH_QUALITY_BPLUS_DROP_FLOOR
                and coverage >= DEPTH_QUALITY_COVERAGE3_MIN
            )
            result.update(
                {
                    "grade": "B+" if is_bplus else "B-",
                    "eligible": bool(is_bplus and b_usd >= SIM_MIN_TRADE_USD),
                    "reason": (
                        "eligible_B+"
                        if b_usd >= SIM_MIN_TRADE_USD
                        else "below_min_trade"
                    )
                    if is_bplus
                    else "rejected_B-",
                }
            )
            return result

        c_usd = capacity * DEPTH_QUALITY_C_FRACTION
        c_normal = _quality_route(
            route, books, market_meta, c_usd, start_mark, end_mark, "normal"
        )
        is_c = bool(c_normal and c_normal["edge"] >= SHADOW_MIN_EDGE)
        result.update(
            {
                "grade": "C" if is_c else "D",
                "eligible": False,
                "reason": "rejected_C" if is_c else "rejected_D",
                "size_fraction": DEPTH_QUALITY_C_FRACTION,
                "selected_usd": c_usd,
                "normal_edge": c_normal["edge"] if c_normal else None,
            }
        )
        return result
    finally:
        result["compute_us"] = (time.perf_counter_ns() - started) / 1000.0
        depth_quality_compute_us.append(result["compute_us"])


def _depth_book_payload(symbol):
    timestamp = now_ms()
    with depth_lock:
        book = state["depth"].get(symbol)
        if not book:
            return {
                "ready": 0,
                "version": None,
                "book_ts": None,
                "send_ts": None,
                "recv_age": None,
                "bids": [],
                "asks": [],
            }
        bids = sorted(book.get("bids", {}).items(), reverse=True)[
            :DEPTH_CAPTURE_LEVELS
        ]
        asks = sorted(book.get("asks", {}).items())[:DEPTH_CAPTURE_LEVELS]
        book_ts = book.get("ts")
        return {
            "ready": 1 if book.get("ready") else 0,
            "version": book.get("version"),
            "book_ts": book_ts,
            "send_ts": book.get("send_ts"),
            "recv_age": timestamp - book_ts if book_ts else None,
            "bids": bids,
            "asks": asks,
        }


def capture_decision_depth(capture, sample_ts):
    route = capture["route"]
    for leg, symbol in enumerate(route["symbols"], 1):
        book = _depth_book_payload(symbol)
        with depth_resync_lock:
            ready_ts = depth_resync_diag[symbol].get("last_ready_ts_ms")
        put_db(
            (
                "depth_sample",
                (
                    capture["did"],
                    route["id"],
                    capture["t0"],
                    sample_ts,
                    max(0, sample_ts - capture["t0"]),
                    leg,
                    symbol,
                    route["path"][leg - 1],
                    route["path"][leg],
                    book["ready"],
                    book["version"],
                    book["book_ts"],
                    book["send_ts"],
                    book["recv_age"],
                    ready_ts,
                    json.dumps(book["bids"], separators=(",", ":")),
                    json.dumps(book["asks"], separators=(",", ":")),
                ),
            )
        )


def symbols_post_resync_safe(route, timestamp):
    with depth_resync_lock:
        for symbol in route["symbols"]:
            ready_ts = depth_resync_diag[symbol].get("last_ready_ts_ms")
            if ready_ts is not None and timestamp - ready_ts < POST_RESYNC_GRACE_MS:
                return False
    return True


# ---------------------------------------------------------------------------
# Independent FAST / TARGET / DEGRADED portfolios.
#
# This is intentionally an atomic paper validator: a sequence changes balances
# only when all three sequential Depth walks are fully observable. An incomplete
# leg is counted explicitly as failed_leg_N and never silently booked as PnL.
# It is a scanner/research model, not a claim that three live market orders are
# atomic. The raw per-leg fills remain in SQLite for later execution modelling.
# ---------------------------------------------------------------------------
def shadow_default(day):
    weights = {asset: SHADOW_TARGET_WEIGHTS.get(asset, 0.0) for asset in STABLES}
    total_weight = sum(weights.values())
    if total_weight <= 0:
        weights = {asset: 1.0 / max(1, len(STABLES)) for asset in STABLES}
    else:
        weights = {asset: value / total_weight for asset, value in weights.items()}
    return {
        "day": day,
        "balances": {asset: SIM_CAPITAL * weights[asset] for asset in STABLES},
        "profit_bank": 0.0,
        "completed": 0,
        "wins": 0,
        "losses": 0,
        "failed_sequences": 0,
        "rebalances": 0,
        "rebalance_cost": 0.0,
        "skipped_balance": 0,
        "skipped_rebalance": 0,
    }


def persist_shadow(profile):
    shadow = shadow_states[profile]
    put_db(
        (
            "shadow_state",
            (
                profile,
                shadow["day"],
                now_ms(),
                json.dumps(shadow["balances"], sort_keys=True),
                shadow["profit_bank"],
                shadow["completed"],
                shadow["wins"],
                shadow["losses"],
                shadow["failed_sequences"],
                shadow["rebalances"],
                shadow["rebalance_cost"],
                shadow["skipped_balance"],
                shadow["skipped_rebalance"],
            ),
        )
    )


def load_shadow_states():
    _, _, day = local_day_bounds_ms(0)
    with shadow_lock:
        for profile in SHADOW_PROFILES:
            shadow_states[profile] = shadow_default(day)
            shadow_reserved[profile].clear()
    try:
        con = sqlite3.connect(DB_PATH, timeout=10)
        for profile in SHADOW_PROFILES:
            row = con.execute(
                """SELECT day,balances_json,profit_bank,completed,wins,losses,
                failed_sequences,rebalances,rebalance_cost,skipped_balance,
                skipped_rebalance FROM shadow_state_3leg
                WHERE profile=? AND day=?""",
                (profile, day),
            ).fetchone()
            if row:
                with shadow_lock:
                    shadow_states[profile] = {
                        "day": row[0],
                        "balances": json.loads(row[1]),
                        "profit_bank": float(row[2]),
                        "completed": int(row[3]),
                        "wins": int(row[4]),
                        "losses": int(row[5]),
                        "failed_sequences": int(row[6]),
                        "rebalances": int(row[7]),
                        "rebalance_cost": float(row[8]),
                        "skipped_balance": int(row[9]),
                        "skipped_rebalance": int(row[10]),
                    }
        con.close()
    except Exception as exc:
        logerr(f"shadow restore: {exc}")


def ensure_shadow_day(profile):
    _, _, day = local_day_bounds_ms(0)
    shadow = shadow_states.get(profile)
    if not shadow or shadow.get("day") != day:
        shadow_states[profile] = shadow_default(day)
        shadow_reserved[profile].clear()
        persist_shadow(profile)
    return shadow_states[profile]


def shadow_choose_rebalance(profile, target_asset, needed_units, event_x, books, market_meta):
    shadow = shadow_states[profile]
    target_mark = stable_mark_usdt(target_asset, books, market_meta)
    if target_mark <= 0 or needed_units <= 0 or shadow["profit_bank"] <= 0:
        return None
    initial_bucket = SIM_CAPITAL / max(1, len(STABLES))
    candidates = []
    for donor, balance in shadow["balances"].items():
        if donor == target_asset:
            continue
        donor_mark = stable_mark_usdt(donor, books, market_meta)
        reserve_units = initial_bucket * SIM_DONOR_RESERVE_PCT / max(donor_mark, 1e-12)
        available = max(
            0.0,
            balance - shadow_reserved[profile].get(donor, 0.0) - reserve_units,
        )
        if available <= 0:
            continue
        approximate_input = needed_units * target_mark / max(donor_mark, 1e-12) * 1.01
        conversion = stable_conversion_depth(
            donor, target_asset, min(available, approximate_input), market_meta
        )
        if not conversion or conversion["output"] <= 0:
            continue
        unlocked_usd = conversion["output"] * target_mark
        expected_profit = unlocked_usd * max(event_x.get("net", 0.0), 0.0)
        budget = shadow["profit_bank"] * REBALANCE_MAX_PROFIT_SHARE
        if conversion["cost_usd"] > budget + 1e-12:
            continue
        if (
            conversion["cost_usd"] > 0
            and expected_profit < conversion["cost_usd"] * REBALANCE_EDGE_MULT
        ):
            continue
        candidates.append(
            (
                expected_profit - conversion["cost_usd"],
                donor,
                conversion,
            )
        )
    if not candidates:
        return None
    candidates.sort(reverse=True, key=lambda row: row[0])
    return candidates[0][1], candidates[0][2]


def apply_shadow_rebalance(profile, donor, target, conversion, reason, timestamp):
    shadow = shadow_states[profile]
    donor_free = max(
        0.0,
        shadow["balances"].get(donor, 0.0)
        - shadow_reserved[profile].get(donor, 0.0),
    )
    if conversion["input_used"] > donor_free + 1e-12:
        return False
    before = json.dumps(shadow["balances"], sort_keys=True)
    shadow["balances"][donor] -= conversion["input_used"]
    shadow["balances"][target] += conversion["output"]
    shadow["rebalances"] += 1
    shadow["rebalance_cost"] += conversion["cost_usd"]
    shadow["profit_bank"] -= conversion["cost_usd"]
    after = json.dumps(shadow["balances"], sort_keys=True)
    put_db(
        (
            "rebalance",
            (
                profile,
                shadow["day"],
                timestamp,
                reason,
                donor,
                target,
                conversion["input_used"],
                conversion["output"],
                conversion["input_usd"],
                conversion["output_usd"],
                conversion["cost_usd"],
                before,
                after,
            ),
        )
    )
    return True


def reserve_shadow_trade(profile, route, event_x, event_start_ts, decision_id, decision_ts):
    with state_lock:
        books = dict(state["bbo"])
        market_meta = state["market_meta"]
    with shadow_lock:
        shadow = ensure_shadow_day(profile)
        start_asset, end_asset = route["path"][0], route["path"][-1]
        if start_asset not in shadow["balances"] or end_asset not in shadow["balances"]:
            return None
        start_mark = event_x["start_mark"]
        desired_units = event_x["size"] / max(start_mark, 1e-12)
        free_units = max(
            0.0,
            shadow["balances"].get(start_asset, 0.0)
            - shadow_reserved[profile].get(start_asset, 0.0),
        )
        shortage = max(0.0, desired_units - free_units)
        if shortage > 0:
            rebalance = shadow_choose_rebalance(
                profile, start_asset, shortage, event_x, books, market_meta
            )
            if rebalance:
                donor, conversion = rebalance
                if apply_shadow_rebalance(
                    profile,
                    donor,
                    start_asset,
                    conversion,
                    "signal_unlock",
                    decision_ts,
                ):
                    free_units = max(
                        0.0,
                        shadow["balances"].get(start_asset, 0.0)
                        - shadow_reserved[profile].get(start_asset, 0.0),
                    )
            else:
                shadow["skipped_rebalance"] += 1

        input_units = min(free_units, desired_units)
        input_usd = input_units * start_mark
        if input_usd < SIM_MIN_TRADE_USD:
            shadow["skipped_balance"] += 1
            persist_shadow(profile)
            return None
        shadow_reserved[profile][start_asset] += input_units
        persist_shadow(profile)
        return {
            "profile": profile,
            "decision_id": decision_id,
            "route": route,
            "event_start_ts": event_start_ts,
            "t0": decision_ts,
            "x0": dict(event_x),
            "start_asset": start_asset,
            "end_asset": end_asset,
            "reserved_units": input_units,
            "reserved_usd": input_usd,
            "current_units": input_units,
            "stage": 0,
            "due_offsets": SHADOW_PROFILES[profile],
            "next_due": decision_ts + SHADOW_PROFILES[profile][0],
            "fills": [],
        }


def record_shadow_attempt(q, timestamp, status, failed_leg=None, output_units=None, output_usd=None, profit_usd=None):
    put_db(
        (
            "attempt",
            (
                q["profile"],
                shadow_states[q["profile"]]["day"],
                q["decision_id"],
                q["route"]["id"],
                ">".join(q["route"]["path"]),
                q["t0"],
                timestamp,
                status,
                failed_leg,
                q["reserved_units"],
                q["reserved_usd"] if status == "completed" else 0.0,
                output_units,
                output_usd,
                profit_usd,
                q["x0"]["net"],
                q["x0"]["size"],
                json.dumps(q["fills"], separators=(",", ":")),
            ),
        )
    )


def fail_shadow_sequence(q, timestamp, failed_leg, reason):
    profile = q["profile"]
    with shadow_lock:
        shadow = ensure_shadow_day(profile)
        shadow_reserved[profile][q["start_asset"]] = max(
            0.0,
            shadow_reserved[profile].get(q["start_asset"], 0.0)
            - q["reserved_units"],
        )
        shadow["failed_sequences"] += 1
        q["fills"].append(
            {
                "leg": failed_leg,
                "ts_ms": timestamp,
                "status": reason,
            }
        )
        record_shadow_attempt(
            q,
            timestamp,
            f"failed_leg_{failed_leg}:{reason}",
            failed_leg=failed_leg,
        )
        persist_shadow(profile)


def execute_shadow_leg(q, timestamp):
    route = q["route"]
    stage = q["stage"]
    leg_number = stage + 1
    symbol = route["symbols"][stage]
    from_asset, to_asset = route["path"][stage : stage + 2]
    with state_lock:
        market_meta = state["market_meta"]
    fill = depth_walk(
        symbol,
        from_asset,
        to_asset,
        q["current_units"],
        market_meta,
        EXEC_BOOK_MAX_AGE_MS,
    )
    if not fill:
        fail_shadow_sequence(q, timestamp, leg_number, "unresolved_book")
        return False
    fill["leg"] = leg_number
    fill["ts_ms"] = timestamp
    if fill["fill_ratio"] < 0.999999:
        q["fills"].append(fill)
        fail_shadow_sequence(q, timestamp, leg_number, "partial_fill")
        return False
    q["fills"].append(fill)
    q["current_units"] = fill["output"]
    q["stage"] += 1
    if q["stage"] < 3:
        q["next_due"] = q["t0"] + q["due_offsets"][q["stage"]]
        with pending_lock:
            pending_shadow.append(q)
        return True

    profile = q["profile"]
    with state_lock:
        books = dict(state["bbo"])
        market_meta = state["market_meta"]
    with shadow_lock:
        shadow = ensure_shadow_day(profile)
        start_asset, end_asset = q["start_asset"], q["end_asset"]
        shadow_reserved[profile][start_asset] = max(
            0.0,
            shadow_reserved[profile].get(start_asset, 0.0) - q["reserved_units"],
        )
        spend = min(q["reserved_units"], shadow["balances"].get(start_asset, 0.0))
        if spend + 1e-12 < q["reserved_units"]:
            shadow["failed_sequences"] += 1
            record_shadow_attempt(
                q, timestamp, "failed_settlement_balance", failed_leg=3
            )
            persist_shadow(profile)
            return False
        shadow["balances"][start_asset] -= spend
        shadow["balances"][end_asset] += q["current_units"]
        output_usd = q["current_units"] * stable_mark_usdt(
            end_asset, books, market_meta
        )
        profit = output_usd - q["reserved_usd"]
        shadow["profit_bank"] += profit
        shadow["completed"] += 1
        if profit > 0:
            shadow["wins"] += 1
        elif profit < 0:
            shadow["losses"] += 1
        record_shadow_attempt(
            q,
            timestamp,
            "completed",
            output_units=q["current_units"],
            output_usd=output_usd,
            profit_usd=profit,
        )
        persist_shadow(profile)
    return True


def _shadow_weight_snapshot(profile, books, market_meta):
    shadow = ensure_shadow_day(profile)
    values = {
        asset: max(
            0.0,
            shadow["balances"].get(asset, 0.0)
            - shadow_reserved[profile].get(asset, 0.0),
        )
        * stable_mark_usdt(asset, books, market_meta)
        for asset in STABLES
    }
    total = sum(values.values())
    weights = {
        asset: values[asset] / total if total > 0 else 0.0 for asset in STABLES
    }
    targets = {asset: SHADOW_TARGET_WEIGHTS.get(asset, 0.0) for asset in STABLES}
    target_total = sum(targets.values()) or 1.0
    targets = {asset: value / target_total for asset, value in targets.items()}
    return values, total, weights, targets


def shadow_target_rebalance(profile, reason):
    with state_lock:
        books = dict(state["bbo"])
        market_meta = state["market_meta"]
    with shadow_lock:
        shadow = ensure_shadow_day(profile)
        if any(value > 1e-9 for value in shadow_reserved[profile].values()):
            return False
        values, total, _, targets = _shadow_weight_snapshot(
            profile, books, market_meta
        )
        if total <= 0 or shadow["profit_bank"] <= 0:
            return False
        changed = False
        for _ in range(6):
            values, total, _, targets = _shadow_weight_snapshot(
                profile, books, market_meta
            )
            deficits = sorted(
                [
                    (targets[asset] * total - values[asset], asset)
                    for asset in STABLES
                    if targets[asset] * total - values[asset] > 0.50
                ],
                reverse=True,
            )
            excesses = sorted(
                [
                    (values[asset] - targets[asset] * total, asset)
                    for asset in STABLES
                    if values[asset] - targets[asset] * total > 0.50
                ],
                reverse=True,
            )
            if not deficits or not excesses:
                break
            needed_usd, target = deficits[0]
            excess_usd, donor = excesses[0]
            donor_mark = stable_mark_usdt(donor, books, market_meta)
            if donor_mark <= 0:
                break
            input_units = min(excess_usd, needed_usd) / donor_mark
            conversion = stable_conversion_depth(
                donor, target, input_units, market_meta
            )
            if not conversion:
                break
            budget = max(0.0, shadow["profit_bank"]) * REBALANCE_MAX_PROFIT_SHARE
            if conversion["cost_usd"] > budget + 1e-12:
                shadow["skipped_rebalance"] += 1
                persist_shadow(profile)
                break
            if not apply_shadow_rebalance(
                profile, donor, target, conversion, reason, now_ms()
            ):
                break
            changed = True
        if changed:
            persist_shadow(profile)
        return changed


def shadow_periodic_rebalance_worker():
    while True:
        time.sleep(5)
        timestamp = now_ms()
        for profile in SHADOW_PROFILES:
            last_check = shadow_last_rebalance_check_ms.get(profile, 0)
            if timestamp - last_check < SHADOW_REBALANCE_CHECK_SEC * 1000:
                continue
            shadow_last_rebalance_check_ms[profile] = timestamp
            with state_lock:
                books = dict(state["bbo"])
                market_meta = state["market_meta"]
            with shadow_lock:
                _, _, weights, targets = _shadow_weight_snapshot(
                    profile, books, market_meta
                )
                max_deviation = max(
                    (
                        abs(weights.get(asset, 0.0) - targets.get(asset, 0.0))
                        for asset in STABLES
                    ),
                    default=0.0,
                )
            reason = None
            if max_deviation >= SHADOW_REBALANCE_EARLY_DEV:
                reason = "early_imbalance"
            elif (
                timestamp - shadow_last_rebalance_ms.get(profile, timestamp)
                >= SHADOW_REBALANCE_MAX_SEC * 1000
                and max_deviation >= SHADOW_REBALANCE_MIN_DEV
            ):
                reason = "periodic_4h"
            if reason and shadow_target_rebalance(profile, reason):
                shadow_last_rebalance_ms[profile] = timestamp


# ---------------------------------------------------------------------------
# T0 decisions, event de-duplication and sequential profile execution.
# ---------------------------------------------------------------------------
def schedule_decision(route, event_x, timestamp, event_start_ts):
    route_id = route["id"]
    broad_due = timestamp - last_decision_ms.get(route_id, 0) >= DECISION_COOLDOWN_MS
    quality_due = (
        event_x.get("net", -1.0) >= SHADOW_MIN_EDGE
        and timestamp - last_quality_decision_ms.get(route_id, 0)
        >= DECISION_COOLDOWN_MS
    )
    if not broad_due and not quality_due:
        return
    if event_x.get("max_age", 999999) > DECISION_MAX_RECV_AGE_MS:
        return
    if event_x.get("bbo_skew", 999999) > DECISION_MAX_SKEW_MS:
        return
    if broad_due:
        last_decision_ms[route_id] = timestamp
    if quality_due:
        last_quality_decision_ms[route_id] = timestamp

    _, _, day = local_day_bounds_ms(0)
    decision_id = f"{route_id}@{timestamp}"
    policy_reason = "base_eligible"
    if event_x.get("net", -1.0) < SHADOW_MIN_EDGE:
        policy_reason = "edge_below_min"
    elif event_x.get("max_age", 999999) > SHADOW_BBO_AGE_MS:
        policy_reason = "book_age"
    elif event_x.get("bbo_skew", 999999) > SHADOW_MAX_SKEW_MS:
        policy_reason = "book_skew"
    elif not symbols_post_resync_safe(route, timestamp):
        policy_reason = "post_resync_grace"

    quality = {
        "grade": "D",
        "eligible": False,
        "reason": policy_reason,
        "capacity_usd": event_x.get("size", 0.0),
        "size_fraction": 0.0,
        "selected_usd": 0.0,
        "normal_edge": event_x.get("net"),
        "half_bbo_edge": None,
        "drop_l1_edge": None,
        "coverage3": None,
        "compute_us": 0.0,
    }
    if policy_reason == "base_eligible":
        quality = depth_quality_decision(route, event_x)
        policy_reason = quality["reason"]

    event_key = (route_id, int(event_start_ts))
    admission = "not_eligible"
    reservations = []
    if quality["eligible"]:
        if event_key in shadow_consumed_events:
            admission = "blocked_same_event"
        elif not shadow_route_armed[route_id]:
            admission = "blocked_not_rearmed"
        elif timestamp - shadow_route_last_trade[route_id] < PAPER_HARD_COOLDOWN_MS:
            admission = "blocked_cooldown"
        else:
            shadow_x = dict(event_x)
            shadow_x.update(
                {
                    "size": quality["selected_usd"],
                    "quality_grade": quality["grade"],
                    "quality_fraction": quality["size_fraction"],
                }
            )
            for profile in SHADOW_PROFILES:
                reservation = reserve_shadow_trade(
                    profile,
                    route,
                    shadow_x,
                    event_start_ts,
                    decision_id,
                    timestamp,
                )
                if reservation:
                    reservations.append(reservation)
            if reservations:
                admission = (
                    "executed_all_profiles"
                    if len(reservations) == len(SHADOW_PROFILES)
                    else "executed_partial_profiles"
                )
                shadow_consumed_events.add(event_key)
                shadow_route_armed[route_id] = False
                shadow_route_last_trade[route_id] = timestamp
                shadow_route_neutral_since.pop(route_id, None)
                with pending_lock:
                    pending_shadow.extend(reservations)
            else:
                admission = "blocked_no_profile_balance"

    legs = []
    for index, symbol in enumerate(route["symbols"]):
        book = event_x["leg_books"][index]
        legs.append(
            {
                "leg": index + 1,
                "symbol": symbol,
                "from": route["path"][index],
                "to": route["path"][index + 1],
                "bid": book.get("bid"),
                "bidq": book.get("bidq"),
                "ask": book.get("ask"),
                "askq": book.get("askq"),
                "send_ts_ms": book.get("send_ts"),
                "recv_ts_ms": book.get("ts"),
            }
        )
    put_db(
        (
            "decision",
            (
                decision_id,
                day,
                route_id,
                ">".join(route["path"]),
                int(event_start_ts),
                timestamp,
                event_x["net"],
                event_x["size"],
                quality["selected_usd"],
                quality["grade"],
                1 if quality["eligible"] else 0,
                policy_reason,
                admission,
                len(reservations),
                event_x["max_age"],
                event_x.get("exchange_max_age", 0),
                event_x.get("bbo_skew", 0),
                event_x.get("exchange_skew", 0),
                json.dumps(legs, separators=(",", ":")),
                quality["compute_us"],
            ),
        )
    )
    put_db(
        (
            "quality",
            (
                decision_id,
                day,
                route_id,
                timestamp,
                quality["grade"],
                1 if quality["eligible"] else 0,
                quality["reason"],
                quality["capacity_usd"],
                quality["size_fraction"],
                quality["selected_usd"],
                quality.get("normal_edge"),
                quality.get("half_bbo_edge"),
                quality.get("drop_l1_edge"),
                quality.get("coverage3"),
                quality["compute_us"],
            ),
        )
    )

    capture = {"did": decision_id, "route": route, "t0": timestamp}
    if 0 in DEPTH_CAPTURE_OFFSETS_MS:
        capture_decision_depth(capture, timestamp)
    with pending_lock:
        for offset in DEPTH_CAPTURE_OFFSETS_MS:
            if offset > 0:
                pending_captures.append(
                    {
                        "due": timestamp + offset,
                        "did": decision_id,
                        "route": route,
                        "t0": timestamp,
                    }
                )


def close_event(key, event, end_ts=None):
    finish = int(end_ts or event["last_ts"])
    average_net = event["sum_net"] / max(1, event["ticks"])
    put_db(
        (
            "event",
            (
                event["route_id"],
                event["path"],
                event["start_asset"],
                event["end_asset"],
                event["start_ts"],
                finish,
                max(0, finish - event["start_ts"]),
                event["ticks"],
                event["entry_net"],
                event["peak_net"],
                average_net,
                event["entry_size"],
                event["max_size"],
                event["entry_profit"],
                event["peak_profit"],
                event["max_bbo_age"],
            ),
        )
    )
    shadow_consumed_events.discard((event["route_id"], int(event["start_ts"])))


def process_route(route, timestamp):
    with state_lock:
        books = dict(state["bbo"])
        market_meta = state["market_meta"]
    result = route_calc(route, books, market_meta)
    if result:
        with state_lock:
            state["latest_routes"][route["id"]] = {
                "ts": timestamp,
                "net": result["net"],
                "size": result["size"],
                "max_age": result["max_age"],
                "bbo_skew": result["bbo_skew"],
            }

    route_id = route["id"]
    if not shadow_route_armed[route_id]:
        if result is not None and result["net"] <= PAPER_REARM_NET:
            neutral_since = shadow_route_neutral_since.get(route_id)
            if neutral_since is None:
                shadow_route_neutral_since[route_id] = timestamp
            elif timestamp - neutral_since >= PAPER_REARM_NEUTRAL_MS:
                shadow_route_armed[route_id] = True
                shadow_route_neutral_since.pop(route_id, None)
        else:
            shadow_route_neutral_since.pop(route_id, None)

    with event_lock:
        event = active_events.get(route_id)
        if result and result["net"] >= MIN_EVENT_NET:
            if event is None:
                event = {
                    "route_id": route_id,
                    "path": ">".join(route["path"]),
                    "start_asset": route["path"][0],
                    "end_asset": route["path"][-1],
                    "start_ts": timestamp,
                    "last_ts": timestamp,
                    "ticks": 1,
                    "entry_net": result["net"],
                    "peak_net": result["net"],
                    "sum_net": result["net"],
                    "entry_size": result["size"],
                    "max_size": result["size"],
                    "entry_profit": result["profit"],
                    "peak_profit": result["profit"],
                    "max_bbo_age": result["max_age"],
                }
                active_events[route_id] = event
            else:
                event["last_ts"] = timestamp
                event["ticks"] += 1
                event["sum_net"] += result["net"]
                event["peak_net"] = max(event["peak_net"], result["net"])
                event["max_size"] = max(event["max_size"], result["size"])
                event["peak_profit"] = max(event["peak_profit"], result["profit"])
                event["max_bbo_age"] = max(
                    event["max_bbo_age"], result["max_age"]
                )
            schedule_decision(route, result, timestamp, event["start_ts"])
        elif event is not None:
            close_event(route_id, event, event["last_ts"])
            active_events.pop(route_id, None)


def scan_worker():
    while True:
        symbol = scanq.get()
        try:
            with state_lock:
                routes = list(state["routes_by_symbol"].get(symbol, ()))[
                    :MAX_ROUTES_PER_SYMBOL_SCAN
                ]
            timestamp = now_ms()
            for route in routes:
                process_route(route, timestamp)
            with state_lock:
                state["scan_updates"] += 1
        except Exception as exc:
            logerr(f"scan {symbol}: {exc}")
        finally:
            with queued_lock:
                queued_symbols.discard(symbol)
            scanq.task_done()


def execution_worker():
    while True:
        time.sleep(0.005)
        timestamp = now_ms()
        due_shadow, due_captures = [], []
        with pending_lock:
            keep_shadow = []
            for item in pending_shadow:
                (due_shadow if item["next_due"] <= timestamp else keep_shadow).append(item)
            pending_shadow[:] = keep_shadow
            keep_captures = []
            for item in pending_captures:
                (due_captures if item["due"] <= timestamp else keep_captures).append(item)
            pending_captures[:] = keep_captures
        for capture in due_captures:
            capture_decision_depth(capture, timestamp)
        for item in due_shadow:
            execute_shadow_leg(item, timestamp)


def event_sweeper():
    while True:
        time.sleep(0.03)
        timestamp = now_ms()
        with event_lock:
            for key, event in list(active_events.items()):
                if timestamp - event["last_ts"] > EVENT_CLOSE_GAP_MS:
                    close_event(key, event, event["last_ts"])
                    active_events.pop(key, None)


def snapshot_worker():
    while True:
        time.sleep(SNAPSHOT_INTERVAL_SEC)
        timestamp = now_ms()
        with state_lock:
            rows = [
                (route_id, value)
                for route_id, value in state["latest_routes"].items()
                if timestamp - value["ts"] <= 3000
            ]
        rows.sort(key=lambda item: item[1]["net"], reverse=True)
        payload = [
            (
                timestamp,
                route_id,
                value["net"],
                value["size"],
                value["max_age"],
                value["bbo_skew"],
            )
            for route_id, value in rows[:SNAPSHOT_TOP_N]
        ]
        if payload:
            put_db(("snapshot_many", payload))


# ---------------------------------------------------------------------------
# WebSocket workers. Busy symbols are spread by 24h quote volume.
# ---------------------------------------------------------------------------
def ws_worker(symbols, worker_id, group_volume_24h=0.0):
    while True:
        opened = False
        last_health_publish = 0
        connection_stop = threading.Event()
        try:
            def application_ping_loop(ws):
                while not connection_stop.wait(MEXC_APP_PING_SEC):
                    timestamp = now_ms()
                    with state_lock:
                        health = state["ws_workers"].get(worker_id)
                        if not health or not health.get("connected"):
                            return
                        last_pong = (
                            health.get("last_pong_ms")
                            or health.get("opened_ms")
                            or timestamp
                        )
                        sent = int(health.get("pings", 0))
                    if sent and timestamp - last_pong > MEXC_APP_PONG_TIMEOUT_SEC * 1000:
                        with state_lock:
                            health = state["ws_workers"].get(worker_id)
                            if health is not None:
                                health["last_error"] = "MEXC application PONG timeout"
                        try:
                            ws.close()
                        except Exception:
                            pass
                        return
                    try:
                        ws.send(json.dumps({"method": "PING"}))
                        with state_lock:
                            state["ws_app_pings"] += 1
                            health = state["ws_workers"].get(worker_id)
                            if health is not None:
                                health["last_ping_ms"] = timestamp
                                health["pings"] = int(health.get("pings", 0)) + 1
                    except Exception as exc:
                        with state_lock:
                            state["ws_ping_errors"] += 1
                            health = state["ws_workers"].get(worker_id)
                            if health is not None:
                                health["ping_errors"] = (
                                    int(health.get("ping_errors", 0)) + 1
                                )
                                health["last_error"] = str(exc)[:300]
                        return

            def on_open(ws):
                nonlocal opened
                opened = True
                timestamp = now_ms()
                with state_lock:
                    state["ws_connected"] += 1
                    previous = state["ws_workers"].get(worker_id, {})
                    opens = int(previous.get("opens", 0)) + 1
                    if opens > 1:
                        state["ws_reconnects"] += 1
                    state["ws_workers"][worker_id] = {
                        "worker_id": worker_id,
                        "symbols": len(symbols),
                        "connected": True,
                        "opens": opens,
                        "disconnects": int(previous.get("disconnects", 0)),
                        "opened_ms": timestamp,
                        "symbol_names": list(symbols),
                        "volume_24h": group_volume_24h,
                        "last_message_ms": None,
                        "last_close_code": previous.get("last_close_code"),
                        "last_error": None,
                        "last_ping_ms": None,
                        "last_pong_ms": None,
                        "pings": int(previous.get("pings", 0)),
                        "pongs": int(previous.get("pongs", 0)),
                        "ping_errors": int(previous.get("ping_errors", 0)),
                    }
                params = [
                    f"spot@public.aggre.depth.v3.api.pb@10ms@{symbol}"
                    for symbol in symbols
                ]
                ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
                threading.Thread(
                    target=application_ping_loop, args=(ws,), daemon=True
                ).start()
                print(
                    f"[3L V{VERSION}] WS {worker_id}: {len(symbols)} symbols "
                    f"load24h={group_volume_24h:,.0f}",
                    flush=True,
                )

            def on_message(ws, message):
                nonlocal last_health_publish
                if isinstance(message, str):
                    try:
                        body = json.loads(message)
                        if str(body.get("msg", "")).upper() == "PONG":
                            timestamp = now_ms()
                            with state_lock:
                                state["ws_app_pongs"] += 1
                                health = state["ws_workers"].get(worker_id)
                                if health is not None:
                                    health["last_pong_ms"] = timestamp
                                    health["pongs"] = int(health.get("pongs", 0)) + 1
                    except Exception:
                        pass
                    return
                try:
                    update = decode_depth(message)
                    if not update:
                        return
                    timestamp = now_ms()
                    if timestamp - last_health_publish >= 1000:
                        last_health_publish = timestamp
                        with state_lock:
                            health = state["ws_workers"].get(worker_id)
                            if health is not None:
                                health["last_message_ms"] = timestamp
                    last_sample = ws_latency_last_sample.get(update["symbol"], 0)
                    if timestamp - last_sample >= WS_LATENCY_SAMPLE_MS:
                        ws_latency_last_sample[update["symbol"]] = timestamp
                        put_db(
                            (
                                "ws_latency",
                                (
                                    timestamp,
                                    update["symbol"],
                                    update["send"],
                                    timestamp,
                                    max(0, timestamp - update["send"]),
                                ),
                            )
                        )
                    apply_depth_update(update)
                except Exception as exc:
                    logerr(f"decode WS {worker_id}: {exc}")

            def on_error(ws, error):
                with state_lock:
                    health = state["ws_workers"].get(worker_id)
                    if health is not None:
                        health["last_error"] = str(error)[:300]
                logerr(f"WS {worker_id}: {error}")

            def on_close(ws, code, message):
                nonlocal opened
                connection_stop.set()
                if opened:
                    with state_lock:
                        state["ws_connected"] = max(0, state["ws_connected"] - 1)
                        state["ws_disconnects"] += 1
                        health = state["ws_workers"].get(worker_id)
                        if health is not None:
                            health["connected"] = False
                            health["disconnects"] = (
                                int(health.get("disconnects", 0)) + 1
                            )
                            health["closed_ms"] = now_ms()
                            health["last_close_code"] = code
                    opened = False

            ws_app = websocket.WebSocketApp(
                WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # MEXC uses application JSON PING/PONG. RFC control-frame timeouts
            # caused false reconnects on busy streams in the previous scanner.
            ws_app.run_forever(ping_interval=0)
        except Exception as exc:
            logerr(f"WS loop {worker_id}: {exc}")
        finally:
            connection_stop.set()
            if opened:
                with state_lock:
                    state["ws_connected"] = max(0, state["ws_connected"] - 1)
                    state["ws_disconnects"] += 1
                    health = state["ws_workers"].get(worker_id)
                    if health is not None:
                        health["connected"] = False
                        health["disconnects"] = int(health.get("disconnects", 0)) + 1
                        health["closed_ms"] = now_ms()
                        health["last_close_code"] = "run_forever_exit"
                opened = False
        time.sleep(2)


# ---------------------------------------------------------------------------
# Dashboard and API.
# ---------------------------------------------------------------------------
def db_connect():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def daily_database_status():
    start, end, day = local_day_bounds_ms(0)
    result = {
        "day": day,
        "opportunities": 0,
        "avg_duration_ms": None,
        "max_peak_net": None,
        "decisions": 0,
        "eligible": 0,
        "depth_samples": 0,
        "quality": [],
        "admissions": [],
        "recent": [],
        "attempts": [],
    }
    try:
        con = db_connect()
        row = con.execute(
            """SELECT COUNT(*),AVG(duration_ms),MAX(peak_net)
            FROM opportunities_3leg WHERE start_ts_ms>=? AND start_ts_ms<?""",
            (start, end),
        ).fetchone()
        result.update(
            {
                "opportunities": row[0] or 0,
                "avg_duration_ms": row[1],
                "max_peak_net": row[2],
            }
        )
        row = con.execute(
            """SELECT COUNT(*),COALESCE(SUM(eligible),0)
            FROM decisions_3leg WHERE ts_ms>=? AND ts_ms<?""",
            (start, end),
        ).fetchone()
        result["decisions"], result["eligible"] = row[0] or 0, row[1] or 0
        result["depth_samples"] = con.execute(
            """SELECT COUNT(*) FROM decision_depth_samples_3leg
            WHERE sample_ts_ms>=? AND sample_ts_ms<?""",
            (start, end),
        ).fetchone()[0]
        result["quality"] = [
            dict(row)
            for row in con.execute(
                """SELECT grade,eligible,reason,COUNT(*) n,
                AVG(selected_usd) avg_selected_usd
                FROM decision_quality_3leg WHERE ts_ms>=? AND ts_ms<?
                GROUP BY grade,eligible,reason ORDER BY grade,eligible DESC""",
                (start, end),
            ).fetchall()
        ]
        result["admissions"] = [
            dict(row)
            for row in con.execute(
                """SELECT admission,COUNT(*) n FROM decisions_3leg
                WHERE ts_ms>=? AND ts_ms<? GROUP BY admission ORDER BY n DESC""",
                (start, end),
            ).fetchall()
        ]
        result["attempts"] = [
            dict(row)
            for row in con.execute(
                """SELECT profile,status,COUNT(*) n,
                AVG(profit_usd) avg_profit_usd,COALESCE(SUM(profit_usd),0) pnl_usd
                FROM shadow_attempts_3leg WHERE finished_ts_ms>=? AND finished_ts_ms<?
                GROUP BY profile,status ORDER BY profile,status""",
                (start, end),
            ).fetchall()
        ]
        result["recent"] = [
            dict(row)
            for row in con.execute(
                """SELECT ts_ms,route_id,decision_net,capacity_usd,selected_usd,
                grade,eligible,policy_reason,admission,recv_age_max_ms,recv_skew_ms
                FROM decisions_3leg WHERE ts_ms>=? AND ts_ms<?
                ORDER BY ts_ms DESC LIMIT 30""",
                (start, end),
            ).fetchall()
        ]
        con.close()
    except Exception as exc:
        logerr(f"dashboard DB: {exc}")
    return result


def shadow_status():
    with state_lock:
        books = dict(state["bbo"])
        market_meta = state["market_meta"]
    out = {}
    with shadow_lock:
        for profile, offsets in SHADOW_PROFILES.items():
            shadow = dict(ensure_shadow_day(profile))
            shadow["balances"] = dict(shadow["balances"])
            nav = sum(
                units * stable_mark_usdt(asset, books, market_meta)
                for asset, units in shadow["balances"].items()
            )
            shadow.update(
                {
                    "profile": profile,
                    "leg1_ms": offsets[0],
                    "leg2_ms": offsets[1],
                    "leg3_ms": offsets[2],
                    "capital": SIM_CAPITAL,
                    "nav": nav,
                    "profit": nav - SIM_CAPITAL,
                    "return_pct": (nav / SIM_CAPITAL - 1.0) * 100
                    if SIM_CAPITAL
                    else 0.0,
                    "reserved": dict(shadow_reserved[profile]),
                }
            )
            out[profile] = shadow
    return out


def runtime_health(timestamp):
    with state_lock:
        workers = [dict(row) for row in state["ws_workers"].values()]
        health = {
            "ws_connected": state["ws_connected"],
            "ws_expected": state["ws_expected"],
            "ws_disconnects": state["ws_disconnects"],
            "ws_reconnects": state["ws_reconnects"],
            "ws_app_pings": state["ws_app_pings"],
            "ws_app_pongs": state["ws_app_pongs"],
            "ws_ping_errors": state["ws_ping_errors"],
            "scan_coalesced": state["scan_coalesced"],
            "scan_queue_drops": state["scan_queue_drops"],
            "db_queue_drops": state["db_queue_drops"],
            "depth_gap_events": state["depth_gap_events"],
            "depth_resync_failures": state["depth_resync_failures"],
            "depth_resync_recoveries": state["depth_resync_recoveries"],
        }
    message_ages = [
        max(0, timestamp - (row.get("last_message_ms") or row.get("opened_ms") or timestamp))
        for row in workers
        if row.get("connected")
    ]
    pong_ages = [
        max(0, timestamp - (row.get("last_pong_ms") or row.get("opened_ms") or timestamp))
        for row in workers
        if row.get("connected")
    ]
    with depth_resync_lock:
        resync_queue = depth_resync_q.qsize()
        resync_pending = len(depth_resync_pending)
    health.update(
        {
            "workers": workers,
            "stale_workers": sum(age > 30000 for age in message_ages),
            "max_worker_message_age_ms": max(message_ages) if message_ages else None,
            "max_pong_age_ms": max(pong_ages) if pong_ages else None,
            "workers_without_pong": sum(
                1
                for row in workers
                if row.get("connected")
                and row.get("pings", 0) > 0
                and not row.get("last_pong_ms")
            ),
            "scan_queue": scanq.qsize(),
            "scan_queue_capacity": scanq.maxsize,
            "db_queue": dbq.qsize(),
            "db_queue_capacity": dbq.maxsize,
            "resync_queue": resync_queue,
            "resync_pending": resync_pending,
            "pending_shadow": len(pending_shadow),
            "pending_captures": len(pending_captures),
        }
    )
    return health


@app.get("/api/status")
def api_status():
    timestamp = now_ms()
    database = daily_database_status()
    with state_lock:
        age = timestamp - state["last_ws_ms"] if state["last_ws_ms"] else None
        response = {
            "version": VERSION,
            "symbols": len(state["symbols"]),
            "routes": len(state["routes"]),
            "ws": state["ws_connected"],
            "ws_expected": state["ws_expected"],
            "age_ms": age,
            "depth_ready": state["depth_ready"],
            "market_source": state["market_source"],
            "errors": list(state["errors"]),
            "scan_updates": state["scan_updates"],
            "diag": state["route_diag"],
        }
    response.update(
        {
            "database": database,
            "shadows": shadow_status(),
            "health": runtime_health(timestamp),
            "fee_per_leg_pct": FEE * 100,
            "route_fee_pct": (1.0 - (1.0 - FEE) ** 3) * 100,
            "stables": STABLES,
            "min_event_net_pct": MIN_EVENT_NET * 100,
            "policy": {
                "shadow_min_edge_pct": SHADOW_MIN_EDGE * 100,
                "a_fraction_pct": DEPTH_QUALITY_A_FRACTION * 100,
                "b_fraction_pct": DEPTH_QUALITY_B_FRACTION * 100,
                "bplus_half_edge_pct": DEPTH_QUALITY_BPLUS_HALF_EDGE * 100,
                "bplus_drop_floor_pct": DEPTH_QUALITY_BPLUS_DROP_FLOOR * 100,
                "coverage_l1_l3_min": DEPTH_QUALITY_COVERAGE3_MIN,
                "target_weights": SHADOW_TARGET_WEIGHTS,
                "book_age_ms": SHADOW_BBO_AGE_MS,
                "book_skew_ms": SHADOW_MAX_SKEW_MS,
                "profiles": SHADOW_PROFILES,
                "capture_offsets_ms": DEPTH_CAPTURE_OFFSETS_MS,
                "atomic_full_fill_required": True,
            },
            "quality_compute": {
                "samples": len(depth_quality_compute_us),
                "p50_us": percentile(list(depth_quality_compute_us), 0.50),
                "p95_us": percentile(list(depth_quality_compute_us), 0.95),
                "max_us": max(depth_quality_compute_us)
                if depth_quality_compute_us
                else None,
            },
        }
    )
    return jsonify(response)


@app.get("/healthz")
def healthz():
    timestamp = now_ms()
    with state_lock:
        connected = state["ws_connected"]
        expected = state["ws_expected"]
        last_ws = state["last_ws_ms"]
    healthy = expected > 0 and connected == expected and last_ws > 0 and timestamp - last_ws < 30000
    return jsonify({"ok": healthy, "version": VERSION}), 200 if healthy else 503


HTML = r'''<!doctype html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MEXC 3-Leg V3.0</title>
<style>
body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}
h2,h3{margin:0 0 10px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}
.card,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.panel{margin-top:12px;overflow:auto}
.value{font-weight:800;font-size:20px}.big{font-size:28px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.bad{color:#ff6677}
.profiles{display:grid;grid-template-columns:repeat(3,minmax(250px,1fr));gap:10px}.profile{background:#0c1421;border:1px solid #2a3850;border-radius:10px;padding:12px}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(95px,1fr));gap:7px;margin-top:10px}.metric{background:#111b2b;border-radius:8px;padding:8px}
.tag{display:inline-block;border:1px solid #334155;border-radius:999px;padding:3px 7px;font-size:11px;color:#bac6d8;margin-bottom:8px}
table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}
@media(max-width:900px){.profiles{grid-template-columns:1fr}}
</style></head><body>
<h2>MEXC — Scanner Spot 3-leg V3.0</h2>
<div class="muted">Recherche uniquement · aucun ordre réel · rafraîchissement 3 s</div>
<div class="cards" id="cards" style="margin-top:12px"></div>
<div class="panel" id="health"></div>
<div class="panel" id="profiles"></div>
<div class="panel" id="policy"></div>
<div class="panel" id="quality"></div>
<div class="panel"><h3>Décisions récentes</h3><table><thead><tr>
<th>Heure</th><th>Route</th><th>Edge net T0</th><th>Capacité</th><th>Taille</th><th>Classe</th><th>Admission</th><th>Âge</th><th>Skew</th>
</tr></thead><tbody id="recent"></tbody></table></div>
<div class="panel" id="universe"></div>
<div class="panel" id="errors"></div>
<script>
const pct=v=>v==null?'-':(100*Number(v)).toFixed(3)+'%';
const num=(v,d=2)=>v==null?'-':Number(v).toFixed(d);
async function refresh(){
 const d=await fetch('/api/status').then(r=>r.json()), db=d.database||{}, he=d.health||{}, p=d.policy||{}, g=d.diag||{};
 document.getElementById('cards').innerHTML=`
 <div class=card><div class=value>${d.symbols}</div><div class=muted>marchés WS</div></div>
 <div class=card><div class=value>${d.routes}</div><div class=muted>routes 3-leg</div></div>
 <div class=card><div class=value>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div>
 <div class=card><div class=value>${d.depth_ready}/${d.symbols}</div><div class=muted>Depth prêts</div></div>
 <div class=card><div class=value>${d.age_ms??'-'} ms</div><div class=muted>dernier flux</div></div>
 <div class=card><div class=value>${db.opportunities||0}</div><div class=muted>événements aujourd'hui</div></div>
 <div class=card><div class=value>${db.decisions||0}</div><div class=muted>décisions T0</div></div>
 <div class=card><div class=value>${db.depth_samples||0}</div><div class=muted>snapshots replay</div></div>`;
 const wsok=(he.ws_connected===he.ws_expected)&&(he.stale_workers||0)===0&&(he.workers_without_pong||0)===0;
 document.getElementById('health').innerHTML=`<div class=tag>SANTÉ TEMPS RÉEL</div><h3 class=${wsok?'good':'bad'}>WS ${he.ws_connected||0}/${he.ws_expected||0} · ${he.stale_workers||0} silencieux &gt;30 s</h3>
 <div class=cards><div><div class=value>${he.ws_disconnects||0}</div><div class=muted>chutes WS</div></div><div><div class=value>${he.ws_reconnects||0}</div><div class=muted>reconnexions</div></div>
 <div><div class=value>${he.ws_app_pings||0}/${he.ws_app_pongs||0}</div><div class=muted>PING/PONG</div></div><div><div class=value>${he.max_pong_age_ms??'-'} ms</div><div class=muted>âge max PONG</div></div>
 <div><div class=value>${he.depth_gap_events||0}</div><div class=muted>gaps Depth</div></div><div><div class=value>${he.depth_resync_failures||0}</div><div class=muted>échecs resync</div></div>
 <div><div class=value>${he.resync_queue||0}/${he.resync_pending||0}</div><div class=muted>resync file/pending</div></div><div><div class=value>${he.scan_queue||0}</div><div class=muted>file scan</div></div>
 <div><div class=value>${he.scan_queue_drops||0}</div><div class=muted>drops scan</div></div><div><div class=value>${he.db_queue||0}</div><div class=muted>file DB</div></div></div>`;
 let ph=`<div class=tag>EXÉCUTION SÉQUENTIELLE</div><h3>FAST / TARGET / DEGRADED — ${num(Object.values(d.shadows||{})[0]?.capital,0)} $ indépendants</h3><div class=profiles>`;
 for(const name of ['FAST','TARGET','DEGRADED']){const x=(d.shadows||{})[name];if(!x)continue;const balances=Object.entries(x.balances||{}).map(([a,v])=>`${a} <b>${num(v)}</b>`).join(' · ');
 ph+=`<div class=profile><div class=muted>${name} · +${x.leg1_ms}/+${x.leg2_ms}/+${x.leg3_ms} ms</div><div class="value big ${x.profit>=0?'good':'bad'}">${x.profit>=0?'+':''}${num(x.profit)} $</div><div class=muted>NAV ${num(x.nav)} $</div>
 <div class=metrics><div class=metric><div class=value>${x.completed}</div><div class=muted>cycles complets</div></div><div class=metric><div class=value>${x.wins}/${x.losses}</div><div class=muted>gagnés/perdus</div></div><div class=metric><div class=value>${x.failed_sequences}</div><div class=muted>séquences incomplètes</div></div><div class=metric><div class=value>${x.rebalances}</div><div class=muted>rebalances</div></div></div>
 <div class=muted style="margin-top:8px">${balances}<br>Coût rebalance ${num(x.rebalance_cost,3)} $ · refus balance ${x.skipped_balance}</div></div>`}
 ph+=`</div><div class=muted style="margin-top:10px">Même signal et même taille Depth cible; les trois carnets sont lus aux échéances propres à chaque profil. Une séquence incomplète est comptée, sans PnL artificiel.</div>`;document.getElementById('profiles').innerHTML=ph;
 const weights=Object.entries(p.target_weights||{}).map(([a,v])=>`${a} ${(100*v).toFixed(0)}%`).join(' · ');
 document.getElementById('policy').innerHTML=`<div class=tag>HYPOTHÈSES 3-LEG</div><h3>0,05% par jambe · ${num(d.route_fee_pct,4)}% composé sur 3 jambes</h3><div class=cards>
 <div><div class=value>${num(p.shadow_min_edge_pct,2)}%</div><div class=muted>edge net T0 minimum</div></div><div><div class=value>A · ${num(p.a_fraction_pct,0)}%</div><div class=muted>capacité robuste</div></div>
 <div><div class=value>B+ · ${num(p.b_fraction_pct,0)}%</div><div class=muted>capacité intermédiaire</div></div><div><div class=value>×${num(p.coverage_l1_l3_min,0)}</div><div class=muted>réserve L1-L3 sur les 3 jambes</div></div>
 <div><div class=value>${weights}</div><div class=muted>allocation cible</div></div><div><div class=value>${p.book_age_ms} / ${p.book_skew_ms} ms</div><div class=muted>âge / skew T0</div></div></div>`;
 let counts={};for(const r of db.quality||[]){counts[r.grade]=(counts[r.grade]||0)+r.n}let adm={};for(const r of db.admissions||[]){adm[r.admission]=r.n}
 document.getElementById('quality').innerHTML=`<div class=tag>ENTONNOIR AUJOURD'HUI</div><div class=cards>
 <div><div class="value good">${counts['A']||0}</div><div class=muted>A</div></div><div><div class="value good">${counts['B+']||0}</div><div class=muted>B+</div></div><div><div class=value>${counts['B-']||0}</div><div class=muted>B− refusés</div></div>
 <div><div class=value>${(counts['C']||0)+(counts['D']||0)}</div><div class=muted>C/D refusés</div></div><div><div class="value good">${(adm.executed_all_profiles||0)+(adm.executed_partial_profiles||0)}</div><div class=muted>signaux réservés</div></div>
 <div><div class=value>${adm.blocked_no_profile_balance||0}</div><div class=muted>bloqués balance</div></div><div><div class=value>${num(d.quality_compute?.p95_us,1)} µs</div><div class=muted>calcul qualité p95</div></div></div>`;
 let rh='';for(const r of db.recent||[]){rh+=`<tr><td>${new Date(r.ts_ms).toLocaleTimeString()}</td><td>${r.route_id}</td><td>${pct(r.decision_net)}</td><td>${num(r.capacity_usd)} $</td><td>${num(r.selected_usd)} $</td><td class=${r.eligible?'good':'muted'}>${r.grade}</td><td>${r.admission}</td><td>${r.recv_age_max_ms} ms</td><td>${r.recv_skew_ms} ms</td></tr>`}document.getElementById('recent').innerHTML=rh;
 document.getElementById('universe').innerHTML=`<div class=tag>UNIVERS</div><div class=cards><div><div class=value>${g.all_markets||0}</div><div class=muted>marchés MEXC</div></div><div><div class=value>${g.candidate_3leg||0}</div><div class=muted>routes candidates</div></div><div><div class="value ${g.selected_3leg===g.candidate_3leg?'good':'bad'}">${g.selected_3leg||0}</div><div class=muted>routes suivies</div></div><div><div class=value>${g.dropped_3leg||0}</div><div class=muted>routes exclues</div></div><div><div class=value>${g.ws_group_size||'-'}</div><div class=muted>symboles/WS</div></div><div><div class=value>${g.ws_grouping_mode||'-'}</div><div class=muted>répartition flux</div></div></div>`;
 document.getElementById('errors').innerHTML=`<div class=tag>DERNIÈRES ERREURS</div><div class=muted>${(d.errors||[]).map(x=>x.replaceAll('<','&lt;')).join('<br>')||'Aucune'}</div>`;
}
refresh();setInterval(refresh,3000);
</script></body></html>'''


@app.get("/")
def index():
    return render_template_string(HTML)


def bootstrap():
    global pair_index
    threading.Thread(target=db_writer, daemon=True).start()
    time.sleep(0.20)
    load_shadow_states()
    boot_ts = now_ms()
    for profile in SHADOW_PROFILES:
        shadow_last_rebalance_ms[profile] = boot_ts
        shadow_last_rebalance_check_ms[profile] = boot_ts

    markets = discover_markets()
    chosen, routes, market_meta, diag = select_routes_under_symbol_budget(markets)
    pair_index = {
        frozenset((market["base"], market["quote"])): symbol
        for symbol, market in market_meta.items()
    }
    routes_by_symbol = defaultdict(list)
    for route in routes:
        for symbol in route["symbols"]:
            routes_by_symbol[symbol].append(route)
    symbols = [market["symbol"] for market in chosen]
    groups, group_loads, grouping_mode = build_balanced_ws_groups(symbols)
    diag.update(
        {
            "ws_grouping_mode": grouping_mode,
            "ws_groups": len(groups),
            "mexc_app_ping_sec": MEXC_APP_PING_SEC,
            "mexc_app_pong_timeout_sec": MEXC_APP_PONG_TIMEOUT_SEC,
        }
    )
    with state_lock:
        state["symbols"] = symbols
        state["routes"] = routes
        state["routes_by_symbol"] = routes_by_symbol
        state["market_meta"] = market_meta
        state["route_diag"] = diag
        state["ws_expected"] = len(groups)

    print(
        f"[3L V{VERSION}] source={state['market_source']} "
        f"markets={diag['all_markets']} candidates={diag['candidate_3leg']} "
        f"selected_symbols={len(symbols)} selected_routes={len(routes)} "
        f"dropped={diag['dropped_3leg']} WS={len(groups)} "
        f"group<={WS_GROUP_SIZE} grouping={grouping_mode} "
        f"fees=3x{FEE*100:.3f}% port={PORT}",
        flush=True,
    )
    metadata = {
        "version": VERSION,
        "route_type": "stable>A>B>stable",
        "three_leg_only": True,
        "fee_per_leg": FEE,
        "route_fee_compounded": 1.0 - (1.0 - FEE) ** 3,
        "profiles_ms": SHADOW_PROFILES,
        "decision_at_t0": True,
        "full_fill_required": True,
        "min_event_net": MIN_EVENT_NET,
        "shadow_min_edge": SHADOW_MIN_EDGE,
        "target_weights": SHADOW_TARGET_WEIGHTS,
        "route_diag": diag,
        "depth_capture_offsets_ms": DEPTH_CAPTURE_OFFSETS_MS,
        "depth_capture_levels": DEPTH_CAPTURE_LEVELS,
        "ws_depth_interval": "10ms",
        "depth_snapshot_levels": 100,
    }
    for key, value in metadata.items():
        put_db(
            (
                "meta",
                (
                    key,
                    json.dumps(value, sort_keys=True)
                    if not isinstance(value, str)
                    else value,
                ),
            )
        )

    for _ in range(SCAN_WORKERS):
        threading.Thread(target=scan_worker, daemon=True).start()
    threading.Thread(target=event_sweeper, daemon=True).start()
    threading.Thread(target=execution_worker, daemon=True).start()
    threading.Thread(target=snapshot_worker, daemon=True).start()
    threading.Thread(target=shadow_periodic_rebalance_worker, daemon=True).start()
    for index, group in enumerate(groups, 1):
        threading.Thread(
            target=ws_worker,
            args=(group, index, group_loads[index - 1]),
            daemon=True,
        ).start()
    time.sleep(1)
    threading.Thread(target=depth_resync_worker, daemon=True).start()
    threading.Thread(
        target=depth_bootstrap_worker, args=(symbols,), daemon=True
    ).start()


if __name__ == "__main__":
    bootstrap()
    app.run(host=HOST, port=PORT, threaded=True, use_reloader=False)

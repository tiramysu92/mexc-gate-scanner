#!/usr/bin/env python3
# Release artifact: upload this file as app.py on the scanner server.
import os, json, time, math, queue, sqlite3, threading, random, requests, hashlib, hmac, uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_CEILING
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, render_template_string
import websocket
from session_live_v2412 import LiveSession, TOTALS_SQL as SESSION_TOTALS_SQL, totals as session_totals

VERSION = "2.4.12-bounded-live-session"
# V2411: entry exit-headroom, bounded confirmed exit retry, manual-close accounting.
# Public fair consumer and existing freshness/fee/size limits are retained.
# Release installation keeps LIVE stopped; exchange-side cancellation is unresolved.
# Baseline SHA256: d4038460cdf499ac9b0b02da487680a2af37fc09e7985a13156cbc02642d2919
# Main changes: independent version-triggered admission; affordable-size A/B+;
# continuous Depth versions/timestamps; midpoint HTTP clock; cached sorted books;
# catch-up scan scheduling; FOK entry/exit (new TEST certificates required);
# no unwind after an uncertain order; fee/accounting reconciliation; per-stage
# book evidence and minute diagnostics. $20/$200/$5 and freshness limits retained.
# TEST journal: mexc_live_v247_test.db. Research: mexc_routes_v247.db.
# LIVE retains mexc_live_v246.db by default, including historical losses/causes,
# and imports only newer compatible certificates from the V247 TEST journal.
# No automatic reset/arming or LIVE rebalance. The versioned arm phrase below
# deliberately rejects V246 arm files. Do not run two scanner processes together.
# FOK price protection is not atomic across both legs: entry can fill and exit
# can cancel. Emergency MARKET unwind can slip; $5 is a stop trigger, not a
# guaranteed maximum loss. TEST acceptance and Shadow PnL do not prove fills.
# Fee assumption remains TAKER_FEE (default .05%/leg), not a queried account fee.
# Unresolved commissions pause entry; the .20% reserve is not realized expense.
# Protocol reference: https://mexcdevelop.github.io/apidocs/spot_v3_en/
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8081"))
DB_PATH = os.getenv("DB_PATH", "mexc_routes_v247.db")
LIVE_DB_PATH = os.getenv("LIVE_DB_PATH", "mexc_live_v246.db")
LIVE_IMPORT_CAPABILITIES_DB = os.getenv("LIVE_IMPORT_CAPABILITIES_DB", "mexc_live_v246.db")
MARKET_CACHE = os.getenv("MARKET_CACHE", "mexc_markets_cache.json")
TZ_NAME = os.getenv("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)

# Trading assumptions
FEE = float(os.getenv("TAKER_FEE", "0.0005"))            # 0.05% taker / leg
STABLES = tuple(x.strip().upper() for x in os.getenv("STABLES", "USDT,USDC,USD1").split(",") if x.strip())
MIN_EXEC_USD = float(os.getenv("MIN_EXEC_USD", "10"))
SHADOW_TRADE_CAP_USD = max(0.0, float(os.getenv("SHADOW_TRADE_CAP_USD", "350")))
MAX_EXEC_USD = SHADOW_TRADE_CAP_USD
MIN_NET = float(os.getenv("MIN_NET_PCT", "0.01")) / 100.0
MAX_BBO_AGE_MS = int(os.getenv("MAX_BBO_AGE_MS", "750"))
EVENT_CLOSE_GAP_MS = int(os.getenv("EVENT_CLOSE_GAP_MS", "2000"))
EVENT_NEUTRAL_CLOSE_MS = int(os.getenv("EVENT_NEUTRAL_CLOSE_MS", "1000"))
EVENT_CLOSE_NET = float(os.getenv("EVENT_CLOSE_NET_PCT", "0.0")) / 100.0
FOLLOW_ALL_2LEG = os.getenv("FOLLOW_ALL_2LEG", "1") == "1"
MAX_WS_SYMBOLS = int(os.getenv("MAX_WS_SYMBOLS", "600"))
WS_GROUP_SIZE = max(1, min(30, int(os.getenv("WS_GROUP_SIZE", "25"))))
MEXC_APP_PING_SEC = max(5.0, float(os.getenv("MEXC_APP_PING_SEC", "15")))
MEXC_APP_PONG_TIMEOUT_SEC = max(MEXC_APP_PING_SEC+5.0, float(os.getenv("MEXC_APP_PONG_TIMEOUT_SEC", "45")))
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
ENABLE_ONLINE_RESEARCH_TRIALS = os.getenv("ENABLE_ONLINE_RESEARCH_TRIALS", "0") == "1"
REPLAY_MIN_EDGE = float(os.getenv("REPLAY_MIN_EDGE_PCT", "0.30")) / 100.0
EXEC_OBSERVE_MAX_AGE_MS = int(os.getenv("EXEC_OBSERVE_MAX_AGE_MS", "1000"))
TRACE_WINDOW_MS = int(os.getenv("TRACE_WINDOW_MS", "300"))
TRACE_MIN_STEP_MS = int(os.getenv("TRACE_MIN_STEP_MS", "2"))
DECISION_MAX_RECV_AGE_MS = int(os.getenv("DECISION_MAX_RECV_AGE_MS", "300"))
DECISION_MAX_SKEW_MS = int(os.getenv("DECISION_MAX_SKEW_MS", "250"))
# Economic re-arm: after a paper entry, the route must be neutral (<= 0%) continuously before another entry.
PAPER_REARM_NEUTRAL_MS = int(os.getenv("PAPER_REARM_NEUTRAL_MS", "1000"))
PAPER_REARM_NET = float(os.getenv("PAPER_REARM_NET_PCT", "0.0")) / 100.0
PAPER_HARD_COOLDOWN_MS = int(os.getenv("PAPER_HARD_COOLDOWN_MS", "5000"))
WS_LATENCY_SAMPLE_MS = int(os.getenv("WS_LATENCY_SAMPLE_MS", "30000"))
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
RESEARCH_SHADOW_PROFILES = tuple(SHADOW_PROFILES)
SHADOW_BBO_AGE_MS = int(os.getenv("SHADOW_BBO_AGE_MS", "150"))
SHADOW_MAX_SKEW_MS = int(os.getenv("SHADOW_MAX_SKEW_MS", "50"))
# V2.3.6 Depth Quality policy. The signal is classified once at T0 and the exact same
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

# Private MEXC micro-live gateway. Safe default: Shadow only. Real orders require
# all three gates: TRADING_MODE=live, LIVE_ARMED=1 and an exact local arm file.
TRADING_MODE = os.getenv("TRADING_MODE", "shadow").strip().lower()
if TRADING_MODE not in ("shadow", "test", "live"):
    TRADING_MODE = "shadow"
LIVE_ARMED = os.getenv("LIVE_ARMED", "0") == "1"
LIVE_CAP_USD = max(0.0, float(os.getenv("LIVE_CAP_USD", "20")))
LIVE_CAPITAL_LIMIT_USD = max(0.0, float(os.getenv("LIVE_CAPITAL_LIMIT_USD", "200")))
LIVE_DAILY_LOSS_LIMIT_USD = max(0.0, float(os.getenv("LIVE_DAILY_LOSS_LIMIT_USD", "5")))
# A separately authorized, finite allocation can be used after a reviewed loss.
# The helper requires an explicit budget argument; historical PnL is untouched.
LIVE_SESSION_REQUIRED = os.getenv("LIVE_SESSION_REQUIRED", "0") == "1"
LIVE_SESSION_ID = os.getenv("LIVE_SESSION_ID", "")
LIVE_EXCLUDED_ASSETS = frozenset(x.strip().upper() for x in os.getenv(
    "LIVE_EXCLUDED_ASSETS", "").split(",") if x.strip())
live_session_control = None
live_session_totals = None
LIVE_MAX_CONCURRENT = max(1, int(os.getenv("LIVE_MAX_CONCURRENT", "1")))
LIVE_BBO_AGE_MS = max(1, int(os.getenv("LIVE_BBO_AGE_MS", "50")))
LIVE_LEG2_BBO_AGE_MS = max(LIVE_BBO_AGE_MS, int(os.getenv("LIVE_LEG2_BBO_AGE_MS", "100")))
LIVE_MAX_SKEW_MS = max(0, int(os.getenv("LIVE_MAX_SKEW_MS", "50")))
# Local receive age alone is unsafe: a delayed MEXC Depth message becomes
# locally "fresh" as soon as it is received.  These gates use MEXC sendTime
# already carried by every public Depth update, so they add no HTTP request and
# no deliberate wait to the order path.
LIVE_EXCHANGE_AGE_MS = max(1, int(os.getenv("LIVE_EXCHANGE_AGE_MS", "100")))
LIVE_LEG2_EXCHANGE_AGE_MS = max(LIVE_EXCHANGE_AGE_MS, int(os.getenv(
    "LIVE_LEG2_EXCHANGE_AGE_MS", "150")))
LIVE_MAX_EXCHANGE_SKEW_MS = max(0, int(os.getenv("LIVE_MAX_EXCHANGE_SKEW_MS", "50")))
LIVE_LEG2_MAX_EXCHANGE_SKEW_MS = max(LIVE_MAX_EXCHANGE_SKEW_MS, int(os.getenv(
    "LIVE_LEG2_MAX_EXCHANGE_SKEW_MS", "150")))
LIVE_ORDER_SETTLE_TIMEOUT_MS = max(500, int(os.getenv("LIVE_ORDER_SETTLE_TIMEOUT_MS", "5000")))
LIVE_ORDER_POLL_MS = max(50, int(os.getenv("LIVE_ORDER_POLL_MS", "100")))
LIVE_API_TIMEOUT_SEC = max(1.0, float(os.getenv("LIVE_API_TIMEOUT_SEC", "5")))
LIVE_RECV_WINDOW_MS = max(1000, min(5000, int(os.getenv("LIVE_RECV_WINDOW_MS", "5000"))))
LIVE_MAX_CONSECUTIVE_UNWINDS = max(1, int(os.getenv("LIVE_MAX_CONSECUTIVE_UNWINDS", "2")))
LIVE_HARD_COOLDOWN_MS = max(0, int(os.getenv("LIVE_HARD_COOLDOWN_MS", "5000")))
LIVE_ACCOUNT_REFRESH_SEC = max(5.0, float(os.getenv("LIVE_ACCOUNT_REFRESH_SEC", "5")))
LIVE_ACCOUNT_CACHE_MAX_AGE_MS = max(1000, int(os.getenv("LIVE_ACCOUNT_CACHE_MAX_AGE_MS", "10000")))
LIVE_DUST_USD = max(0.0, float(os.getenv("LIVE_DUST_USD", "0.05")))
LIVE_FEE_SAFETY = max(FEE, float(os.getenv("LIVE_FEE_SAFETY_PCT", "0.20")) / 100.0)
LIVE_LEG2_MIN_EDGE = float(os.getenv("LIVE_LEG2_MIN_EDGE_PCT", "0.15")) / 100.0
# The leg-1 fill quantity is known from GET /order.  Its commission detail is
# useful for exact accounting but must not add another private REST round trip
# while the route is exposed to the intermediate asset.  A conservative fee
# haircut sizes leg 2; the exact commission is reconciled after leg 2/unwind.
LIVE_DEFER_LEG1_COMMISSION = os.getenv("LIVE_DEFER_LEG1_COMMISSION", "1") == "1"
LIVE_REQUIRE_ALL_WS = os.getenv("LIVE_REQUIRE_ALL_WS", "0") == "1"
LIVE_REQUIRE_ROUTE_WS = os.getenv("LIVE_REQUIRE_ROUTE_WS", "1") == "1"
LIVE_REQUIRE_TESTED_ROUTE = os.getenv("LIVE_REQUIRE_TESTED_ROUTE", "1") == "1"
LIVE_TEST_MAX_AGE_HOURS = max(1.0, float(os.getenv("LIVE_TEST_MAX_AGE_HOURS", "72")))
LIVE_TEST_PROBE_DEPTH_AGE_MS = max(LIVE_BBO_AGE_MS, int(os.getenv("LIVE_TEST_PROBE_DEPTH_AGE_MS", "10000")))
LIVE_PREVALIDATE_ENABLED = os.getenv("LIVE_PREVALIDATE_ENABLED", "1") == "1"
LIVE_PREVALIDATE_INTERVAL_SEC = max(1.0, float(os.getenv("LIVE_PREVALIDATE_INTERVAL_SEC", "5")))
LIVE_PREVALIDATE_REFRESH_HOURS = min(LIVE_TEST_MAX_AGE_HOURS, max(1.0, float(
    os.getenv("LIVE_PREVALIDATE_REFRESH_HOURS", "60"))))
# Syntax-only /order/test probes may use a quiet book: the full paced sweep can
# take about one hour. This age can never admit a BOT signal; the real gateway
# independently enforces local receive age/skew and MEXC sendTime age/skew.
LIVE_PREVALIDATE_DEPTH_AGE_MS = max(LIVE_BBO_AGE_MS, int(os.getenv(
    "LIVE_PREVALIDATE_DEPTH_AGE_MS", "7200000")))
# Route discovery may wait for a fresh A/B+ window because /order/test never
# reaches the matching engine. Real orders use a deliberately much shorter
# retry budget: a late arbitrage is discarded instead of being chased.
LIVE_RETRY_WINDOW_MS = max(0, int(os.getenv("LIVE_RETRY_WINDOW_MS", "1200")))
LIVE_RETRY_INTERVAL_MS = max(5, int(os.getenv("LIVE_RETRY_INTERVAL_MS", "10")))
LIVE_RETRY_MAX = max(0, int(os.getenv("LIVE_RETRY_MAX", "120")))
LIVE_EXEC_RETRY_WINDOW_MS = max(0, int(os.getenv("LIVE_EXEC_RETRY_WINDOW_MS", "30")))
LIVE_EXEC_RETRY_INTERVAL_MS = max(5, int(os.getenv("LIVE_EXEC_RETRY_INTERVAL_MS", "10")))
LIVE_EXEC_RETRY_MAX = max(0, int(os.getenv("LIVE_EXEC_RETRY_MAX", "2")))
LIVE_MAX_T0_TO_LEG1_POST_MS = max(LIVE_EXEC_RETRY_WINDOW_MS, int(os.getenv(
    "LIVE_MAX_T0_TO_LEG1_POST_MS", "50")))
LIVE_HTTP_KEEPALIVE_SEC = max(5.0, float(os.getenv("LIVE_HTTP_KEEPALIVE_SEC", "15")))
LIVE_HTTP_KEEPALIVE_TIMEOUT_SEC = max(0.25, float(os.getenv("LIVE_HTTP_KEEPALIVE_TIMEOUT_SEC", "0.75")))
# Private order stream. HYBRID releases the next leg on the first matching
# terminal WS/REST result. The independent REST probe always continues for
# reconciliation and timing evidence after a WS-first confirmation.
LIVE_PRIVATE_WS_MODE = os.getenv("LIVE_PRIVATE_WS_MODE", "hybrid").strip().lower()
if LIVE_PRIVATE_WS_MODE not in ("off", "observe", "hybrid"):
    LIVE_PRIVATE_WS_MODE = "hybrid"
LIVE_PRIVATE_WS_KEEPALIVE_SEC = max(60.0, min(3300.0, float(os.getenv(
    "LIVE_PRIVATE_WS_KEEPALIVE_SEC", "1500"))))
LIVE_PRIVATE_WS_PING_SEC = max(5.0, float(os.getenv("LIVE_PRIVATE_WS_PING_SEC", "15")))
LIVE_PRIVATE_WS_PONG_TIMEOUT_SEC = max(LIVE_PRIVATE_WS_PING_SEC+5.0, float(os.getenv(
    "LIVE_PRIVATE_WS_PONG_TIMEOUT_SEC", "45")))
LIVE_PRIVATE_WS_MAX_CONNECTION_SEC = max(3600.0, min(23.75*3600.0, float(os.getenv(
    "LIVE_PRIVATE_WS_MAX_CONNECTION_SEC", str(23*3600)))))
LIVE_RESET_CIRCUIT = False  # Never silently acknowledge historical LIVE causes on upgrade.
PROTECT_EXISTING_BAGS = os.getenv("PROTECT_EXISTING_BAGS", "1") == "1"
MEXC_ALLOWED_START_ASSETS = tuple(x.strip().upper() for x in os.getenv(
    "MEXC_ALLOWED_START_ASSETS", "USDT,USDC,USD1").split(",") if x.strip())
MEXC_API_KEY = os.getenv("MEXC_API_KEY", "").strip()
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "").strip()
LIVE_ARM_FILE = os.getenv("LIVE_ARM_FILE", "/home/ubuntu/.config/mexc-bot/LIVE_ARMED")
LIVE_STOP_FILE = os.getenv("LIVE_STOP_FILE", "/home/ubuntu/.config/mexc-bot/LIVE_STOP")
LIVE_ARM_PHRASE = f"ENABLE MEXC V247 LIVE {LIVE_CAP_USD:g} USD"
# TEST cannot contaminate the historical LIVE loss budget or reconciliation.
if TRADING_MODE != "live":
    LIVE_DB_PATH = os.getenv("LIVE_TEST_DB_PATH", "mexc_live_v247_test.db")
elif "LIVE_IMPORT_CAPABILITIES_DB" not in os.environ:
    LIVE_IMPORT_CAPABILITIES_DB = "mexc_live_v247_test.db"
# Market data freshness remains strict. Price protection is a separate control,
# not a reason to make old exchange timestamps appear fresh.
LIVE_PRICE_PROTECTED = os.getenv("LIVE_PRICE_PROTECTED", "1") == "1"
LIVE_FUTURE_TOLERANCE_MS = 25
LIVE_RECHECK_INTERVAL_MS = 10
LIVE_SIZE_SEARCH_STEPS = 6
LIVE_PREVALIDATE_ERROR_BACKOFF_MS = 900000

# V2411: a recent exit book needs headroom for a real first-leg confirmation.
# 75 ms floor covers the observed CTO 69 ms; 10 ms reserves post-confirm work.
# These are estimates, not guarantees; the existing L2 freshness guards remain.
LIVE_EXIT_CONFIRM_FLOOR_MS = 75
LIVE_EXIT_PREP_MARGIN_MS = 10
LIVE_UNWIND_MAX_ATTEMPTS = 2
LIVE_UNWIND_RETRY_WINDOW_MS = 1500

# Coverage accounting is updated in RAM on the scan path and persisted as only
# 800 aggregate rows at this cadence. It never writes one row per market tick.
ROUTE_COVERAGE_FLUSH_SEC = max(30.0, float(os.getenv("ROUTE_COVERAGE_FLUSH_SEC", "60")))
BOT_SHADOW_REBALANCE_ENABLED = os.getenv("BOT_SHADOW_REBALANCE_ENABLED", "0") == "1"

# Independent Shadow portfolios mirror the private gateway: the three historic
# clocks plus deliberately slower stress profiles, only on test-validated
# routes, with the live cap/capital and the same 30 min / 4 h simulated rebalance.
BOT_SHADOW_PROFILES = {
    "BOT_FAST": SHADOW_PROFILES["FAST"],
    "BOT_TARGET": SHADOW_PROFILES["TARGET"],
    "BOT_DEGRADED": SHADOW_PROFILES["DEGRADED"],
    "BOT_100_200": (100, 200),
    "BOT_150_300": (150, 300),
    "BOT_200_400": (200, 400),
}
SHADOW_PROFILES = {**SHADOW_PROFILES, **BOT_SHADOW_PROFILES}

# Replay-grade capture. Every broad T0 decision gets multi-level books at these future offsets,
# even if the live Shadow policy rejects it. This is what lets the next export replay balance,
# edge, persistence and latency policies chronologically.
DEPTH_CAPTURE_OFFSETS_MS = tuple(int(x) for x in os.getenv(
    "DEPTH_CAPTURE_OFFSETS_MS", "0,10,15,20,25,30,40,50,60,75,100,125,150,200,250,300,400,500,750,1000,1500,2000,3000,5000").split(","))
DEPTH_CAPTURE_LEVELS = int(os.getenv("DEPTH_CAPTURE_LEVELS", "25"))
STABLE_DEPTH_INTERVAL_SEC = float(os.getenv("STABLE_DEPTH_INTERVAL_SEC", "10"))
DB_COMMIT_BATCH = max(1, int(os.getenv("DB_COMMIT_BATCH", "250")))
DB_COMMIT_MAX_MS = max(5, int(os.getenv("DB_COMMIT_MAX_MS", "50")))
DASHBOARD_CACHE_SEC = max(5.0, float(os.getenv("DASHBOARD_CACHE_SEC", "30")))

# Historical research data
# Broad market snapshots are descriptive only; the decision-linked Depth
# captures remain the authoritative replay dataset.  V2.4.2 wrote 50 rows
# every two seconds, which added avoidable disk churn and database growth.
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "10.0"))
SNAPSHOT_TOP_N = int(os.getenv("SNAPSHOT_TOP_N", "20"))
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
    "scan_updates": 0, "scan_coalesced": 0, "scan_queue_drops": 0, "db_queue_drops": 0,
    "ws_disconnects": 0, "ws_reconnects": 0, "ws_app_pings": 0, "ws_app_pongs": 0,
    "ws_ping_errors": 0, "ws_workers": {}, "ws_disconnect_times": deque(maxlen=20000),
    "depth_gap_events": 0, "depth_resync_failures": 0, "depth_resync_recoveries": 0,
    "latest_routes": {}, "route_diag": {},
    "depth": {}, "depth_ready": 0,
    "replay_decisions": 0, "quality_decisions": 0,
}
active_events = {}
dbq = queue.Queue(maxsize=50000)
scanq = queue.Queue(maxsize=30000)
queued_symbols = set()
scan_generations = defaultdict(int)
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
# The bot-aligned portfolios are admitted by the fresh private gateway, not by
# the broader research decision at T0. They therefore need an independent
# event/re-arm lifecycle from FAST/TARGET/DEGRADED.
bot_shadow_consumed_events = set()
bot_shadow_route_armed = defaultdict(lambda: True)
bot_shadow_route_neutral_since = {}
bot_shadow_route_last_trade = defaultdict(int)
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
db_ready = threading.Event()
dashboard_cache_lock = threading.RLock()
dashboard_cache = {"updated_ms":0,"data":None,"last_error":None}

# Fixed-size per-route telemetry. Scan workers only mutate already allocated
# dictionaries; the writer takes one aggregate snapshot per minute.
route_coverage = {}
route_coverage_day = None
route_coverage_flush_lock = threading.Lock()

# The private gateway is deliberately independent from the three Shadow portfolios.
# It has one serial queue and its own durable journal. No HTTP route can arm it.
live_lock = threading.RLock()
live_db_lock = threading.RLock()
live_db_connection = None
liveq = queue.Queue(maxsize=100)
live_consumed_events = set()
# Cooldown is scoped to the intermediate crypto, so opposite routes for the
# same token cannot churn while an unrelated crypto remains immediately free.
live_asset_last_attempt_ms = defaultdict(int)
live_candidate_versions = {}
live_v247_counts = defaultdict(int)
live_v247_lock = threading.Lock()
prevalidation_retry_after = {}
bot_shadow_schedule_q = queue.Queue(maxsize=1000)
live_runtime = {
    "mode": TRADING_MODE, "api_ok": False, "account_ts_ms": None, "balances": {},
    "circuit_open": False, "circuit_reason": None, "last_error": None, "busy": False,
    "queued": 0, "skipped": 0, "last_skip_reason": None,
    "attempts": 0, "test_validations": 0, "capability_reuses": 0, "completed": 0,
    "wins": 0, "losses": 0, "realized_pnl": 0.0, "forced_unwinds": 0,
    "open_exposures": 0, "open_exposure_value_usd": 0.0,
    "open_exposure_unrealized_pnl": 0.0, "open_exposure_economic_pnl": 0.0,
    "circuit_causes": [],
    "consecutive_unwinds": 0, "last_attempt_id": None, "last_route": None,
    "last_status": None, "last_leg1_ms": None, "last_leg2_ms": None,
    "last_t0_to_leg1_submit_ms": None, "last_t0_to_leg1_done_ms": None,
    "last_leg1_done_to_leg2_submit_ms": None, "last_t0_to_done_ms": None,
    "last_admission_to_leg1_post_ms": None,
    "last_leg1_post_http_ms": None, "last_leg2_post_http_ms": None,
    "last_leg1_http_queue_ms": None, "last_leg2_http_queue_ms": None,
    "last_leg1_confirm_ms": None, "last_leg2_confirm_ms": None,
    "last_leg1_prepare_journal_ms": None, "last_leg2_prepare_journal_ms": None,
    "last_route_exchange_age_t0_ms": None, "last_route_exchange_skew_t0_ms": None,
    "last_route_exchange_age_ms": None, "last_route_exchange_skew_ms": None,
    "last_leg2_exchange_age_ms": None, "last_leg2_exchange_skew_ms": None,
    "last_first_check_delay_ms": None, "last_gateway_admit_delay_ms": None,
    "bot_shadow_admissions": 0, "bot_shadow_schedule_drops": 0,
    "last_keepalive_rtt_ms": None, "last_keepalive_ts_ms": None,
    "last_status_keepalive_rtt_ms": None,
    "keepalive_ok": 0, "keepalive_errors": 0, "last_keepalive_error": None,
    "trade_epoch": 0,
}
live_client = None
live_order_status_client = None
live_account_client = None
live_prevalidation_client = None
live_user_stream_client = None
live_route_capabilities = {}
live_account_refresh_event = threading.Event()
live_gateway_first_check_ms = deque(maxlen=10000)
live_gateway_admit_ms = deque(maxlen=10000)
live_gateway_check_us = deque(maxlen=10000)
live_admission_to_post_ms = deque(maxlen=10000)
live_leg1_post_http_ms = deque(maxlen=10000)
live_leg2_post_http_ms = deque(maxlen=10000)
live_leg1_http_queue_ms = deque(maxlen=10000)
live_leg2_http_queue_ms = deque(maxlen=10000)
live_leg1_confirm_ms = deque(maxlen=10000)
live_leg2_confirm_ms = deque(maxlen=10000)
live_prepare_journal_ms = deque(maxlen=10000)
live_response_journal_ms = deque(maxlen=10000)
bot_shadow_schedule_lag_ms = deque(maxlen=10000)
private_ws_post_to_event_ms = deque(maxlen=10000)
private_ws_rest_delta_ms = deque(maxlen=10000)
private_ws_lock = threading.RLock()
private_ws_waiters = {}
private_ws_order_aliases = {}
private_ws_event_cache = {}
private_ws_submission_meta = {}
private_ws_journal_q = queue.Queue(maxsize=1000)
private_ws_runtime = {
    "mode": LIVE_PRIVATE_WS_MODE,
    "connected": False, "subscribed": False,
    "opens": 0, "disconnects": 0, "reconnects": 0,
    "messages": 0, "order_events": 0, "terminal_events": 0,
    "matched_events": 0, "unmatched_events": 0, "decode_errors": 0,
    "pings": 0, "pongs": 0,
    "listenkey_creates": 0, "listenkey_keepalive_ok": 0,
    "listenkey_keepalive_errors": 0,
    "ws_before_rest": 0, "rest_before_ws": 0, "same_ms": 0,
    "confirmations_ws": 0, "confirmations_rest": 0, "confirmations_post": 0,
    "journal_drops": 0,
    "last_open_ms": None, "last_close_ms": None,
    "last_message_ms": None, "last_order_event_ms": None,
    "last_pong_ms": None, "listenkey_created_ms": None,
    "last_listenkey_keepalive_ms": None, "last_error": None,
}
prevalidation_runtime = {
    "enabled": LIVE_PREVALIDATE_ENABLED,
    "running": False,
    "checked": 0,
    "validated": 0,
    "skipped_depth": 0,
    "skipped_size": 0,
    "errors": 0,
    "last_route": None,
    "last_status": None,
    "last_error": None,
    "last_latency_ms": None,
    "cycle_started_ts_ms": None,
    "cycle_completed_ts_ms": None,
}


def now_ms(): return int(time.time()*1000)

def mexc_now_ms():
    """Current time expressed on MEXC's clock when the private sync is available."""
    try: offset=int(getattr(live_client,"time_offset_ms",0) or 0)
    except (TypeError,ValueError): offset=0
    return now_ms()+offset

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
    con.execute("PRAGMA wal_autocheckpoint=2000")
    con.execute("PRAGMA journal_size_limit=67108864")
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

    CREATE TABLE IF NOT EXISTS shadow_skips_v239(
      profile TEXT NOT NULL, day TEXT NOT NULL, ts_ms INTEGER NOT NULL,
      decision_id TEXT NOT NULL, route_id TEXT NOT NULL, stage TEXT NOT NULL,
      reason TEXT NOT NULL, scheduled_ts_ms INTEGER NOT NULL, book_age_ms INTEGER,
      requested_usd REAL NOT NULL, executable_usd REAL,
      PRIMARY KEY(profile,decision_id,stage)
    );
    CREATE INDEX IF NOT EXISTS idx_shadow_skip_v239_day ON shadow_skips_v239(profile,day,ts_ms,reason);

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

    CREATE TABLE IF NOT EXISTS shadow_admissions_v236(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL,
      event_start_ts_ms INTEGER NOT NULL, ts_ms INTEGER NOT NULL, grade TEXT NOT NULL,
      selected_usd REAL NOT NULL, disposition TEXT NOT NULL, profiles_reserved INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_shadow_admission_v236_ts ON shadow_admissions_v236(day,ts_ms,disposition);

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
    CREATE INDEX IF NOT EXISTS idx_dec_v247_ts ON decisions_v22(ts_ms);
    CREATE TABLE IF NOT EXISTS diagnostics_v247(
      ts_ms INTEGER PRIMARY KEY, started_ms INTEGER NOT NULL, payload_json TEXT NOT NULL
    );

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

    CREATE TABLE IF NOT EXISTS live_state_v240(
      singleton INTEGER PRIMARY KEY CHECK(singleton=1), updated_ts_ms INTEGER NOT NULL,
      mode TEXT NOT NULL, api_ok INTEGER NOT NULL, balances_json TEXT NOT NULL,
      circuit_open INTEGER NOT NULL, circuit_reason TEXT, last_error TEXT,
      attempts INTEGER NOT NULL, test_validations INTEGER NOT NULL, completed INTEGER NOT NULL,
      wins INTEGER NOT NULL, losses INTEGER NOT NULL, realized_pnl REAL NOT NULL,
      forced_unwinds INTEGER NOT NULL, consecutive_unwinds INTEGER NOT NULL,
      last_attempt_id TEXT, last_route TEXT, last_status TEXT,
      last_leg1_ms INTEGER, last_leg2_ms INTEGER, account_ts_ms INTEGER
    );

    CREATE TABLE IF NOT EXISTS live_attempts_v240(
      attempt_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL UNIQUE, day TEXT NOT NULL,
      created_ts_ms INTEGER NOT NULL, updated_ts_ms INTEGER NOT NULL,
      route_id TEXT NOT NULL, path TEXT NOT NULL, grade TEXT NOT NULL, mode TEXT NOT NULL,
      status TEXT NOT NULL, requested_usd REAL NOT NULL, input_asset TEXT NOT NULL,
      mid_asset TEXT NOT NULL, output_asset TEXT NOT NULL, input_units REAL,
      mid_acquired REAL, mid_used REAL, output_units REAL, unwind_input REAL,
      unwind_output REAL, residual_mid REAL, realized_pnl REAL,
      cash_output_usd REAL, residual_mark_usd REAL, residual_value_usd REAL,
      economic_pnl REAL, realized_cost_usd REAL, residual_cost_usd REAL,
      unrealized_pnl REAL, unwind_error TEXT,
      leg1_client_id TEXT, leg2_client_id TEXT, unwind_client_id TEXT,
      leg1_latency_ms INTEGER, leg2_latency_ms INTEGER, error TEXT,
      decision_ts_ms INTEGER, event_start_ts_ms INTEGER, route_edge_t0 REAL,
      route_edge_submit REAL, t0_to_leg1_submit_ms INTEGER,
      t0_to_leg1_done_ms INTEGER, leg1_done_to_leg2_submit_ms INTEGER,
      t0_to_done_ms INTEGER, leg1_order_type TEXT, leg2_order_type TEXT,
      unwind_order_type TEXT, admission_to_leg1_post_ms INTEGER,
      leg1_post_http_ms REAL, leg2_post_http_ms REAL,
      leg1_http_queue_ms REAL, leg2_http_queue_ms REAL,
      leg1_confirm_ms INTEGER, leg2_confirm_ms INTEGER,
      leg1_prepare_journal_ms REAL, leg2_prepare_journal_ms REAL,
      leg2_guard_edge REAL,
      route_exchange_age_t0_ms INTEGER, route_exchange_skew_t0_ms INTEGER,
      leg2_exchange_age_ms INTEGER, leg2_exchange_skew_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_live_attempts_day ON live_attempts_v240(day,created_ts_ms);

    CREATE TABLE IF NOT EXISTS live_open_exposures_v246(
      attempt_id TEXT PRIMARY KEY, day TEXT NOT NULL, opened_ts_ms INTEGER NOT NULL,
      updated_ts_ms INTEGER NOT NULL, route_id TEXT NOT NULL, symbol TEXT NOT NULL,
      asset TEXT NOT NULL, start_asset TEXT NOT NULL, units REAL NOT NULL,
      input_usd REAL NOT NULL, cash_output_usd REAL NOT NULL, residual_cost_usd REAL NOT NULL,
      last_mark_usd REAL, last_value_usd REAL, economic_pnl REAL,
      status TEXT NOT NULL DEFAULT 'open', resolution_note TEXT, closed_ts_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_live_open_exposures_v246_status
      ON live_open_exposures_v246(status,day,opened_ts_ms);

    CREATE TABLE IF NOT EXISTS live_circuit_events_v246(
      event_id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
      day TEXT NOT NULL, attempt_id TEXT, reason TEXT NOT NULL, error TEXT,
      active INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_live_circuit_events_v246_active
      ON live_circuit_events_v246(active,ts_ms);

    CREATE TABLE IF NOT EXISTS live_orders_v240(
      client_order_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, leg INTEGER NOT NULL,
      purpose TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
      order_type TEXT NOT NULL, quantity TEXT, quote_order_qty TEXT,
      price TEXT,
      status TEXT NOT NULL, exchange_order_id TEXT, executed_qty REAL,
      cumulative_quote_qty REAL, commissions_json TEXT NOT NULL DEFAULT '{}',
      prepared_ts_ms INTEGER NOT NULL, submitted_ts_ms INTEGER, final_ts_ms INTEGER,
      response_json TEXT, error TEXT, request_started_ts_ms INTEGER,
      response_received_ts_ms INTEGER, post_http_ms REAL,
      http_queue_ms REAL, response_journal_ms REAL, settle_ms INTEGER, fallback_index INTEGER,
      confirmation_source TEXT, rest_poll_count INTEGER,
      ws_received_ts_ms INTEGER, ws_send_ts_ms INTEGER,
      ws_post_ms REAL, ws_rest_delta_ms REAL
    );
    CREATE INDEX IF NOT EXISTS idx_live_orders_attempt ON live_orders_v240(attempt_id,leg);

    CREATE TABLE IF NOT EXISTS live_route_capabilities_v241(
      route_id TEXT PRIMARY KEY, validated_ts_ms INTEGER NOT NULL,
      expires_ts_ms INTEGER NOT NULL, leg1_json TEXT NOT NULL,
      leg2_json TEXT NOT NULL, unwind_json TEXT NOT NULL,
      last_decision_id TEXT NOT NULL, last_error TEXT
    );

    CREATE TABLE IF NOT EXISTS live_admissions_v241(
      decision_id TEXT PRIMARY KEY, day TEXT NOT NULL, route_id TEXT NOT NULL,
      event_start_ts_ms INTEGER NOT NULL, decision_ts_ms INTEGER NOT NULL,
      updated_ts_ms INTEGER NOT NULL, mode TEXT NOT NULL, grade TEXT NOT NULL,
      selected_usd REAL NOT NULL, requested_usd REAL, edge_t0 REAL,
      disposition TEXT NOT NULL, reason TEXT, attempt_id TEXT,
      retry_count INTEGER NOT NULL DEFAULT 0, route_tested INTEGER NOT NULL DEFAULT 0,
      first_check_ts_ms INTEGER, first_check_delay_ms INTEGER, last_check_ts_ms INTEGER,
      gateway_admit_ts_ms INTEGER, gateway_delay_ms INTEGER,
      fresh_grade TEXT, fresh_selected_usd REAL, fresh_edge REAL,
      route_depth_age_ms INTEGER, route_depth_skew_ms INTEGER, gateway_check_us REAL,
      route_exchange_age_ms INTEGER, route_exchange_skew_ms INTEGER,
      retry_reasons_json TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_live_admissions_v241_day
      ON live_admissions_v241(day,decision_ts_ms,disposition);

    CREATE TABLE IF NOT EXISTS private_order_ws_events_v245(
      event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      received_ts_ms INTEGER NOT NULL, send_ts_ms INTEGER, create_ts_ms INTEGER,
      channel TEXT NOT NULL, symbol TEXT, exchange_order_id TEXT,
      client_order_id TEXT, status_code INTEGER, status TEXT,
      executed_qty REAL, cumulative_quote_qty REAL,
      post_started_ts_ms INTEGER, post_to_ws_ms REAL,
      rest_confirmed_ts_ms INTEGER, ws_rest_delta_ms REAL,
      matched_bot_order INTEGER NOT NULL DEFAULT 0, raw_json TEXT NOT NULL,
      UNIQUE(client_order_id,send_ts_ms,status_code,executed_qty,cumulative_quote_qty)
    );
    CREATE INDEX IF NOT EXISTS idx_private_order_ws_v245_client
      ON private_order_ws_events_v245(client_order_id,received_ts_ms);

    CREATE TABLE IF NOT EXISTS route_coverage_v244(
      day TEXT NOT NULL, route_id TEXT NOT NULL, path TEXT NOT NULL,
      first_ts_ms INTEGER, last_ts_ms INTEGER,
      evaluations INTEGER NOT NULL DEFAULT 0,
      calculable INTEGER NOT NULL DEFAULT 0,
      research_fresh INTEGER NOT NULL DEFAULT 0,
      live_fresh INTEGER NOT NULL DEFAULT 0,
      positive INTEGER NOT NULL DEFAULT 0,
      edge_ge_030 INTEGER NOT NULL DEFAULT 0,
      edge_ge_035 INTEGER NOT NULL DEFAULT 0,
      quality_a INTEGER NOT NULL DEFAULT 0,
      quality_bplus INTEGER NOT NULL DEFAULT 0,
      quality_eligible INTEGER NOT NULL DEFAULT 0,
      validated_seen INTEGER NOT NULL DEFAULT 0,
      bot_admitted INTEGER NOT NULL DEFAULT 0,
      max_edge REAL,
      PRIMARY KEY(day,route_id)
    );
    CREATE INDEX IF NOT EXISTS idx_route_coverage_v244_day
      ON route_coverage_v244(day,last_ts_ms);
    """)
    coverage_existing={r[1] for r in con.execute("PRAGMA table_info(route_coverage_v244)")}
    if "quality_eligible" not in coverage_existing:
        con.execute("ALTER TABLE route_coverage_v244 ADD COLUMN quality_eligible INTEGER NOT NULL DEFAULT 0")
    # Safe migration when an operator deliberately reuses a V2.4.0 database.
    existing={r[1] for r in con.execute("PRAGMA table_info(live_attempts_v240)")}
    for name,decl in (
        ("decision_ts_ms","INTEGER"),("event_start_ts_ms","INTEGER"),("route_edge_t0","REAL"),
        ("route_edge_submit","REAL"),("t0_to_leg1_submit_ms","INTEGER"),
        ("t0_to_leg1_done_ms","INTEGER"),("leg1_done_to_leg2_submit_ms","INTEGER"),
        ("t0_to_done_ms","INTEGER"),("leg1_order_type","TEXT"),("leg2_order_type","TEXT"),
        ("unwind_order_type","TEXT"),("admission_to_leg1_post_ms","INTEGER"),
        ("leg1_post_http_ms","REAL"),("leg2_post_http_ms","REAL"),
        ("leg1_http_queue_ms","REAL"),("leg2_http_queue_ms","REAL"),
        ("leg1_confirm_ms","INTEGER"),("leg2_confirm_ms","INTEGER"),
        ("leg1_prepare_journal_ms","REAL"),("leg2_prepare_journal_ms","REAL"),
        ("leg2_guard_edge","REAL"),("cash_output_usd","REAL"),
        ("residual_mark_usd","REAL"),("residual_value_usd","REAL"),
        ("economic_pnl","REAL"),("realized_cost_usd","REAL"),
        ("residual_cost_usd","REAL"),("unrealized_pnl","REAL"),("unwind_error","TEXT"),
        ("route_exchange_age_t0_ms","INTEGER"),("route_exchange_skew_t0_ms","INTEGER"),
        ("leg2_exchange_age_ms","INTEGER"),("leg2_exchange_skew_ms","INTEGER")):
        if name not in existing: con.execute(f"ALTER TABLE live_attempts_v240 ADD COLUMN {name} {decl}")
    order_existing={r[1] for r in con.execute("PRAGMA table_info(live_orders_v240)")}
    for name,decl in (("price","TEXT"),("request_started_ts_ms","INTEGER"),
        ("response_received_ts_ms","INTEGER"),("post_http_ms","REAL"),
        ("http_queue_ms","REAL"),("response_journal_ms","REAL"),("settle_ms","INTEGER"),
        ("fallback_index","INTEGER"),("confirmation_source","TEXT"),
        ("rest_poll_count","INTEGER"),("ws_received_ts_ms","INTEGER"),
        ("ws_send_ts_ms","INTEGER"),("ws_post_ms","REAL"),
        ("ws_rest_delta_ms","REAL")):
        if name not in order_existing: con.execute(f"ALTER TABLE live_orders_v240 ADD COLUMN {name} {decl}")
    admission_existing={r[1] for r in con.execute("PRAGMA table_info(live_admissions_v241)")}
    for name,decl in (
        ("first_check_ts_ms","INTEGER"),("first_check_delay_ms","INTEGER"),("last_check_ts_ms","INTEGER"),
        ("gateway_admit_ts_ms","INTEGER"),("gateway_delay_ms","INTEGER"),("fresh_grade","TEXT"),
        ("fresh_selected_usd","REAL"),("fresh_edge","REAL"),("route_depth_age_ms","INTEGER"),
        ("route_depth_skew_ms","INTEGER"),("gateway_check_us","REAL"),
        ("route_exchange_age_ms","INTEGER"),("route_exchange_skew_ms","INTEGER"),
        ("retry_reasons_json","TEXT")):
        if name not in admission_existing: con.execute(f"ALTER TABLE live_admissions_v241 ADD COLUMN {name} {decl}")
    con.commit()
    db_ready.set()
    pending_commit=0; last_commit=time.monotonic()
    while True:
        try:
            item = dbq.get(timeout=DB_COMMIT_MAX_MS/1000.0)
        except queue.Empty:
            if pending_commit:
                con.commit(); pending_commit=0; last_commit=time.monotonic()
            continue
        if item is None:
            if pending_commit: con.commit()
            dbq.task_done(); break
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
            elif typ == "diagnostics_v247":
                con.execute("INSERT OR REPLACE INTO diagnostics_v247 VALUES(?,?,?)",payload)
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
            elif typ == "shadow_skip_v239":
                con.execute("""INSERT OR REPLACE INTO shadow_skips_v239(profile,day,ts_ms,decision_id,route_id,stage,reason,scheduled_ts_ms,book_age_ms,requested_usd,executable_usd) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", payload)
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
            elif typ == "shadow_admission_v236":
                con.execute("""INSERT OR REPLACE INTO shadow_admissions_v236(decision_id,day,route_id,event_start_ts_ms,ts_ms,grade,selected_usd,disposition,profiles_reserved) VALUES(?,?,?,?,?,?,?,?,?)""", payload)
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
            elif typ == "private_order_ws_v245":
                con.execute("""INSERT OR IGNORE INTO private_order_ws_events_v245(
                    received_ts_ms,send_ts_ms,create_ts_ms,channel,symbol,exchange_order_id,
                    client_order_id,status_code,status,executed_qty,cumulative_quote_qty,
                    post_started_ts_ms,post_to_ws_ms,rest_confirmed_ts_ms,ws_rest_delta_ms,
                    matched_bot_order,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",payload)
            elif typ == "private_order_ws_rest_v245":
                con.execute("""UPDATE private_order_ws_events_v245
                    SET rest_confirmed_ts_ms=?,ws_rest_delta_ms=?
                    WHERE client_order_id=?""",payload)
            elif typ == "depth_sync":
                con.execute("INSERT INTO depth_sync_events(ts_ms,symbol,event,reason,duration_ms,attempt) VALUES(?,?,?,?,?,?)", payload)
            elif typ == "trial":
                con.execute("""INSERT INTO execution_trials(day,decision_id,route_id,decision_ts_ms,exec_ts_ms,bbo_age_limit_ms,latency_ms,
                    decision_net,decision_size_usd,decision_max_bbo_age_ms,decision_bbo_skew_ms,exec_net,exec_size_usd,exec_max_bbo_age_ms,
                    exec_bbo_skew_ms,pnl_usd,status) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", payload)
            elif typ == "live_admission_v242":
                con.execute("""INSERT INTO live_admissions_v241(
                    decision_id,day,route_id,event_start_ts_ms,decision_ts_ms,updated_ts_ms,mode,grade,
                    selected_usd,requested_usd,edge_t0,disposition,reason,attempt_id,retry_count,route_tested,
                    first_check_ts_ms,first_check_delay_ms,last_check_ts_ms,gateway_admit_ts_ms,gateway_delay_ms,
                    fresh_grade,fresh_selected_usd,fresh_edge,route_depth_age_ms,route_depth_skew_ms,gateway_check_us,
                    route_exchange_age_ms,route_exchange_skew_ms,retry_reasons_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(decision_id) DO UPDATE SET
                    updated_ts_ms=excluded.updated_ts_ms,
                    requested_usd=COALESCE(excluded.requested_usd,live_admissions_v241.requested_usd),
                    disposition=excluded.disposition,reason=excluded.reason,
                    attempt_id=COALESCE(excluded.attempt_id,live_admissions_v241.attempt_id),
                    retry_count=excluded.retry_count,route_tested=excluded.route_tested,
                    first_check_ts_ms=COALESCE(excluded.first_check_ts_ms,live_admissions_v241.first_check_ts_ms),
                    first_check_delay_ms=COALESCE(excluded.first_check_delay_ms,live_admissions_v241.first_check_delay_ms),
                    last_check_ts_ms=COALESCE(excluded.last_check_ts_ms,live_admissions_v241.last_check_ts_ms),
                    gateway_admit_ts_ms=COALESCE(excluded.gateway_admit_ts_ms,live_admissions_v241.gateway_admit_ts_ms),
                    gateway_delay_ms=COALESCE(excluded.gateway_delay_ms,live_admissions_v241.gateway_delay_ms),
                    fresh_grade=COALESCE(excluded.fresh_grade,live_admissions_v241.fresh_grade),
                    fresh_selected_usd=COALESCE(excluded.fresh_selected_usd,live_admissions_v241.fresh_selected_usd),
                    fresh_edge=COALESCE(excluded.fresh_edge,live_admissions_v241.fresh_edge),
                    route_depth_age_ms=COALESCE(excluded.route_depth_age_ms,live_admissions_v241.route_depth_age_ms),
                    route_depth_skew_ms=COALESCE(excluded.route_depth_skew_ms,live_admissions_v241.route_depth_skew_ms),
                    gateway_check_us=COALESCE(excluded.gateway_check_us,live_admissions_v241.gateway_check_us),
                    route_exchange_age_ms=COALESCE(excluded.route_exchange_age_ms,live_admissions_v241.route_exchange_age_ms),
                    route_exchange_skew_ms=COALESCE(excluded.route_exchange_skew_ms,live_admissions_v241.route_exchange_skew_ms),
                    retry_reasons_json=COALESCE(excluded.retry_reasons_json,live_admissions_v241.retry_reasons_json)""", payload)
            elif typ == "route_coverage_many_v244":
                con.executemany("""INSERT INTO route_coverage_v244(
                    day,route_id,path,first_ts_ms,last_ts_ms,evaluations,calculable,research_fresh,
                    live_fresh,positive,edge_ge_030,edge_ge_035,quality_a,quality_bplus,
                    quality_eligible,validated_seen,bot_admitted,max_edge)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(day,route_id) DO UPDATE SET
                    path=excluded.path,first_ts_ms=excluded.first_ts_ms,last_ts_ms=excluded.last_ts_ms,
                    evaluations=excluded.evaluations,calculable=excluded.calculable,
                    research_fresh=excluded.research_fresh,live_fresh=excluded.live_fresh,
                    positive=excluded.positive,edge_ge_030=excluded.edge_ge_030,
                    edge_ge_035=excluded.edge_ge_035,quality_a=excluded.quality_a,
                    quality_bplus=excluded.quality_bplus,quality_eligible=excluded.quality_eligible,
                    validated_seen=excluded.validated_seen,
                    bot_admitted=excluded.bot_admitted,max_edge=excluded.max_edge""", payload)
            pending_commit+=1
            if pending_commit>=DB_COMMIT_BATCH or (time.monotonic()-last_commit)*1000>=DB_COMMIT_MAX_MS:
                con.commit(); pending_commit=0; last_commit=time.monotonic()
        except Exception as e:
            logerr(f"DB: {e}")
        finally:
            dbq.task_done()
    con.commit(); con.close()


def put_db(item):
    try: dbq.put_nowait(item)
    except queue.Full:
        with state_lock: state["db_queue_drops"]+=1
        logerr("DB queue full")


# ---------- Fixed-size route coverage audit (V2.4.4) ----------
_ROUTE_COVERAGE_COUNTERS = (
    "evaluations","calculable","research_fresh","live_fresh","positive",
    "edge_ge_030","edge_ge_035","quality_a","quality_bplus",
    "quality_eligible","validated_seen","bot_admitted",
)


def _empty_route_coverage(route, day):
    row={"day":day,"route_id":route["id"],"path":">".join(route["path"]),
         "first_ts_ms":None,"last_ts_ms":None,"max_edge":None}
    row.update({name:0 for name in _ROUTE_COVERAGE_COUNTERS})
    return row


def initialize_route_coverage(routes):
    """Restore today's 800 aggregate rows; no raw per-tick records are created."""
    global route_coverage_day
    _,_,day=local_day_bounds_ms(0)
    restored={}
    try:
        con=sqlite3.connect(DB_PATH,timeout=10); con.row_factory=sqlite3.Row
        for row in con.execute("SELECT * FROM route_coverage_v244 WHERE day=?",(day,)):
            restored[row["route_id"]]=dict(row)
        con.close()
    except Exception as exc:
        logerr(f"route coverage restore: {exc}")
    route_coverage.clear()
    for route in routes:
        route_coverage[route["id"]]=restored.get(route["id"],_empty_route_coverage(route,day))
    route_coverage_day=day


def _coverage_observe(route, x, ts):
    """Constant-time RAM counters only; called by scan workers."""
    row=route_coverage.get(route["id"])
    if row is None: return
    row["evaluations"]+=1
    row["first_ts_ms"]=row["first_ts_ms"] or ts; row["last_ts_ms"]=ts
    if not x: return
    row["calculable"]+=1
    net=float(x.get("net") or 0.0)
    old=row.get("max_edge")
    if old is None or net>old: row["max_edge"]=net
    research_fresh=(x.get("max_age",999999)<=SHADOW_BBO_AGE_MS and
                    x.get("bbo_skew",999999)<=SHADOW_MAX_SKEW_MS and _research_exchange_reason(x) is None)
    live_fresh=(x.get("max_age",999999)<=LIVE_BBO_AGE_MS and
                x.get("bbo_skew",999999)<=LIVE_MAX_SKEW_MS and
                bool(x.get("exchange_ts_complete")) and
                x.get("exchange_max_age",999999)<=LIVE_EXCHANGE_AGE_MS and
                x.get("exchange_skew",999999)<=LIVE_MAX_EXCHANGE_SKEW_MS)
    if research_fresh:
        row["research_fresh"]+=1
        if net>0: row["positive"]+=1
        if net>=REPLAY_MIN_EDGE: row["edge_ge_030"]+=1
        if net>=SHADOW_MIN_EDGE: row["edge_ge_035"]+=1
    if live_fresh: row["live_fresh"]+=1


def _coverage_mark_quality(route_id, quality):
    row=route_coverage.get(route_id)
    if row is None or not quality or not quality.get("eligible"): return
    row["quality_eligible"]+=1
    if quality.get("grade")=="A": row["quality_a"]+=1
    elif quality.get("grade")=="B+": row["quality_bplus"]+=1


def _coverage_mark_bot(route_id):
    row=route_coverage.get(route_id)
    if row is not None: row["bot_admitted"]+=1


def _route_coverage_rows():
    ts=now_ms()
    with live_lock:
        valid={rid for rid,item in live_route_capabilities.items()
               if int(item.get("expires_ts_ms") or 0)>=ts}
    rows=[]
    for rid,row in list(route_coverage.items()):
        if rid in valid: row["validated_seen"]=1
        rows.append((row["day"],rid,row["path"],row.get("first_ts_ms"),row.get("last_ts_ms"),
            *[int(row.get(name,0)) for name in _ROUTE_COVERAGE_COUNTERS],row.get("max_edge")))
    return rows


def route_coverage_flush_worker():
    global route_coverage_day
    while True:
        time.sleep(ROUTE_COVERAGE_FLUSH_SEC)
        if not route_coverage_flush_lock.acquire(blocking=False): continue
        try:
            rows=_route_coverage_rows()
            if rows: put_db(("route_coverage_many_v244",rows))
            with live_v247_lock: counts=dict(live_v247_counts)
            # One diagnostic checkpoint/minute, never a row for each WS tick.
            put_db(("meta",("v247_diagnostics",json.dumps({"ts_ms":now_ms(),"counts":counts,
                "version":VERSION,"price_protected":LIVE_PRICE_PROTECTED},sort_keys=True))))
            with state_lock:
                symbols=list(state["symbols"]); started_ms=state["started_ms"]
                transport={"scan_updates":state["scan_updates"],"scan_drops":state["scan_queue_drops"],
                    "db_drops":state["db_queue_drops"],"disconnects":state["ws_disconnects"]}
            with depth_lock:
                freshness={sym:{"ready":ob.get("ready"),"version":ob.get("version"),
                    "received_ts_ms":ob.get("ts"),"send_ts_ms":ob.get("send_ts")}
                    for sym in symbols if (ob:=state["depth"].get(sym))}
            put_db(("diagnostics_v247",(now_ms(),started_ms,json.dumps({"version":VERSION,
                "counts":counts,"transport":transport,"books":freshness,
                "scan_queue":scanq.qsize(),"db_queue":dbq.qsize(),"public_flow":public_flow_status()},sort_keys=True))))
            _,_,today=local_day_bounds_ms(0)
            if route_coverage_day!=today:
                with state_lock: routes=list(state.get("routes") or [])
                initialize_route_coverage(routes)
        finally:
            route_coverage_flush_lock.release()


def route_coverage_status():
    rows=list(route_coverage.values()); total=len(rows)
    def routes_with(field): return sum(1 for row in rows if int(row.get(field,0))>0)
    def ticks(field): return sum(int(row.get(field,0)) for row in rows)
    with live_lock:
        valid=sum(1 for item in live_route_capabilities.values()
                  if int(item.get("expires_ts_ms") or 0)>=now_ms())
    return {
        "day":route_coverage_day,"total_routes":total,
        "route_counts":{field:routes_with(field) for field in _ROUTE_COVERAGE_COUNTERS},
        "tick_counts":{field:ticks(field) for field in _ROUTE_COVERAGE_COUNTERS},
        "validated_now":valid,"flush_sec":ROUTE_COVERAGE_FLUSH_SEC,
        "storage_rows_per_day":total,
    }

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
                if base and quote and sym and base!=quote:
                    # Keep the official trading constraints for the private gateway.
                    # Shadow math only needs symbol/base/quote and remains unchanged.
                    out.append({"symbol":sym,"base":base,"quote":quote,
                        "status":status,"apiRulesAvailable":True,
                        "baseAssetPrecision":s.get("baseAssetPrecision"),
                        "quoteAssetPrecision":s.get("quoteAssetPrecision"),
                        "quotePrecision":s.get("quotePrecision"),
                        "quoteAmountPrecision":s.get("quoteAmountPrecision"),
                        "baseSizePrecision":s.get("baseSizePrecision"),
                        "orderTypes":s.get("orderTypes") or [],
                        "quoteOrderQtyMarketAllowed":s.get("quoteOrderQtyMarketAllowed"),
                        "isSpotTradingAllowed":s.get("isSpotTradingAllowed"),
                        "permissions":s.get("permissions") or [],
                        "maxQuoteAmount":s.get("maxQuoteAmount"),
                        "quoteAmountPrecisionMarket":s.get("quoteAmountPrecisionMarket"),
                        "maxQuoteAmountMarket":s.get("maxQuoteAmountMarket"),
                        "makerCommission":s.get("makerCommission"),
                        "takerCommission":s.get("takerCommission"),
                        "tradeSideType":s.get("tradeSideType")})
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
            if base and quote and base!=quote:
                out.append({"symbol":base+quote,"base":base,"quote":quote,"status":status,"apiRulesAvailable":False})
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
    # This scanner is deliberately 2-leg only. 3-leg candidates are counted for diagnostics
    # but never subscribed/scanned; they will live in a separate scanner later.
    direct_stable=[m["symbol"] for m in bysym.values() if m["base"] in STABLES and m["quote"] in STABLES]
    if FOLLOW_ALL_2LEG:
        # V2.3.6: subscribe every symbol needed by every 2-leg route. Keep 3-leg
        # routes diagnostic-only, as agreed, and keep conservative WS groups of 20.
        selected=sorted(r2,key=lambda r:r["id"])
        symbols=set(direct_stable)
        for r in selected: symbols.update(r["symbols"])
    else:
        selected=[]; symbols=set(direct_stable[:MAX_WS_SYMBOLS])
        remaining=list(r2)
        while remaining:
            remaining.sort(key=lambda r:(len(set(r["symbols"])-symbols), r["id"]))
            r=remaining.pop(0); new=set(r["symbols"])-symbols
            if len(symbols)+len(new)>MAX_WS_SYMBOLS: continue
            selected.append(r); symbols.update(r["symbols"])
    chosen=[bysym[x] for x in sorted(symbols) if x in bysym]
    diag={"all_markets":len(bysym),"candidate_routes":len(candidates),"candidate_2leg":len(r2),
          "candidate_3leg":len(r3),"selected_symbols":len(chosen),"selected_routes":len(selected),
          "selected_2leg":len(selected),"selected_3leg":0,"dropped_2leg":len(r2)-len(selected),
          "dropped_routes":len(candidates)-len(selected),"direct_stable_symbols":len(direct_stable),
          "follow_all_2leg":FOLLOW_ALL_2LEG,"ws_group_size":WS_GROUP_SIZE,"mode":"2-leg only"}
    return chosen,selected,bysym,diag

def fetch_ws_activity_weights(symbols):
    """Approximate per-symbol stream load from MEXC rolling 24h quote volume."""
    try:
        headers={"User-Agent":"Mozilla/5.0 mexc-routes-scanner/2.4.6a", "Accept":"application/json"}
        r=requests.get(REST+"/api/v3/ticker/24hr",headers=headers,timeout=25); r.raise_for_status()
        rows=r.json()
        if not isinstance(rows,list): return {},"ticker_unavailable"
        wanted=set(symbols); weights={}
        for x in rows:
            sym=str(x.get("symbol","")).upper()
            if sym not in wanted: continue
            try: weights[sym]=max(0.0,float(x.get("quoteVolume") or 0.0))
            except (TypeError,ValueError): weights[sym]=0.0
        return weights,"mexc_quote_volume_24h"
    except Exception as e:
        logerr(f"24h WS load weights: {e}")
        return {},"round_robin_fallback"

def build_balanced_ws_groups(symbols):
    """Keep <= WS_GROUP_SIZE symbols/socket while spreading the busiest feeds."""
    symbols=sorted(set(symbols))
    if not symbols: return [],[],"empty"
    count=max(1,math.ceil(len(symbols)/WS_GROUP_SIZE))
    weights,mode=fetch_ws_activity_weights(symbols)
    groups=[[] for _ in range(count)]; loads=[0.0 for _ in range(count)]
    # With no ticker data, the same greedy loop becomes a deterministic round-robin.
    ordered=sorted(symbols,key=lambda s:(-weights.get(s,0.0),s)) if weights else symbols
    for sym in ordered:
        available=[i for i,g in enumerate(groups) if len(g)<WS_GROUP_SIZE]
        i=min(available,key=lambda j:(loads[j],len(groups[j]),j)) if weights else min(available,key=lambda j:(len(groups[j]),j))
        groups[i].append(sym); loads[i]+=weights.get(sym,0.0)
    for g in groups: g.sort()
    return groups,loads,mode

# ---------- V2.4.8 bounded public reception ----------
# Feed recovery thresholds are NOT admission thresholds. The 50/100/50 ms
# private gates below remain unchanged. No individual Depth delta is skipped
# while continuing the same book: hard overflow invalidates the connection.
# Local age alone pauses admission and drains ALL deltas without reconnecting.
PUBLIC_RX_QUEUE_MAX = 1024
PUBLIC_RX_QUEUE_BYTES = 4 * 1024 * 1024
PUBLIC_RX_MAX_WAIT_MS = 100
PUBLIC_RX_RESUME_MS = 25
PUBLIC_RX_STALL_MS = 5000
PUBLIC_LAG_RECOVERY_MS = 1000
PUBLIC_LAG_PERSIST_MS = 200
PUBLIC_BATCH_MAX = 32
PUBLIC_DISPATCH_MODE = 'single_fair_consumer'
public_work_available = threading.Event()
public_dispatch_metrics = {'rounds':0,'processed':0,'max_round_ms':0}
public_flow_totals = {'catchup_started':0,'catchup_completed':0,'hard_recoveries':0}
public_feeds = {}
public_symbol_feeds = {}
public_recovery_events = deque(maxlen=200)
public_recovery_lock = threading.Lock()
public_catchup_events = deque(maxlen=200)
public_reconnect_lock = threading.Lock()
public_next_reconnect_mono = 0.0


class PublicInbox(queue.Queue):
    """Bound both packet count and queued raw bytes, with atomic accounting."""
    def __init__(self):
        super().__init__(PUBLIC_RX_QUEUE_MAX)
        self.byte_count = 0

    def put_nowait(self, item):
        with self.not_full:
            if self._qsize() >= self.maxsize or self.byte_count + len(item[0]) > PUBLIC_RX_QUEUE_BYTES:
                raise queue.Full
            self._put(item)
            self.byte_count += len(item[0])
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def _get(self):
        item = super()._get()
        self.byte_count -= len(item[0])
        return item


def _public_symbol_receiving(symbol):
    feed = public_symbol_feeds.get(symbol)
    return feed is None or (feed.active and not feed.halt.is_set())


def _public_symbol_usable(symbol):
    feed = public_symbol_feeds.get(symbol)
    return feed is None or feed.usable()


def _wait_public_reconnect_slot():
    """Spread actual reconnects; a local catch-up needs no reconnect at all."""
    global public_next_reconnect_mono
    with public_reconnect_lock:
        slot = max(time.monotonic(), public_next_reconnect_mono)
        public_next_reconnect_mono = slot + 0.35
    delay = slot - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def _public_epoch(symbol):
    feed = public_symbol_feeds.get(symbol)
    return feed.epoch if feed else 0


def _invalidate_public_books(symbols, source=None):
    with depth_lock:
        for sym in symbols:
            if source is not None and public_symbol_feeds.get(sym) is not source: continue
            ob = state['depth'].get(sym)
            if ob is not None:
                ob['ready'] = False
            depth_buffers.pop(sym, None)
        state['depth_ready'] = sum(1 for ob in state['depth'].values() if ob.get('ready'))
        with state_lock:
            for sym in symbols:
                if source is not None and public_symbol_feeds.get(sym) is not source: continue
                state['bbo'].pop(sym, None)


def _public_depth_header(payload):
    """Read envelope only; do not copy/decode every price in the receive thread.

    Official mexcdevelop/websocket-proto: symbol=3, sendTime=6,
    publicAggreDepths=313. Missing sendTime remains zero, never local time.
    """
    pos = 0; symbol = None; sent = 0; depth = False
    while pos < len(payload):
        key, pos = _pb_varint(payload, pos); field, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = _pb_varint(payload, pos)
            if field == 6: sent = value
        elif wire == 2:
            length, pos = _pb_varint(payload, pos); end = pos + length
            if end > len(payload): raise ValueError('truncated public envelope')
            if field == 3: symbol = payload[pos:end].decode('utf-8')
            if field == 313: depth = True
            pos = end
        elif wire in (1, 5):
            pos += 8 if wire == 1 else 4
            if pos > len(payload): raise ValueError('truncated fixed field')
        else: raise ValueError('invalid public envelope wire')
    return (symbol, int(sent)) if depth and symbol else None


class PublicFeed:
    """One bounded reader/consumer pair per socket, with one source epoch.

    The reader only parses the header and queues bytes. The consumer applies
    ALL contiguous deltas, then publishes the latest book once per tiny batch.
    Once failed, this feed cannot publish or admit a route, including while the
    close helper is waiting for a lock. A reconnect always starts a new epoch.
    """
    def __init__(self, symbols, worker_id, epoch):
        self.symbols = tuple(symbols); self.allowed = set(symbols)
        self.worker_id = worker_id; self.epoch = epoch
        self.inbox = PublicInbox()
        self.halt = threading.Event(); self.active = False
        self.catching_up = threading.Event()
        self.pressure_lock = threading.RLock()
        self.inflight_mono = None
        self.catchup_started_mono = None
        self.dirty_symbols = set()
        self.fail_lock = threading.Lock(); self.close_callback = None
        self.bad_by_symbol = {}; self.reason = None
        self.symbol_messages = defaultdict(int)
        self.threads = []
        self.metrics = {'messages': 0, 'bytes': 0, 'applied': 0, 'batches': 0,
                        'max_queue': 0, 'lag_ms': None, 'max_lag_ms': 0,
                        'max_queue_wait_ms': 0, 'max_decode_us': 0,
                        'max_apply_us': 0, 'max_callback_us': 0,
                        'lock_wait_us_max': 0, 'processed': 0,
                        'catchup_count': 0, 'catchup_completed': 0,
                        'max_catchup_ms': 0}
        self.started_mono = time.monotonic()
        self.last_progress_mono = self.started_mono

    def backlog_age_ms(self):
        with self.inbox.mutex:
            oldest = self.inbox.queue[0][2] if self.inbox.queue else None
        inflight = self.inflight_mono
        pending = [t for t in (oldest, inflight) if t is not None]
        return max(0.0, (time.monotonic() - min(pending)) * 1000) if pending else 0.0

    def note_catchup(self, waited):
        # No depth lock: block entry even if the consumer is waiting for it.
        with self.pressure_lock:
            if not self.catching_up.is_set():
                self.catchup_started_mono = time.monotonic()
                self.catching_up.set()
                self.metrics['catchup_count'] += 1
                with public_recovery_lock:
                    public_flow_totals['catchup_started'] += 1
                    public_catchup_events.append({'ts_ms': now_ms(), 'worker_id': self.worker_id,
                        'epoch': self.epoch, 'event': 'paused', 'queue_age_ms': round(waited, 2)})

    def check_pressure(self, allow_stall=False):
        waited = self.backlog_age_ms()
        self.metrics['max_queue_wait_ms'] = max(self.metrics['max_queue_wait_ms'], waited)
        if waited > PUBLIC_RX_MAX_WAIT_MS:
            self.note_catchup(waited)
        if (allow_stall and waited > PUBLIC_RX_STALL_MS and
                (time.monotonic() - self.last_progress_mono) * 1000 > PUBLIC_RX_STALL_MS):
            self.fail('consumer_stalled')
        return waited

    def usable(self):
        if not self.active or self.halt.is_set():
            return False
        self.check_pressure()
        return not self.catching_up.is_set() and not self.halt.is_set()

    def finish_batch(self):
        with self.pressure_lock:
            self.inflight_mono = None
            waited = self.check_pressure()
            if self.catching_up.is_set() and waited <= PUBLIC_RX_RESUME_MS and not self.halt.is_set():
                elapsed = (time.monotonic() - self.catchup_started_mono) * 1000
                self.metrics['max_catchup_ms'] = max(self.metrics['max_catchup_ms'], elapsed)
                self.metrics['catchup_completed'] += 1
                self.catching_up.clear()
                # Also publish books rebuilt while this connection was paused.
                self.dirty_symbols.update(self.symbols)
                with public_recovery_lock:
                    public_flow_totals['catchup_completed'] += 1
                    public_catchup_events.append({'ts_ms': now_ms(), 'worker_id': self.worker_id,
                        'epoch': self.epoch, 'event': 'resumed', 'duration_ms': round(elapsed, 2)})
        if self.usable():
            for symbol in tuple(self.dirty_symbols):
                if not self.usable():
                    break
                _publish_depth_bbo(symbol)
                self.dirty_symbols.discard(symbol)

    def fail(self, reason):
        with self.fail_lock:
            if self.halt.is_set(): return
            self.reason = reason; self.active = False; self.halt.set()
        with public_recovery_lock:
            public_flow_totals['hard_recoveries'] += 1
            public_recovery_events.append({"ts_ms":now_ms(),"worker_id":self.worker_id,"epoch":self.epoch,"reason":reason,"lag_ms":self.metrics["lag_ms"],"queue":self.inbox.qsize(),
                "queue_age_ms":round(self.backlog_age_ms(),2),
                "queue_bytes":getattr(self.inbox,'byte_count',None),
                "max_apply_us":self.metrics['max_apply_us'],
                "lock_wait_us_max":self.metrics['lock_wait_us_max']})
        # Keep the socket-reader callback short. Fail-closed visibility precedes
        # lock acquisition and actual closure, which happen in the helper.
        def finish():
            _invalidate_public_books(self.symbols, self)
            if self.close_callback:
                try: self.close_callback()
                except Exception: pass
        threading.Thread(target=finish, daemon=True).start()

    def stop(self):
        self.active = False; self.halt.set()
        _invalidate_public_books(self.symbols, self)

    def accept(self, payload, received_ms, received_mono):
        if not self.active or self.halt.is_set(): return False
        started = time.perf_counter_ns()
        try:
            header = _public_depth_header(payload)
            if header is None: return False
            symbol, sent = header
            if symbol not in self.allowed:
                self.fail('unexpected_symbol'); return False
            self.metrics['messages'] += 1; self.metrics['bytes'] += len(payload)
            self.symbol_messages[symbol] += 1
            # Raw transport delay, separate from age of an inactive book.
            lag = received_ms + (mexc_now_ms() - now_ms()) - sent if sent else None
            self.metrics['lag_ms'] = lag
            if lag is not None:
                self.metrics['max_lag_ms'] = max(self.metrics['max_lag_ms'], lag)
            if lag is not None and lag > PUBLIC_LAG_RECOVERY_MS:
                first,count=self.bad_by_symbol.get(symbol,(received_mono,0))
                self.bad_by_symbol[symbol]=(first,count+1)
                if count+1 >= 3 and (received_mono-first)*1000 >= PUBLIC_LAG_PERSIST_MS:
                    self.fail('exchange_backlog'); return False
            else:
                self.bad_by_symbol.pop(symbol,None)
            try:
                self.inbox.put_nowait((payload, received_ms, received_mono, symbol, sent))
            except queue.Full:
                self.fail('receive_queue_full'); return False
            self.metrics['max_queue'] = max(self.metrics['max_queue'], self.inbox.qsize())
            public_work_available.set()
            return True
        except Exception:
            self.fail('invalid_public_packet'); return False
        finally:
            self.metrics['max_callback_us'] = max(self.metrics['max_callback_us'],
                (time.perf_counter_ns()-started)/1000.0)

    def consume_batch(self, first):
        # Drain only frames already available, without delaying a fresh packet.
        # One lock acquisition for <=32 updates. Waiting for that lock no
        # longer consumes a 2 ms wall-clock budget and collapses the batch.
        items = [first]
        self.inflight_mono = first[2]
        while len(items) < PUBLIC_BATCH_MAX:
            try: items.append(self.inbox.get_nowait())
            except queue.Empty: break
        prepared = []; resync = []; latency_rows = []
        try:
            for item in items:
                if self.halt.is_set(): return
                payload, received, received_mono, symbol, sent = item
                waited = (time.monotonic()-received_mono)*1000
                self.metrics['max_queue_wait_ms'] = max(self.metrics['max_queue_wait_ms'], waited)
                if waited > PUBLIC_RX_MAX_WAIT_MS:
                    self.note_catchup(waited)
                decode_start = time.perf_counter_ns()
                x = decode_depth(payload)
                self.metrics['max_decode_us'] = max(self.metrics['max_decode_us'],
                    (time.perf_counter_ns()-decode_start)/1000.0)
                if not x or x['symbol'] != symbol or x['send'] != sent:
                    self.fail('public_header_mismatch'); return
                x['received_ts'] = received; x['source_epoch'] = self.epoch
                prepared.append((x, received, symbol, sent))
            lock_start = time.perf_counter_ns()
            with depth_lock:
                self.metrics['lock_wait_us_max'] = max(self.metrics['lock_wait_us_max'],
                    (time.perf_counter_ns()-lock_start)/1000.0)
                for x, received, symbol, sent in prepared:
                    if self.halt.is_set(): return
                    apply_start = time.perf_counter_ns()
                    if apply_depth_update(x, publish=False, resync_out=resync):
                        self.dirty_symbols.add(symbol); self.metrics['applied'] += 1
                    self.metrics['processed'] += 1
                    self.last_progress_mono = time.monotonic()
                    self.inbox.task_done()
                    self.metrics['max_apply_us'] = max(self.metrics['max_apply_us'],
                        (time.perf_counter_ns()-apply_start)/1000.0)
                    last = ws_latency_last_sample.get(symbol, 0)
                    if sent > 0 and received-last >= WS_LATENCY_SAMPLE_MS:
                        ws_latency_last_sample[symbol] = received
                        offset = mexc_now_ms()-now_ms()
                        latency_rows.append(('ws_lat_v22', (received, symbol, sent, received, max(0, received+offset-sent))))
            self.inflight_mono = None
            for symbol in dict.fromkeys(resync):
                queue_depth_resync(symbol, 'version_or_book_invalid')
            for row in latency_rows: put_db(row)
            if not self.halt.is_set():
                self.finish_batch()
                self.metrics['batches'] += 1
        except Exception:
            # A decode/apply failure cannot be skipped while retaining a book.
            self.fail('public_processing_error')

    def run(self):
        while not self.halt.is_set():
            try: item = self.inbox.get(timeout=0.05)
            except queue.Empty: continue
            self.consume_batch(item)

    def watchdog(self):
        while not self.halt.wait(0.05):
            self.check_pressure(allow_stall=True)

    def snapshot(self):
        usable = self.usable()
        result = dict(self.metrics)
        elapsed = max(0.001, time.monotonic()-self.started_mono)
        result.update(worker_id=self.worker_id, epoch=self.epoch, symbols=list(self.symbols),
                      active=self.active and not self.halt.is_set(), recovery_reason=self.reason,
                      catching_up=self.catching_up.is_set(), usable=usable,
                      queue=self.inbox.qsize(), queue_age_ms=round(self.backlog_age_ms(),2),
                      queue_bytes=getattr(self.inbox,'byte_count',None),
                      symbol_messages=dict(self.symbol_messages),
                      messages_per_sec=result['messages']/elapsed)
        return result


def public_dispatch_worker(stop_event=None):
    """One public consumer, rotating among sockets with a 32-frame quantum.

    No wait for a batch to fill. Busy sockets get subsequent rounds; every
    nonempty socket gets a turn. Readers retain their independent bounded queues.
    """
    stop_event = stop_event or threading.Event()
    cursor = 0
    while not stop_event.is_set():
        public_work_available.wait(0.05)
        public_work_available.clear()
        feeds = list(public_feeds.values())
        if not feeds: continue
        started = time.monotonic()
        cursor %= len(feeds)
        for feed in feeds[cursor:] + feeds[:cursor]:
            if stop_event.is_set(): return
            if not feed.active or feed.halt.is_set(): continue
            try: first = feed.inbox.get_nowait()
            except queue.Empty: continue
            before = feed.metrics['processed']
            feed.consume_batch(first)
            public_dispatch_metrics['processed'] += feed.metrics['processed'] - before
        cursor += 1
        public_dispatch_metrics['rounds'] += 1
        public_dispatch_metrics['max_round_ms'] = max(public_dispatch_metrics['max_round_ms'],
            (time.monotonic()-started)*1000)
        if any(feed.active and not feed.halt.is_set() and not feed.inbox.empty() for feed in list(public_feeds.values())):
            public_work_available.set()


def public_watchdog_worker(stop_event=None):
    stop_event = stop_event or threading.Event()
    while not stop_event.wait(0.05):
        for feed in list(public_feeds.values()):
            if feed.active and not feed.halt.is_set():
                feed.check_pressure(allow_stall=True)


def public_flow_status():
    cpu_times=os.times()
    with public_recovery_lock:
        recoveries=list(public_recovery_events); catchups=list(public_catchup_events); totals=dict(public_flow_totals)
    return {'protobuf_backend':PUBLIC_PROTOBUF_BACKEND,'queue_limit': PUBLIC_RX_QUEUE_MAX, 'queue_max_wait_ms': PUBLIC_RX_MAX_WAIT_MS,
            'queue_byte_limit':PUBLIC_RX_QUEUE_BYTES,'queue_resume_ms':PUBLIC_RX_RESUME_MS,
            'consumer_stall_ms':PUBLIC_RX_STALL_MS,'local_age_action':'pause_and_drain_in_order',
            'recovery_lag_ms': PUBLIC_LAG_RECOVERY_MS, 'recent_recoveries':recoveries,
            'recent_catchups':catchups,'totals':totals,
            'dispatch_mode':PUBLIC_DISPATCH_MODE,'batch_max':PUBLIC_BATCH_MAX,
            'dispatch':dict(public_dispatch_metrics),
            'process_cpu_seconds':cpu_times.user+cpu_times.system,
            'process_started_ms':state.get('started_ms'),
            'process_threads':threading.active_count(),
            'workers': [feed.snapshot() for feed in list(public_feeds.values())]}


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

def _decode_depth_python(payload):
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
    return {"symbol":symbol,"send":int(send or 0),"received_ts":now_ms(),
            "asks":asks,"bids":bids,"from":fv,"to":tv}


FAST_DEPTH_MESSAGE = None
PUBLIC_PROTOBUF_BACKEND = "not_initialized"


def _initialize_depth_codec():
    """Native protobuf parser, using the official public schema field numbers.

    No change to private order decoding. Refuse a slow pure-Python protobuf
    backend rather than silently returning to the original hot-path cost.
    """
    global FAST_DEPTH_MESSAGE,PUBLIC_PROTOBUF_BACKEND
    try:
        from google.protobuf import descriptor_pb2,descriptor_pool,message_factory
        from google.protobuf.internal import api_implementation
    except ImportError as exc:
        raise RuntimeError("V248 requires protobuf: python3 -m pip install 'protobuf==7.36.1'") from exc
    backend=api_implementation.Type()
    if backend not in ("upb","cpp"):
        raise RuntimeError("V248 requires native protobuf backend (upb/cpp); pure Python backend refused")
    fd=descriptor_pb2.FileDescriptorProto(name="mexc_public_v248.proto",package="mexc248",syntax="proto3")
    def message(name):
        item=fd.message_type.add();item.name=name;return item
    def field(msg,name,num,kind,repeated=False,target=None):
        f=msg.field.add();f.name=name;f.number=num;f.type=kind
        f.label=3 if repeated else 1
        if target:f.type_name=".mexc248."+target
    item=message("Level");field(item,"price",1,9);field(item,"quantity",2,9)
    depth=message("Depth")
    field(depth,"asks",1,11,True,"Level");field(depth,"bids",2,11,True,"Level")
    field(depth,"fromVersion",4,9);field(depth,"toVersion",5,9)
    wrapper=message("Envelope")
    field(wrapper,"channel",1,9);field(wrapper,"symbol",3,9);field(wrapper,"sendTime",6,3)
    field(wrapper,"depth",313,11,False,"Depth")
    pool=descriptor_pool.DescriptorPool();pool.Add(fd)
    FAST_DEPTH_MESSAGE=message_factory.GetMessageClass(pool.FindMessageTypeByName("mexc248.Envelope"))
    PUBLIC_PROTOBUF_BACKEND=backend


def decode_depth(payload):
    if FAST_DEPTH_MESSAGE is None:
        # Offline reference/tests only: production bootstrap initializes native
        # decoding before starting any socket or private gateway.
        return _decode_depth_python(payload)
    envelope=FAST_DEPTH_MESSAGE();envelope.ParseFromString(payload)
    if not envelope.symbol or not envelope.HasField("depth"):return None
    body=envelope.depth
    return {"symbol":envelope.symbol,"send":int(envelope.sendTime),"received_ts":now_ms(),
            "asks":[(float(z.price),float(z.quantity)) for z in body.asks],
            "bids":[(float(z.price),float(z.quantity)) for z in body.bids],
            "from":int(body.fromVersion),"to":int(body.toVersion)}


_PRIVATE_ORDER_STATUS = {
    1:"NEW", 2:"FILLED", 3:"PARTIALLY_FILLED", 4:"CANCELED",
    5:"PARTIALLY_CANCELED",
}
_TERMINAL_ORDER_STATUS = {
    "FILLED","CANCELED","PARTIALLY_CANCELED","REJECTED","EXPIRED",
}


def _pb_text(value):
    try: return value.decode("utf-8","ignore")
    except Exception: return ""


def decode_private_order(payload):
    """Decode MEXC's official privateOrders wrapper without a protobuf runtime."""
    channel=None; symbol=None; create_ts=None; send_ts=None; body=None
    for field,wire,value in _pb_fields(payload):
        if field==1 and wire==2: channel=_pb_text(value)
        elif field==3 and wire==2: symbol=_pb_text(value)
        elif field==5 and wire==0: create_ts=int(value)
        elif field==6 and wire==0: send_ts=int(value)
        elif field==304 and wire==2: body=value
    if body is None: return None
    texts={}; ints={}
    text_fields={1,2,3,4,5,6,10,11,12,13,14,17,19,21,22,23,24,25,26}
    int_fields={7,8,9,15,16,18,20}
    for field,wire,value in _pb_fields(body):
        if wire==2 and field in text_fields: texts[field]=_pb_text(value)
        elif wire==0 and field in int_fields: ints[field]=int(value)
    status_code=ints.get(15)
    status=_PRIVATE_ORDER_STATUS.get(status_code,str(status_code) if status_code is not None else "")
    order_id=texts.get(1) or None
    client_id=texts.get(2) or None
    try: executed=float(texts.get(13) or 0.0)
    except (TypeError,ValueError): executed=0.0
    try: cumulative_quote=float(texts.get(14) or 0.0)
    except (TypeError,ValueError): cumulative_quote=0.0
    nested_create=ints.get(16)
    event={
        "channel":channel or "spot@private.orders.v3.api.pb",
        "symbol":symbol or texts.get(17) or "",
        "send_ts_ms":send_ts,
        "create_ts_ms":nested_create or create_ts,
        "exchange_order_id":order_id,
        "client_order_id":client_id,
        "status_code":status_code,
        "status":status,
        "executed_qty":executed,
        "cumulative_quote_qty":cumulative_quote,
    }
    event["terminal"]=status in _TERMINAL_ORDER_STATUS
    event["order"]={
        "symbol":event["symbol"], "orderId":order_id,
        "clientOrderId":client_id, "price":texts.get(3,"0"),
        "origQty":texts.get(4,"0"), "executedQty":texts.get(13,"0"),
        "cummulativeQuoteQty":texts.get(14,"0"), "status":status,
        "type":str(ints.get(7,"")), "side":str(ints.get(8,"")),
        "time":event["create_ts_ms"],
        # Envelope emission time is not the matching-engine fill time.
        "_terminal_event_send_ms":send_ts if event["terminal"] else None,
    }
    return event

def _depth_rows(ob):
    cached=ob.get("_rows")
    if cached is None:
        cached=(tuple(sorted(ob["bids"].items(),reverse=True)),tuple(sorted(ob["asks"].items())))
        ob["_rows"]=cached
    return cached


def _valid_depth_delta(x):
    fv=x.get("from"); tv=x.get("to")
    return (isinstance(fv,int) and isinstance(tv,int) and 0<=fv<=tv and
        all(math.isfinite(p) and math.isfinite(q) and p>0 and q>=0
            for side in ("bids","asks") for p,q in x.get(side,())))


def _book_top(ob):
    cached=ob.get("_best")
    if cached is None:
        cached=(max(ob["bids"],default=None),min(ob["asks"],default=None))
        ob["_best"]=cached
    return cached


def _merge_depth_delta(ob,x):
    bp,ap=_book_top(ob)
    for side in ("asks","bids"):
        book=ob[side]
        for price,qty in x[side]:
            if qty==0: book.pop(price,None)
            else:
                book[price]=qty
                if side=="bids" and (bp is None or price>bp): bp=price
                if side=="asks" and (ap is None or price<ap): ap=price
    # Full scan is needed only when the previous best level was removed.
    if bp not in ob["bids"]: bp=max(ob["bids"],default=None)
    if ap not in ob["asks"]: ap=min(ob["asks"],default=None)
    ob["_best"]=(bp,ap)
    ob.update(version=x["to"],send_ts=int(x.get("send") or 0),
              ts=int(x.get("received_ts") or now_ms()))
    ob.pop("_rows",None)
    return bp is not None and ap is not None and bp<ap


def _publish_depth_bbo(sym, send_ts=None):
    with depth_lock:
        ob=state["depth"].get(sym)
        if not _public_symbol_usable(sym) or not ob or not ob.get("ready") or not ob["bids"] or not ob["asks"]: return
        bp,ap=_book_top(ob); bq=ob["bids"][bp]; aq=ob["asks"][ap]
        if bp>=ap: return
        ts=int(ob.get("ts") or 0); send_ts=int(ob.get("send_ts") or 0)
        with state_lock:
            state["bbo"][sym]={"bid":bp,"bidq":bq,"ask":ap,"askq":aq,"ts":ts,
                "send_ts":send_ts,"version":ob.get("version"),
                "lat":mexc_now_ms()-send_ts if send_ts else None}
            state["last_ws_ms"]=now_ms()
    enqueue_scan(sym)

def queue_depth_resync(sym, reason="gap"):
    """Queue one serialized REST resync per symbol; never spawn resync threads from WS callbacks."""
    ts=now_ms()
    new_gap=False
    with depth_resync_lock:
        d=depth_resync_diag[sym]
        d["last_reason"]=reason
        if d["not_ready_since"] is None:
            d["not_ready_since"]=ts; d["drops"]+=1
            new_gap=reason!="bootstrap"
            put_db(("depth_sync",(ts,sym,"NOT_READY",reason,None,None)))
        if sym in depth_resync_pending:
            return False
        depth_resync_pending.add(sym)
        depth_resync_q.put(sym)
    if new_gap:
        with state_lock: state["depth_gap_events"]+=1
    return True


def apply_depth_update(x, publish=True, resync_out=None):
    sym=x["symbol"]
    x.setdefault("received_ts",now_ms())
    need_resync=False
    lock_started=time.perf_counter_ns()
    with depth_lock:
        feed=public_symbol_feeds.get(sym)
        if feed:
            feed.metrics["lock_wait_us_max"]=max(feed.metrics["lock_wait_us_max"],(time.perf_counter_ns()-lock_started)/1000.0)
        if not _public_symbol_receiving(sym) or x.get("source_epoch",_public_epoch(sym))!=_public_epoch(sym): return False
        ob=state["depth"].get(sym)
        if not ob or not ob.get("ready"):
            depth_buffers[sym].append(x)
            # Startup symbols are queued by bootstrap; a dropped live symbol is already pending.
            return False
        prev=ob.get("version")
        fv=x.get("from"); tv=x.get("to")
        if not _valid_depth_delta(x) or prev is None:
            ob["ready"]=False; need_resync=True
        elif tv<=prev: return False
        elif fv>prev+1 or (x.get("send",0)>0 and ob.get("send_ts",0)>x["send"]):
            ob["ready"]=False; depth_buffers[sym].append(x); need_resync=True
            state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
        else:
            if not _merge_depth_delta(ob,x):
                ob["ready"]=False; need_resync=True
        if need_resync:
            state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
    if need_resync:
        if resync_out is None:
            queue_depth_resync(sym,"version_or_book_invalid")
        else:
            resync_out.append(sym)
        return False
    if publish: _publish_depth_bbo(sym,x["send"])
    return True


def init_depth_symbol(sym):
    """Build/replay off the global lock; atomically commit a reconciled epoch.

    Consumers continue buffering while a temporary snapshot is replayed. A
    changed buffer tail requires another catch-up pass before publication.
    Neither a failed pass nor an obsolete HTTP response replaces a book.
    """
    try:
        source_epoch=_public_epoch(sym)
        if not _public_symbol_receiving(sym): return False
        r=requests.get(REST+"/api/v3/depth",params={"symbol":sym,"limit":100},timeout=8); r.raise_for_status(); j=r.json()
        bids={float(a):float(b) for a,b in j.get("bids",[]) if float(b)>0}; asks={float(a):float(b) for a,b in j.get("asks",[]) if float(b)>0}
        ver=int(j.get("lastUpdateId",0)); received=now_ms()
        if (ver<=0 or not bids or not asks or max(bids)>=min(asks) or
            not all(math.isfinite(p) and math.isfinite(q) and p>0 and q>0
                    for book in (bids,asks) for p,q in book.items())):
            return False
        # REST has no certified exchange timestamp. Do not fabricate one.
        ob={"bids":bids,"asks":asks,"version":ver,"ready":True,"ts":received,
            "send_ts":0,"snapshot_received_ts":received}
        deadline=time.monotonic()+1.0
        while time.monotonic()<deadline:
            with depth_lock:
                if not _public_symbol_receiving(sym) or source_epoch!=_public_epoch(sym): return False
                buf=list(depth_buffers.get(sym,()))
                tail=buf[-1] if buf else None
            # Potentially thousands of deltas: never hold the global lock here.
            for x in buf:
                if not _valid_depth_delta(x): return False
                if x.get('source_epoch',source_epoch)!=source_epoch: return False
                tv=x.get("to")
                if tv<=ob["version"]: continue
                fv=x.get("from")
                if (fv>ob["version"]+1 or
                    (x.get("send",0)>0 and ob.get("send_ts",0)>x["send"])):
                    return False
                if not _merge_depth_delta(ob,x): return False
            with depth_lock:
                if not _public_symbol_receiving(sym) or source_epoch!=_public_epoch(sym): return False
                current=depth_buffers.get(sym,())
                if (current[-1] if current else None) is tail:
                    previous=state['depth'].get(sym)
                    if previous and previous.get('ready') and int(previous.get('version') or 0)>ob['version']:
                        return False
                    state["depth"][sym]=ob
                    depth_buffers.pop(sym,None)
                    state["depth_ready"]=sum(1 for z in state["depth"].values() if z.get("ready"))
                    break
            time.sleep(0)  # Let readers/consumers run between catch-up passes.
        else:
            return False
        _publish_depth_bbo(sym)
        return True
    except Exception as e:
        logerr(f"depth snapshot {sym}: {e}")
        return False


def depth_resync_worker():
    """Single rate-limited REST worker with retry/backoff; prevents 429 resync storms."""
    while True:
        sym=depth_resync_q.get(); attempt=0; requeue=False
        try:
            while True:
                attempt+=1
                ok=init_depth_symbol(sym)
                if ok:
                    ts=now_ms()
                    with depth_resync_lock:
                        d=depth_resync_diag[sym]; since=d.get("not_ready_since")
                        dur=max(0,ts-since) if since is not None else None
                        reason=d.get("last_reason","")
                        d["resync_ok"]+=1; d["not_ready_since"]=None; d["last_resync_ms"]=dur; d["last_ready_ts_ms"]=ts
                    put_db(("depth_sync",(ts,sym,"READY",reason,dur,attempt)))
                    if reason!="bootstrap":
                        with state_lock: state["depth_resync_recoveries"]+=1
                    break
                with depth_resync_lock: depth_resync_diag[sym]["resync_fail"]+=1
                with state_lock: state["depth_resync_failures"]+=1
                put_db(("depth_sync",(now_ms(),sym,"RETRY",depth_resync_diag[sym].get("last_reason",""),None,attempt)))
                # 0.5, 1, 2, 4, 8s; capped. One worker means no REST burst.
                time.sleep(min(8.0,0.5*(2**min(attempt-1,4))))
                if attempt>=2:
                    requeue=True
                    break
            # Keep successful snapshot requests under ~4/s even during a queue backlog.
            time.sleep(0.25)
        finally:
            with depth_resync_lock:
                if requeue: depth_resync_q.put(sym)
                else: depth_resync_pending.discard(sym)
            depth_resync_q.task_done()


def depth_bootstrap_worker(symbols):
    # All initial snapshots use the same serialized/rate-limited resync path.
    for sym in symbols:
        with depth_lock:
            if sym not in state["depth"]: state["depth"][sym]={"bids":{},"asks":{},"version":None,"ready":False}
        queue_depth_resync(sym,"bootstrap")


def depth_walk(symbol, frm, to, input_units, meta, age_limit_ms=PRIMARY_BBO_AGE_MS):
    m=meta.get(symbol)
    if not m or input_units<=0 or not _public_symbol_usable(symbol): return None
    with depth_lock:
        ob=state["depth"].get(symbol)
        if not ob or not ob.get("ready"): return None
        bids,asks=_depth_rows(ob)
        book_ts=ob.get("ts",0); send_ts=ob.get("send_ts",book_ts)
    if now_ms()-book_ts > age_limit_ms: return None
    remain=float(input_units); out=0.0; used=0.0; notional=0.0; levels_used=0; gross_out=0.0; side=None; worst_price=None
    if frm==m["base"] and to==m["quote"]:
        side="sell_base"
        for price,qty in bids:
            take=min(remain,qty)
            if take<=0: continue
            levels_used+=1; used+=take; gross_out+=take*price; notional+=take*price; remain-=take; worst_price=price
            if remain<=1e-12: break
        out=gross_out*(1.0-FEE)
        vwap=(notional/used) if used>0 else None
    elif frm==m["quote"] and to==m["base"]:
        side="buy_base"; base_gross=0.0
        for price,qty in asks:
            max_quote=price*qty; takeq=min(remain,max_quote)
            if takeq<=0: continue
            levels_used+=1; base=takeq/price; used+=takeq; base_gross+=base; notional+=takeq; remain-=takeq; worst_price=price
            if remain<=1e-12: break
        gross_out=base_gross; out=base_gross*(1.0-FEE)
        vwap=(notional/base_gross) if base_gross>0 else None
    else: return None
    if used<=0: return None
    return {"input_requested":input_units,"input_used":used,"output":out,"gross_output":gross_out,"fill_ratio":used/input_units,
            "book_ts":book_ts,"send_ts":send_ts,"levels_used":levels_used,"vwap":vwap,
            "worst_price":worst_price,"side":side}


def _quality_books(route):
    """Atomically copy the two local Depth books used by the T0 quality decision."""
    ts=now_ms(); out={}
    with depth_lock:
        for sym in route["symbols"]:
            ob=state["depth"].get(sym)
            if not _public_symbol_usable(sym) or not ob or not ob.get("ready") or ts-ob.get("ts",0)>SHADOW_BBO_AGE_MS:
                return None
            bids,asks=_depth_rows(ob)
            out[sym]={
                "bids":bids[:DEPTH_QUALITY_LEVELS],
                "asks":asks[:DEPTH_QUALITY_LEVELS],
                "book_ts":ob.get("ts",0),
                "send_ts":ob.get("send_ts",0),"version":ob.get("version"),
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
    return {"input_used":used,"output":gross_out*(1.0-FEE),"gross_output":gross_out,
            "fill_ratio":min(1.0,used/input_units),"levels_used":levels_used}


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
        if SHADOW_TRADE_CAP_USD>0: a_usd=min(a_usd,SHADOW_TRADE_CAP_USD)
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
        if SHADOW_TRADE_CAP_USD>0: b_usd=min(b_usd,SHADOW_TRADE_CAP_USD)
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

        # C is logged for counterfactual replay but is never admitted by V2.3.6.
        c_usd=capacity*DEPTH_QUALITY_C_FRACTION
        if SHADOW_TRADE_CAP_USD>0: c_usd=min(c_usd,SHADOW_TRADE_CAP_USD)
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

def route_calc(route,books,meta, age_limit_ms=None, allow_small_capacity=False):
    start,end=route["path"][0],route["path"][-1]
    sv=stable_mark_usdt(start,books,meta); ev=stable_mark_usdt(end,books,meta)
    if sv<=0 or ev<=0: return None
    multiplier=1.0; max_start=float("inf"); ts=now_ms(); exchange_ts=mexc_now_ms(); max_age=0; max_exchange_age=0; leg_ts=[]; leg_send_ts=[]; leg_books=[]
    exchange_ts_complete=True
    age_limit_ms = MAX_BBO_AGE_MS if age_limit_ms is None else age_limit_ms
    symbols=route["symbols"]
    if len(symbols)!=len(route["path"])-1: return None
    for i,sym in enumerate(symbols):
        if not _public_symbol_usable(sym): return None
        b=books.get(sym); m=meta.get(sym)
        if not b or not m: return None
        raw_send_ts=b.get("send_ts")
        if raw_send_ts is None:
            exchange_ts_complete=False; send_ts=int(b["ts"])
        else:
            try: send_ts=int(raw_send_ts)
            except (TypeError,ValueError): exchange_ts_complete=False; send_ts=int(b["ts"])
            if send_ts<=0: exchange_ts_complete=False; send_ts=int(b["ts"])
        age=ts-b["ts"]; exch_age=max(0,exchange_ts-send_ts)
        max_age=max(max_age,age); max_exchange_age=max(max_exchange_age,exch_age); leg_ts.append(b["ts"]); leg_send_ts.append(send_ts); leg_books.append(dict(b))
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
    if executable_usd<MIN_EXEC_USD and not allow_small_capacity: return None
    net=multiplier*ev/sv-1.0
    return {"net":net,"size":executable_usd,"profit":executable_usd*net,"out_ratio":multiplier,
            "start_mark":sv,"end_mark":ev,"max_age":int(max_age), "exchange_max_age":int(max_exchange_age),
            "bbo_skew":int(max(leg_ts)-min(leg_ts)) if leg_ts else 0,
            "exchange_skew":int(max(leg_send_ts)-min(leg_send_ts)) if leg_send_ts else 0,
            "exchange_ts_complete":exchange_ts_complete,
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

# ---------- Private MEXC gateway (test by default; real orders triple-gated) ----------
class MexcAPIError(RuntimeError):
    def __init__(self, message, status_code=None, payload=None):
        super().__init__(message); self.status_code=status_code; self.payload=payload


class MexcOrderUncertain(MexcAPIError):
    pass


class MexcSignalExpired(MexcAPIError):
    """The opportunity became too old before any real order was submitted."""
    pass


class MexcPrivateClient:
    def __init__(self, api_key, api_secret, session=None, role="private"):
        self.api_key=api_key.strip(); self.api_secret=api_secret.strip().encode("utf-8")
        self.session=session or requests.Session(); self.time_offset_ms=0; self.role=role
        # One connection pool per role. Orders never contend with the periodic
        # account refresh, and a small idle ping keeps the order TLS channel hot.
        adapter=requests.adapters.HTTPAdapter(pool_connections=1,pool_maxsize=1,max_retries=0,pool_block=True)
        self.session.mount("https://",adapter)
        self.session.headers.update({"Accept":"application/json","Connection":"keep-alive"})
        self.http_lock=threading.Lock(); self.last_activity_ms=0

    @staticmethod
    def _payload(response):
        try: return response.json()
        except Exception: return {"raw":response.text[:1000]}

    def _request(self, method, url, *, params=None, headers=None, timeout=None, on_start=None):
        queued_ns=time.perf_counter_ns()
        with self.http_lock:
            started_wall=now_ms(); started_ns=time.perf_counter_ns()
            if on_start is not None:
                on_start(started_wall)  # A failed final guard must prevent the POST.
            r=self.session.request(method,url,params=params,headers=headers,
                                   timeout=timeout or LIVE_API_TIMEOUT_SEC)
            received_wall=now_ms(); received_ns=time.perf_counter_ns(); self.last_activity_ms=received_wall
        timing={"request_started_ts_ms":started_wall,"response_received_ts_ms":received_wall,
                "http_queue_ms":(started_ns-queued_ns)/1_000_000.0,
                "http_ms":(received_ns-started_ns)/1_000_000.0,
                "status_code":r.status_code,"role":self.role}
        return r,timing

    def sync_time(self):
        r,timing=self._request("GET",REST+"/api/v3/time")
        payload=self._payload(r)
        if not r.ok or not isinstance(payload,dict) or payload.get("serverTime") is None:
            raise MexcAPIError("Impossible de synchroniser l'heure MEXC",r.status_code,payload)
        # Midpoint removes most of the HTTP round-trip bias from the offset.
        midpoint=(int(timing["request_started_ts_ms"])+int(timing["response_received_ts_ms"]))//2
        self.time_offset_ms=int(payload["serverTime"])-midpoint
        return self.time_offset_ms

    def signed(self, method, path, params=None, retry_timestamp=False, with_timing=False,
               on_start=None):
        params=dict(params or {})
        params["recvWindow"]=LIVE_RECV_WINDOW_MS
        params["timestamp"]=now_ms()+self.time_offset_ms
        query=urlencode(params,doseq=True)
        params["signature"]=hmac.new(self.api_secret,query.encode("utf-8"),hashlib.sha256).hexdigest()
        headers={"X-MEXC-APIKEY":self.api_key,"Accept":"application/json"}
        try:
            response,timing=self._request(method,REST+path,params=params,headers=headers,on_start=on_start)
        except requests.RequestException as exc:
            if method.upper()=="POST" and path=="/api/v3/order":
                raise MexcOrderUncertain(f"Résultat POST inconnu: {exc}") from exc
            raise MexcAPIError(f"Erreur réseau MEXC: {exc}") from exc
        payload=self._payload(response)
        code=payload.get("code") if isinstance(payload,dict) else None
        try: code_num=int(code) if code is not None else None
        except (TypeError,ValueError): code_num=code
        real_post=method.upper()=="POST" and path=="/api/v3/order"
        if response.ok and code_num in (None,0,200):
            if real_post and (not isinstance(payload,dict) or not payload.get("orderId")):
                raise MexcOrderUncertain("Réponse POST sans orderId: réconciliation requise")
            return (payload,timing) if with_timing else payload
        if retry_timestamp and method.upper()!="POST" and code_num in (-1021,700003):
            self.sync_time(); return self.signed(method,path,{k:v for k,v in params.items() if k not in ("timestamp","recvWindow","signature")},False,with_timing,on_start)
        msg=payload.get("msg") if isinstance(payload,dict) else None
        if real_post and (response.status_code>=500 or code_num in (-1006,-1007)):
            raise MexcOrderUncertain(f"Ordre potentiellement accepté, HTTP {response.status_code}: {msg or payload}",response.status_code,payload)
        raise MexcAPIError(f"MEXC HTTP {response.status_code}: {msg or payload}",response.status_code,payload)

    def account(self):
        return self.signed("GET","/api/v3/account",retry_timestamp=True)

    def new_order(self, symbol, side, order_type, client_order_id, quantity=None,
                  quote_order_qty=None, price=None, test=False, with_timing=False,
                  on_start=None):
        p={"symbol":symbol,"side":side,"type":order_type,"newClientOrderId":client_order_id}
        if quantity is not None: p["quantity"]=quantity
        if quote_order_qty is not None: p["quoteOrderQty"]=quote_order_qty
        if price is not None: p["price"]=price
        return self.signed("POST","/api/v3/order/test" if test else "/api/v3/order",p,
                           with_timing=with_timing,on_start=on_start)

    def new_market_order(self, symbol, side, client_order_id, quantity=None, quote_order_qty=None, test=False):
        return self.new_order(symbol,side,"MARKET",client_order_id,quantity,quote_order_qty,None,test)

    def order(self, symbol, client_order_id):
        return self.signed("GET","/api/v3/order",{"symbol":symbol,"origClientOrderId":client_order_id},retry_timestamp=True)

    def trades(self, symbol, order_id):
        return self.signed("GET","/api/v3/myTrades",{"symbol":symbol,"orderId":order_id},retry_timestamp=True)

    def api_key_request(self, method, path, params=None):
        headers={"X-MEXC-APIKEY":self.api_key,"Accept":"application/json"}
        try:
            response,_=self._request(method,REST+path,params=params,headers=headers)
        except requests.RequestException as exc:
            raise MexcAPIError(f"Erreur réseau user stream MEXC: {exc}") from exc
        payload=self._payload(response)
        code=payload.get("code") if isinstance(payload,dict) else None
        if response.ok and code in (None,0,200,"0","200"): return payload
        msg=payload.get("msg") if isinstance(payload,dict) else payload
        raise MexcAPIError(f"MEXC user stream HTTP {response.status_code}: {msg}",response.status_code,payload)

    def create_listen_key(self):
        # MEXC's published listen-key table still lists no request parameters,
        # but the production endpoint currently enforces the same SIGNED
        # authentication as the other private REST endpoints.  Reuse the
        # already time-synchronised HMAC path so timestamp, recvWindow and
        # signature are sent in the query string.
        payload=self.signed("POST","/api/v3/userDataStream")
        key=payload.get("listenKey") if isinstance(payload,dict) else None
        if not key: raise MexcAPIError(f"listenKey absent: {payload}")
        return str(key)

    def keepalive_listen_key(self, listen_key):
        return self.signed("PUT","/api/v3/userDataStream",{"listenKey":listen_key},retry_timestamp=True)

    def close_listen_key(self, listen_key):
        return self.signed("DELETE","/api/v3/userDataStream",{"listenKey":listen_key},retry_timestamp=True)

    def keepalive_if_idle(self, idle_ms, timeout_sec, abort_if=None):
        """Warm this exact pool without ever queueing behind an order."""
        if now_ms()-int(self.last_activity_ms or 0)<idle_ms: return None
        if not self.http_lock.acquire(blocking=False): return None
        try:
            if now_ms()-int(self.last_activity_ms or 0)<idle_ms: return None
            # The gateway marks itself busy before queueing a candidate. Check
            # again after owning the HTTP lock so a keep-alive cannot knowingly
            # start while an order is waiting for this same warm connection.
            if abort_if is not None and abort_if(): return None
            started_wall=now_ms(); started=time.perf_counter_ns()
            r=self.session.get(REST+"/api/v3/time",timeout=timeout_sec)
            elapsed=(time.perf_counter_ns()-started)/1_000_000.0
            self.last_activity_ms=now_ms()
            payload=self._payload(r)
            if not r.ok or not isinstance(payload,dict) or payload.get("serverTime") is None:
                raise MexcAPIError("Keep-alive MEXC invalide",r.status_code,payload)
            self.time_offset_ms=int(payload["serverTime"])-(started_wall+self.last_activity_ms)//2
            return elapsed
        finally:
            self.http_lock.release()


def _private_ws_prune_locked(ts=None):
    ts=int(ts or now_ms())
    stale=[]
    for client_id,waiter in private_ws_waiters.items():
        age=ts-int(waiter.get("created_ts_ms") or ts)
        if age>300000 and (waiter.get("released") or age>900000): stale.append(client_id)
    for client_id in stale:
        waiter=private_ws_waiters.pop(client_id,None)
        if waiter:
            order_id=waiter.get("exchange_order_id")
            if order_id is not None: private_ws_order_aliases.pop(str(order_id),None)
        private_ws_submission_meta.pop(client_id,None)


def _private_ws_register(spec, attempt_id, leg, purpose):
    client_id=str(spec["client_order_id"])
    waiter={
        "client_order_id":client_id,"attempt_id":attempt_id,"leg":int(leg),
        "purpose":purpose,"symbol":spec["symbol"],"created_ts_ms":now_ms(),
        "post_started_ts_ms":None,"exchange_order_id":None,
        "ws_event":threading.Event(),"rest_event":threading.Event(),"terminal_event":threading.Event(),
        "ws_order":None,"ws_received_ts_ms":None,"ws_send_ts_ms":None,
        "rest_order":None,"rest_confirmed_ts_ms":None,"rest_error":None,
        "rest_poll_count":0,"comparison_recorded":False,"released":False,
    }
    with private_ws_lock:
        _private_ws_prune_locked()
        private_ws_waiters[client_id]=waiter
        private_ws_submission_meta[client_id]={
            "attempt_id":attempt_id,"leg":int(leg),"purpose":purpose,
            "symbol":spec["symbol"],"created_ts_ms":waiter["created_ts_ms"],
        }
    return waiter


def _private_ws_link_submission(waiter, post_started_ts_ms, exchange_order_id=None):
    if not waiter: return
    with private_ws_lock:
        waiter["post_started_ts_ms"]=int(post_started_ts_ms)
        meta=private_ws_submission_meta.get(waiter["client_order_id"])
        if meta is not None: meta["post_started_ts_ms"]=int(post_started_ts_ms)
        if exchange_order_id is not None:
            waiter["exchange_order_id"]=str(exchange_order_id)
            private_ws_order_aliases[str(exchange_order_id)]=waiter["client_order_id"]


def _private_ws_queue(item):
    try: private_ws_journal_q.put_nowait(item)
    except queue.Full:
        with private_ws_lock:
            private_ws_runtime["journal_drops"]+=1


def _private_ws_comparison_locked(waiter):
    ws_ts=waiter.get("ws_received_ts_ms"); rest_ts=waiter.get("rest_confirmed_ts_ms")
    if ws_ts is None or rest_ts is None or waiter.get("comparison_recorded"): return None
    delta=float(ws_ts-rest_ts)
    waiter["comparison_recorded"]=True
    private_ws_rest_delta_ms.append(delta)
    if delta<0: private_ws_runtime["ws_before_rest"]+=1
    elif delta>0: private_ws_runtime["rest_before_ws"]+=1
    else: private_ws_runtime["same_ms"]+=1
    _private_ws_queue({"kind":"rest_update","client_order_id":waiter["client_order_id"],
        "rest_confirmed_ts_ms":rest_ts,"ws_rest_delta_ms":delta,
        "rest_poll_count":int(waiter.get("rest_poll_count") or 0)})
    return delta


def _private_ws_note_rest(waiter, order=None, confirmed_ts_ms=None, poll_count=0, error=None,
                          source="rest"):
    if not waiter: return
    with private_ws_lock:
        waiter["rest_order"]=order if isinstance(order,dict) else waiter.get("rest_order")
        waiter["rest_confirmed_ts_ms"]=int(confirmed_ts_ms or now_ms())
        waiter["rest_poll_count"]=int(poll_count or 0)
        waiter["rest_error"]=str(error)[:500] if error else None
        waiter["rest_event"].set()
        waiter["terminal_event"].set()
        # POST can be terminal immediately. A later REST status poll is only
        # counted as decision authority by _query_order_until_terminal; a
        # reconciliation arriving after WS must not masquerade as REST-first.
        if source=="post": private_ws_runtime["confirmations_post"]+=1
        _private_ws_comparison_locked(waiter)
        mismatch=_private_ws_terminal_mismatch(waiter)
    if mismatch:
        live_trip("private_ws_rest_mismatch",str(waiter.get("client_order_id")),waiter.get("attempt_id"))


def _private_ws_terminal_mismatch(waiter):
    ws_order=waiter.get("ws_order"); rest_order=waiter.get("rest_order")
    if not (_is_terminal_order(ws_order) and _is_terminal_order(rest_order)): return False
    try:
        return (str(ws_order.get("status"))!=str(rest_order.get("status")) or
                any(not math.isclose(w,r,rel_tol=1e-8,abs_tol=1e-10)
                    for w,r in zip(_order_numbers(ws_order),_order_numbers(rest_order))))
    except (TypeError,ValueError): return True


def _private_ws_release(waiter, keep_rest_probe=False):
    if not waiter: return
    with private_ws_lock:
        waiter["released"]=True
        waiter["keep_rest_probe"]=bool(keep_rest_probe)
        _private_ws_prune_locked()


def _private_ws_publish(event, received_ts_ms=None):
    received=int(received_ts_ms or now_ms())
    client_id=str(event.get("client_order_id") or "")
    order_id=event.get("exchange_order_id")
    mismatch=False
    with private_ws_lock:
        private_ws_runtime["order_events"]+=1
        private_ws_runtime["last_order_event_ms"]=received
        if event.get("terminal"): private_ws_runtime["terminal_events"]+=1
        if not client_id and order_id is not None:
            client_id=private_ws_order_aliases.get(str(order_id),"")
        waiter=private_ws_waiters.get(client_id) if client_id else None
        meta=private_ws_submission_meta.get(client_id) if client_id else None
        matched=bool(waiter or meta or client_id.startswith(("v244","v245","v246","v247")))
        private_ws_runtime["matched_events" if matched else "unmatched_events"]+=1
        post_started=(waiter or {}).get("post_started_ts_ms") or (meta or {}).get("post_started_ts_ms")
        post_to_ws=float(received-post_started) if post_started is not None else None
        if post_to_ws is not None: private_ws_post_to_event_ms.append(post_to_ws)
        if waiter and order_id is not None:
            waiter["exchange_order_id"]=str(order_id)
            private_ws_order_aliases[str(order_id)]=client_id
        if waiter and event.get("terminal"):
            ws_order=dict(event.get("order") or {})
            ws_order["clientOrderId"]=client_id
            # Do not invent a symbol from the pending order: an incomplete
            # event must not pass the exact-identity authority check.
            ws_order["symbol"]=event.get("symbol") or ws_order.get("symbol") or ""
            waiter["ws_order"]=ws_order
            waiter["ws_received_ts_ms"]=received
            waiter["ws_send_ts_ms"]=event.get("send_ts_ms")
            waiter["ws_event"].set()
            waiter["terminal_event"].set()
            _private_ws_comparison_locked(waiter)
            mismatch=_private_ws_terminal_mismatch(waiter)
        payload=(received,event.get("send_ts_ms"),event.get("create_ts_ms"),
            event.get("channel") or "spot@private.orders.v3.api.pb",event.get("symbol"),
            str(order_id) if order_id is not None else None,client_id or None,
            event.get("status_code"),event.get("status"),event.get("executed_qty"),
            event.get("cumulative_quote_qty"),post_started,post_to_ws,
            (waiter or {}).get("rest_confirmed_ts_ms"),
            (float(received-waiter["rest_confirmed_ts_ms"]) if waiter and waiter.get("rest_confirmed_ts_ms") is not None else None),
            1 if matched else 0,json.dumps(event,separators=(",",":"),ensure_ascii=False,default=str))
    if mismatch:
        live_trip("private_ws_rest_mismatch",client_id,waiter.get("attempt_id"))
    _private_ws_queue({"kind":"event","payload":payload})


def private_ws_journal_worker():
    while True:
        item=private_ws_journal_q.get()
        try:
            if item.get("kind")=="event":
                payload=item["payload"]
                put_db(("private_order_ws_v245",payload))
                # The critical order journal wins every SQLite lock while a route
                # is exposed. Private stream telemetry catches up afterwards.
                while True:
                    with live_lock: busy=bool(live_runtime.get("busy"))
                    if not busy: break
                    time.sleep(0.005)
                live_db_execute("""INSERT OR IGNORE INTO private_order_ws_events_v245(
                    received_ts_ms,send_ts_ms,create_ts_ms,channel,symbol,exchange_order_id,
                    client_order_id,status_code,status,executed_qty,cumulative_quote_qty,
                    post_started_ts_ms,post_to_ws_ms,rest_confirmed_ts_ms,ws_rest_delta_ms,
                    matched_bot_order,raw_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",payload)
            elif item.get("kind")=="rest_update":
                params=(item.get("rest_confirmed_ts_ms"),item.get("ws_rest_delta_ms"),item.get("client_order_id"))
                put_db(("private_order_ws_rest_v245",params))
                while True:
                    with live_lock: busy=bool(live_runtime.get("busy"))
                    if not busy: break
                    time.sleep(0.005)
                live_db_transaction([
                    ("""UPDATE private_order_ws_events_v245
                        SET rest_confirmed_ts_ms=?,ws_rest_delta_ms=? WHERE client_order_id=?""",params),
                    ("""UPDATE live_orders_v240 SET ws_rest_delta_ms=?,
                        rest_poll_count=MAX(COALESCE(rest_poll_count,0),?)
                        WHERE client_order_id=?""",
                     (item.get("ws_rest_delta_ms"),int(item.get("rest_poll_count") or 0),
                      item.get("client_order_id"))),
                ])
        except Exception as exc:
            logerr(f"private WS journal: {exc}")
        finally:
            private_ws_journal_q.task_done()


def private_order_ws_worker():
    if LIVE_PRIVATE_WS_MODE=="off": return
    retry_delay=2.0
    while True:
        listen_key=None; opened=False; stop=threading.Event(); started=time.monotonic()
        try:
            if live_user_stream_client is None or not MEXC_API_KEY:
                raise MexcAPIError("Client user stream privé indisponible")
            listen_key=live_user_stream_client.create_listen_key()
            with private_ws_lock:
                private_ws_runtime["listenkey_creates"]+=1
                private_ws_runtime["listenkey_created_ms"]=now_ms()

            def maintenance(ws):
                next_ping=time.monotonic()+LIVE_PRIVATE_WS_PING_SEC
                next_key=time.monotonic()+LIVE_PRIVATE_WS_KEEPALIVE_SEC
                unanswered_since=None
                while not stop.wait(0.25):
                    now=time.monotonic(); ts=now_ms()
                    if now-started>=LIVE_PRIVATE_WS_MAX_CONNECTION_SEC:
                        try: ws.close()
                        except Exception: pass
                        return
                    with private_ws_lock:
                        last_pong=private_ws_runtime.get("last_pong_ms") or private_ws_runtime.get("last_open_ms") or ts
                    if unanswered_since is not None and last_pong>=unanswered_since:
                        unanswered_since=None
                    if unanswered_since is not None and ts-unanswered_since>LIVE_PRIVATE_WS_PONG_TIMEOUT_SEC*1000:
                        with private_ws_lock: private_ws_runtime["last_error"]="private application PONG timeout"
                        try: ws.close()
                        except Exception: pass
                        return
                    if now>=next_ping:
                        try:
                            ws.send(json.dumps({"method":"PING"}))
                            with private_ws_lock: private_ws_runtime["pings"]+=1
                            if unanswered_since is None: unanswered_since=ts
                        except Exception as exc:
                            with private_ws_lock: private_ws_runtime["last_error"]=str(exc)[:500]
                            return
                        next_ping=now+LIVE_PRIVATE_WS_PING_SEC
                    if now>=next_key:
                        try:
                            live_user_stream_client.keepalive_listen_key(listen_key)
                            with private_ws_lock:
                                private_ws_runtime["listenkey_keepalive_ok"]+=1
                                private_ws_runtime["last_listenkey_keepalive_ms"]=now_ms()
                        except Exception as exc:
                            with private_ws_lock:
                                private_ws_runtime["listenkey_keepalive_errors"]+=1
                                private_ws_runtime["last_error"]=str(exc)[:500]
                        next_key=now+LIVE_PRIVATE_WS_KEEPALIVE_SEC

            def on_open(ws):
                nonlocal opened
                opened=True; ts=now_ms()
                with private_ws_lock:
                    private_ws_runtime["connected"]=True
                    private_ws_runtime["opens"]+=1
                    if private_ws_runtime["opens"]>1: private_ws_runtime["reconnects"]+=1
                    private_ws_runtime["last_open_ms"]=ts
                    private_ws_runtime["last_pong_ms"]=ts
                    private_ws_runtime["last_error"]=None
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":["spot@private.orders.v3.api.pb"]}))
                with private_ws_lock: private_ws_runtime["subscribed"]=True
                threading.Thread(target=maintenance,args=(ws,),daemon=True).start()
                print(f"[Routes V{VERSION}] private order WS connected mode={LIVE_PRIVATE_WS_MODE}")

            def on_message(ws,msg):
                ts=now_ms()
                with private_ws_lock:
                    private_ws_runtime["messages"]+=1
                    private_ws_runtime["last_message_ms"]=ts
                if isinstance(msg,str):
                    try:
                        data=json.loads(msg)
                        if str(data.get("msg","")).upper()=="PONG":
                            with private_ws_lock:
                                private_ws_runtime["pongs"]+=1
                                private_ws_runtime["last_pong_ms"]=ts
                    except Exception: pass
                    return
                try:
                    event=decode_private_order(msg)
                    if event: _private_ws_publish(event,ts)
                except Exception as exc:
                    with private_ws_lock:
                        private_ws_runtime["decode_errors"]+=1
                        private_ws_runtime["last_error"]=str(exc)[:500]
                    logerr(f"decode private order WS: {exc}")

            def on_error(ws,exc):
                with private_ws_lock: private_ws_runtime["last_error"]=str(exc)[:500]

            def on_close(ws,code,msg):
                nonlocal opened
                stop.set()
                if opened:
                    with private_ws_lock:
                        private_ws_runtime["connected"]=False
                        private_ws_runtime["subscribed"]=False
                        private_ws_runtime["disconnects"]+=1
                        private_ws_runtime["last_close_ms"]=now_ms()
                    opened=False

            url=WS+"?"+urlencode({"listenKey":listen_key})
            socket=websocket.WebSocketApp(url,on_open=on_open,on_message=on_message,
                on_error=on_error,on_close=on_close)
            socket.run_forever(ping_interval=0)
        except Exception as exc:
            with private_ws_lock: private_ws_runtime["last_error"]=str(exc)[:500]
            logerr(f"private order WS: {exc}")
        finally:
            stop.set()
            if opened:
                with private_ws_lock:
                    private_ws_runtime["connected"]=False
                    private_ws_runtime["subscribed"]=False
                    private_ws_runtime["disconnects"]+=1
                    private_ws_runtime["last_close_ms"]=now_ms()
            if listen_key and live_user_stream_client is not None:
                try: live_user_stream_client.close_listen_key(listen_key)
                except Exception: pass
        lived=time.monotonic()-started
        retry_delay=2.0 if lived>=120 else min(30.0,max(2.0,retry_delay*1.5))
        time.sleep(retry_delay+random.uniform(0.0,1.0))


def initialize_live_journal():
    """Open the small crash-safe order journal, isolated from research writes."""
    global live_db_connection
    with live_db_lock:
        if live_db_connection is not None: return live_db_connection
        parent=os.path.dirname(os.path.abspath(LIVE_DB_PATH))
        if parent: os.makedirs(parent,exist_ok=True)
        con=sqlite3.connect(LIVE_DB_PATH,timeout=30,check_same_thread=False)
        con.row_factory=sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=FULL")
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA wal_autocheckpoint=100")
        con.executescript("""
        CREATE TABLE IF NOT EXISTS live_observations_v247(
          attempt_id TEXT NOT NULL, stage TEXT NOT NULL, ts_ms INTEGER NOT NULL,
          version TEXT NOT NULL, payload_json TEXT NOT NULL,
          PRIMARY KEY(attempt_id,stage)
        );
        CREATE TABLE IF NOT EXISTS live_state_v240(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1), updated_ts_ms INTEGER NOT NULL,
          mode TEXT NOT NULL, api_ok INTEGER NOT NULL, balances_json TEXT NOT NULL,
          circuit_open INTEGER NOT NULL, circuit_reason TEXT, last_error TEXT,
          attempts INTEGER NOT NULL, test_validations INTEGER NOT NULL, completed INTEGER NOT NULL,
          wins INTEGER NOT NULL, losses INTEGER NOT NULL, realized_pnl REAL NOT NULL,
          forced_unwinds INTEGER NOT NULL, consecutive_unwinds INTEGER NOT NULL,
          last_attempt_id TEXT, last_route TEXT, last_status TEXT,
          last_leg1_ms INTEGER, last_leg2_ms INTEGER, account_ts_ms INTEGER
        );
        CREATE TABLE IF NOT EXISTS live_attempts_v240(
          attempt_id TEXT PRIMARY KEY, decision_id TEXT NOT NULL UNIQUE, day TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL, updated_ts_ms INTEGER NOT NULL,
          route_id TEXT NOT NULL, path TEXT NOT NULL, grade TEXT NOT NULL, mode TEXT NOT NULL,
          status TEXT NOT NULL, requested_usd REAL NOT NULL, input_asset TEXT NOT NULL,
	          mid_asset TEXT NOT NULL, output_asset TEXT NOT NULL, input_units REAL,
	          mid_acquired REAL, mid_used REAL, output_units REAL, unwind_input REAL,
	          unwind_output REAL, residual_mid REAL, realized_pnl REAL,
	          cash_output_usd REAL, residual_mark_usd REAL, residual_value_usd REAL,
	          economic_pnl REAL, realized_cost_usd REAL, residual_cost_usd REAL,
	          unrealized_pnl REAL, unwind_error TEXT,
	          leg1_client_id TEXT, leg2_client_id TEXT, unwind_client_id TEXT,
          leg1_latency_ms INTEGER, leg2_latency_ms INTEGER, error TEXT,
          decision_ts_ms INTEGER, event_start_ts_ms INTEGER, route_edge_t0 REAL,
          route_edge_submit REAL, t0_to_leg1_submit_ms INTEGER,
          t0_to_leg1_done_ms INTEGER, leg1_done_to_leg2_submit_ms INTEGER,
          t0_to_done_ms INTEGER, leg1_order_type TEXT, leg2_order_type TEXT,
          unwind_order_type TEXT, admission_to_leg1_post_ms INTEGER,
          leg1_post_http_ms REAL, leg2_post_http_ms REAL,
          leg1_http_queue_ms REAL, leg2_http_queue_ms REAL,
          leg1_confirm_ms INTEGER, leg2_confirm_ms INTEGER,
          leg1_prepare_journal_ms REAL, leg2_prepare_journal_ms REAL,
          leg2_guard_edge REAL,
          route_exchange_age_t0_ms INTEGER, route_exchange_skew_t0_ms INTEGER,
          leg2_exchange_age_ms INTEGER, leg2_exchange_skew_ms INTEGER
	        );
	        CREATE INDEX IF NOT EXISTS idx_live_attempts_day ON live_attempts_v240(day,created_ts_ms);
	        CREATE TABLE IF NOT EXISTS live_open_exposures_v246(
	          attempt_id TEXT PRIMARY KEY, day TEXT NOT NULL, opened_ts_ms INTEGER NOT NULL,
	          updated_ts_ms INTEGER NOT NULL, route_id TEXT NOT NULL, symbol TEXT NOT NULL,
	          asset TEXT NOT NULL, start_asset TEXT NOT NULL, units REAL NOT NULL,
	          input_usd REAL NOT NULL, cash_output_usd REAL NOT NULL, residual_cost_usd REAL NOT NULL,
	          last_mark_usd REAL, last_value_usd REAL, economic_pnl REAL,
	          status TEXT NOT NULL DEFAULT 'open', resolution_note TEXT, closed_ts_ms INTEGER
	        );
	        CREATE INDEX IF NOT EXISTS idx_live_open_exposures_v246_status
	          ON live_open_exposures_v246(status,day,opened_ts_ms);
	        CREATE TABLE IF NOT EXISTS live_circuit_events_v246(
	          event_id INTEGER PRIMARY KEY AUTOINCREMENT, ts_ms INTEGER NOT NULL,
	          day TEXT NOT NULL, attempt_id TEXT, reason TEXT NOT NULL, error TEXT,
	          active INTEGER NOT NULL DEFAULT 1
	        );
	        CREATE INDEX IF NOT EXISTS idx_live_circuit_events_v246_active
	          ON live_circuit_events_v246(active,ts_ms);
	        CREATE TABLE IF NOT EXISTS live_orders_v240(
          client_order_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, leg INTEGER NOT NULL,
          purpose TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
          order_type TEXT NOT NULL, quantity TEXT, quote_order_qty TEXT, price TEXT,
          status TEXT NOT NULL, exchange_order_id TEXT, executed_qty REAL,
          cumulative_quote_qty REAL, commissions_json TEXT NOT NULL DEFAULT '{}',
          prepared_ts_ms INTEGER NOT NULL, submitted_ts_ms INTEGER, final_ts_ms INTEGER,
          response_json TEXT, error TEXT, request_started_ts_ms INTEGER,
          response_received_ts_ms INTEGER, post_http_ms REAL,
          http_queue_ms REAL, response_journal_ms REAL, settle_ms INTEGER, fallback_index INTEGER,
          confirmation_source TEXT, rest_poll_count INTEGER,
          ws_received_ts_ms INTEGER, ws_send_ts_ms INTEGER,
          ws_post_ms REAL, ws_rest_delta_ms REAL
        );
        CREATE INDEX IF NOT EXISTS idx_live_orders_attempt ON live_orders_v240(attempt_id,leg);
        CREATE TABLE IF NOT EXISTS live_route_capabilities_v241(
          route_id TEXT PRIMARY KEY, validated_ts_ms INTEGER NOT NULL,
          expires_ts_ms INTEGER NOT NULL, leg1_json TEXT NOT NULL,
          leg2_json TEXT NOT NULL, unwind_json TEXT NOT NULL,
          last_decision_id TEXT NOT NULL, last_error TEXT
        );
        CREATE TABLE IF NOT EXISTS private_order_ws_events_v245(
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          received_ts_ms INTEGER NOT NULL, send_ts_ms INTEGER, create_ts_ms INTEGER,
          channel TEXT NOT NULL, symbol TEXT, exchange_order_id TEXT,
          client_order_id TEXT, status_code INTEGER, status TEXT,
          executed_qty REAL, cumulative_quote_qty REAL,
          post_started_ts_ms INTEGER, post_to_ws_ms REAL,
          rest_confirmed_ts_ms INTEGER, ws_rest_delta_ms REAL,
          matched_bot_order INTEGER NOT NULL DEFAULT 0, raw_json TEXT NOT NULL,
          UNIQUE(client_order_id,send_ts_ms,status_code,executed_qty,cumulative_quote_qty)
        );
        CREATE INDEX IF NOT EXISTS idx_private_order_ws_v245_client
          ON private_order_ws_events_v245(client_order_id,received_ts_ms);
        """)
        # Forward-compatible migration if LIVE_DB_PATH points at an earlier journal.
        existing={r[1] for r in con.execute("PRAGMA table_info(live_attempts_v240)")}
        for name,decl in (("admission_to_leg1_post_ms","INTEGER"),("leg1_post_http_ms","REAL"),
            ("leg2_post_http_ms","REAL"),("leg1_http_queue_ms","REAL"),("leg2_http_queue_ms","REAL"),
            ("leg1_confirm_ms","INTEGER"),("leg2_confirm_ms","INTEGER"),
            ("leg1_prepare_journal_ms","REAL"),("leg2_prepare_journal_ms","REAL"),
	            ("leg2_guard_edge","REAL"),("cash_output_usd","REAL"),
	            ("residual_mark_usd","REAL"),("residual_value_usd","REAL"),
	            ("economic_pnl","REAL"),("realized_cost_usd","REAL"),
	            ("residual_cost_usd","REAL"),("unrealized_pnl","REAL"),("unwind_error","TEXT"),
            ("route_exchange_age_t0_ms","INTEGER"),("route_exchange_skew_t0_ms","INTEGER"),
            ("leg2_exchange_age_ms","INTEGER"),("leg2_exchange_skew_ms","INTEGER")):
            if name not in existing: con.execute(f"ALTER TABLE live_attempts_v240 ADD COLUMN {name} {decl}")
        order_existing={r[1] for r in con.execute("PRAGMA table_info(live_orders_v240)")}
        for name,decl in (("price","TEXT"),("request_started_ts_ms","INTEGER"),
            ("response_received_ts_ms","INTEGER"),("post_http_ms","REAL"),
            ("http_queue_ms","REAL"),("response_journal_ms","REAL"),("settle_ms","INTEGER"),
            ("fallback_index","INTEGER"),("confirmation_source","TEXT"),
            ("rest_poll_count","INTEGER"),("ws_received_ts_ms","INTEGER"),
            ("ws_send_ts_ms","INTEGER"),("ws_post_ms","REAL"),
            ("ws_rest_delta_ms","REAL")):
            if name not in order_existing: con.execute(f"ALTER TABLE live_orders_v240 ADD COLUMN {name} {decl}")
        con.commit()
        # Preserve still-valid route probes from V2.4.5 on the first V2.4.6 start.
        # Set LIVE_IMPORT_CAPABILITIES_DB="" to deliberately start empty.
        try:
            cap_count=con.execute("SELECT COUNT(*) FROM live_route_capabilities_v241").fetchone()[0]
            legacy=LIVE_IMPORT_CAPABILITIES_DB.strip()
            if cap_count==0 and legacy and os.path.exists(legacy) and os.path.abspath(legacy)!=os.path.abspath(LIVE_DB_PATH):
                src=sqlite3.connect(legacy,timeout=10)
                rows=src.execute("""SELECT route_id,validated_ts_ms,expires_ts_ms,leg1_json,leg2_json,
                    unwind_json,last_decision_id,last_error FROM live_route_capabilities_v241
                    WHERE expires_ts_ms>=?""",(now_ms(),)).fetchall()
                src.close()
                if rows:
                    con.executemany("""INSERT OR REPLACE INTO live_route_capabilities_v241(
                        route_id,validated_ts_ms,expires_ts_ms,leg1_json,leg2_json,unwind_json,
                        last_decision_id,last_error) VALUES(?,?,?,?,?,?,?,?)""",rows)
                    con.commit()
                    print(f"[Routes V{VERSION}] imported {len(rows)} live route capabilities from {legacy}")
        except Exception as exc:
            logerr(f"live capability import: {exc}")
        live_db_connection=con
        return con


def live_db_execute(sql, params=(), one=False, many=False):
    """Critical live journal: persistent, isolated and FULL-sync."""
    with live_db_lock:
        con=initialize_live_journal()
        cur=con.execute(sql,params)
        result=cur.fetchone() if one else (cur.fetchall() if many else None)
        con.commit(); return result


def live_db_transaction(operations):
    """Commit related safety records with one fsync before an order POST."""
    with live_db_lock:
        con=initialize_live_journal()
        try:
            con.execute("BEGIN IMMEDIATE")
            for sql,params in operations: con.execute(sql,params)
            con.commit()
        except Exception:
            con.rollback(); raise


def _live_json(value):
    try: return json.dumps(value,sort_keys=True,separators=(",",":"),default=str)[:20000]
    except Exception: return json.dumps({"unserializable":str(value)[:1000]})


def persist_live_state():
    with live_lock:
        r=dict(live_runtime); balances=_live_json(r.get("balances",{}))
    live_db_execute("""INSERT OR REPLACE INTO live_state_v240(
        singleton,updated_ts_ms,mode,api_ok,balances_json,circuit_open,circuit_reason,last_error,
        attempts,test_validations,completed,wins,losses,realized_pnl,forced_unwinds,consecutive_unwinds,
        last_attempt_id,last_route,last_status,last_leg1_ms,last_leg2_ms,account_ts_ms)
        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (now_ms(),TRADING_MODE,1 if r.get("api_ok") else 0,balances,1 if r.get("circuit_open") else 0,
         r.get("circuit_reason"),r.get("last_error"),int(r.get("attempts",0)),int(r.get("test_validations",0)),
         int(r.get("completed",0)),int(r.get("wins",0)),int(r.get("losses",0)),float(r.get("realized_pnl",0.0)),
         int(r.get("forced_unwinds",0)),int(r.get("consecutive_unwinds",0)),r.get("last_attempt_id"),
         r.get("last_route"),r.get("last_status"),r.get("last_leg1_ms"),r.get("last_leg2_ms"),r.get("account_ts_ms")))


def load_live_state():
    try:
        row=live_db_execute("SELECT * FROM live_state_v240 WHERE singleton=1",one=True)
        if not row: return
        with live_lock:
            for key in ("attempts","test_validations","completed","wins","losses","realized_pnl",
                        "forced_unwinds","consecutive_unwinds","last_attempt_id","last_route","last_status",
                        "last_leg1_ms","last_leg2_ms","account_ts_ms","last_error"):
                if key in row.keys(): live_runtime[key]=row[key]
            if not LIVE_RESET_CIRCUIT:
                live_runtime["circuit_open"]=bool(row["circuit_open"])
                live_runtime["circuit_reason"]=row["circuit_reason"]
        if LIVE_RESET_CIRCUIT:
            live_db_execute("UPDATE live_circuit_events_v246 SET active=0 WHERE active=1")
            with live_lock:
                live_runtime["circuit_open"]=False
                live_runtime["circuit_reason"]=None
                live_runtime["circuit_causes"]=[]
        else:
            rows=live_db_execute("""SELECT event_id,ts_ms,attempt_id,reason,error
                FROM live_circuit_events_v246 WHERE active=1 ORDER BY event_id""",many=True)
            causes=[dict(x) for x in (rows or [])]
            # If an operator explicitly reuses an older live journal, retain
            # its last circuit reason instead of losing it on the first V2.4.6
            # event. New V2.4.6 causes are append-only below.
            if not causes and bool(row["circuit_open"]) and row["circuit_reason"]:
                _,_,legacy_day=local_day_bounds_ms(0)
                legacy_ts=int(row["updated_ts_ms"] or now_ms())
                legacy_reason=str(row["circuit_reason"])[:200]
                legacy_error=str(row["last_error"])[:1000] if row["last_error"] else None
                live_db_execute("""INSERT INTO live_circuit_events_v246(
                    ts_ms,day,attempt_id,reason,error,active) VALUES(?,?,?,?,?,1)""",
                    (legacy_ts,legacy_day,None,legacy_reason,legacy_error))
                causes=[{"event_id":None,"ts_ms":legacy_ts,"attempt_id":None,
                         "reason":legacy_reason,"error":legacy_error}]
            with live_lock:
                live_runtime["circuit_causes"]=causes
                if causes:
                    live_runtime["circuit_open"]=True
                    live_runtime["circuit_reason"]=" | ".join(dict.fromkeys(
                        str(x["reason"]) for x in causes))[:1000]
    except Exception as exc: logerr(f"live state restore: {exc}")


def live_open_exposure_snapshot():
    """Mark open inventory separately; never turn its notional into realized PnL."""
    try:
        rows=live_db_execute("""SELECT attempt_id,day,opened_ts_ms,updated_ts_ms,route_id,
            symbol,asset,start_asset,units,input_usd,cash_output_usd,residual_cost_usd,last_mark_usd,
            last_value_usd,economic_pnl FROM live_open_exposures_v246
            WHERE status='open' AND units>0 ORDER BY opened_ts_ms""",many=True)
    except Exception:
        rows=[]
    with state_lock:
        books=dict(state.get("bbo") or {}); meta=state.get("market_meta") or {}
    details=[]; total_value=0.0; total_unrealized=0.0; total_economic=0.0; marks_known=True
    for row in rows or []:
        item=dict(row); units=float(item.get("units") or 0.0)
        mark=_asset_mark_usd(item.get("asset"),books,meta)
        if mark is None:
            stored=item.get("last_mark_usd")
            mark=float(stored) if stored is not None else None
        value=units*mark if mark is not None else None
        unrealized=(value-float(item.get("residual_cost_usd") or 0.0)) if value is not None else None
        economic=(float(item.get("cash_output_usd") or 0.0)+value-
                  float(item.get("input_usd") or 0.0)) if value is not None else None
        item.update({"mark_usd":mark,"value_usd":value,"unrealized_pnl":unrealized,
                     "economic_pnl":economic})
        if value is not None: total_value+=value
        if economic is None: marks_known=False
        else:
            total_unrealized+=unrealized; total_economic+=economic
        details.append(item)
    return {"count":len(details),"value_usd":total_value,
            "unrealized_pnl":total_unrealized if marks_known else None,
            "economic_pnl":total_economic if marks_known else None,
            "items":details[:20]}


def refresh_live_daily_stats():
    global live_session_totals
    _,_,day=local_day_bounds_ms(0)
    try:
        row=live_db_execute("""SELECT COUNT(*) attempts,
            SUM(CASE WHEN status='test_validated' THEN 1 ELSE 0 END) test_validations,
            SUM(CASE WHEN mode='live' AND status IN ('completed','forced_unwind','manual_closed') AND realized_pnl IS NOT NULL THEN 1 ELSE 0 END) completed,
            SUM(CASE WHEN mode='live' AND status IN ('completed','forced_unwind','manual_closed') AND realized_pnl>0 THEN 1 ELSE 0 END) wins,
            SUM(CASE WHEN mode='live' AND status IN ('completed','forced_unwind','manual_closed') AND realized_pnl<0 THEN 1 ELSE 0 END) losses,
            COALESCE(SUM(CASE WHEN mode='live' AND (status IN ('completed','forced_unwind','manual_closed')
                OR (status='open_exposure' AND residual_cost_usd IS NOT NULL))
                THEN realized_pnl ELSE 0 END),0) pnl,
            SUM(CASE WHEN mode='live' AND status='forced_unwind' THEN 1 ELSE 0 END) forced_unwinds
            FROM live_attempts_v240 WHERE day=?""",(day,),one=True)
        exposure=live_open_exposure_snapshot()
        if LIVE_SESSION_REQUIRED:
            live_session_totals=session_totals(live_db_execute(SESSION_TOTALS_SQL,one=True))
        with live_lock:
            live_runtime["attempts"]=int(row["attempts"] or 0); live_runtime["test_validations"]=int(row["test_validations"] or 0)
            live_runtime["completed"]=int(row["completed"] or 0); live_runtime["wins"]=int(row["wins"] or 0)
            live_runtime["losses"]=int(row["losses"] or 0); live_runtime["realized_pnl"]=float(row["pnl"] or 0.0)
            live_runtime["forced_unwinds"]=int(row["forced_unwinds"] or 0)
            live_runtime["open_exposures"]=int(exposure["count"])
            live_runtime["open_exposure_value_usd"]=float(exposure["value_usd"] or 0.0)
            live_runtime["open_exposure_unrealized_pnl"]=exposure["unrealized_pnl"]
            live_runtime["open_exposure_economic_pnl"]=exposure["economic_pnl"]
    except Exception as exc:
        live_session_totals=None
        logerr(f"live daily stats: {exc}")


def live_session_status():
    if not LIVE_SESSION_REQUIRED: return {"required":False,"reason":None}
    if live_session_control is None:
        return {"required":True,"reason":"session_missing_or_invalid"}
    try: return live_session_control.status(live_session_totals)
    except Exception: return {"required":True,"reason":"session_accounting_invalid"}


def live_loss_limit_reached(daily_pnl):
    if LIVE_SESSION_REQUIRED:
        return live_session_status().get("reason")=="session_loss_limit"
    return LIVE_DAILY_LOSS_LIMIT_USD>0 and daily_pnl<=-LIVE_DAILY_LOSS_LIMIT_USD


def live_trip(reason, error=None, attempt_id=None):
    ts=now_ms(); _,_,day=local_day_bounds_ms(0)
    reason_text=str(reason)[:200]; error_text=str(error)[:1000] if error is not None else None
    event_id=None
    try:
        live_db_execute("""INSERT INTO live_circuit_events_v246(
            ts_ms,day,attempt_id,reason,error,active) VALUES(?,?,?,?,?,1)""",
            (ts,day,attempt_id,reason_text,error_text))
    except Exception as exc:
        logerr(f"live circuit event persist: {exc}")
    with live_lock:
        cause={"event_id":event_id,"ts_ms":ts,"attempt_id":attempt_id,
               "reason":reason_text,"error":error_text}
        causes=list(live_runtime.get("circuit_causes") or [])
        causes.append(cause); live_runtime["circuit_causes"]=causes[-100:]
        live_runtime["circuit_open"]=True
        live_runtime["circuit_reason"]=" | ".join(dict.fromkeys(
            str(x.get("reason")) for x in live_runtime["circuit_causes"]))[:1000]
        if error_text is not None: live_runtime["last_error"]=error_text[:500]
    try: persist_live_state()
    except Exception as exc: logerr(f"live circuit persist: {exc}")
    logerr(f"LIVE CIRCUIT OPEN: {reason_text}" + (f" — {error_text}" if error_text else ""))


def live_arm_status():
    stop=os.path.exists(LIVE_STOP_FILE); file_ok=False; file_fresh=False
    session_reason=live_session_status().get("reason") if TRADING_MODE=="live" else None
    try:
        with open(LIVE_ARM_FILE,"r",encoding="utf-8") as f: file_ok=f.read().strip()==LIVE_ARM_PHRASE
        file_fresh=int(os.path.getmtime(LIVE_ARM_FILE)*1000)>=int(state.get("started_ms",0))-1000
    except OSError: pass
    with live_lock: circuit=bool(live_runtime.get("circuit_open")); api_ok=bool(live_runtime.get("api_ok"))
    with private_ws_lock:
        private_connected=bool(private_ws_runtime.get("connected"))
        private_subscribed=bool(private_ws_runtime.get("subscribed"))
        private_pong=private_ws_runtime.get("last_pong_ms")
    private_pong_fresh=bool(private_pong and
        now_ms()-int(private_pong)<=int(LIVE_PRIVATE_WS_PONG_TIMEOUT_SEC*1000))
    if TRADING_MODE=="shadow": effective=False; reason="mode_shadow"
    elif stop: effective=False; reason="stop_file"
    elif circuit: effective=False; reason="circuit_open"
    elif session_reason: effective=False; reason=session_reason
    elif not api_ok: effective=False; reason="api_not_ready"
    elif TRADING_MODE=="test": effective=True; reason="test_endpoint_only"
    elif LIVE_PRIVATE_WS_MODE=="hybrid" and not private_connected: effective=False; reason="private_ws_disconnected"
    elif LIVE_PRIVATE_WS_MODE=="hybrid" and not private_subscribed: effective=False; reason="private_ws_not_subscribed"
    elif LIVE_PRIVATE_WS_MODE=="hybrid" and not private_pong_fresh: effective=False; reason="private_ws_pong_stale"
    elif not LIVE_ARMED: effective=False; reason="env_not_armed"
    elif not file_ok: effective=False; reason="arm_file_missing_or_invalid"
    elif not file_fresh: effective=False; reason="arm_file_predates_process"
    else: effective=True; reason="live_triple_gate_open"
    return {"effective":effective,"reason":reason,"env_armed":LIVE_ARMED,"arm_file_ok":file_ok,"arm_file_fresh":file_fresh,
            "private_ws_ready":bool(private_connected and private_subscribed and private_pong_fresh),
            "stop_file":stop,"real_orders":bool(effective and TRADING_MODE=="live")}


def _as_decimal(value, default="0"):
    try: return Decimal(str(value))
    except (InvalidOperation,TypeError,ValueError): return Decimal(default)


def _floor_amount(value, step):
    d=_as_decimal(value); step=_as_decimal(step)
    if d<=0 or step<=0: return None
    floored=(d/step).to_integral_value(rounding=ROUND_DOWN)*step
    if floored<=0: return None
    return format(floored,"f")


def _ceil_amount(value, step):
    d=_as_decimal(value); step=_as_decimal(step)
    if d<=0 or step<=0: return None
    rounded=(d/step).to_integral_value(rounding=ROUND_CEILING)*step
    return format(rounded,"f") if rounded>0 else None


def _precision_step(market, field, fallback=8):
    try: digits=int(market.get(field))
    except (TypeError,ValueError): digits=fallback
    return Decimal(1).scaleb(-max(0,min(18,digits)))


def _validate_trade_side(symbol, frm, to, market):
    if not market or not market.get("apiRulesAvailable"):
        raise MexcAPIError(f"Règles API absentes pour {symbol}")
    if market.get("isSpotTradingAllowed") is False:
        raise MexcAPIError(f"Spot interdit pour {symbol}")
    trade_side=str(market.get("tradeSideType") or "1")
    if frm==market.get("quote") and to==market.get("base"):
        if trade_side not in ("1","2"): raise MexcAPIError(f"BUY interdit par tradeSideType={trade_side} pour {symbol}")
        return "BUY"
    if frm==market.get("base") and to==market.get("quote"):
        if trade_side not in ("1","3"): raise MexcAPIError(f"SELL interdit par tradeSideType={trade_side} pour {symbol}")
        return "SELL"
    raise MexcAPIError(f"Conversion {frm}->{to} incohérente avec {symbol}")


def live_order_candidates(symbol, frm, to, input_units, market, meta, client_id_factory,
                          age_limit_ms=LIVE_BBO_AGE_MS, exchange_age_limit_ms=None):
    """Build executable order shapes from current Depth, ignoring incomplete exchangeInfo flags.

    /order/test, persisted per route, is the authority. MARKET is preferred; FOK and
    then IOC use the worst currently required Depth price, so they cannot execute
    beyond the price already included in the fresh profitability calculation.
    """
    side=_validate_trade_side(symbol,frm,to,market)
    fill=depth_walk(symbol,frm,to,input_units,meta,age_limit_ms=age_limit_ms)
    if not fill or fill.get("fill_ratio",0)<0.999999 or not fill.get("worst_price"):
        raise MexcAPIError(f"Depth frais insuffisant pour construire l'ordre {symbol}")
    if exchange_age_limit_ms is not None:
        send_ts=fill.get("send_ts")
        if send_ts is None or int(send_ts)<=0:
            raise MexcAPIError(f"Timestamp MEXC Depth absent pour {symbol}")
        exchange_age=max(0,mexc_now_ms()-int(send_ts))
        if exchange_age>exchange_age_limit_ms:
            raise MexcAPIError(
                f"Depth MEXC ancien {exchange_age} ms > {exchange_age_limit_ms} ms pour {symbol}")
    quantity_step=_precision_step(market,"baseAssetPrecision")
    price_step=_precision_step(market,"quotePrecision",fallback=8)
    candidates=[]
    if side=="BUY":
        quote_amount=_floor_amount(input_units,_precision_step(market,"quoteAssetPrecision"))
        if quote_amount:
            candidates.append({"symbol":symbol,"side":side,"order_type":"MARKET","amount_mode":"quoteOrderQty",
                "client_order_id":client_id_factory("MARKET"),"quantity":None,"quote_order_qty":quote_amount,"price":None})
        limit_price=_ceil_amount(fill["worst_price"],price_step)
        max_base=min(float(fill.get("gross_output") or 0.0),float(input_units)/max(float(limit_price or 0),1e-18))
        quantity=_floor_amount(max_base,quantity_step)
    else:
        quantity=_floor_amount(input_units,quantity_step)
        if quantity:
            candidates.append({"symbol":symbol,"side":side,"order_type":"MARKET","amount_mode":"quantity",
                "client_order_id":client_id_factory("MARKET"),"quantity":quantity,"quote_order_qty":None,"price":None})
        limit_price=_floor_amount(fill["worst_price"],price_step)
    if not quantity or not limit_price:
        if candidates: return candidates
        raise MexcAPIError(f"Quantité ou prix nul après arrondi pour {symbol}")
    for order_type in ("FILL_OR_KILL","IMMEDIATE_OR_CANCEL"):
        candidates.append({"symbol":symbol,"side":side,"order_type":order_type,"amount_mode":"quantity",
            "client_order_id":client_id_factory(order_type),"quantity":quantity,"quote_order_qty":None,
            "price":limit_price,"depth_worst_price":fill["worst_price"]})
    return candidates


def live_order_spec(symbol, frm, to, input_units, market, meta, client_order_id,
                    capability=None, age_limit_ms=LIVE_BBO_AGE_MS,
                    exchange_age_limit_ms=None):
    candidates=live_order_candidates(symbol,frm,to,input_units,market,meta,
        lambda order_type: client_order_id, age_limit_ms=age_limit_ms,
        exchange_age_limit_ms=exchange_age_limit_ms)
    if capability:
        wanted_type=str(capability.get("order_type") or "").upper()
        wanted_mode=str(capability.get("amount_mode") or "")
        for spec in candidates:
            if spec["order_type"]==wanted_type and spec["amount_mode"]==wanted_mode:
                spec["fallback_index"]=int(capability.get("fallback_index") or 0)
                return spec
        raise MexcAPIError(f"Forme validée {wanted_type}/{wanted_mode} devenue impossible pour {symbol}")
    return candidates[0]


def live_emergency_market_spec(symbol, frm, to, input_units, market, client_order_id,
                               capability):
    """Build a prevalidated MARKET unwind without consulting local Depth.

    Fresh Depth remains mandatory for entry and leg 2. Once leg 1 has filled,
    however, stale local telemetry must never prevent an exposure-reducing
    MARKET request from reaching MEXC's current matching engine.
    """
    side=_validate_trade_side(symbol,frm,to,market)
    wanted_mode="quoteOrderQty" if side=="BUY" else "quantity"
    if not capability or str(capability.get("order_type") or "").upper()!="MARKET":
        raise MexcAPIError(f"Unwind MARKET non prévalidé pour {symbol}")
    if str(capability.get("amount_mode") or "")!=wanted_mode:
        raise MexcAPIError(f"Forme unwind MARKET incompatible pour {symbol}")
    if capability.get("symbol") and str(capability.get("symbol"))!=symbol:
        raise MexcAPIError(f"Capacité unwind liée à un autre marché que {symbol}")
    if capability.get("side") and str(capability.get("side")).upper()!=side:
        raise MexcAPIError(f"Sens unwind prévalidé incompatible pour {symbol}")
    if side=="BUY":
        quote_amount=_floor_amount(input_units,_precision_step(market,"quoteAssetPrecision"))
        if not quote_amount: raise MexcAPIError(f"Montant unwind nul après arrondi pour {symbol}")
        quantity=None
    else:
        quantity=_floor_amount(input_units,_precision_step(market,"baseAssetPrecision"))
        if not quantity: raise MexcAPIError(f"Quantité unwind nulle après arrondi pour {symbol}")
        quote_amount=None
    return {"symbol":symbol,"side":side,"order_type":"MARKET","amount_mode":wanted_mode,
            "client_order_id":client_order_id,"quantity":quantity,
            "quote_order_qty":quote_amount,"price":None,
            "fallback_index":int(capability.get("fallback_index") or 0),
            "emergency_unwind":True}


def _live_client_id(attempt_id, purpose, variant=""):
    tag={"leg1":"L1","leg2":"L2","unwind":"UW"}.get(purpose,"OR")
    vt={"MARKET":"M","FILL_OR_KILL":"F","IMMEDIATE_OR_CANCEL":"I"}.get(str(variant).upper(),"")
    return ("v247"+tag+vt+attempt_id.replace("-","")[-22:])[:32]


def _live_attempt_insert_values(q, requested_usd, x, attempt_id=None, status="prepared"):
    attempt_id=attempt_id or uuid.uuid4().hex
    route=q["route"]; _,_,day=local_day_bounds_ms(0); ts=now_ms()
    sql="""INSERT INTO live_attempts_v240(
        attempt_id,decision_id,day,created_ts_ms,updated_ts_ms,route_id,path,grade,mode,status,
        requested_usd,input_asset,mid_asset,output_asset,decision_ts_ms,event_start_ts_ms,route_edge_t0,route_edge_submit,
        route_exchange_age_t0_ms,route_exchange_skew_t0_ms)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    params=(attempt_id,q["decision_id"],day,ts,ts,route["id"],">".join(route["path"]),q["quality"]["grade"],
            TRADING_MODE,status,requested_usd,route["path"][0],route["path"][1],route["path"][2],
            int(q.get("decision_ts_ms") or ts),int(q.get("event_start_ts") or ts),q.get("edge_t0"),x.get("net"),
            q.get("route_exchange_age_t0_ms",x.get("exchange_max_age")),
            q.get("route_exchange_skew_t0_ms",x.get("exchange_skew")))
    return attempt_id,sql,params


def _live_attempt_insert(q, requested_usd, x, attempt_id=None):
    attempt_id,sql,params=_live_attempt_insert_values(q,requested_usd,x,attempt_id)
    live_db_execute(sql,params)
    return attempt_id


_LIVE_ATTEMPT_COLUMNS={"status","input_units","mid_acquired","mid_used","output_units","unwind_input",
    "unwind_output","residual_mid","realized_pnl","leg1_client_id","leg2_client_id","unwind_client_id",
    "leg1_latency_ms","leg2_latency_ms","error","route_edge_submit","t0_to_leg1_submit_ms",
    "t0_to_leg1_done_ms","leg1_done_to_leg2_submit_ms","t0_to_done_ms","leg1_order_type",
    "leg2_order_type","unwind_order_type","admission_to_leg1_post_ms","leg1_post_http_ms",
    "leg2_post_http_ms","leg1_http_queue_ms","leg2_http_queue_ms",
    "leg1_confirm_ms","leg2_confirm_ms","leg1_prepare_journal_ms",
    "leg2_prepare_journal_ms","leg2_guard_edge","cash_output_usd","residual_mark_usd",
    "residual_value_usd","economic_pnl","realized_cost_usd","residual_cost_usd",
    "unrealized_pnl","unwind_error","route_exchange_age_t0_ms","route_exchange_skew_t0_ms",
    "leg2_exchange_age_ms","leg2_exchange_skew_ms"}


def _live_attempt_update(attempt_id, **values):
    values={k:v for k,v in values.items() if k in _LIVE_ATTEMPT_COLUMNS}
    if not values: return
    values["updated_ts_ms"]=now_ms()
    columns=list(values)
    live_db_execute("UPDATE live_attempts_v240 SET "+",".join(f"{k}=?" for k in columns)+" WHERE attempt_id=?",
                    tuple(values[k] for k in columns)+(attempt_id,))


def _live_finalize_attempt(attempt_id, status, exposure=None, **values):
    """Atomically finalize route accounting and, if needed, open inventory."""
    values={k:v for k,v in values.items() if k in _LIVE_ATTEMPT_COLUMNS}
    values["status"]=status; values["updated_ts_ms"]=now_ms()
    columns=list(values)
    operations=[("UPDATE live_attempts_v240 SET "+
        ",".join(f"{k}=?" for k in columns)+" WHERE attempt_id=?",
        tuple(values[k] for k in columns)+(attempt_id,))]
    if exposure is not None:
        opened=int(exposure.get("opened_ts_ms") or now_ms())
        operations.append(("""INSERT INTO live_open_exposures_v246(
            attempt_id,day,opened_ts_ms,updated_ts_ms,route_id,symbol,asset,start_asset,
            units,input_usd,cash_output_usd,residual_cost_usd,last_mark_usd,last_value_usd,
            economic_pnl,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'open')
            ON CONFLICT(attempt_id) DO UPDATE SET
            updated_ts_ms=excluded.updated_ts_ms,units=excluded.units,
            cash_output_usd=excluded.cash_output_usd,residual_cost_usd=excluded.residual_cost_usd,
            last_mark_usd=excluded.last_mark_usd,
            last_value_usd=excluded.last_value_usd,economic_pnl=excluded.economic_pnl,
            status='open',resolution_note=NULL,closed_ts_ms=NULL""",
            (attempt_id,exposure["day"],opened,now_ms(),exposure["route_id"],
             exposure["symbol"],exposure["asset"],exposure["start_asset"],
             exposure["units"],exposure["input_usd"],exposure["cash_output_usd"],
             exposure["residual_cost_usd"],
             exposure.get("last_mark_usd"),exposure.get("last_value_usd"),
             exposure.get("economic_pnl"))))
    else:
        operations.append(("""UPDATE live_open_exposures_v246 SET status='closed',
            updated_ts_ms=?,closed_ts_ms=? WHERE attempt_id=? AND status='open'""",
            (now_ms(),now_ms(),attempt_id)))
    live_db_transaction(operations)


def _live_prepare_order_values(attempt_id, leg, purpose, spec):
    sql="""INSERT INTO live_orders_v240(client_order_id,attempt_id,leg,purpose,symbol,side,order_type,
        quantity,quote_order_qty,price,status,prepared_ts_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"""
    return sql,(spec["client_order_id"],attempt_id,leg,purpose,spec["symbol"],spec["side"],spec["order_type"],
                spec.get("quantity"),spec.get("quote_order_qty"),spec.get("price"),"prepared",now_ms())


def _live_prepare_order(attempt_id, leg, purpose, spec):
    sql,params=_live_prepare_order_values(attempt_id,leg,purpose,spec)
    live_db_execute(sql,params)


def _live_prepare_initial_order(q, requested_usd, x, attempt_id, spec, t0, admitted_ts):
    """Durably create attempt + leg 1 order with one FULL-sync commit."""
    _,attempt_sql,attempt_params=_live_attempt_insert_values(
        q,requested_usd,x,attempt_id,status="leg1_submitting")
    order_sql,order_params=_live_prepare_order_values(attempt_id,1,"leg1",spec)
    post_ts=now_ms(); started=time.perf_counter_ns()
    live_db_transaction([
        _observation_sql(attempt_id,"entry",q["route"]["symbols"],
                         {"signal":x,"quality":q.get("quality"),"requested_usd":requested_usd}),
        (attempt_sql,attempt_params),
        ("""UPDATE live_attempts_v240 SET leg1_client_id=?,leg1_order_type=?,
            t0_to_leg1_submit_ms=?,admission_to_leg1_post_ms=?,updated_ts_ms=? WHERE attempt_id=?""",
         (spec["client_order_id"],spec["order_type"],max(0,post_ts-t0),
          max(0,post_ts-admitted_ts),post_ts,attempt_id)),
        (order_sql,order_params),
    ])
    elapsed=(time.perf_counter_ns()-started)/1_000_000.0
    live_prepare_journal_ms.append(elapsed)
    return elapsed


def _live_prepare_followup_order(attempt_id, leg, purpose, spec, **attempt_values):
    """Durably update inventory state + prepare the next order in one commit."""
    values={k:v for k,v in attempt_values.items() if k in _LIVE_ATTEMPT_COLUMNS}
    values["updated_ts_ms"]=now_ms(); columns=list(values)
    update_sql="UPDATE live_attempts_v240 SET "+",".join(f"{k}=?" for k in columns)+" WHERE attempt_id=?"
    update_params=tuple(values[k] for k in columns)+(attempt_id,)
    order_sql,order_params=_live_prepare_order_values(attempt_id,leg,purpose,spec)
    started=time.perf_counter_ns()
    live_db_transaction([(update_sql,update_params),(order_sql,order_params),
        _observation_sql(attempt_id,purpose,[spec["symbol"]],attempt_values)])
    elapsed=(time.perf_counter_ns()-started)/1_000_000.0
    live_prepare_journal_ms.append(elapsed)
    return elapsed


def _observation_sql(attempt_id,stage,symbols,detail):
    ts=now_ms(); books={}
    with depth_lock:
        for sym in symbols:
            ob=state["depth"].get(sym)
            if not ob: continue
            bids,asks=_depth_rows(ob)
            books[sym]={"ready":ob.get("ready"),"version":ob.get("version"),
                "received_ts_ms":ob.get("ts"),"send_ts_ms":ob.get("send_ts"),
                "bids":bids[:DEPTH_CAPTURE_LEVELS],"asks":asks[:DEPTH_CAPTURE_LEVELS]}
    payload={"books":books,"detail":detail,"mexc_now_ms":mexc_now_ms(),
        "fee_assumed":FEE,"haircut":LIVE_FEE_SAFETY,"price_protected":LIVE_PRICE_PROTECTED}
    return ("INSERT OR REPLACE INTO live_observations_v247 VALUES(?,?,?,?,?)",
            (attempt_id,stage,ts,VERSION,_live_json(payload)))


def _live_admission_upsert(q, disposition, reason=None, requested_usd=None, attempt_id=None,
                           retry_count=0, route_tested=False):
    _,_,day=local_day_bounds_ms(0); route=q["route"]
    quality=q.get("initial_quality") or q.get("quality") or {}
    fresh=q.get("fresh_quality") or {}
    # Admission telemetry is deliberately non-blocking. The durable FULL-sync
    # journal remains mandatory for any real order, but SQLite can no longer
    # delay the first market/depth check or a retry.
    put_db(("live_admission_v242",(
        q["decision_id"],day,route["id"],int(q.get("event_start_ts") or 0),
        int(q.get("decision_ts_ms") or now_ms()),now_ms(),TRADING_MODE,str(quality.get("grade") or "?"),
        float(quality.get("selected_usd") or 0.0),requested_usd,q.get("edge_t0"),disposition,
        str(reason)[:500] if reason else None,attempt_id,int(retry_count),1 if route_tested else 0,
        q.get("first_check_ts_ms"),q.get("first_check_delay_ms"),q.get("last_check_ts_ms"),
        q.get("gateway_admit_ts_ms"),q.get("gateway_delay_ms"),fresh.get("grade"),
        fresh.get("selected_usd"),q.get("fresh_edge"),q.get("route_depth_age_ms"),
        q.get("route_depth_skew_ms"),q.get("gateway_check_us"),q.get("route_exchange_age_ms"),
        q.get("route_exchange_skew_ms"),_live_json(q.get("retry_reasons") or {})
    )))


def _capability_template(spec):
    return {"symbol":spec["symbol"],"side":spec["side"],"order_type":spec["order_type"],
            "amount_mode":spec["amount_mode"],"fallback_index":int(spec.get("fallback_index") or 0)}


def _save_route_capability(route_id, decision_id, leg1, leg2, unwind):
    validated=now_ms(); expires=validated+int(LIVE_TEST_MAX_AGE_HOURS*3600000)
    item={"route_id":route_id,"validated_ts_ms":validated,"expires_ts_ms":expires,
          "leg1":_capability_template(leg1),"leg2":_capability_template(leg2),
          "unwind":_capability_template(unwind),"last_decision_id":decision_id}
    live_db_execute("""INSERT OR REPLACE INTO live_route_capabilities_v241(
        route_id,validated_ts_ms,expires_ts_ms,leg1_json,leg2_json,unwind_json,last_decision_id,last_error)
        VALUES(?,?,?,?,?,?,?,NULL)""",(route_id,validated,expires,_live_json(item["leg1"]),
        _live_json(item["leg2"]),_live_json(item["unwind"]),decision_id))
    with live_lock: live_route_capabilities[route_id]=item
    return item


def _load_route_capabilities():
    try:
        # Merge NEWER compatible TEST certificates even if the historical
        # LIVE journal already holds MARKET certificates. Do not copy TEST
        # balances/PnL/circuits and do not renew the original test timestamp.
        legacy=LIVE_IMPORT_CAPABILITIES_DB
        if (legacy and os.path.isfile(legacy) and
            os.path.abspath(legacy)!=os.path.abspath(LIVE_DB_PATH)):
            source=sqlite3.connect("file:"+os.path.abspath(legacy)+"?mode=ro",uri=True)
            source.row_factory=sqlite3.Row
            try:
                certificates=source.execute("SELECT * FROM live_route_capabilities_v241 WHERE expires_ts_ms>=?",(now_ms(),)).fetchall()
            finally: source.close()
            operations=[]
            for row in certificates:
                try:
                    item={leg:json.loads(row[leg+"_json"]) for leg in ("leg1","leg2","unwind")}
                    if not _capability_matches_policy(item): continue
                except (ValueError,TypeError): continue
                operations.append(("""INSERT INTO live_route_capabilities_v241 VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(route_id) DO UPDATE SET validated_ts_ms=excluded.validated_ts_ms,
                    expires_ts_ms=excluded.expires_ts_ms,leg1_json=excluded.leg1_json,
                    leg2_json=excluded.leg2_json,unwind_json=excluded.unwind_json,
                    last_decision_id=excluded.last_decision_id,last_error=NULL
                    WHERE excluded.validated_ts_ms>live_route_capabilities_v241.validated_ts_ms""",
                    (row["route_id"],row["validated_ts_ms"],row["expires_ts_ms"],row["leg1_json"],
                     row["leg2_json"],row["unwind_json"],row["last_decision_id"],None)))
            if operations: live_db_transaction(operations)
        rows=live_db_execute("SELECT * FROM live_route_capabilities_v241 WHERE expires_ts_ms>=?",(now_ms(),),many=True)
        loaded={}
        for row in rows or []:
            try:
                loaded[row["route_id"]]={"route_id":row["route_id"],"validated_ts_ms":row["validated_ts_ms"],
                    "expires_ts_ms":row["expires_ts_ms"],"leg1":json.loads(row["leg1_json"]),
                    "leg2":json.loads(row["leg2_json"]),"unwind":json.loads(row["unwind_json"]),
                    "last_decision_id":row["last_decision_id"]}
            except Exception: continue
        with live_lock:
            live_route_capabilities.clear(); live_route_capabilities.update(loaded)
    except Exception as exc: logerr(f"live capability restore: {exc}")


def _route_capability(route_id):
    with live_lock: item=live_route_capabilities.get(route_id)
    if not item or int(item.get("expires_ts_ms") or 0)<now_ms(): return None
    return item


def _capability_matches_policy(item):
    if not _capability_has_emergency_market_unwind(item): return False
    return (not LIVE_PRICE_PROTECTED or
        all((item.get(leg) or {}).get("order_type")=="FILL_OR_KILL" for leg in ("leg1","leg2")))


def _policy_candidates(candidates,market_only=False):
    if market_only: return [s for s in candidates if s["order_type"]=="MARKET"]
    if LIVE_PRICE_PROTECTED: return [s for s in candidates if s["order_type"]=="FILL_OR_KILL"]
    return candidates


def _capability_has_emergency_market_unwind(item):
    unwind=(item or {}).get("unwind") or {}
    return (str(unwind.get("order_type") or "").upper()=="MARKET" and
            str(unwind.get("amount_mode") or "") in ("quantity","quoteOrderQty"))


def _live_order_update(client_order_id, status, response=None, error=None, submitted=False, final=False,
                       exchange_order_id=None, executed_qty=None, cumulative_quote_qty=None, commissions=None,
                       request_started_ts_ms=None,response_received_ts_ms=None,post_http_ms=None,
                       http_queue_ms=None,response_journal_ms=None,settle_ms=None,fallback_index=None,
                       confirmation_source=None,rest_poll_count=None,ws_received_ts_ms=None,
                       ws_send_ts_ms=None,ws_post_ms=None,ws_rest_delta_ms=None):
    live_db_execute("""UPDATE live_orders_v240 SET status=?,response_json=COALESCE(?,response_json),
        error=COALESCE(?,error),submitted_ts_ms=CASE WHEN ? THEN COALESCE(submitted_ts_ms,?) ELSE submitted_ts_ms END,
        final_ts_ms=CASE WHEN ? THEN ? ELSE final_ts_ms END,exchange_order_id=COALESCE(?,exchange_order_id),
        executed_qty=COALESCE(?,executed_qty),cumulative_quote_qty=COALESCE(?,cumulative_quote_qty),
        commissions_json=COALESCE(?,commissions_json),
        request_started_ts_ms=COALESCE(?,request_started_ts_ms),
        response_received_ts_ms=COALESCE(?,response_received_ts_ms),post_http_ms=COALESCE(?,post_http_ms),
        http_queue_ms=COALESCE(?,http_queue_ms),response_journal_ms=COALESCE(?,response_journal_ms),
        settle_ms=COALESCE(?,settle_ms),fallback_index=COALESCE(?,fallback_index),
        confirmation_source=COALESCE(?,confirmation_source),
        rest_poll_count=COALESCE(?,rest_poll_count),
        ws_received_ts_ms=COALESCE(?,ws_received_ts_ms),
        ws_send_ts_ms=COALESCE(?,ws_send_ts_ms),ws_post_ms=COALESCE(?,ws_post_ms),
        ws_rest_delta_ms=COALESCE(?,ws_rest_delta_ms) WHERE client_order_id=?""",
        (status,_live_json(response) if response is not None else None,str(error)[:1000] if error else None,
         1 if submitted else 0,now_ms(),1 if final else 0,now_ms(),str(exchange_order_id) if exchange_order_id is not None else None,
         executed_qty,cumulative_quote_qty,_live_json(commissions) if commissions is not None else None,
         request_started_ts_ms,response_received_ts_ms,post_http_ms,http_queue_ms,response_journal_ms,settle_ms,
         fallback_index,confirmation_source,rest_poll_count,ws_received_ts_ms,ws_send_ts_ms,
         ws_post_ms,ws_rest_delta_ms,client_order_id))


def _order_numbers(order):
    executed=float(order.get("executedQty") or 0.0)
    quote=float(order.get("cummulativeQuoteQty") or order.get("cumulativeQuoteQty") or 0.0)
    return executed,quote


def _order_exchange_ts_ms(order):
    """Best available MEXC-side order/fill timestamp from WS or REST."""
    if not isinstance(order,dict): return None
    # Creation/acceptance time is not a fill timestamp. WS send time is an
    # explicitly labelled terminal-event proxy, not exact matching latency.
    for key in ("_terminal_event_send_ms","updateTime"):
        try: value=int(order.get(key) or 0)
        except (TypeError,ValueError): value=0
        if value>0: return value
    return None


def _order_commissions(trades):
    out=defaultdict(float)
    if isinstance(trades,list):
        for trade in trades:
            asset=str(trade.get("commissionAsset") or "").upper()
            try: amount=float(trade.get("commission") or 0.0)
            except (TypeError,ValueError): amount=0.0
            if asset and amount>0: out[asset]+=amount
    return dict(out)


def _order_effects(order, commissions, market, side, commission_known=True):
    base,quote=market["base"],market["quote"]; executed,cum_quote=_order_numbers(order)
    if side=="BUY":
        # Unknown commission: reserve one conservative haircut on the acquired
        # asset only. Inflating input *and* shrinking output counted 0.20% twice.
        safe_input=cum_quote+commissions.get(quote,0.0) if commission_known else cum_quote
        safe_output=max(0.0,executed-commissions.get(base,0.0)) if commission_known else executed*(1.0-LIVE_FEE_SAFETY)
        return {"input":safe_input,"output":safe_output,
                "executed_qty":executed,"quote_qty":cum_quote}
    safe_input=executed+commissions.get(base,0.0) if commission_known else executed
    safe_output=max(0.0,cum_quote-commissions.get(quote,0.0)) if commission_known else cum_quote*(1.0-LIVE_FEE_SAFETY)
    return {"input":safe_input,"output":safe_output,
            "executed_qty":executed,"quote_qty":cum_quote}


def _is_terminal_order(order):
    return bool(isinstance(order,dict) and
        str(order.get("status","")).upper() in _TERMINAL_ORDER_STATUS)


def _private_ws_order_is_authoritative(spec, order):
    """Accept only a terminal event tied to the exact submitted client id/symbol."""
    if not _is_terminal_order(order): return False
    if str(order.get("clientOrderId") or "")!=str(spec.get("client_order_id") or ""): return False
    if str(order.get("symbol") or "")!=str(spec.get("symbol") or ""): return False
    try: executed,quote=_order_numbers(order)
    except (ValueError,TypeError): return False
    if not all(math.isfinite(v) and v>=0 for v in (executed,quote)): return False
    if executed>0 and quote<=0: return False
    if str(order.get("status") or "").upper()=="FILLED" and (executed<=0 or quote<=0): return False
    if spec.get("quantity") and executed>float(spec["quantity"])*(1+1e-8): return False
    return True


def _rest_confirmation_worker(spec, waiter, deadline):
    client=live_order_status_client or live_client
    last_error=None; polls=0
    while time.monotonic()<deadline:
        try:
            order=client.order(spec["symbol"],spec["client_order_id"]); polls+=1
            if _private_ws_order_is_authoritative(spec,order):
                _private_ws_note_rest(waiter,order,now_ms(),polls,source="rest")
                return
        except Exception as exc:
            last_error=exc; polls+=1
        remaining=deadline-time.monotonic()
        if remaining<=0: break
        time.sleep(min(LIVE_ORDER_POLL_MS/1000.0,remaining))
    with private_ws_lock:
        waiter["rest_poll_count"]=polls
        waiter["rest_error"]=str(last_error)[:500] if last_error else "terminal REST timeout"
        waiter["rest_event"].set(); waiter["terminal_event"].set()


def _query_order_until_terminal(spec, first=None, waiter=None):
    """Return the first trusted terminal result; REST keeps reconciling WS-first."""
    if waiter is None:
        waiter=_private_ws_register(spec,"query-only",0,"query")
    if _private_ws_order_is_authoritative(spec,first):
        _private_ws_note_rest(waiter,first,now_ms(),0,source="post")
        return first,"post",0
    deadline=time.monotonic()+LIVE_ORDER_SETTLE_TIMEOUT_MS/1000.0
    threading.Thread(target=_rest_confirmation_worker,args=(spec,waiter,deadline),daemon=True).start()
    while time.monotonic()<deadline:
        waiter["terminal_event"].wait(max(0.0,min(0.05,deadline-time.monotonic())))
        with private_ws_lock:
            ws_order=waiter.get("ws_order"); rest_order=waiter.get("rest_order")
            rest_error=waiter.get("rest_error"); polls=int(waiter.get("rest_poll_count") or 0)
            # OBSERVE is deliberately incapable of advancing a real route. It
            # measures whether the stream could win while REST stays authoritative.
            ws_first=(waiter.get("rest_confirmed_ts_ms") is None or
                int(waiter.get("ws_received_ts_ms") or 0)<=int(waiter.get("rest_confirmed_ts_ms") or 0))
            if (LIVE_PRIVATE_WS_MODE=="hybrid" and _private_ws_order_is_authoritative(spec,ws_order) and
                    (not _is_terminal_order(rest_order) or ws_first)):
                private_ws_runtime["confirmations_ws"]+=1
                return dict(ws_order),"private_ws",polls
            if _private_ws_order_is_authoritative(spec,rest_order):
                private_ws_runtime["confirmations_rest"]+=1
                return dict(rest_order),"rest",polls
            waiter["terminal_event"].clear()
            if rest_error and waiter["rest_event"].is_set(): break
    raise MexcOrderUncertain(
        f"Statut final introuvable pour {spec['client_order_id']} ({waiter.get('rest_error')})")


def _reconcile_unknown_order(spec):
    """Query by our durable client id; never submit the POST a second time."""
    deadline=time.monotonic()+LIVE_ORDER_SETTLE_TIMEOUT_MS/1000.0; last_error=None
    while time.monotonic()<deadline:
        try: return (live_order_status_client or live_client).order(spec["symbol"],spec["client_order_id"])
        except Exception as exc: last_error=exc
        time.sleep(LIVE_ORDER_POLL_MS/1000.0)
    raise MexcOrderUncertain(f"Ordre inconnu après réconciliation: {spec['client_order_id']} ({last_error})")


def _submit_live_order(attempt_id, leg, purpose, spec, test, prepared=False,
                       query_commissions=True):
    prepare_journal_ms=float(spec.get("_prepare_journal_ms") or 0.0)
    if not prepared:
        prepare_started=time.perf_counter_ns()
        _live_prepare_order(attempt_id,leg,purpose,spec)
        prepare_journal_ms=(time.perf_counter_ns()-prepare_started)/1_000_000.0
        live_prepare_journal_ms.append(prepare_journal_ms)
    started=now_ms(); total_started_ns=time.perf_counter_ns()
    if test:
        try:
            response,http=live_client.new_order(spec["symbol"],spec["side"],spec["order_type"],spec["client_order_id"],
                spec.get("quantity"),spec.get("quote_order_qty"),spec.get("price"),test=True,with_timing=True)
            journal_started=time.perf_counter_ns()
            _live_order_update(spec["client_order_id"],"test_validated",response=response,submitted=True,final=True,
                request_started_ts_ms=http["request_started_ts_ms"],response_received_ts_ms=http["response_received_ts_ms"],
                post_http_ms=http["http_ms"],http_queue_ms=http["http_queue_ms"],
                fallback_index=spec.get("fallback_index",0))
            journal_ms=(time.perf_counter_ns()-journal_started)/1_000_000.0
            live_response_journal_ms.append(journal_ms)
            # Test mode is outside the future live execution path, so persist
            # this self-measured commit duration with a separate update.
            _live_order_update(spec["client_order_id"],"test_validated",response_journal_ms=journal_ms)
            return {"test":True,"latency_ms":(time.perf_counter_ns()-total_started_ns)/1_000_000.0,
                    "http_ms":http["http_ms"],"http_queue_ms":http["http_queue_ms"],
                    "response_journal_ms":journal_ms,
                    "confirm_ms":0.0,"prepare_journal_ms":prepare_journal_ms,
                    "request_started_ts_ms":http["request_started_ts_ms"],
                    "confirmed_ts_ms":http["response_received_ts_ms"],"response":response}
        except Exception as exc:
            _live_order_update(spec["client_order_id"],"test_rejected",error=exc,submitted=True,final=True)
            raise
    waiter=_private_ws_register(spec,attempt_id,leg,purpose)
    source=None
    def before_post(ts):
        guard=spec.get("_post_guard")
        if guard is not None: guard(ts)
        _private_ws_link_submission(waiter,ts)
    try:
        try:
            first,http=live_client.new_order(spec["symbol"],spec["side"],spec["order_type"],spec["client_order_id"],
                spec.get("quantity"),spec.get("quote_order_qty"),spec.get("price"),test=False,with_timing=True,
                on_start=before_post)
            order_id_first=first.get("orderId") if isinstance(first,dict) else None
            _private_ws_link_submission(waiter,http["request_started_ts_ms"],order_id_first)
            journal_started=time.perf_counter_ns()
            _live_order_update(spec["client_order_id"],"submitted",response=first,submitted=True,
                               exchange_order_id=order_id_first,
                               request_started_ts_ms=http["request_started_ts_ms"],
                               response_received_ts_ms=http["response_received_ts_ms"],post_http_ms=http["http_ms"],
                               http_queue_ms=http["http_queue_ms"],
                               fallback_index=spec.get("fallback_index",0))
            response_journal_ms=(time.perf_counter_ns()-journal_started)/1_000_000.0
            live_response_journal_ms.append(response_journal_ms)
        except MexcOrderUncertain as exc:
            http=None; response_journal_ms=None
            _live_order_update(spec["client_order_id"],"submit_unknown",error=exc,submitted=True)
            try:
                first=_reconcile_unknown_order(spec)
                _private_ws_link_submission(waiter,waiter.get("post_started_ts_ms") or started,
                    first.get("orderId") if isinstance(first,dict) else None)
            except Exception as query_exc:
                live_trip("order_status_unknown",f"{exc}; reconciliation: {query_exc}",attempt_id)
                raise MexcOrderUncertain(f"Ordre inconnu et non réconcilié: {spec['client_order_id']}") from query_exc
        except MexcSignalExpired as exc:
            _live_order_update(spec["client_order_id"],"expired_before_submit",error=exc,final=True)
            raise
        except MexcAPIError as exc:
            _live_order_update(spec["client_order_id"],"rejected",error=exc,submitted=True,final=True)
            raise
        except Exception as exc:
            raise MexcOrderUncertain("Erreur après début du POST; réconcilier avant tout autre ordre") from exc

        order,source,rest_polls=_query_order_until_terminal(spec,first,waiter)
        with private_ws_lock:
            ws_received=waiter.get("ws_received_ts_ms")
            ws_send=waiter.get("ws_send_ts_ms")
            post_started=waiter.get("post_started_ts_ms")
            rest_confirmed=waiter.get("rest_confirmed_ts_ms")
            ws_rest_delta=(float(ws_received-rest_confirmed)
                if ws_received is not None and rest_confirmed is not None else None)
        confirmed=(ws_received if source=="private_ws" and ws_received is not None
            else rest_confirmed if rest_confirmed is not None else now_ms())
        confirm_origin=int(post_started or (http["request_started_ts_ms"] if http else started))
        confirm_ms=max(0,confirmed-confirm_origin)
        ws_post_ms=float(ws_received-confirm_origin) if ws_received is not None else None
        order_id=order.get("orderId"); fills=order.get("fills") if isinstance(order,dict) else None
        if order_id is not None: _private_ws_link_submission(waiter,confirm_origin,order_id)
        commissions=_order_commissions(fills); commission_known=bool(isinstance(fills,list) and fills)
        executed,quote=_order_numbers(order)
        if query_commissions and order_id is not None and executed>0 and not commission_known:
            try:
                trades=live_client.trades(spec["symbol"],order_id)
                commissions=_order_commissions(trades); commission_known=isinstance(trades,list) and bool(trades)
            except Exception as exc: logerr(f"commission query {spec['symbol']} {order_id}: {exc}")
        if executed>0 and not commission_known and query_commissions:
            logerr(f"commission inconnue {spec['symbol']} {order_id}: haircut sécurité appliqué")
        _live_order_update(spec["client_order_id"],str(order.get("status") or "final"),response=order,final=True,
            exchange_order_id=order_id,executed_qty=executed,cumulative_quote_qty=quote,commissions=commissions,
            response_journal_ms=response_journal_ms,settle_ms=confirm_ms,
            confirmation_source=source,rest_poll_count=rest_polls,
            ws_received_ts_ms=ws_received,ws_send_ts_ms=ws_send,ws_post_ms=ws_post_ms,
            ws_rest_delta_ms=ws_rest_delta)
        return {"test":False,"latency_ms":(time.perf_counter_ns()-total_started_ns)/1_000_000.0,
                "http_ms":http["http_ms"] if http else None,
                "http_queue_ms":http["http_queue_ms"] if http else None,
                "response_journal_ms":response_journal_ms,
                "confirm_ms":confirm_ms,"prepare_journal_ms":prepare_journal_ms,
                "request_started_ts_ms":http["request_started_ts_ms"] if http else post_started,
                "confirmed_ts_ms":confirmed,"confirmation_source":source,
                "rest_poll_count":rest_polls,"ws_received_ts_ms":ws_received,
                "ws_send_ts_ms":ws_send,"ws_post_ms":ws_post_ms,
                "ws_rest_delta_ms":ws_rest_delta,
                "order":order,"commissions":commissions,
                "commission_known":commission_known}
    except MexcAPIError:
        raise
    except Exception as exc:
        # Includes a failed final journal commit: the exchange may already
        # have filled the order even when local bookkeeping failed.
        raise MexcOrderUncertain("Résultat/journal ordre non confirmé localement") from exc
    finally:
        _private_ws_release(waiter,keep_rest_probe=(source=="private_ws"))


def _reconcile_order_commissions(result, spec):
    """Complete exact fee accounting after the latency-sensitive route phase."""
    if not result or result.get("test") or result.get("commission_known"): return result
    order=result.get("order") if isinstance(result.get("order"),dict) else {}
    executed,_=_order_numbers(order); order_id=order.get("orderId")
    if order_id is None or executed<=0: return result
    try:
        trades=live_client.trades(spec["symbol"],order_id)
        commissions=_order_commissions(trades)
        known=isinstance(trades,list) and bool(trades)
        if known:
            result["commissions"]=commissions; result["commission_known"]=True
            _live_order_update(spec["client_order_id"],str(order.get("status") or "final"),
                               commissions=commissions)
        else:
            logerr(f"commission différée absente {spec['symbol']} {order_id}: haircut conservé")
    except Exception as exc:
        logerr(f"commission différée {spec['symbol']} {order_id}: {exc}")
    return result


def _fresh_live_signal(route):
    ts=now_ms()
    with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta,age_limit_ms=LIVE_BBO_AGE_MS,allow_small_capacity=True)
    if not x or x.get("net",-1)<SHADOW_MIN_EDGE: return None,None,"edge_or_book"
    if x.get("bbo_skew",999999)>LIVE_MAX_SKEW_MS: return x,None,"book_skew"
    if not x.get("exchange_ts_complete"): return x,None,"route_exchange_timestamp_missing"
    if x.get("exchange_max_age",999999)>LIVE_EXCHANGE_AGE_MS:
        return x,None,"route_exchange_depth_stale"
    if x.get("exchange_skew",999999)>LIVE_MAX_EXCHANGE_SKEW_MS:
        return x,None,"route_exchange_depth_skew"
    if not symbols_post_resync_safe(route,ts): return x,None,"post_resync_grace"
    balances,account_reason=_cached_live_account()
    if account_reason: return x,None,account_reason
    free=float(balances.get(route["path"][0],{}).get("free",0.0))
    # Reserve input-side fees as well, without double charging the edge model.
    budget=min(LIVE_CAP_USD,LIVE_CAPITAL_LIMIT_USD,free*x["start_mark"])/(1.0+LIVE_FEE_SAFETY)
    quality=live_sized_quality(route,x,budget)
    if not quality.get("eligible"): return None,quality,quality.get("reason","quality_rejected")
    return x,quality,None


def _v247_count(reason):
    with live_v247_lock: live_v247_counts[str(reason)]+=1


def _snapshot_freshness(books, local_limit, exchange_limit, skew_limit=None):
    local_now=now_ms(); exchange_now=mexc_now_ms(); sends=[]; receives=[]
    for symbol,book in books.items():
        if not _public_symbol_usable(symbol): return "route_public_feed_recovering"
        received=int(book.get("book_ts",book.get("ts",0)) or 0)
        sent=int(book.get("send_ts") or 0)
        if not sent: return "route_exchange_timestamp_missing"
        if sent-exchange_now>LIVE_FUTURE_TOLERANCE_MS or received>local_now:
            return "route_clock_inconsistent"
        if local_now-received>local_limit: return "route_depth_stale"
        if exchange_now-sent>exchange_limit: return "route_exchange_depth_stale"
        sends.append(sent); receives.append(received)
    if not sends: return "depth_unavailable"
    if skew_limit is not None and max(sends)-min(sends)>skew_limit:
        return "route_exchange_depth_skew"
    if skew_limit is not None and max(receives)-min(receives)>LIVE_MAX_SKEW_MS:
        return "route_depth_skew"
    return None


def live_sized_quality(route,x,budget):
    """Independent LIVE A/B+ at affordable size, using one immutable Depth pair.

    Keep research grades unchanged. Seek the largest passing size (bounded
    bisection, not a backtest optimizer). Never reduce below the user's $10
    minimum or enlarge the $20 cap. Include the .20% inventory reserve once.
    """
    started=time.perf_counter_ns()
    out={"grade":"D","eligible":False,"reason":"depth_unavailable",
         "selected_usd":0.0,"capacity_usd":x.get("size",0.0),"size_fraction":0.0}
    books=_quality_books(route)
    if not books: return out
    reason=_snapshot_freshness(books,LIVE_BBO_AGE_MS,LIVE_EXCHANGE_AGE_MS,LIVE_MAX_EXCHANGE_SKEW_MS)
    if reason: out["reason"]=reason; return out
    with state_lock: meta=state["market_meta"]
    sm=x["start_mark"]; em=x["end_mark"]
    first_market=meta[route["symbols"][0]]
    second_market=meta[route["symbols"][1]]
    if (first_market.get("quote")!=route["path"][0] or first_market.get("base")!=route["path"][1] or
        second_market.get("base")!=route["path"][1] or second_market.get("quote")!=route["path"][2]):
        out["reason"]="unsupported_live_orientation"; return out
    step=_precision_step(first_market,"quoteAssetPrecision")
    def evaluate(usd):
        rounded=_floor_amount(usd/max(sm,1e-12),step)
        usd=float(rounded or 0)*sm
        result=dict(out,selected_usd=usd,size_fraction=usd/max(x.get("size",0),1e-12),
                    reason="below_min_trade",grade="D",eligible=False)
        if usd<MIN_EXEC_USD-1e-8: return result
        trials={s:_quality_roundtrip(route,books,meta,usd,sm,em,s)
                for s in ("normal","half_bbo","drop_l1")}
        normal=trials["normal"]; half=trials["half_bbo"]; drop=trials["drop_l1"]
        coverage=_quality_coverage3(route,books,meta,usd,sm)
        result.update(normal_edge=normal["edge"] if normal else None,
            half_bbo_edge=half["edge"] if half else None,drop_l1_edge=drop["edge"] if drop else None,
            coverage3=coverage,reason="sized_quality_rejected")
        if not normal: return result
        first=normal["fills"][0]
        reserve_mid=first["gross_output"]*(1.0-LIVE_FEE_SAFETY)
        mid_step=_precision_step(meta[route["symbols"][1]],"baseAssetPrecision")
        reserve_mid=float(_floor_amount(reserve_mid,mid_step) or 0.0)
        second=_quality_walk(books[route["symbols"][1]],meta[route["symbols"][1]],
                             route["path"][1],route["path"][2],reserve_mid)
        guard=(second["output"]*em/usd-1 if second and second["fill_ratio"]>=.999999 else -1)
        result["projected_leg2_edge"]=guard
        if guard<LIVE_LEG2_MIN_EDGE:
            result["reason"]="projected_leg2_edge"; return result
        a=bool(drop and drop["edge"]>=SHADOW_MIN_EDGE)
        b=bool(half and drop and half["edge"]>=DEPTH_QUALITY_BPLUS_HALF_EDGE and
               drop["edge"]>=DEPTH_QUALITY_BPLUS_DROP_FLOOR and coverage>=DEPTH_QUALITY_COVERAGE3_MIN)
        result.update(grade="A" if a else "B+" if b else "B-",eligible=a or b,
                      reason="eligible_sized_A" if a else "eligible_sized_B+" if b else "sized_quality_rejected")
        return result
    high=max(0.0,min(float(budget),LIVE_CAP_USD,LIVE_CAPITAL_LIMIT_USD))
    out=evaluate(high)
    if not out["eligible"] and high>MIN_EXEC_USD:
        low=MIN_EXEC_USD+float(step)*sm
        best=evaluate(low)
        if best["eligible"]:
            for _ in range(LIVE_SIZE_SEARCH_STEPS):
                mid=(low+high)/2; candidate=evaluate(mid)
                if candidate["eligible"]: low=mid; best=candidate
                else: high=mid
            out=best
    out["compute_us"]=(time.perf_counter_ns()-started)/1000.0
    out["sizing_scope"]="live_affordable_size"
    out["book_versions"]={s:books[s]["version"] for s in route["symbols"]}
    return out


def _live_health_status(route):
    if TRADING_MODE=="live" and LIVE_EXCLUDED_ASSETS.intersection(route.get("path",())):
        return "route_asset_excluded",None,None,None,None
    if any(not _public_symbol_usable(sym) for sym in route.get("symbols",())):
        return "route_public_feed_recovering",None,None,None,None
    ts=now_ms(); exchange_ts=mexc_now_ms()
    with state_lock:
        if LIVE_REQUIRE_ALL_WS and state.get("ws_connected")!=state.get("ws_expected"): return "websocket_count",None,None,None,None
        if LIVE_REQUIRE_ALL_WS and state.get("depth_ready")!=len(state.get("symbols",[])): return "depth_not_all_ready",None,None,None,None
        workers=[dict(x) for x in state.get("ws_workers",{}).values()]
    if LIVE_REQUIRE_ALL_WS and any(not w.get("connected") or ts-(w.get("last_message_ms") or w.get("opened_ms") or ts)>30000 for w in workers):
        return "websocket_stale",None,None,None,None
    if LIVE_REQUIRE_ROUTE_WS:
        route_symbols=set(route.get("symbols") or ())
        relevant=[w for w in workers if route_symbols.intersection(w.get("symbol_names") or ())]
        covered=set()
        for w in relevant:
            covered.update(route_symbols.intersection(w.get("symbol_names") or ()))
            if not w.get("connected"): return "route_websocket_disconnected",None,None,None,None
            last_ping=w.get("last_ping_ms") or 0; last_pong=w.get("last_pong_ms") or w.get("opened_ms") or 0
            if last_ping>last_pong and ts-last_ping>MEXC_APP_PONG_TIMEOUT_SEC*1000:
                return "route_websocket_pong_timeout",None,None,None,None
        if covered!=route_symbols: return "route_websocket_unmapped",None,None,None,None
    with depth_resync_lock:
        if any(sym in depth_resync_pending for sym in route.get("symbols",())): return "route_resync_pending",None,None,None,None
    with depth_lock:
        times=[]; ages=[]; send_times=[]; exchange_ages=[]
        for sym in route["symbols"]:
            ob=state["depth"].get(sym)
            if not ob or not ob.get("ready"): return "route_depth_not_ready",None,None,None,None
            book_ts=int(ob.get("ts",0)); times.append(book_ts); ages.append(max(0,ts-book_ts))
            try: send_ts=int(ob.get("send_ts") or 0)
            except (TypeError,ValueError): send_ts=0
            if send_ts<=0: return "route_exchange_timestamp_missing",None,None,None,None
            if send_ts-exchange_ts>LIVE_FUTURE_TOLERANCE_MS or book_ts>ts:
                return "route_clock_inconsistent",None,None,None,None
            send_times.append(send_ts); exchange_ages.append(max(0,exchange_ts-send_ts))
    max_age=max(ages) if ages else None
    skew=max(times)-min(times) if times else None
    exchange_age=max(exchange_ages) if exchange_ages else None
    exchange_skew=max(send_times)-min(send_times) if send_times else None
    result=(max_age,skew,exchange_age,exchange_skew)
    if max_age is not None and max_age>LIVE_BBO_AGE_MS: return ("route_depth_stale",)+result
    if skew is not None and skew>LIVE_MAX_SKEW_MS: return ("route_depth_skew",)+result
    if exchange_age is not None and exchange_age>LIVE_EXCHANGE_AGE_MS:
        return ("route_exchange_depth_stale",)+result
    if exchange_skew is not None and exchange_skew>LIVE_MAX_EXCHANGE_SKEW_MS:
        return ("route_exchange_depth_skew",)+result
    return (None,)+result


def _refresh_live_account():
    client=live_account_client or live_client
    with live_lock: refresh_epoch=int(live_runtime.get("trade_epoch",0))
    account=client.account(); balances={}
    for row in account.get("balances",[]) if isinstance(account,dict) else []:
        asset=str(row.get("asset") or "").upper()
        try: free=float(row.get("free") or 0.0); locked=float(row.get("locked") or 0.0)
        except (TypeError,ValueError): continue
        if asset and free+locked>0: balances[asset]={"free":free,"locked":locked,"total":free+locked}
    with live_lock:
        # If a route began while the REST request was in flight, discard this
        # snapshot: it may contain the temporary intermediate asset.
        if live_runtime.get("busy") or int(live_runtime.get("trade_epoch",0))!=refresh_epoch:
            return None
        live_runtime["api_ok"]=bool(account.get("canTrade"))
        live_runtime["account_ts_ms"]=now_ms(); live_runtime["balances"]=balances
        if not account.get("canTrade"): live_runtime["last_error"]="canTrade=false"
    # Balance snapshots are refreshable cache, not safety-critical journal
    # records. Avoid a FULL fsync every five seconds on the order lock.
    return balances


def _cached_live_account():
    """Return a recent account snapshot without putting REST on the trade path."""
    ts=now_ms()
    with live_lock:
        api_ok=bool(live_runtime.get("api_ok")); account_ts=live_runtime.get("account_ts_ms")
        balances={asset:dict(row) for asset,row in live_runtime.get("balances",{}).items()}
    if not api_ok or account_ts is None:
        live_account_refresh_event.set()
        return None,"private_api_unavailable"
    if ts-int(account_ts)>LIVE_ACCOUNT_CACHE_MAX_AGE_MS:
        live_account_refresh_event.set()
        return None,"account_cache_stale"
    return balances,None


def _asset_mark_usd(asset, books, meta):
    if asset in STABLES: return stable_mark_usdt(asset,books,meta)
    for stable in STABLES:
        sym,m=direct_pair(asset,stable,meta)
        b=books.get(sym) if sym else None
        if not m or not b or now_ms()-b.get("ts",0)>MAX_BBO_AGE_MS: continue
        stable_mark=stable_mark_usdt(stable,books,meta)
        if m["base"]==asset and b.get("bid",0)>0: return b["bid"]*stable_mark
        if m["quote"]==asset and b.get("ask",0)>0: return stable_mark/b["ask"]
    return None


def _live_pnl_breakdown(input_usd, cash_output_usd, mid_acquired, disposed_units,
                        residual_units, residual_mark_usd):
    """Split realized and marked PnL without calling inventory a cash loss."""
    acquired=max(0.0,float(mid_acquired or 0.0))
    disposed=max(0.0,min(acquired,float(disposed_units or 0.0)))
    fraction=min(1.0,disposed/max(acquired,1e-18)) if acquired>0 else 0.0
    realized_cost=float(input_usd or 0.0)*fraction
    residual_cost=max(0.0,float(input_usd or 0.0)-realized_cost)
    realized=float(cash_output_usd or 0.0)-realized_cost
    residual_value=(float(residual_units or 0.0)*float(residual_mark_usd)
                    if residual_mark_usd is not None else None)
    unrealized=(residual_value-residual_cost) if residual_value is not None else None
    economic=(realized+unrealized) if unrealized is not None else None
    return {"realized_cost_usd":realized_cost,"residual_cost_usd":residual_cost,
            "realized_pnl":realized,"residual_value_usd":residual_value,
            "unrealized_pnl":unrealized,"economic_pnl":economic}


def _protected_bag_reason(balances):
    if not PROTECT_EXISTING_BAGS: return None
    with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
    for asset,row in balances.items():
        if asset in MEXC_ALLOWED_START_ASSETS: continue
        total=float(row.get("total",0.0))
        if total<=0: continue
        mark=_asset_mark_usd(asset,books,meta)
        if mark is None: return f"protected_unknown_asset:{asset}"
        if total*mark>LIVE_DUST_USD: return f"protected_asset:{asset}:{total*mark:.4f}USD"
    return None


def _route_has_recent_test(route_id):
    # FOK is a different order shape: an old MARKET test cannot certify it.
    return _capability_matches_policy(_route_capability(route_id))


def _test_best_order_spec(attempt_id, leg, purpose, symbol, frm, to, input_units, market, meta,
                          market_only=False):
    errors=[]
    candidates=live_order_candidates(symbol,frm,to,input_units,market,meta,
        lambda order_type:_live_client_id(attempt_id,purpose,order_type),
        age_limit_ms=LIVE_TEST_PROBE_DEPTH_AGE_MS)
    candidates=_policy_candidates(candidates,market_only)
    if not candidates: raise MexcAPIError(f"Unwind MARKET impossible à construire pour {symbol}")
    for fallback_index,spec in enumerate(candidates):
        spec["fallback_index"]=fallback_index
        try:
            response=_submit_live_order(attempt_id,leg,purpose+"_probe",spec,True)
            return spec,response
        except Exception as exc:
            errors.append(f"{spec['order_type']}/{spec['amount_mode']}: {exc}")
    raise MexcAPIError(f"Aucune forme d'ordre validée pour {purpose} {symbol}: " + " | ".join(errors))


def _live_test_attempt(q, attempt_id, x, quality, requested_usd, balances, meta):
    route=q["route"]; start,mid,end=route["path"]; sm=x["start_mark"]
    input_units=requested_usd/max(sm,1e-12)
    t0=int(q.get("decision_ts_ms") or now_ms()); admitted_ts=int(q.get("gateway_admit_ts_ms") or now_ms())
    # The opportunity was already admitted from <= LIVE_BBO_AGE_MS. This wider
    # age only builds syntax probes for /order/test (which never reaches the
    # matching engine); it cannot authorize a real order.
    fill1=depth_walk(route["symbols"][0],start,mid,input_units,meta,age_limit_ms=LIVE_TEST_PROBE_DEPTH_AGE_MS)
    if not fill1 or fill1.get("fill_ratio",0)<0.999999: raise MexcAPIError("Depth jambe 1 devenu inexécutable")
    _live_attempt_update(attempt_id,status="testing",input_units=input_units,
                         t0_to_leg1_submit_ms=max(0,now_ms()-t0))
    spec1,r1=_test_best_order_spec(attempt_id,1,"leg1",route["symbols"][0],start,mid,input_units,
                                   meta[route["symbols"][0]],meta)
    l1_done=now_ms()
    l2_submit=now_ms()
    spec2,r2=_test_best_order_spec(attempt_id,2,"leg2",route["symbols"][1],mid,end,fill1["output"],
                                   meta[route["symbols"][1]],meta)
    uwspec,ruw=_test_best_order_spec(attempt_id,3,"unwind",route["symbols"][0],mid,start,fill1["output"],
                                     meta[route["symbols"][0]],meta,market_only=True)
    _save_route_capability(route["id"],q["decision_id"],spec1,spec2,uwspec)
    _live_attempt_update(attempt_id,status="test_validated",mid_acquired=fill1["output"],mid_used=fill1["output"],
                         leg1_client_id=spec1["client_order_id"],leg2_client_id=spec2["client_order_id"],
                         unwind_client_id=uwspec["client_order_id"],leg1_order_type=spec1["order_type"],
                         leg2_order_type=spec2["order_type"],unwind_order_type=uwspec["order_type"],
                         leg1_latency_ms=r1["latency_ms"],leg2_latency_ms=r2["latency_ms"],
                         leg1_post_http_ms=r1.get("http_ms"),leg2_post_http_ms=r2.get("http_ms"),
                         leg1_http_queue_ms=r1.get("http_queue_ms"),leg2_http_queue_ms=r2.get("http_queue_ms"),
                         leg1_confirm_ms=r1.get("confirm_ms"),leg2_confirm_ms=r2.get("confirm_ms"),
                         leg1_prepare_journal_ms=r1.get("prepare_journal_ms"),
                         leg2_prepare_journal_ms=r2.get("prepare_journal_ms"),
                         t0_to_leg1_submit_ms=max(0,int(r1.get("request_started_ts_ms") or t0)-t0),
                         admission_to_leg1_post_ms=max(0,int(r1.get("request_started_ts_ms") or admitted_ts)-admitted_ts),
                         t0_to_leg1_done_ms=max(0,l1_done-t0),
                         leg1_done_to_leg2_submit_ms=max(0,l2_submit-l1_done),
                         t0_to_done_ms=max(0,now_ms()-t0))
    return "test_validated",r1["latency_ms"],r2["latency_ms"],None


def _prevalidation_test_best(route_id, leg, purpose, symbol, frm, to, input_units, market, meta,
                             market_only=False):
    """Probe one order shape on a dedicated TEST-only HTTP session."""
    if live_prevalidation_client is None:
        raise MexcAPIError("Client de prévalidation indisponible")
    nonce=uuid.uuid4().hex
    candidates=live_order_candidates(symbol,frm,to,input_units,market,meta,
        lambda order_type:("v247PV"+str(leg)+order_type[:1]+nonce[-22:])[:32],
        age_limit_ms=LIVE_PREVALIDATE_DEPTH_AGE_MS)
    candidates=_policy_candidates(candidates,market_only)
    if not candidates: raise MexcAPIError(f"Prévalidation unwind MARKET impossible pour {symbol}")
    errors=[]
    for fallback_index,spec in enumerate(candidates):
        spec["fallback_index"]=fallback_index
        try:
            _,timing=live_prevalidation_client.new_order(
                spec["symbol"],spec["side"],spec["order_type"],spec["client_order_id"],
                quantity=spec.get("quantity"),quote_order_qty=spec.get("quote_order_qty"),
                price=spec.get("price"),test=True,with_timing=True)
            return spec,float(timing.get("http_ms") or 0.0)
        except Exception as exc:
            errors.append(f"{spec['order_type']}/{spec['amount_mode']}: {exc}")
    raise MexcAPIError(f"Prévalidation impossible pour {purpose} {symbol}: " + " | ".join(errors))


def _route_needs_prevalidation(route_id, ts=None):
    ts=int(ts or now_ms())
    if ts<prevalidation_retry_after.get(route_id,0): return False
    with live_lock: item=live_route_capabilities.get(route_id)
    if not item or not _capability_matches_policy(item): return True
    return ts-int(item.get("validated_ts_ms") or 0)>=int(LIVE_PREVALIDATE_REFRESH_HOURS*3600000)


def _prevalidate_route(route):
    """Validate leg1/leg2/unwind syntax without requiring an arbitrage signal."""
    if not _route_needs_prevalidation(route["id"]): return "recent",None
    with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta,age_limit_ms=LIVE_PREVALIDATE_DEPTH_AGE_MS)
    if not x: return "depth",None
    requested=min(LIVE_CAP_USD,LIVE_CAPITAL_LIMIT_USD,float(x.get("size") or 0.0))
    if requested<MIN_EXEC_USD: return "size",None
    start,mid,end=route["path"]; input_units=requested/max(float(x["start_mark"]),1e-12)
    fill1=depth_walk(route["symbols"][0],start,mid,input_units,meta,
                     age_limit_ms=LIVE_PREVALIDATE_DEPTH_AGE_MS)
    if not fill1 or fill1.get("fill_ratio",0)<0.999999: return "depth",None
    http_total=0.0
    spec1,ms=_prevalidation_test_best(route["id"],1,"leg1",route["symbols"][0],
        start,mid,input_units,meta[route["symbols"][0]],meta); http_total+=ms
    spec2,ms=_prevalidation_test_best(route["id"],2,"leg2",route["symbols"][1],
        mid,end,fill1["output"],meta[route["symbols"][1]],meta); http_total+=ms
    unwind,ms=_prevalidation_test_best(route["id"],3,"unwind",route["symbols"][0],
        mid,start,fill1["output"],meta[route["symbols"][0]],meta,market_only=True); http_total+=ms
    _save_route_capability(route["id"],f"prevalidate:{route['id']}@{now_ms()}",spec1,spec2,unwind)
    return "validated",http_total


def live_prevalidation_worker():
    """Progressively cover all 800 routes in TEST, fully outside the order path."""
    if not LIVE_PREVALIDATE_ENABLED or TRADING_MODE!="test" or live_prevalidation_client is None:
        return
    with live_lock: prevalidation_runtime["running"]=True
    while True:
        with state_lock: routes=list(state.get("routes") or [])
        # Untested routes first; old capabilities are renewed before 72 h.
        with live_lock:
            routes.sort(key=lambda r:int((live_route_capabilities.get(r["id"]) or {}).get("validated_ts_ms") or 0))
            prevalidation_runtime["cycle_started_ts_ms"]=now_ms()
            prevalidation_runtime["cycle_completed_ts_ms"]=None
            prevalidation_runtime["checked"]=0
        api_calls=0
        for route in routes:
            if TRADING_MODE!="test": return
            with live_lock:
                prevalidation_runtime["checked"]+=1
                busy=bool(live_runtime.get("busy"))
            if busy:
                time.sleep(0.10)
                continue
            if not _route_needs_prevalidation(route["id"]): continue
            try:
                status,latency=_prevalidate_route(route)
                with live_lock:
                    prevalidation_runtime["last_route"]=route["id"]
                    prevalidation_runtime["last_status"]=status
                    prevalidation_runtime["last_latency_ms"]=latency
                    prevalidation_runtime["last_error"]=None
                    if status=="validated": prevalidation_runtime["validated"]+=1
                    elif status=="depth": prevalidation_runtime["skipped_depth"]+=1
                    elif status=="size": prevalidation_runtime["skipped_size"]+=1
                if status=="validated":
                    api_calls+=3
                    time.sleep(LIVE_PREVALIDATE_INTERVAL_SEC)
            except Exception as exc:
                prevalidation_retry_after[route["id"]]=now_ms()+LIVE_PREVALIDATE_ERROR_BACKOFF_MS
                with live_lock:
                    prevalidation_runtime["errors"]+=1
                    prevalidation_runtime["last_route"]=route["id"]
                    prevalidation_runtime["last_status"]="error"
                    prevalidation_runtime["last_error"]=str(exc)[:500]
                # An endpoint/rate error gets the same conservative pacing.
                time.sleep(LIVE_PREVALIDATE_INTERVAL_SEC)
        with live_lock: prevalidation_runtime["cycle_completed_ts_ms"]=now_ms()
        # Fast no-Depth passes must not spin; completed coverage is revisited
        # periodically to catch quiet books and capabilities approaching 72 h.
        time.sleep(30.0 if api_calls else 10.0)


def _exit_confirmation_budget_ms():
    samples=sorted(float(x) for x in list(live_leg1_confirm_ms)[-200:]
        if x is not None and math.isfinite(float(x)) and float(x)>0)
    observed=samples[min(len(samples)-1,math.ceil(.95*len(samples))-1)] if samples else 0
    return max(LIVE_EXIT_CONFIRM_FLOOR_MS,observed)+LIVE_EXIT_PREP_MARGIN_MS


def _entry_exit_headroom(route):
    sym=route['symbols'][1]
    with depth_lock: ob=dict(state['depth'].get(sym) or {})
    reason=_snapshot_freshness({sym:ob},LIVE_BBO_AGE_MS,LIVE_EXCHANGE_AGE_MS)
    if not ob.get('ready'): reason='route_depth_not_ready'
    if reason: raise MexcSignalExpired(reason)
    budget=_exit_confirmation_budget_ms()
    age=max(0,now_ms()-int(ob.get('ts') or 0))
    exchange_age=max(0,mexc_now_ms()-int(ob.get('send_ts') or 0))
    if age+budget>LIVE_LEG2_BBO_AGE_MS:
        raise MexcSignalExpired(f'exit_local_headroom: {age}+{budget:g} ms > {LIVE_LEG2_BBO_AGE_MS} ms')
    if exchange_age+budget>LIVE_LEG2_EXCHANGE_AGE_MS:
        raise MexcSignalExpired(f'exit_exchange_headroom: {exchange_age}+{budget:g} ms > {LIVE_LEG2_EXCHANGE_AGE_MS} ms')
    return {'local_age_ms':age,'exchange_age_ms':exchange_age,'budget_ms':budget}


def _entry_price_guard(route,spec,t0,requested_usd):
    if now_ms()-t0>LIVE_MAX_T0_TO_LEG1_POST_MS:
        raise MexcSignalExpired("signal_too_old_before_post")
    arm=live_arm_status()
    if not arm["effective"]: raise MexcSignalExpired("gate_"+arm["reason"])
    reason,*_= _live_health_status(route)
    if reason: raise MexcSignalExpired(reason)
    _entry_exit_headroom(route)
    with state_lock: meta=state["market_meta"]; books=dict(state["bbo"])
    if spec["order_type"]=="FILL_OR_KILL":
        # At the frozen entry price, including quantity/price rounding, the
        # subsequent exit must still clear the guard at its worst used bid.
        cost=float(spec["quantity"])*float(spec["price"])
        sm=stable_mark_usdt(route["path"][0],books,meta)
        if cost*sm>requested_usd+1e-8: raise MexcSignalExpired("rounded_entry_over_budget")
        mid=float(spec["quantity"])*(1.0-LIVE_FEE_SAFETY)
        sell_qty=float(_floor_amount(mid,_precision_step(meta[route["symbols"][1]],"baseAssetPrecision")) or 0)
        fill=depth_walk(route["symbols"][1],route["path"][1],route["path"][2],sell_qty,meta,
                        age_limit_ms=LIVE_BBO_AGE_MS)
        em=stable_mark_usdt(route["path"][2],books,meta)
        worst=fill.get("worst_price") if fill else None
        if not worst or fill["fill_ratio"]<.999999:
            raise MexcSignalExpired("sized_exit_depth_unavailable")
        worst=float(_floor_amount(worst,_precision_step(meta[route["symbols"][1]],"quotePrecision")) or 0)
        edge=sell_qty*worst*(1-FEE)*em/max(cost*sm,1e-12)-1
        if edge<LIVE_LEG2_MIN_EDGE: raise MexcSignalExpired("rounded_price_edge_below_guard")


def _leg2_post_guard(route,spec,fill_ts,input_usd):
    sym=route["symbols"][1]
    with depth_lock: ob=dict(state["depth"].get(sym) or {})
    reason=_snapshot_freshness({sym:ob},LIVE_LEG2_BBO_AGE_MS,LIVE_LEG2_EXCHANGE_AGE_MS)
    if not ob.get("ready"): reason="route_depth_not_ready"
    if fill_ts and fill_ts-int(ob.get("send_ts") or 0)>LIVE_LEG2_MAX_EXCHANGE_SKEW_MS:
        reason="leg2_event_skew"
    if reason: raise MexcSignalExpired(reason)
    if spec["order_type"]=="FILL_OR_KILL":
        with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
        edge=float(spec["quantity"])*float(spec["price"])*(1-FEE)*stable_mark_usdt(route["path"][2],books,meta)/max(input_usd,1e-12)-1
        if edge<LIVE_LEG2_MIN_EDGE: raise MexcSignalExpired("rounded_leg2_price_edge")


def _unwind_retry_inventory(result,spec,remaining,baseline_units,asset):
    """A second SELL requires an independent, identical terminal REST result.

    Never interpret a timeout, a WS/REST mismatch or an unavailable balance as
    permission to sell again. The pre-trade inventory is reserved for its owner.
    """
    try:
        order=(live_order_status_client or live_client).order(spec['symbol'],spec['client_order_id'])
    except Exception as exc:
        raise MexcOrderUncertain('Unwind: confirmation REST indisponible avant seconde sortie') from exc
    before=result['order']
    if (not _private_ws_order_is_authoritative(spec,order) or
        'executedQty' not in order or not any(k in order for k in ('cummulativeQuoteQty','cumulativeQuoteQty')) or
        str(order.get('orderId'))!=str(before.get('orderId')) or
        str(order.get('side','')).upper()!='SELL' or
        str(order.get('status','')).upper()!=str(before.get('status','')).upper() or
        any(not math.isclose(a,b,rel_tol=1e-8,abs_tol=1e-10)
            for a,b in zip(_order_numbers(order),_order_numbers(before)))):
        raise MexcOrderUncertain('Unwind: résultats WS/REST divergents, aucune seconde vente')
    executed,_=_order_numbers(order)
    if executed>0 and not result.get('commission_known'):
        raise MexcAPIError('Unwind partiel: commission inconnue, seconde sortie bloquée')
    account=(live_account_client or live_client).account()
    if not isinstance(account,dict) or account.get('canTrade') is not True:
        raise MexcAPIError('Unwind: solde de vente non confirmé')
    row=next((r for r in account.get('balances',[]) if r.get('asset')==asset),None)
    if row is None: raise MexcAPIError('Unwind: actif absent du relevé de solde')
    free=float(row.get('free',0))
    if not math.isfinite(free) or free<0: raise MexcAPIError('Unwind: solde invalide')
    return min(remaining,max(0.0,free-baseline_units))


def _bounded_emergency_unwind(attempt_id,route,remaining,balances,meta,capability,
                              estimated_unit_value,attempt_values):
    """At most two immediate MARKET exits, only for this attempt's inventory.

    This is not a background liquidation loop. Uncertain orders stop the chain;
    unsuccessful confirmed exits leave a durable exposure and a visible circuit.
    """
    start,asset,_=route['path']; symbol=route['symbols'][0]
    baseline=balances.get(asset) or {}
    try:
        baseline_units=float(baseline.get('total',float(baseline.get('free',0))+float(baseline.get('locked',0))))
        if not math.isfinite(baseline_units) or baseline_units<0:raise ValueError()
    except (ValueError,TypeError):
        return [],0.0,0.0,MexcAPIError('Unwind: inventaire initial invalide'),None
    records=[];used=output=0.0;error=None;last_spec=None
    deadline=time.monotonic()+LIVE_UNWIND_RETRY_WINDOW_MS/1000.0
    for index in range(LIVE_UNWIND_MAX_ATTEMPTS):
        left=max(0.0,remaining-used)
        if left<=0 or left*estimated_unit_value<=LIVE_DUST_USD: break
        try:
            quantity=left
            if index:
                if time.monotonic()>=deadline:
                    raise MexcAPIError('Unwind: fenêtre de seconde sortie expirée')
                previous,previous_spec=records[-1]
                quantity=_unwind_retry_inventory(previous,previous_spec,left,baseline_units,asset)
                if quantity<=0: raise MexcAPIError('Unwind: aucun solde libre propre au trade')
            client_id=(_live_client_id(attempt_id,'unwind') if index==0 else
                       ('v2411UW'+str(index)+attempt_id[-22:])[:32])
            spec=live_emergency_market_spec(symbol,asset,start,quantity,meta[symbol],client_id,capability)
            last_spec=spec
            if index:
                def guard(ts):
                    if time.monotonic()>=deadline:
                        raise MexcSignalExpired('Unwind: fenêtre de seconde sortie expirée avant POST')
                spec['_post_guard']=guard
            purpose='unwind' if index==0 else 'unwind_retry'
            prep=_live_prepare_followup_order(attempt_id,3+index,purpose,spec,
                **dict(attempt_values,status='unwind_submitting',unwind_client_id=client_id,
                       unwind_order_type=spec['order_type'],unwind_input=used,unwind_output=output))
            spec['_prepare_journal_ms']=prep
            result=_submit_live_order(attempt_id,3+index,purpose,spec,False,prepared=True)
            records.append((result,spec))
            effects=_order_effects(result['order'],result['commissions'],meta[symbol],spec['side'],result['commission_known'])
            if not all(math.isfinite(float(effects[k])) and effects[k]>=0 for k in ('input','output')):
                raise MexcOrderUncertain('Unwind: quantités finales invalides')
            if effects['input']>left+1e-8:
                raise MexcOrderUncertain('Unwind: quantité consommée supérieure à l’inventaire suivi')
            used+=effects['input'];output+=effects['output']
            if max(0.0,remaining-used)*estimated_unit_value>LIVE_DUST_USD:
                detail=f"Unwind {result['order'].get('status')}: exécuté {effects['input']:g}, reste {remaining-used:g} {asset}"
                _live_order_update(client_id,str(result['order'].get('status') or 'final'),error=detail)
                error=MexcAPIError(detail)
            else: error=None
        except MexcOrderUncertain:
            _live_attempt_update(attempt_id,status='reconciliation_required',
                unwind_input=used,unwind_output=output,realized_pnl=None,
                error='Sortie incertaine: aucune vente supplémentaire automatique')
            raise
        except Exception as exc:
            error=exc;break
    return records,used,output,error,last_spec


def _live_real_attempt(q, attempt_id, x, quality, requested_usd, balances, meta):
    route=q["route"]; start,mid,end=route["path"]; sm=x["start_mark"]
    capability=_route_capability(route["id"])
    if not capability: raise MexcAPIError("Route sans capacité /order/test récente")
    if not _capability_matches_policy(capability):
        raise MexcAPIError("Route sans prévalidation correspondant à la politique de prix")
    t0=int(q.get("decision_ts_ms") or now_ms()); admitted_ts=int(q.get("gateway_admit_ts_ms") or now_ms())
    if now_ms()-t0>LIVE_MAX_T0_TO_LEG1_POST_MS:
        raise MexcSignalExpired("Signal trop ancien avant préparation de la jambe 1")
    requested_units=requested_usd/max(sm,1e-12); l1id=_live_client_id(attempt_id,"leg1")
    spec1=live_order_spec(route["symbols"][0],start,mid,requested_units,
        meta[route["symbols"][0]],meta,l1id,capability["leg1"],
        exchange_age_limit_ms=LIVE_EXCHANGE_AGE_MS)
    _entry_price_guard(route,spec1,t0,requested_usd)
    spec1["_post_guard"]=lambda ts:_entry_price_guard(route,spec1,t0,requested_usd)
    prep1=_live_prepare_initial_order(q,requested_usd,x,attempt_id,spec1,t0,admitted_ts)
    q["attempt_durable"]=True
    spec1["_prepare_journal_ms"]=prep1
    # The crash-safe journal is mandatory, but a slow fsync must never turn an
    # old quote into a real order. A prepared/not-submitted row is finalized so
    # startup reconciliation will not mistake it for an uncertain POST.
    post_health_reason,post_recv_age,post_recv_skew,post_exchange_age,post_exchange_skew=_live_health_status(route)
    if now_ms()-t0>LIVE_MAX_T0_TO_LEG1_POST_MS or post_health_reason:
        detail=(post_health_reason or "signal_too_old_before_post")
        _live_order_update(l1id,"expired_before_submit",final=True)
        _live_attempt_update(attempt_id,status="expired_before_submit",
            t0_to_leg1_submit_ms=max(0,now_ms()-t0),error=detail)
        raise MexcSignalExpired(detail)
    r1=_submit_live_order(attempt_id,1,"leg1",spec1,False,prepared=True,
                          query_commissions=not LIVE_DEFER_LEG1_COMMISSION)
    l1_done=int(r1.get("confirmed_ts_ms") or now_ms())
    e1=_order_effects(r1["order"],r1["commissions"],meta[route["symbols"][0]],spec1["side"],r1["commission_known"])
    if e1["output"]<=0:
        _live_finalize_attempt(attempt_id,"entry_canceled",input_units=0.0,mid_acquired=0.0,
            realized_pnl=0.0,cash_output_usd=0.0,
            leg1_latency_ms=r1["latency_ms"],leg1_post_http_ms=r1.get("http_ms"),
            leg1_confirm_ms=r1.get("confirm_ms"),
            t0_to_leg1_submit_ms=max(0,int(r1.get("request_started_ts_ms") or t0)-t0),
            t0_to_done_ms=max(0,now_ms()-t0))
        return "entry_canceled",r1["latency_ms"],None,0.0
    mid_acquired=e1["output"]; l2id=_live_client_id(attempt_id,"leg2")
    mid_used=0.0; end_out=0.0; r2=None; leg2_error=None; guard_edge=None
    leg2_exchange_age=None; leg2_exchange_skew=None
    leg1_exchange_ts=_order_exchange_ts_ms(r1.get("order"))
    try:
        # Recompute leg 2 from the acquired intermediate balance after exactly
        # one 0.20% conservative haircut. Continue at >=0.15%; otherwise unwind.
        fill2=depth_walk(route["symbols"][1],mid,end,mid_acquired,meta,age_limit_ms=LIVE_LEG2_BBO_AGE_MS)
        if not fill2:
            with depth_lock:
                ob=state["depth"].get(route["symbols"][1]) or {}
                ready=bool(ob.get("ready")); book_ts=int(ob.get("ts") or 0)
                try: leg2_send_ts=int(ob.get("send_ts") or 0)
                except (TypeError,ValueError): leg2_send_ts=0
            age=max(0,now_ms()-book_ts) if book_ts else None
            if leg2_send_ts>0:
                leg2_exchange_age=max(0,mexc_now_ms()-leg2_send_ts)
                if leg1_exchange_ts is not None:
                    leg2_exchange_skew=leg1_exchange_ts-leg2_send_ts
            if not ready: detail="Depth absent/non prêt"
            elif age is not None and age>LIVE_LEG2_BBO_AGE_MS:
                detail=f"Depth ancien {age} ms > {LIVE_LEG2_BBO_AGE_MS} ms"
            else: detail="liquidité absente"
            raise MexcAPIError(f"Jambe 2 devenue inexécutable: {detail}")
        if fill2.get("fill_ratio",0)<0.999999:
            raise MexcAPIError("Jambe 2 profondeur insuffisante: "
                f"{100*float(fill2.get('fill_ratio') or 0):.2f}% de la quantité")
        try: leg2_send_ts=int(fill2.get("send_ts") or 0)
        except (TypeError,ValueError): leg2_send_ts=0
        if leg2_send_ts<=0:
            raise MexcAPIError("Jambe 2 sans timestamp MEXC Depth")
        leg2_exchange_age=max(0,mexc_now_ms()-leg2_send_ts)
        if leg2_exchange_age>LIVE_LEG2_EXCHANGE_AGE_MS:
            raise MexcAPIError("Jambe 2 Depth MEXC ancien: "
                f"{leg2_exchange_age} ms > {LIVE_LEG2_EXCHANGE_AGE_MS} ms")
        if leg1_exchange_ts is not None:
            # Positive: the leg-2 quote predates the real leg-1 fill.  A newer
            # quote is safe and produces a negative value, so it is not rejected.
            leg2_exchange_skew=leg1_exchange_ts-leg2_send_ts
            if leg2_exchange_skew>LIVE_LEG2_MAX_EXCHANGE_SKEW_MS:
                raise MexcAPIError("Jambe 2 désynchronisée du fill jambe 1: "
                    f"{leg2_exchange_skew} ms > {LIVE_LEG2_MAX_EXCHANGE_SKEW_MS} ms")
        with state_lock: live_books=dict(state["bbo"])
        input_usd_guard=e1["input"]*stable_mark_usdt(start,live_books,meta)
        output_usd_guard=fill2["output"]*stable_mark_usdt(end,live_books,meta)
        guard_edge=output_usd_guard/max(input_usd_guard,1e-12)-1.0
        if guard_edge<LIVE_LEG2_MIN_EDGE:
            raise MexcAPIError(f"Edge après jambe 1 insuffisant: {guard_edge*100:.4f}% < {LIVE_LEG2_MIN_EDGE*100:.4f}%")
        spec2=live_order_spec(route["symbols"][1],mid,end,mid_acquired,
            meta[route["symbols"][1]],meta,l2id,capability["leg2"],
            age_limit_ms=LIVE_LEG2_BBO_AGE_MS,
            exchange_age_limit_ms=LIVE_LEG2_EXCHANGE_AGE_MS)
        spec2["_post_guard"]=lambda ts:_leg2_post_guard(route,spec2,leg1_exchange_ts,input_usd_guard)
        prep2=_live_prepare_followup_order(attempt_id,2,"leg2",spec2,status="leg2_submitting",
            input_units=e1["input"],mid_acquired=mid_acquired,leg1_client_id=l1id,leg2_client_id=l2id,
            leg1_order_type=spec1["order_type"],leg2_order_type=spec2["order_type"],
            leg1_latency_ms=r1["latency_ms"],leg1_post_http_ms=r1.get("http_ms"),
            leg1_http_queue_ms=r1.get("http_queue_ms"),
            leg1_confirm_ms=r1.get("confirm_ms"),leg1_prepare_journal_ms=prep1,
            t0_to_leg1_submit_ms=max(0,int(r1.get("request_started_ts_ms") or t0)-t0),
            admission_to_leg1_post_ms=max(0,int(r1.get("request_started_ts_ms") or admitted_ts)-admitted_ts),
            t0_to_leg1_done_ms=max(0,l1_done-t0),leg2_guard_edge=guard_edge,
            leg2_exchange_age_ms=leg2_exchange_age,
            leg2_exchange_skew_ms=leg2_exchange_skew)
        spec2["_prepare_journal_ms"]=prep2
        r2=_submit_live_order(attempt_id,2,"leg2",spec2,False,prepared=True,query_commissions=False)
        e2=_order_effects(r2["order"],r2["commissions"],meta[route["symbols"][1]],spec2["side"],r2["commission_known"])
        mid_used=min(mid_acquired,e2["input"]); end_out=e2["output"]
    except MexcOrderUncertain:
        # L2 might already have sold the acquired asset. Never fire a second
        # SELL against that uncertain balance (nor against protected bags).
        _live_attempt_update(attempt_id,status="reconciliation_required",
            input_units=e1["input"],mid_acquired=mid_acquired,
            realized_pnl=None,error="Statut jambe 2 inconnu: aucun unwind automatique")
        raise
    except Exception as exc:
        leg2_error=exc
    remaining=max(0.0,mid_acquired-mid_used)
    # Do not manufacture an unwind failure from harmless precision/fee dust.
    # The valuation may use an old BBO because it only decides whether the
    # residual is material; it never supplies a price to the MARKET order.
    with state_lock: unwind_books=dict(state["bbo"])
    remaining_mark=_asset_mark_usd(mid,unwind_books,meta)
    remaining_value_est=(remaining*remaining_mark if remaining_mark is not None else
        requested_usd*remaining/max(mid_acquired,1e-18))
    unwind_required=bool(remaining>0 and remaining_value_est>LIVE_DUST_USD)
    unwind_used=0.0; unwind_out=0.0; uwid=None; unwind_error=None; uwspec=None
    unwind_records=[]
    if unwind_required:
        unit_value=remaining_value_est/max(remaining,1e-18)
        unwind_records,unwind_used,unwind_out,unwind_error,uwspec=_bounded_emergency_unwind(
            attempt_id,route,remaining,balances,meta,capability['unwind'],unit_value,
            {'input_units':e1['input'],'mid_acquired':mid_acquired,'mid_used':mid_used,
             'output_units':end_out,'leg1_client_id':l1id})
        uwid=uwspec['client_order_id'] if uwspec else None
    # Once the market-risk phase is over, replace the conservative leg-1 fee
    # haircut with the exact commission.  This REST call can no longer delay
    # leg 2 or the emergency unwind.
    if LIVE_DEFER_LEG1_COMMISSION:
        _reconcile_order_commissions(r1,spec1)
    if r2:
        _reconcile_order_commissions(r2,spec2)
        e2=_order_effects(r2["order"],r2["commissions"],meta[route["symbols"][1]],spec2["side"],True)
        mid_used=e2["input"]; end_out=e2["output"]
    fee_results=[(r1,spec1)]+([(r2,spec2)] if r2 else [])+unwind_records
    fees_known=all(r.get("commission_known") or _order_numbers(r["order"])[0]<=0 for r,_ in fee_results)
    e1=_order_effects(r1["order"],r1["commissions"],meta[route["symbols"][0]],
                      spec1["side"],True)
    mid_acquired=e1["output"]
    residual=max(0.0,mid_acquired-mid_used-unwind_used)
    with state_lock: books=dict(state["bbo"])
    input_usd=e1["input"]*stable_mark_usdt(start,books,meta)
    output_usd=end_out*stable_mark_usdt(end,books,meta)+unwind_out*stable_mark_usdt(start,books,meta)
    # Fees paid in a third asset (e.g. MX) also belong to the realized result.
    extra_fees=0.0
    for result,spec in fee_results:
        market=meta[spec["symbol"]]
        for asset,amount in result.get("commissions",{}).items():
            if asset in (market["base"],market["quote"]): continue
            mark=_asset_mark_usd(asset,books,meta)
            if mark is None: fees_known=False
            else: extra_fees+=amount*mark
    output_usd-=extra_fees
    disposed_units=max(0.0,min(mid_acquired,mid_used+unwind_used))
    residual_mark=_asset_mark_usd(mid,books,meta)
    pnl_parts=_live_pnl_breakdown(input_usd,output_usd,mid_acquired,disposed_units,
                                  residual,residual_mark)
    residual_usd=pnl_parts["residual_value_usd"] or 0.0
    realized_cost_usd=pnl_parts["realized_cost_usd"]
    residual_cost_usd=pnl_parts["residual_cost_usd"]
    realized_pnl=pnl_parts["realized_pnl"]
    unrealized_pnl=pnl_parts["unrealized_pnl"]
    economic_pnl=pnl_parts["economic_pnl"]
    material_residual=bool(residual>0 and (residual_mark is None or residual_usd>LIVE_DUST_USD))
    status=("open_exposure" if material_residual else
            ("forced_unwind" if unwind_required else "completed"))
    if not fees_known:
        # A haircut is a reserve, not an actual fee or a realized loss.
        realized_pnl=None; economic_pnl=None; unrealized_pnl=None
        if status!="open_exposure": status="accounting_pending"
    _,_,day=local_day_bounds_ms(0)
    exposure=({"day":day,"route_id":route["id"],"symbol":route["symbols"][0],
        "asset":mid,"start_asset":start,"units":residual,"input_usd":input_usd,
        "cash_output_usd":output_usd,"residual_cost_usd":residual_cost_usd,
        "last_mark_usd":residual_mark,
        "last_value_usd":residual_usd if residual_mark is not None else None,
        "economic_pnl":economic_pnl} if status=="open_exposure" else None)
    _live_finalize_attempt(attempt_id,status,exposure=exposure,input_units=e1["input"],
        mid_acquired=mid_acquired,mid_used=mid_used,
        output_units=end_out,unwind_input=unwind_used,unwind_output=unwind_out,residual_mid=residual,
        realized_pnl=realized_pnl,cash_output_usd=output_usd,residual_mark_usd=residual_mark,
        residual_value_usd=residual_usd if residual_mark is not None else None,
        economic_pnl=economic_pnl,realized_cost_usd=realized_cost_usd,
        residual_cost_usd=residual_cost_usd,unrealized_pnl=unrealized_pnl,
        unwind_error=str(unwind_error)[:1000] if unwind_error else None,
        leg1_latency_ms=r1["latency_ms"],leg2_latency_ms=r2["latency_ms"] if r2 else None,
        leg1_post_http_ms=r1.get("http_ms"),leg2_post_http_ms=r2.get("http_ms") if r2 else None,
        leg1_http_queue_ms=r1.get("http_queue_ms"),
        leg2_http_queue_ms=r2.get("http_queue_ms") if r2 else None,
        leg1_confirm_ms=r1.get("confirm_ms"),leg2_confirm_ms=r2.get("confirm_ms") if r2 else None,
        leg1_prepare_journal_ms=prep1,leg2_prepare_journal_ms=r2.get("prepare_journal_ms") if r2 else None,
        t0_to_leg1_submit_ms=max(0,int(r1.get("request_started_ts_ms") or t0)-t0),
        admission_to_leg1_post_ms=max(0,int(r1.get("request_started_ts_ms") or admitted_ts)-admitted_ts),
        t0_to_leg1_done_ms=max(0,l1_done-t0),
        leg1_done_to_leg2_submit_ms=(max(0,int(r2.get("request_started_ts_ms") or l1_done)-l1_done) if r2 else None),
        t0_to_done_ms=max(0,now_ms()-t0),
        leg2_guard_edge=guard_edge,
        leg2_exchange_age_ms=leg2_exchange_age,
        leg2_exchange_skew_ms=leg2_exchange_skew,
        error=str(leg2_error)[:1000] if leg2_error else None)
    # Preserve every causal safety event after the inventory/accounting row is
    # durable. An exposure can no longer be mistaken for a completed loss.
    if unwind_error is not None: live_trip("unwind_failed",unwind_error,attempt_id)
    if not fees_known: live_trip("fee_reconciliation_required",attempt_id=attempt_id)
    if status=="open_exposure":
        live_trip("residual_intermediate_asset",f"{residual} {mid}",attempt_id)
    return status,r1["latency_ms"],r2["latency_ms"] if r2 else None,realized_pnl


def _schedule_bot_shadows_from_gateway(q, x, quality, admitted_ts, route_tested):
    """Admit BOT_* only from the final fresh A/B+ gateway snapshot.

    Research FAST/TARGET/DEGRADED deliberately remain tied to the original T0.
    A route being validated for the first time is not retroactively simulated:
    only capabilities that already existed when the signal arrived qualify.
    """
    if q.get("bot_shadow_scheduled") or not route_tested: return 0
    q["bot_shadow_scheduled"]=True
    route=q["route"]; rid=route["id"]
    event_key=(rid,int(q.get("event_start_ts") or 0))
    shadow_x=dict(x)
    shadow_x.update({"size":float(quality.get("selected_usd") or 0.0),
        "quality_grade":quality.get("grade"),"quality_fraction":quality.get("size_fraction"),
        "quality_normal_edge":quality.get("normal_edge"),
        "quality_half_bbo_edge":quality.get("half_bbo_edge"),
        "quality_drop_l1_edge":quality.get("drop_l1_edge"),
        "quality_coverage3":quality.get("coverage3"),"quality_compute_us":quality.get("compute_us")})
    with shadow_lock:
        if event_key in bot_shadow_consumed_events: return 0
        if not bot_shadow_route_armed[rid]: return 0
        if admitted_ts-bot_shadow_route_last_trade[rid]<LIVE_HARD_COOLDOWN_MS: return 0
        qs=[]
        for profile in BOT_SHADOW_PROFILES:
            sq=reserve_shadow_trade(profile,route,shadow_x,q.get("event_start_ts"),q["decision_id"],admitted_ts)
            if sq: qs.append(sq)
        if not qs: return 0
        bot_shadow_consumed_events.add(event_key); bot_shadow_route_armed[rid]=False
        bot_shadow_route_last_trade[rid]=admitted_ts; bot_shadow_route_neutral_since.pop(rid,None)
        with pending_lock: pending_shadow_trials.extend(qs)
    with live_lock: live_runtime["bot_shadow_admissions"]+=1
    _coverage_mark_bot(rid)
    return len(qs)


def _queue_bot_shadows_from_gateway(q, x, quality, admitted_ts, route_tested):
    """Copy the admitted snapshot to a priority worker; never block an order."""
    if not route_tested or q.get("bot_shadow_queued"): return False
    q["bot_shadow_queued"]=True
    payload=(dict(q),dict(x),dict(quality),int(admitted_ts),bool(route_tested),now_ms())
    try:
        bot_shadow_schedule_q.put_nowait(payload)
        return True
    except queue.Full:
        with live_lock: live_runtime["bot_shadow_schedule_drops"]+=1
        return False


def bot_shadow_schedule_worker():
    while True:
        q,x,quality,admitted_ts,route_tested,queued_ts=bot_shadow_schedule_q.get()
        try:
            bot_shadow_schedule_lag_ms.append(max(0,now_ms()-queued_ts))
            _schedule_bot_shadows_from_gateway(q,x,quality,admitted_ts,route_tested)
        except Exception as exc:
            logerr(f"bot shadow schedule: {exc}")
        finally:
            bot_shadow_schedule_q.task_done()


def process_live_candidate(q):
    retry_count=int(q.get("retry_count",0)); tested=_route_has_recent_test(q["route"]["id"])
    check_started=time.perf_counter_ns(); check_ts=now_ms(); q["last_check_ts_ms"]=check_ts
    if q.get("first_check_ts_ms") is None:
        q["first_check_ts_ms"]=check_ts
        q["first_check_delay_ms"]=max(0,check_ts-int(q.get("decision_ts_ms") or check_ts))
        live_gateway_first_check_ms.append(q["first_check_delay_ms"])
        with live_lock: live_runtime["last_first_check_delay_ms"]=q["first_check_delay_ms"]
    check_recorded=False
    def record_check():
        nonlocal check_recorded
        if check_recorded: return
        check_recorded=True
        q["gateway_check_us"]=(time.perf_counter_ns()-check_started)/1000.0
        live_gateway_check_us.append(q["gateway_check_us"])
    def skipped(reason):
        _v247_count("gateway:"+reason)
        reasons=q.setdefault("retry_reasons",{})
        reasons[reason]=int(reasons.get(reason,0))+1
        record_check()
        return "skipped_"+reason,None,None,None
    # In live mode a delayed first scheduling slot is not a second chance: it
    # is already an old arbitrage.  The V2.4.2 export showed that the few
    # gateway admissions beyond 30 ms disproportionately contained the losses.
    if (TRADING_MODE=="live" and
            check_ts-int(q.get("decision_ts_ms") or check_ts)>LIVE_EXEC_RETRY_WINDOW_MS):
        return skipped("signal_too_old")
    gate=live_arm_status()
    if not gate["effective"]: return skipped("gate_"+gate["reason"])
    with live_lock: daily_pnl=float(live_runtime.get("realized_pnl",0.0))
    if live_loss_limit_reached(daily_pnl):
        live_trip("session_loss_limit" if LIVE_SESSION_REQUIRED else "daily_loss_limit"); return skipped("daily_loss_limit")
    reason,depth_age,depth_skew,exchange_age,exchange_skew=_live_health_status(q["route"])
    q["route_depth_age_ms"]=depth_age; q["route_depth_skew_ms"]=depth_skew
    q["route_exchange_age_ms"]=exchange_age; q["route_exchange_skew_ms"]=exchange_skew
    with live_lock:
        live_runtime["last_route_exchange_age_ms"]=exchange_age
        live_runtime["last_route_exchange_skew_ms"]=exchange_skew
    if reason: return skipped(reason)
    x,quality,reason=_fresh_live_signal(q["route"])
    if x:
        q["route_depth_age_ms"]=x.get("max_age")
        q["route_depth_skew_ms"]=x.get("bbo_skew")
        q["route_exchange_age_ms"]=x.get("exchange_max_age")
        q["route_exchange_skew_ms"]=x.get("exchange_skew")
        with live_lock:
            live_runtime["last_route_exchange_age_ms"]=q["route_exchange_age_ms"]
            live_runtime["last_route_exchange_skew_ms"]=q["route_exchange_skew_ms"]
    if quality:
        q["fresh_quality"]=dict(quality)
        q["fresh_edge"]=x.get("net") if x else quality.get("normal_edge")
    if reason: return skipped(reason)
    if TRADING_MODE=='live':
        try: q['exit_headroom']=_entry_exit_headroom(q['route'])
        except MexcSignalExpired as exc: return skipped(str(exc).split(':',1)[0])
    q["quality"]=dict(quality)
    if TRADING_MODE=="live" and not tested:
        return skipped("route_not_test_validated")
    requested=min(float(quality["selected_usd"]),LIVE_CAP_USD,LIVE_CAPITAL_LIMIT_USD)
    if requested<MIN_EXEC_USD: return skipped("below_min")
    balances,account_reason=_cached_live_account()
    if account_reason: return skipped(account_reason)
    bag_reason=_protected_bag_reason(balances)
    if bag_reason:
        if TRADING_MODE=="live": live_trip("protected_asset_detected",bag_reason)
        return skipped(bag_reason)
    start=q["route"]["path"][0]; free=float(balances.get(start,{}).get("free",0.0))
    max_units=free; sm=x["start_mark"]
    requested=min(requested,max_units*sm)
    if requested<MIN_EXEC_USD: return skipped("balance")
    admitted_ts=now_ms(); q["gateway_admit_ts_ms"]=admitted_ts
    q["gateway_delay_ms"]=max(0,admitted_ts-int(q.get("decision_ts_ms") or admitted_ts))
    live_gateway_admit_ms.append(q["gateway_delay_ms"])
    with live_lock: live_runtime["last_gateway_admit_delay_ms"]=q["gateway_delay_ms"]
    # Snapshot is handed to a dedicated worker; portfolio bookkeeping can no
    # longer delay durable preparation or the real POST.
    _queue_bot_shadows_from_gateway(q,x,quality,admitted_ts,tested)
    record_check()
    event_key=(q["route"]["id"],int(q["event_start_ts"]))
    cooldown_asset=q["route"]["path"][1]
    # A valid capability is reused for 72 h. Re-running three /order/test calls
    # on every appearance added latency, API load and misleading counters.
    if TRADING_MODE=="test" and tested:
        with live_lock:
            live_consumed_events.add(event_key); live_asset_last_attempt_ms[cooldown_asset]=admitted_ts
            live_runtime["capability_reuses"]+=1; live_runtime["last_route"]=q["route"]["id"]
            live_runtime["last_status"]="validated_route_reused"
        _live_admission_upsert(q,"validated_route_reused",requested_usd=requested,
                               retry_count=retry_count,route_tested=True)
        return "validated_route_reused",None,None,None
    attempt_id=uuid.uuid4().hex
    if TRADING_MODE=="test":
        _live_attempt_insert(q,requested,x,attempt_id); q["attempt_durable"]=True
    else:
        q["attempt_durable"]=False
    with live_lock:
        live_consumed_events.add(event_key); live_asset_last_attempt_ms[cooldown_asset]=admitted_ts
        live_runtime["attempts"]+=1; live_runtime["last_attempt_id"]=attempt_id
        live_runtime["last_route"]=q["route"]["id"]; live_runtime["last_status"]="prepared"
    _live_admission_upsert(q,"attempt_started",requested_usd=requested,attempt_id=attempt_id,
                           retry_count=retry_count,route_tested=tested)
    with state_lock: meta=state["market_meta"]
    try:
        if TRADING_MODE=="test": result=_live_test_attempt(q,attempt_id,x,quality,requested,balances,meta)
        else: result=_live_real_attempt(q,attempt_id,x,quality,requested,balances,meta)
        status,l1ms,l2ms,pnl=result
        timing=live_db_execute("""SELECT t0_to_leg1_submit_ms,t0_to_leg1_done_ms,
            leg1_done_to_leg2_submit_ms,t0_to_done_ms,leg1_order_type,leg2_order_type,unwind_order_type,
            admission_to_leg1_post_ms,leg1_post_http_ms,leg2_post_http_ms,
            leg1_http_queue_ms,leg2_http_queue_ms,
            leg1_confirm_ms,leg2_confirm_ms,leg1_prepare_journal_ms,leg2_prepare_journal_ms,
            route_exchange_age_t0_ms,route_exchange_skew_t0_ms,
            leg2_exchange_age_ms,leg2_exchange_skew_ms
            FROM live_attempts_v240 WHERE attempt_id=?""",(attempt_id,),one=True)
        with live_lock:
            live_runtime["last_status"]=status; live_runtime["last_leg1_ms"]=l1ms; live_runtime["last_leg2_ms"]=l2ms
            if timing:
                for key in ("t0_to_leg1_submit_ms","t0_to_leg1_done_ms","leg1_done_to_leg2_submit_ms","t0_to_done_ms",
                            "admission_to_leg1_post_ms","leg1_post_http_ms","leg2_post_http_ms",
                            "leg1_http_queue_ms","leg2_http_queue_ms",
                            "leg1_confirm_ms","leg2_confirm_ms","leg1_prepare_journal_ms","leg2_prepare_journal_ms",
                            "route_exchange_age_t0_ms","route_exchange_skew_t0_ms",
                            "leg2_exchange_age_ms","leg2_exchange_skew_ms"):
                    live_runtime["last_"+key]=timing[key]
                live_runtime["last_order_types"]={"leg1":timing["leg1_order_type"],"leg2":timing["leg2_order_type"],
                                                   "unwind":timing["unwind_order_type"]}
                if timing["admission_to_leg1_post_ms"] is not None: live_admission_to_post_ms.append(timing["admission_to_leg1_post_ms"])
                if timing["leg1_post_http_ms"] is not None: live_leg1_post_http_ms.append(timing["leg1_post_http_ms"])
                if timing["leg2_post_http_ms"] is not None: live_leg2_post_http_ms.append(timing["leg2_post_http_ms"])
                if timing["leg1_http_queue_ms"] is not None: live_leg1_http_queue_ms.append(timing["leg1_http_queue_ms"])
                if timing["leg2_http_queue_ms"] is not None: live_leg2_http_queue_ms.append(timing["leg2_http_queue_ms"])
                if TRADING_MODE=="live" and timing["leg1_confirm_ms"] is not None: live_leg1_confirm_ms.append(timing["leg1_confirm_ms"])
                if TRADING_MODE=="live" and timing["leg2_confirm_ms"] is not None: live_leg2_confirm_ms.append(timing["leg2_confirm_ms"])
            if status=="test_validated": live_runtime["test_validations"]+=1
            elif status=="entry_canceled":
                # No fill and no inventory: a later new version can try again
                # after the ordinary crypto cooldown, without a blind POST retry.
                live_consumed_events.discard(event_key)
            elif status in ("completed","forced_unwind"):
                if status=="forced_unwind":
                    live_runtime["forced_unwinds"]+=1; live_runtime["consecutive_unwinds"]+=1
                else: live_runtime["consecutive_unwinds"]=0
            elif status=="open_exposure":
                # Inventory is tracked separately and is neither completed nor
                # won/lost until it has actually been converted back to stablecoin.
                live_runtime["last_status"]="open_exposure"
        if TRADING_MODE=="live":
            refresh_live_daily_stats()
            with live_lock:
                daily_pnl=float(live_runtime.get("realized_pnl") or 0.0)
                consecutive=int(live_runtime.get("consecutive_unwinds") or 0)
            if live_loss_limit_reached(daily_pnl):
                live_trip("session_loss_limit" if LIVE_SESSION_REQUIRED else "daily_loss_limit",attempt_id=attempt_id)
            if consecutive>=LIVE_MAX_CONSECUTIVE_UNWINDS:
                live_trip("consecutive_unwind_limit",attempt_id=attempt_id)
        _live_admission_upsert(q,status,requested_usd=requested,attempt_id=attempt_id,
                               retry_count=retry_count,route_tested=_route_has_recent_test(q["route"]["id"]))
        if TRADING_MODE=="live":
            with live_lock: live_runtime["account_ts_ms"]=None
            live_account_refresh_event.set()
        persist_live_state(); return result
    except MexcOrderUncertain as exc:
        _live_attempt_update(attempt_id,status="reconciliation_required",realized_pnl=None,error=str(exc)[:1000])
        _live_admission_upsert(q,"reconciliation_required",reason=str(exc),attempt_id=attempt_id,
                               requested_usd=requested,retry_count=retry_count,route_tested=tested)
        live_trip("order_status_unknown",exc,attempt_id)
        with live_lock: live_runtime["last_status"]="reconciliation_required"
        return "reconciliation_required",None,None,None
    except MexcSignalExpired as exc:
        expired_detail=str(exc)[:500] or "signal_too_old_before_post"
        if not q.get("attempt_durable"):
            try:
                _live_attempt_insert(q,requested,x,attempt_id); q["attempt_durable"]=True
            except Exception as journal_exc:
                live_trip("expired_attempt_not_journaled",journal_exc,attempt_id)
        if q.get("attempt_durable"):
            _live_attempt_update(attempt_id,status="expired_before_submit",error=str(exc)[:1000])
        _live_admission_upsert(q,"skipped",reason=expired_detail,
            requested_usd=requested,attempt_id=attempt_id,retry_count=retry_count,
            route_tested=_route_has_recent_test(q["route"]["id"]))
        with live_lock:
            # No POST was sent: a genuinely newer book may be evaluated later.
            live_consumed_events.discard(event_key)
            if live_asset_last_attempt_ms.get(cooldown_asset)==admitted_ts:
                live_asset_last_attempt_ms.pop(cooldown_asset,None)
            live_runtime["last_status"]="skipped_signal_too_old_before_post"
            live_runtime["last_skip_reason"]=expired_detail
        return "skipped_signal_too_old_before_post",None,None,None
    except Exception as exc:
        if not q.get("attempt_durable"):
            try:
                _live_attempt_insert(q,requested,x,attempt_id); q["attempt_durable"]=True
            except Exception as journal_exc:
                live_trip("failed_attempt_not_journaled",journal_exc,attempt_id)
        _live_attempt_update(attempt_id,status="failed",error=str(exc)[:1000])
        _live_admission_upsert(q,"failed",reason=str(exc),requested_usd=requested,attempt_id=attempt_id,
                               retry_count=retry_count,route_tested=_route_has_recent_test(q["route"]["id"]))
        with live_lock: live_runtime["last_status"]="failed"; live_runtime["last_error"]=str(exc)[:500]
        if TRADING_MODE=="live": live_trip("live_attempt_failed",exc,attempt_id)
        else: persist_live_state()
        return "failed",None,None,None


def maybe_live_candidate(route,x,ts,event_start_ts):
    """A new book version is a new observation, not a replay of an old order.

    A rejected research T0 must not consume the later executable window. No
    order is retried here; the consumed event and crypto cooldown still apply.
    """
    if TRADING_MODE=="shadow" or not x or x.get("net",-1)<SHADOW_MIN_EDGE: return False
    if TRADING_MODE=="live" and LIVE_EXCLUDED_ASSETS.intersection(route.get("path",())): return False
    key=(route["id"],int(event_start_ts))
    versions=tuple(b.get("version") for b in x.get("leg_books",()))
    with live_lock:
        if key in live_consumed_events: return False
        if live_runtime.get("busy"): _v247_count("candidate:busy"); return False
        if ts-live_asset_last_attempt_ms[route["path"][1]]<LIVE_HARD_COOLDOWN_MS:
            _v247_count("candidate:crypto_cooldown"); return False
        old=live_candidate_versions.get(route["id"])
        if old and (old[0]==versions or ts-old[1]<LIVE_RECHECK_INTERVAL_MS): return False
        live_candidate_versions[route["id"]]=(versions,ts)
    reason=_snapshot_freshness({s:b for s,b in zip(route["symbols"],x["leg_books"])},
        LIVE_BBO_AGE_MS,LIVE_EXCHANGE_AGE_MS,LIVE_MAX_EXCHANGE_SKEW_MS)
    if reason: _v247_count("candidate:"+reason); return False
    _v247_count("candidate:fresh")
    # This is a separate, explicitly labelled admission identity. The research
    # T0 table is neither rewritten nor silently promoted from B- to A.
    initial={"grade":"pending","eligible":False,"selected_usd":0,
             "capacity_usd":x["size"],"sizing_scope":"live_affordable_size"}
    return enqueue_live_candidate(route,initial,f"{route['id']}@{ts}:v247",event_start_ts,
        decision_ts_ms=ts,edge_t0=x["net"],exchange_age_t0_ms=x.get("exchange_max_age"),
        exchange_skew_t0_ms=x.get("exchange_skew"))


def enqueue_live_candidate(route, quality, decision_id, event_start_ts, decision_ts_ms=None, edge_t0=None,
                           exchange_age_t0_ms=None, exchange_skew_t0_ms=None):
    q={"route":route,"quality":dict(quality),"initial_quality":dict(quality),
       "decision_id":decision_id,"event_start_ts":event_start_ts,
       "decision_ts_ms":int(decision_ts_ms or now_ms()),"edge_t0":edge_t0,
       "route_exchange_age_t0_ms":exchange_age_t0_ms,
       "route_exchange_skew_t0_ms":exchange_skew_t0_ms,"retry_count":0}
    if TRADING_MODE=="shadow":
        _live_admission_upsert(q,"not_queued",reason="mode_shadow",route_tested=_route_has_recent_test(route["id"])); return False
    gate=live_arm_status()
    if not gate["effective"]:
        _live_admission_upsert(q,"not_queued",reason="gate_"+gate["reason"],route_tested=_route_has_recent_test(route["id"])); return False
    event_key=(route["id"],int(event_start_ts)); ts=now_ms(); cooldown_asset=route["path"][1]
    with live_lock:
        if event_key in live_consumed_events: reason="same_event"
        elif live_runtime.get("busy"): reason="busy"
        elif ts-live_asset_last_attempt_ms[cooldown_asset]<LIVE_HARD_COOLDOWN_MS: reason="crypto_cooldown"
        elif liveq.full(): reason="queue_full"
        else: reason=None
        if reason:
            _live_admission_upsert(q,"not_queued",reason=reason,route_tested=_route_has_recent_test(route["id"])); return False
        # Reserve the sole gateway slot at enqueue time. Without this, several
        # scan workers can all observe busy=False before the live worker wakes.
        live_runtime["busy"]=True
        live_runtime["trade_epoch"]=int(live_runtime.get("trade_epoch",0))+1
        liveq.put_nowait(q)
        live_runtime["queued"]=liveq.qsize()
    _live_admission_upsert(q,"queued",route_tested=_route_has_recent_test(route["id"]))
    return True


def live_gateway_worker():
    transient={"websocket_count","depth_not_all_ready","websocket_stale","route_websocket_disconnected",
        "route_websocket_pong_timeout","route_resync_pending","route_depth_not_ready","route_depth_stale",
        "route_depth_skew","route_exchange_timestamp_missing","route_exchange_depth_stale",
        "route_exchange_depth_skew","edge_or_book","book_skew","post_resync_grace","depth_unavailable",
        "rejected_B-","rejected_C","rejected_D","quality_rejected","below_min_trade","account_cache_stale",
        "private_api_unavailable"}
    while True:
        q=liveq.get()
        try:
            with live_lock: live_runtime["busy"]=True; live_runtime["queued"]=liveq.qsize()
            if TRADING_MODE=="live":
                retry_max=LIVE_EXEC_RETRY_MAX; retry_window=LIVE_EXEC_RETRY_WINDOW_MS
                retry_interval=LIVE_EXEC_RETRY_INTERVAL_MS
            else:
                retry_max=LIVE_RETRY_MAX; retry_window=LIVE_RETRY_WINDOW_MS
                retry_interval=LIVE_RETRY_INTERVAL_MS
            result=None
            while True:
                # Never let a scheduled retry silently run beyond its budget.
                # process_live_candidate applies the same ceiling to the first
                # check in live mode; test discovery deliberately stays wider.
                if int(q.get("retry_count",0))>0 and now_ms()-int(q.get("decision_ts_ms") or now_ms())>=retry_window:
                    break
                result=process_live_candidate(q)
                status=str(result[0]) if result else ""
                reason=status[8:] if status.startswith("skipped_") else None
                age=now_ms()-int(q.get("decision_ts_ms") or now_ms())
                remaining=max(0,retry_window-age)
                if (reason in transient and int(q.get("retry_count",0))<retry_max and
                        retry_interval<remaining):
                    q["retry_count"]=int(q.get("retry_count",0))+1
                    time.sleep(retry_interval/1000.0)
                    continue
                break
            if result and str(result[0]).startswith("skipped_"):
                final_reason=str(result[0])[8:]
                _live_admission_upsert(q,"skipped",reason=final_reason,retry_count=int(q.get("retry_count",0)),
                                       route_tested=_route_has_recent_test(q["route"]["id"]))
                with live_lock:
                    live_runtime["skipped"]+=1; live_runtime["last_skip_reason"]=final_reason
                    live_runtime["last_status"]=result[0]
        except Exception as exc:
            if TRADING_MODE=="live": live_trip("gateway_worker_exception",exc)
            else:
                with live_lock: live_runtime["last_status"]="test_worker_error"; live_runtime["last_error"]=str(exc)[:500]
                try: persist_live_state()
                except Exception: pass
        finally:
            with live_lock: live_runtime["busy"]=False; live_runtime["queued"]=liveq.qsize()
            liveq.task_done()


def live_account_worker():
    while True:
        live_account_refresh_event.wait(LIVE_ACCOUNT_REFRESH_SEC)
        live_account_refresh_event.clear()
        if TRADING_MODE=="shadow" or live_account_client is None: continue
        # Never snapshot the account between the two legs: the temporary
        # intermediate asset is bot inventory, not an existing user bag.
        # Refresh immediately after the serial route releases the gateway.
        while True:
            with live_lock: busy=bool(live_runtime.get("busy"))
            if not busy: break
            time.sleep(0.05)
        try: _refresh_live_account()
        except Exception as exc:
            with live_lock: live_runtime["api_ok"]=False; live_runtime["last_error"]=str(exc)[:500]
            try: persist_live_state()
            except Exception: pass


def live_http_keepalive_worker():
    """Keep the independent order and order-status pools warm."""
    while True:
        time.sleep(1.0)
        if TRADING_MODE=="shadow" or live_client is None: continue
        with live_lock: busy=bool(live_runtime.get("busy"))
        if busy: continue
        for client,key in ((live_client,"last_keepalive_rtt_ms"),
                           (live_order_status_client,"last_status_keepalive_rtt_ms")):
            if client is None: continue
            try:
                rtt=client.keepalive_if_idle(int(LIVE_HTTP_KEEPALIVE_SEC*1000),
                    LIVE_HTTP_KEEPALIVE_TIMEOUT_SEC,
                    abort_if=lambda: bool(live_runtime.get("busy")))
                if rtt is None: continue
                with live_lock:
                    live_runtime[key]=rtt
                    live_runtime["last_keepalive_ts_ms"]=now_ms()
                    live_runtime["keepalive_ok"]+=1
                    live_runtime["last_keepalive_error"]=None
            except Exception as exc:
                with live_lock:
                    live_runtime["keepalive_errors"]+=1
                    live_runtime["last_keepalive_error"]=str(exc)[:400]


def initialize_live_gateway():
    global live_client,live_order_status_client,live_account_client,live_prevalidation_client,live_user_stream_client
    global live_session_control
    initialize_live_journal()
    load_live_state()
    if LIVE_SESSION_REQUIRED:
        try:
            row=live_db_execute("SELECT policy_json FROM live_rearm_sessions_v2412 WHERE session_id=?",
                (LIVE_SESSION_ID,),one=True)
            policy=json.loads(row['policy_json'])
            if policy['session_id']!=LIVE_SESSION_ID: raise ValueError('session mismatch')
            live_session_control=LiveSession(policy,now_ms=now_ms)
        except Exception:
            live_session_control=None
            live_trip('session_missing_or_invalid')
    # Restore measured LIVE confirmation times without adding any request to
    # the entry path. TEST timings must never make the budget look faster.
    try:
        times=live_db_execute("""SELECT leg1_confirm_ms FROM live_attempts_v240
            WHERE mode='live' AND leg1_confirm_ms>0 ORDER BY created_ts_ms DESC LIMIT 200""",many=True)
        live_leg1_confirm_ms.extend(float(r['leg1_confirm_ms']) for r in reversed(times or []))
    except Exception as exc:
        live_trip('confirmation_history_unavailable',type(exc).__name__)
    _load_route_capabilities()
    refresh_live_daily_stats()
    try:
        unresolved=live_db_execute("""SELECT COUNT(*) AS n FROM live_orders_v240
            WHERE status IN ('prepared','submitted','submit_unknown')""",one=True)
        unfinished=live_db_execute("""SELECT COUNT(*) AS n FROM live_attempts_v240
            WHERE mode='live' AND status IN ('prepared','leg1_submitting','leg2_submitting','unwind_submitting',
                'reconciliation_required','accounting_pending')""",one=True)
        exposures=live_db_execute("""SELECT COUNT(*) AS n FROM live_open_exposures_v246
            WHERE status='open' AND units>0""",one=True)
        if int(unresolved["n"] if unresolved else 0) or int(unfinished["n"] if unfinished else 0):
            live_trip("unreconciled_live_journal_on_startup")
        if int(exposures["n"] if exposures else 0):
            live_trip("open_exposure_on_startup",f"{int(exposures['n'])} exposition(s) non résolue(s)")
    except Exception as exc: live_trip("live_journal_check_failed",exc)
    if TRADING_MODE=="shadow":
        persist_live_state(); return
    if not MEXC_API_KEY or not MEXC_API_SECRET:
        with live_lock: live_runtime["last_error"]="MEXC_API_KEY/MEXC_API_SECRET absentes"
        persist_live_state(); return
    live_client=MexcPrivateClient(MEXC_API_KEY,MEXC_API_SECRET,role="orders")
    live_order_status_client=MexcPrivateClient(MEXC_API_KEY,MEXC_API_SECRET,role="order_status")
    live_account_client=MexcPrivateClient(MEXC_API_KEY,MEXC_API_SECRET,role="account")
    live_user_stream_client=MexcPrivateClient(MEXC_API_KEY,MEXC_API_SECRET,role="user_stream")
    if TRADING_MODE=="test" and LIVE_PREVALIDATE_ENABLED:
        live_prevalidation_client=MexcPrivateClient(MEXC_API_KEY,MEXC_API_SECRET,role="prevalidation")
    try:
        live_client.sync_time(); live_order_status_client.sync_time(); live_account_client.sync_time()
        live_user_stream_client.sync_time()
        if live_prevalidation_client is not None: live_prevalidation_client.sync_time()
        _refresh_live_account(); persist_live_state()
    except Exception as exc:
        with live_lock: live_runtime["api_ok"]=False; live_runtime["last_error"]=str(exc)[:500]
        persist_live_state(); logerr(f"private API startup: {exc}")
    threading.Thread(target=live_gateway_worker,daemon=True).start()
    threading.Thread(target=live_account_worker,daemon=True).start()
    threading.Thread(target=live_http_keepalive_worker,daemon=True).start()
    threading.Thread(target=private_ws_journal_worker,daemon=True).start()
    if LIVE_PRIVATE_WS_MODE!="off":
        threading.Thread(target=private_order_ws_worker,daemon=True).start()
    threading.Thread(target=bot_shadow_schedule_worker,daemon=True).start()
    if live_prevalidation_client is not None:
        threading.Thread(target=live_prevalidation_worker,daemon=True).start()


def _runtime_series_stats(series, unit="ms"):
    values=sorted(list(series))
    def pct(frac):
        return values[min(len(values)-1,int((len(values)-1)*frac))] if values else None
    return {"samples":len(values),"p50_"+unit:pct(.50),"p95_"+unit:pct(.95),
            "p99_"+unit:pct(.99),"max_"+unit:max(values) if values else None}


def live_status():
    arm=live_arm_status()
    with live_lock:
        out={k:v for k,v in live_runtime.items() if k!="balances"}; out["balances"]={k:dict(v) for k,v in live_runtime.get("balances",{}).items()}
        validated_routes=sum(int(v.get("expires_ts_ms") or 0)>=now_ms() and
        _capability_matches_policy(v) for v in live_route_capabilities.values())
        account_ts=live_runtime.get("account_ts_ms")
        prevalidation=dict(prevalidation_runtime)
    exposure=live_open_exposure_snapshot()
    out["open_exposures"]=exposure["count"]
    out["open_exposure_value_usd"]=exposure["value_usd"]
    out["open_exposure_unrealized_pnl"]=exposure["unrealized_pnl"]
    out["open_exposure_economic_pnl"]=exposure["economic_pnl"]
    out["open_exposure_items"]=exposure["items"]
    with private_ws_lock:
        private_stream=dict(private_ws_runtime)
        private_stream["pending_orders"]=sum(1 for w in private_ws_waiters.values() if not w.get("released"))
    private_stream["message_age_ms"]=(max(0,now_ms()-int(private_stream["last_message_ms"]))
        if private_stream.get("last_message_ms") else None)
    private_stream["pong_age_ms"]=(max(0,now_ms()-int(private_stream["last_pong_ms"]))
        if private_stream.get("last_pong_ms") else None)
    private_stream["post_to_event"]=_runtime_series_stats(private_ws_post_to_event_ms)
    private_stream["ws_rest_delta"]=_runtime_series_stats(private_ws_rest_delta_ms)
    retry_window=LIVE_EXEC_RETRY_WINDOW_MS if TRADING_MODE=="live" else LIVE_RETRY_WINDOW_MS
    retry_interval=LIVE_EXEC_RETRY_INTERVAL_MS if TRADING_MODE=="live" else LIVE_RETRY_INTERVAL_MS
    retry_max=LIVE_EXEC_RETRY_MAX if TRADING_MODE=="live" else LIVE_RETRY_MAX
    with live_v247_lock: v247_counts=dict(live_v247_counts)
    out.update({"configured_mode":TRADING_MODE,"arm":arm,"cap_usd":LIVE_CAP_USD,
        "session":live_session_status(),"excluded_assets":sorted(LIVE_EXCLUDED_ASSETS),
        "max_consecutive_unwinds":LIVE_MAX_CONSECUTIVE_UNWINDS,
        "v247_counts":v247_counts,"price_protected":LIVE_PRICE_PROTECTED,
        "entry_order_policy":"FILL_OR_KILL" if LIVE_PRICE_PROTECTED else "capability",
        "sizing_scope":"live_affordable_size","research_grades_unchanged":True,
        "shadow_model":"counterfactual_not_a_fill_guarantee",
        "fees_assumed_per_leg":FEE,"fees_are_account_specific":False,
        "legacy_capabilities_stored":len(live_route_capabilities),
        "capital_limit_usd":LIVE_CAPITAL_LIMIT_USD,"daily_loss_limit_usd":LIVE_DAILY_LOSS_LIMIT_USD,
        "max_concurrent":LIVE_MAX_CONCURRENT,"bbo_age_ms":LIVE_BBO_AGE_MS,
        "leg2_bbo_age_ms":LIVE_LEG2_BBO_AGE_MS,"max_skew_ms":LIVE_MAX_SKEW_MS,
        "exchange_age_ms":LIVE_EXCHANGE_AGE_MS,
        "leg2_exchange_age_ms":LIVE_LEG2_EXCHANGE_AGE_MS,
        "max_exchange_skew_ms":LIVE_MAX_EXCHANGE_SKEW_MS,
        "leg2_max_exchange_skew_ms":LIVE_LEG2_MAX_EXCHANGE_SKEW_MS,
        "mexc_clock_offset_ms":int(getattr(live_client,"time_offset_ms",0) or 0) if live_client else 0,
        "fee_safety_pct":LIVE_FEE_SAFETY*100,
        "leg2_min_edge_pct":LIVE_LEG2_MIN_EDGE*100,
        "defer_leg1_commission":LIVE_DEFER_LEG1_COMMISSION,
        "protected_bags":PROTECT_EXISTING_BAGS,"allowed_start_assets":MEXC_ALLOWED_START_ASSETS,
        "automatic_rebalancing":False,"bot_shadow_rebalancing":BOT_SHADOW_REBALANCE_ENABLED,
        "require_tested_route":LIVE_REQUIRE_TESTED_ROUTE,
        "test_max_age_hours":LIVE_TEST_MAX_AGE_HOURS,"validated_routes":validated_routes,
        "require_all_ws":LIVE_REQUIRE_ALL_WS,"require_route_ws":LIVE_REQUIRE_ROUTE_WS,
        "retry_window_ms":retry_window,"retry_interval_ms":retry_interval,"retry_max":retry_max,
        "test_retry_window_ms":LIVE_RETRY_WINDOW_MS,"test_retry_interval_ms":LIVE_RETRY_INTERVAL_MS,
        "test_retry_max":LIVE_RETRY_MAX,"live_retry_window_ms":LIVE_EXEC_RETRY_WINDOW_MS,
        "live_retry_interval_ms":LIVE_EXEC_RETRY_INTERVAL_MS,"live_retry_max":LIVE_EXEC_RETRY_MAX,
        "max_t0_to_leg1_post_ms":LIVE_MAX_T0_TO_LEG1_POST_MS,
        "hard_cooldown_ms":LIVE_HARD_COOLDOWN_MS,"cooldown_scope":"intermediate_crypto",
        "account_refresh_sec":LIVE_ACCOUNT_REFRESH_SEC,"account_cache_max_age_ms":LIVE_ACCOUNT_CACHE_MAX_AGE_MS,
        "live_journal_path":LIVE_DB_PATH,"http_keepalive_sec":LIVE_HTTP_KEEPALIVE_SEC,
        "http_keepalive_timeout_sec":LIVE_HTTP_KEEPALIVE_TIMEOUT_SEC,
        "prevalidation":prevalidation,
        "prevalidation_interval_sec":LIVE_PREVALIDATE_INTERVAL_SEC,
        "prevalidation_refresh_hours":LIVE_PREVALIDATE_REFRESH_HOURS,
        "account_cache_age_ms":max(0,now_ms()-int(account_ts)) if account_ts else None,
        "first_check_latency":_runtime_series_stats(live_gateway_first_check_ms),
        "gateway_admit_latency":_runtime_series_stats(live_gateway_admit_ms),
        "gateway_check_compute":_runtime_series_stats(live_gateway_check_us,"us"),
        "admission_to_post":_runtime_series_stats(live_admission_to_post_ms),
        "leg1_post_http":_runtime_series_stats(live_leg1_post_http_ms),
        "leg2_post_http":_runtime_series_stats(live_leg2_post_http_ms),
        "leg1_http_queue":_runtime_series_stats(live_leg1_http_queue_ms),
        "leg2_http_queue":_runtime_series_stats(live_leg2_http_queue_ms),
        "leg1_confirmation":_runtime_series_stats(live_leg1_confirm_ms),
        "leg2_confirmation":_runtime_series_stats(live_leg2_confirm_ms),
        "prepare_journal":_runtime_series_stats(live_prepare_journal_ms),
        "response_journal":_runtime_series_stats(live_response_journal_ms),
        "bot_shadow_schedule_lag":_runtime_series_stats(bot_shadow_schedule_lag_ms),
        "private_order_stream":private_stream,
        "private_order_stream_mode":LIVE_PRIVATE_WS_MODE,
        "private_order_stream_authoritative":LIVE_PRIVATE_WS_MODE=="hybrid",
        "emergency_unwind":"prevalidated_market_without_depth_gate",
        "unwind_max_attempts":LIVE_UNWIND_MAX_ATTEMPTS,
        "unwind_retry_window_ms":LIVE_UNWIND_RETRY_WINDOW_MS,
        "exit_confirmation_budget_ms":_exit_confirmation_budget_ms(),
        "exit_confirmation_floor_ms":LIVE_EXIT_CONFIRM_FLOOR_MS,
        "exit_prepare_margin_ms":LIVE_EXIT_PREP_MARGIN_MS})
    return out

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
                    paper["profit_bank"]-=conv["cost"]
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
def is_bot_shadow_profile(profile): return profile in BOT_SHADOW_PROFILES


def shadow_profile_capital(profile):
    return LIVE_CAPITAL_LIMIT_USD if is_bot_shadow_profile(profile) else SIM_CAPITAL


def shadow_default(day, profile=None):
    weights={a:SHADOW_TARGET_WEIGHTS.get(a,0.0) for a in STABLES}; sw=sum(weights.values())
    if sw<=0: weights={a:1.0/max(1,len(STABLES)) for a in STABLES}
    else: weights={a:w/sw for a,w in weights.items()}
    capital=shadow_profile_capital(profile)
    return {"day":day,"balances":{a:capital*weights[a] for a in STABLES},"profit_bank":0.0,"trades":0,"wins":0,"losses":0,
            "rebalances":0,"rebalance_cost":0.0,"skipped_flash":0,"skipped_balance":0,"skipped_rebalance":0}

def load_shadow_state():
    _,_,day=local_day_bounds_ms(0)
    with shadow_lock:
        for profile in SHADOW_PROFILES:
            shadow_states[profile]=shadow_default(day,profile)
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
        shadow_states[profile]=shadow_default(day,profile); shadow_reserved[profile].clear(); shadow_open_exposures[profile].clear(); persist_shadow(profile)
    return shadow_states[profile]

def persist_shadow(profile):
    st=shadow_states[profile]
    put_db(("shadow_state",(profile,st["day"],now_ms(),json.dumps(st["balances"],sort_keys=True),st["profit_bank"],st["trades"],st["wins"],st["losses"],st["rebalances"],st["rebalance_cost"],st["skipped_flash"],st["skipped_balance"],st["skipped_rebalance"])))

def shadow_choose_rebalance(profile,target_asset,needed_units,event_x,books,meta):
    if is_bot_shadow_profile(profile): return None
    st=shadow_states[profile]; target_mark=stable_mark_usdt(target_asset,books,meta)
    if target_mark<=0 or needed_units<=0 or st["profit_bank"]<=0: return None
    initial_each=shadow_profile_capital(profile)/len(STABLES); candidates=[]
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
        if is_bot_shadow_profile(profile) and not _route_has_recent_test(route["id"]): return None
        st=ensure_shadow_day(profile)
        with state_lock: books=dict(state["bbo"]); meta=state["market_meta"]
        s,e=route["path"][0],route["path"][-1]
        if s not in st["balances"] or e not in st["balances"]: return None
        sm=event_x["start_mark"]
        desired_usd=min(float(event_x["size"]),LIVE_CAP_USD) if is_bot_shadow_profile(profile) else float(event_x["size"])
        desired=desired_usd/max(sm,1e-12)
        free=max(0.0,st["balances"].get(s,0.0)-shadow_reserved[profile].get(s,0.0)); shortage=max(0.0,desired-free)
        if shortage>0 and not is_bot_shadow_profile(profile):
            rb=shadow_choose_rebalance(profile,s,shortage,event_x,books,meta)
            if rb:
                donor,conv=rb; st["balances"][donor]-=conv["input"]; st["balances"][s]+=conv["output"]; st["rebalances"]+=1; st["rebalance_cost"]+=conv["cost"]
                # Preserve signed accounting: subtract the real cost only and
                # never erase a loss by clamping the bank back to zero.
                st["profit_bank"]-=conv["cost"]
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
    market=meta.get(sym)
    if not market:
        q["skip_detail"]={"reason":"market_missing","book_age_ms":None,"executable_usd":None}; return False
    with depth_lock:
        ob=state["depth"].get(sym)
        if not ob or not ob.get("ready"):
            q["skip_detail"]={"reason":"depth_not_ready","book_age_ms":None,"executable_usd":None}; return False
        book_age=max(0,now_ms()-int(ob.get("ts",0)))
    age_limit=LIVE_BBO_AGE_MS if is_bot_shadow_profile(q["profile"]) else PRIMARY_BBO_AGE_MS
    if book_age>age_limit:
        q["skip_detail"]={"reason":"depth_stale","book_age_ms":book_age,"executable_usd":None}; return False
    if is_bot_shadow_profile(q["profile"]):
        reason,*_=_live_health_status(r)
        if reason:
            q["skip_detail"]={"reason":reason,"book_age_ms":book_age,"executable_usd":None}; return False
    fill=depth_walk(sym,frm,to,q["reserved_units"],meta,age_limit_ms=age_limit)
    if not fill:
        q["skip_detail"]={"reason":"no_liquidity","book_age_ms":book_age,"executable_usd":0.0}; return False
    if is_bot_shadow_profile(q["profile"]) and fill["fill_ratio"]<.999999:
        q["skip_detail"]={"reason":"fok_insufficient_depth","book_age_ms":book_age,"executable_usd":0}; return False
    fill_usd=fill["input_used"]*q["x0"]["start_mark"]
    if fill_usd<SIM_MIN_TRADE_USD:
        q["skip_detail"]={"reason":"below_min_fill","book_age_ms":book_age,"executable_usd":fill_usd}; return False
    q["leg1"]=fill; q["leg1_ts_ms"]=now_ms(); q["stage"]=2; return True

def settle_shadow_trade(q,exec_ts):
    profile=q["profile"]
    with shadow_lock:
        st=ensure_shadow_day(profile); s,e=q["start_asset"],q["end_asset"]; reserved=q["reserved_units"]; r=q["route"]; mid=r["path"][1]; sym1,sym2=r["symbols"]
        with state_lock: meta=state["market_meta"]; books=dict(state["bbo"])
        leg1=q["leg1"]; mid_total=leg1["output"]; input_units=min(leg1["input_used"],st["balances"].get(s,0.0)); input_usd=input_units*q["x0"]["start_mark"]
        age_limit=LIVE_LEG2_BBO_AGE_MS if is_bot_shadow_profile(profile) else PRIMARY_BBO_AGE_MS
        fill2=depth_walk(sym2,mid,e,mid_total,meta,age_limit_ms=age_limit)
        if is_bot_shadow_profile(profile):
            if fill2:
                reason=_snapshot_freshness({sym2:fill2},LIVE_LEG2_BBO_AGE_MS,LIVE_LEG2_EXCHANGE_AGE_MS)
                if reason or fill2["fill_ratio"]<.999999: fill2=None
            # Mirror the live guard without altering final Shadow accounting:
            # test one 0.20% total haircut on the modeled leg-1 gross output,
            # then allow leg 2 only while the conservative route keeps >=0.15%.
            gross_mid=float(leg1.get("gross_output") or 0.0)
            guard_mid=gross_mid*(1.0-LIVE_FEE_SAFETY)
            guard_fill2=depth_walk(sym2,mid,e,guard_mid,meta,age_limit_ms=age_limit)
            predicted=(guard_fill2["output"]*stable_mark_usdt(e,books,meta)/max(input_usd,1e-12)-1.0) if guard_fill2 and guard_fill2.get("fill_ratio",0)>=0.999999 else -1.0
            if predicted<LIVE_LEG2_MIN_EDGE: fill2=None
            # Modeled execution instant, not a measured matching-engine fill.
            if fill2 and q.get("leg1_ts_ms",0)+int(getattr(live_client,"time_offset_ms",0) or 0)-int(fill2.get("send_ts") or 0)>LIVE_LEG2_MAX_EXCHANGE_SKEW_MS:
                fill2=None
        leg2_used=fill2["input_used"] if fill2 else 0.0; end_out=fill2["output"] if fill2 else 0.0
        remaining=max(0.0,mid_total-leg2_used); unwind=depth_walk(sym1,mid,s,remaining,meta,age_limit_ms=age_limit) if remaining>1e-12 else None
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
    if is_bot_shadow_profile(profile) and not BOT_SHADOW_REBALANCE_ENABLED:
        return False
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
            budget=max(0.0,st["profit_bank"])*REBALANCE_MAX_PROFIT_SHARE
            if cost>budget+1e-12:
                st["skipped_rebalance"]+=1; persist_shadow(profile); break
            bbefore=json.dumps(st["balances"],sort_keys=True)
            st["balances"][from_asset]=max(0.0,st["balances"].get(from_asset,0.0)-used)
            st["balances"][to_asset]=st["balances"].get(to_asset,0.0)+out
            st["rebalances"]+=1; st["rebalance_cost"]+=cost; st["profit_bank"]-=cost
            bafter=json.dumps(st["balances"],sort_keys=True)
            put_db(("shadow_rebalance_v234",(profile,st["day"],now_ms(),reason,from_asset,to_asset,used,out,in_usd,out_usd,cost,bbefore,bafter)))
            changed=True
        if changed: persist_shadow(profile)
        return changed


def shadow_periodic_rebalance_worker():
    while True:
        time.sleep(5.0); ts=now_ms()
        for profile in SHADOW_PROFILES:
            if is_bot_shadow_profile(profile) and not BOT_SHADOW_REBALANCE_ENABLED: continue
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
            capital=shadow_profile_capital(profile)
            st.update({"profile":profile,"leg1_ms":l1,"leg2_ms":l2,"capital":capital,"nav":nav,"profit":nav-capital,"return_pct":(nav/capital-1)*100 if capital else 0.0,"open_exposures":len(exposures),"open_exposure_nav":exp_nav,"reserved":dict(shadow_reserved[profile])})
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
# One replay snapshot is kept at the first configured replay crossing (0.30% by default). If the
# same event later reaches the live threshold, one additional >=0.35% snapshot
# is kept. Lower-edge events remain summarized in opportunities without spawning
# multi-level captures or latency matrices.
decision_event_flags={}
# The live-like Shadow clock is intentionally isolated from all replay/capture work.
# These samples measure scheduler delay in addition to the configured market delay.
shadow_execution_lag_ms=deque(maxlen=10000)

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

def _research_exchange_reason(x):
    if not x.get("exchange_ts_complete"): return "exchange_timestamp_missing"
    if x.get("exchange_max_age",999999)>LIVE_EXCHANGE_AGE_MS: return "exchange_depth_stale"
    if x.get("exchange_skew",999999)>LIVE_MAX_EXCHANGE_SKEW_MS: return "exchange_depth_skew"
    return None


def schedule_decision(route,x,ts,event_start_ts):
    rid=route["id"]
    event_key=(rid,int(event_start_ts))
    flags=decision_event_flags.setdefault(event_key,{"replay":False,"quality":False})
    # First replay-grade crossing, plus at most the first live-policy crossing.
    # An event born above 0.35% satisfies both with one decision.
    broad_due=x.get("net",-1.0)>=REPLAY_MIN_EDGE and not flags["replay"]
    quality_due=x.get("net",-1.0)>=SHADOW_MIN_EDGE and not flags["quality"]
    if not broad_due and not quality_due: return
    if x.get("max_age",999999)>DECISION_MAX_RECV_AGE_MS: return
    if x.get("bbo_skew",999999)>DECISION_MAX_SKEW_MS: return
    if broad_due: flags["replay"]=True
    if quality_due: flags["quality"]=True
    with state_lock:
        if broad_due: state["replay_decisions"]+=1
        if quality_due: state["quality_decisions"]+=1
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
        if ENABLE_ONLINE_RESEARCH_TRIALS:
            for age_lim in RESEARCH_BBO_AGES_MS:
                if x["max_age"]>age_lim: continue
                for lat in RESEARCH_LATENCIES_MS:
                    pending_trials.append({"due":ts+lat,"day":day,"did":did,"route":route,"t0":ts,
                        "age_lim":age_lim,"lat":lat,"x0":dict(x)})
        for off in DEPTH_CAPTURE_OFFSETS_MS:
            if off>0: pending_depth_captures.append({"due":ts+off,"did":did,"route":route,"t0":ts})
    # Separate research portfolio: one executable entry per arbitrage event, real capital reserved at T0.
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
    temporal_reason=_research_exchange_reason(x)
    if quality.get("eligible") and temporal_reason:
        quality=dict(quality,eligible=False,reason=temporal_reason)
        _policy_reason=temporal_reason
    if quality_due:
        _coverage_mark_quality(rid,quality)
    # Private gateway gets the same A/B+ signal but performs a fresh, stricter
    # 50 ms validation before touching either /order/test or the real endpoint.
    # It is queued before research persistence/capture work so an 8 ms freshness
    # margin such as LIT's cannot be consumed by JSON serialization.
    # V247: LIVE is driven by maybe_live_candidate, independently of this
    # once-per-event research capture and its uncapped size classification.
    put_db(("decision_context_v234",(did,day,route["id"],int(event_start_ts),ts,">".join(route["path"]),route["path"][0],route["path"][-1],x["net"],x["size"],x.get("start_mark"),x.get("end_mark"),ready_ages[0] if len(ready_ages)>0 else None,ready_ages[1] if len(ready_ages)>1 else None,1 if quality["eligible"] else 0,_policy_reason)))
    put_db(("decision_quality_v235",(did,day,route["id"],ts,quality["grade"],1 if quality["eligible"] else 0,quality["reason"],
        quality["capacity_usd"],quality["size_fraction"],quality["selected_usd"],quality.get("normal_edge"),
        quality.get("half_bbo_edge"),quality.get("drop_l1_edge"),quality.get("coverage3"),quality["compute_us"])))
    if 0 in DEPTH_CAPTURE_OFFSETS_MS:
        capture_decision_depth({"did":did,"route":route,"t0":ts},ts)
    paper_ok = ENABLE_LEGACY_PAPER and (paper_route_armed[rid] and ts-paper_route_last_trade[rid]>=PAPER_HARD_COOLDOWN_MS)
    if paper_ok and event_key not in paper_consumed_events and x.get("max_age",999999)<=PRIMARY_BBO_AGE_MS and x.get("bbo_skew",999999)<=PRIMARY_MAX_SKEW_MS:
        q=reserve_paper_trade(route,x,event_start_ts,did,ts)
        if q:
            paper_consumed_events.add(event_key); paper_route_armed[rid]=False; paper_route_last_trade[rid]=ts; paper_route_neutral_since.pop(rid,None)
            with pending_lock: pending_paper_trials.append(q)

    # Shadow-live: same event, three independent portfolios and measured depth at profile latencies.
    if quality["eligible"]:
        disposition=None; reserved_profiles=0
        if event_key in shadow_consumed_events: disposition="blocked_same_event"
        elif not shadow_route_armed[rid]: disposition="blocked_not_rearmed"
        elif ts-shadow_route_last_trade[rid]<PAPER_HARD_COOLDOWN_MS: disposition="blocked_cooldown"
        else: disposition="candidate"
        shadow_x=dict(x)
        shadow_x.update({"size":quality["selected_usd"],"quality_grade":quality["grade"],
            "quality_fraction":quality["size_fraction"],"quality_normal_edge":quality.get("normal_edge"),
            "quality_half_bbo_edge":quality.get("half_bbo_edge"),"quality_drop_l1_edge":quality.get("drop_l1_edge"),
            "quality_coverage3":quality.get("coverage3"),"quality_compute_us":quality["compute_us"]})
        qs=[]; research_qs=[]
        if disposition=="candidate":
            for profile in RESEARCH_SHADOW_PROFILES:
                sq=reserve_shadow_trade(profile,route,shadow_x,event_start_ts,did,ts)
                if sq:
                    qs.append(sq)
                    research_qs.append(sq)
            reserved_profiles=len(research_qs)
            if research_qs:
                disposition="executed_all_profiles" if len(research_qs)==len(RESEARCH_SHADOW_PROFILES) else "executed_partial_profiles"
                shadow_consumed_events.add(event_key); shadow_route_armed[rid]=False; shadow_route_last_trade[rid]=ts; shadow_route_neutral_since.pop(rid,None)
                with pending_lock: pending_shadow_trials.extend(qs)
            else: disposition="blocked_no_profile_balance"
        put_db(("shadow_admission_v236",(did,day,route["id"],int(event_start_ts),ts,quality["grade"],quality["selected_usd"],disposition,reserved_profiles)))

def execution_trial_worker():
    while True:
        time.sleep(.005); ts=now_ms(); due=[]; paper_due=[]; depth_due=[]
        with pending_lock:
            keep=[]
            for q in pending_trials:
                (due if q["due"]<=ts else keep).append(q)
            pending_trials[:] = keep
            pkeep=[]
            for q in pending_paper_trials:
                (paper_due if (q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"])<=ts else pkeep).append(q)
            pending_paper_trials[:] = pkeep
            dkeep=[]
            for q in pending_depth_captures:
                (depth_due if q["due"]<=ts else dkeep).append(q)
            pending_depth_captures[:] = dkeep
        if not due and not paper_due and not depth_due: continue
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

def shadow_execution_worker():
    """Priority clock for research and gateway-aligned BOT profiles."""
    while True:
        time.sleep(.001); ts=now_ms(); due=[]
        with pending_lock:
            keep=[]
            for q in pending_shadow_trials:
                due_ts=q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"]
                (due if due_ts<=ts else keep).append(q)
            pending_shadow_trials[:] = keep
        if not due: continue
        for q in due:
            due_ts=q["leg1_due"] if q.get("stage",1)==1 else q["leg2_due"]
            with pending_lock: shadow_execution_lag_ms.append(max(0,now_ms()-due_ts))
            if q.get("stage",1)==1:
                if shadow_leg1(q):
                    with pending_lock: pending_shadow_trials.append(q)
                else:
                    with shadow_lock:
                        p=q["profile"]; detail=q.get("skip_detail") or {}
                        shadow_reserved[p][q["start_asset"]]=max(0.0,shadow_reserved[p].get(q["start_asset"],0.0)-q["reserved_units"])
                        shadow_states[p]["skipped_flash"]+=1
                        put_db(("shadow_skip_v239",(p,shadow_states[p]["day"],now_ms(),q["decision_id"],q["route"]["id"],"leg1",
                            detail.get("reason","unknown"),q["leg1_due"],detail.get("book_age_ms"),q["reserved_usd"],detail.get("executable_usd"))))
                        persist_shadow(p)
            else:
                settle_shadow_trade(q,now_ms())

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
    bot_shadow_consumed_events.discard((ev["route_id"],int(ev["start_ts"])))
    live_consumed_events.discard((ev["route_id"],int(ev["start_ts"])))
    decision_event_flags.pop((ev["route_id"],int(ev["start_ts"])),None)

def process_route(route,ts):
    with state_lock:
        books=dict(state["bbo"]); meta=state["market_meta"]
    x=route_calc(route,books,meta,allow_small_capacity=True)
    _coverage_observe(route,x,ts)
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
    if not bot_shadow_route_armed[key]:
        if x is not None and x["net"]<=PAPER_REARM_NET:
            since=bot_shadow_route_neutral_since.get(key)
            if since is None: bot_shadow_route_neutral_since[key]=ts
            elif ts-since>=PAPER_REARM_NEUTRAL_MS:
                bot_shadow_route_armed[key]=True; bot_shadow_route_neutral_since.pop(key,None)
        else:
            bot_shadow_route_neutral_since.pop(key,None)
    with event_lock:
        ev=active_events.get(key)
        if x and x["net"]>=MIN_NET:
            if ev is None:
                ev={"route_id":route["id"],"type":route["type"],"path":">".join(route["path"]),"start_asset":route["path"][0],
                    "end_asset":route["path"][-1],"start_ts":ts,"last_ts":ts,"last_observed_ts":ts,"ticks":1,"entry_net":x["net"],"peak_net":x["net"],
                    "sum_net":x["net"],"entry_size":x["size"],"max_size":x["size"],"entry_profit":x["profit"],"peak_profit":x["profit"],
                    "max_bbo_age":x["max_age"],"confirmed":False}
                active_events[key]=ev
            else:
                ev["last_ts"]=ts; ev["last_observed_ts"]=ts; ev.pop("neutral_since",None)
                ev["ticks"]+=1; ev["sum_net"]+=x["net"]; ev["max_bbo_age"]=max(ev["max_bbo_age"],x["max_age"])
                ev["peak_net"]=max(ev["peak_net"],x["net"]); ev["max_size"]=max(ev["max_size"],x["size"]); ev["peak_profit"]=max(ev["peak_profit"],x["profit"])

            # Decision is immediate at T0. The simulation can enter once per event when its stricter BBO/skew rules are met.
            maybe_live_candidate(route,x,now_ms(),ev["start_ts"])
            schedule_decision(route,x,ts,ev["start_ts"])

            # No confirmation gate. Event duration remains descriptive only.
            if not ev["confirmed"]:
                ev["confirmed"]=True; ev["confirm_ts"]=ev["start_ts"]; ev["confirm_net"]=ev["entry_net"]; ev["confirm_size"]=ev["entry_size"]
                ev["confirm_profit"]=ev["entry_profit"]; ev["confirm_ticks"]=1
        elif ev is not None and x is not None:
            # Do not turn threshold chatter around +0.01% into thousands of new
            # events. Close only after a truly neutral edge has persisted.
            ev["last_observed_ts"]=ts
            if x["net"]<=EVENT_CLOSE_NET:
                since=ev.get("neutral_since")
                if since is None: ev["neutral_since"]=ts
                elif ts-since>=EVENT_NEUTRAL_CLOSE_MS:
                    close_event(key,ev,ev["last_ts"]); active_events.pop(key,None)
            else:
                ev.pop("neutral_since",None)

def event_sweeper():
    while True:
        time.sleep(.03); ts=now_ms()
        with event_lock:
            for key,ev in list(active_events.items()):
                if ts-ev.get("last_observed_ts",ev["last_ts"])>EVENT_CLOSE_GAP_MS:
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
        scan_generations[symbol]+=1
        if symbol in queued_symbols:
            with state_lock: state["scan_coalesced"]+=1
            return
        queued_symbols.add(symbol)
    try: scanq.put_nowait(symbol)
    except queue.Full:
        with queued_lock: queued_symbols.discard(symbol)
        with state_lock: state["scan_queue_drops"]+=1

def scan_worker():
    while True:
        sym=scanq.get()
        with queued_lock: generation=scan_generations[sym]
        try:
            with state_lock: routes=list(state["routes_by_symbol"].get(sym,()))[:MAX_ROUTES_PER_SYMBOL_SCAN]
            ts=now_ms()
            for r in routes: process_route(r,ts)
            with state_lock: state["scan_updates"]+=1
        except Exception as e: logerr(f"scan {sym}: {e}")
        finally:
            with queued_lock:
                if scan_generations[sym]!=generation:
                    # At least one delta arrived while processing. Keep the
                    # symbol reserved and schedule exactly one catch-up pass.
                    try: scanq.put_nowait(sym)
                    except queue.Full:
                        queued_symbols.discard(sym)
                        with state_lock: state["scan_queue_drops"]+=1
                else: queued_symbols.discard(sym)
            scanq.task_done()

# ---------- WS ----------
def ws_worker(symbols,worker_id,group_volume_24h=0.0):
    retry_delay=2.0; source_epoch=0
    while True:
        if source_epoch:
            _wait_public_reconnect_slot()
        source_epoch+=1
        feed=PublicFeed(symbols,worker_id,source_epoch)
        public_feeds[worker_id]=feed
        for sym in symbols: public_symbol_feeds[sym]=feed
        _invalidate_public_books(symbols,feed)
        opened=False; health_last_publish=0; connection_stop=threading.Event(); connection_started=time.monotonic()
        try:
            def application_ping_loop(ws, connection_stop=connection_stop):
                with state_lock: expected=max(1,state.get("ws_expected",1))
                # Spread all application PINGs over one interval instead of
                # creating a synchronized burst on every socket.
                if connection_stop.wait(MEXC_APP_PING_SEC*((worker_id-1)%expected)/expected): return
                while not connection_stop.wait(MEXC_APP_PING_SEC):
                    ts=now_ms()
                    with state_lock:
                        h=state["ws_workers"].get(worker_id)
                        if not h or not h.get("connected"): return
                        last_pong=h.get("last_pong_ms") or h.get("opened_ms") or ts
                        sent=int(h.get("pings",0))
                    if sent and ts-last_pong > MEXC_APP_PONG_TIMEOUT_SEC*1000:
                        with state_lock:
                            h=state["ws_workers"].get(worker_id)
                            if h is not None: h["last_error"]="MEXC application PONG timeout"
                        try: ws.close()
                        except Exception: pass
                        return
                    try:
                        ws.send(json.dumps({"method":"PING"}))
                        with state_lock:
                            state["ws_app_pings"]+=1
                            h=state["ws_workers"].get(worker_id)
                            if h is not None:
                                h["last_ping_ms"]=ts; h["pings"]=int(h.get("pings",0))+1
                    except Exception as e:
                        with state_lock:
                            state["ws_ping_errors"]+=1
                            h=state["ws_workers"].get(worker_id)
                            if h is not None:
                                h["ping_errors"]=int(h.get("ping_errors",0))+1; h["last_error"]=str(e)[:300]
                        return
            def on_open(ws):
                nonlocal opened; opened=True
                feed.active=True; feed.close_callback=ws.close
                for sym in symbols: queue_depth_resync(sym,"public_connection_epoch")
                ts=now_ms()
                with state_lock:
                    state["ws_connected"]+=1
                    prev=state["ws_workers"].get(worker_id,{})
                    opens=int(prev.get("opens",0))+1
                    if opens>1: state["ws_reconnects"]+=1
                    state["ws_workers"][worker_id]={"worker_id":worker_id,"symbols":len(symbols),"connected":True,
                        "opens":opens,"disconnects":int(prev.get("disconnects",0)),"opened_ms":ts,
                        "symbol_names":list(symbols),"volume_24h":group_volume_24h,
                        "last_message_ms":None,"last_close_code":prev.get("last_close_code"),
                        "last_error":None,"last_ping_ms":None,"last_pong_ms":None,
                        "pings":int(prev.get("pings",0)),"pongs":int(prev.get("pongs",0)),
                        "ping_errors":int(prev.get("ping_errors",0))}
                params=[f"spot@public.aggre.depth.v3.api.pb@10ms@{s}" for s in symbols]
                ws.send(json.dumps({"method":"SUBSCRIPTION","params":params}))
                threading.Thread(target=application_ping_loop,args=(ws,),daemon=True).start()
                print(f"[Routes V{VERSION}] WS {worker_id}: {len(symbols)} symbols load24h={group_volume_24h:,.0f}")
            def on_message(ws,msg):
                nonlocal health_last_publish
                callback_started=time.perf_counter_ns()
                received=now_ms(); received_mono=time.monotonic()
                if isinstance(msg,str):
                    try:
                        j=json.loads(msg)
                        if str(j.get("msg","")).upper()=="PONG":
                            ts=now_ms()
                            with state_lock:
                                state["ws_app_pongs"]+=1
                                h=state["ws_workers"].get(worker_id)
                                if h is not None:
                                    h["last_pong_ms"]=ts; h["pongs"]=int(h.get("pongs",0))+1
                    except Exception: pass
                    return
                if feed.accept(msg,received,received_mono):
                    if received-health_last_publish>=1000:
                        health_last_publish=received
                        with state_lock:
                            h=state["ws_workers"].get(worker_id)
                            if h is not None: h["last_message_ms"]=received
                feed.metrics["max_callback_us"]=max(feed.metrics["max_callback_us"],(time.perf_counter_ns()-callback_started)/1000.0)
            def on_error(ws,e):
                with state_lock:
                    h=state["ws_workers"].get(worker_id)
                    if h is not None: h["last_error"]=str(e)[:300]
                logerr(f"WS {worker_id}: {e}")
            def on_close(ws,code,msg):
                nonlocal opened
                connection_stop.set(); feed.stop()
                if opened:
                    with state_lock:
                        closed_ts=now_ms()
                        state["ws_connected"]=max(0,state["ws_connected"]-1); state["ws_disconnects"]+=1
                        state["ws_disconnect_times"].append(closed_ts)
                        h=state["ws_workers"].get(worker_id)
                        if h is not None:
                            h["connected"]=False; h["disconnects"]=int(h.get("disconnects",0))+1
                            h["closed_ms"]=closed_ts; h["last_close_code"]=code
                    opened=False
            w=websocket.WebSocketApp(WS,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
            # MEXC specifies JSON {"method":"PING"}; RFC control-frame ping timeouts
            # caused healthy but busy feeds to reconnect and invalidate their books.
            w.run_forever(ping_interval=0)
        except Exception as e: logerr(f"WS loop {worker_id}: {e}")
        finally:
            connection_stop.set(); feed.stop()
            for thread in feed.threads: thread.join(timeout=0.5)
            # websocket-client normally invokes on_close. Keep the counters exact
            # even if run_forever exits through an exception before that callback.
            if opened:
                with state_lock:
                    closed_ts=now_ms()
                    state["ws_connected"]=max(0,state["ws_connected"]-1); state["ws_disconnects"]+=1
                    state["ws_disconnect_times"].append(closed_ts)
                    h=state["ws_workers"].get(worker_id)
                    if h is not None:
                        h["connected"]=False; h["disconnects"]=int(h.get("disconnects",0))+1
                        h["closed_ms"]=closed_ts; h["last_close_code"]="run_forever_exit"
                opened=False
        lived=time.monotonic()-connection_started
        retry_delay=2.0 if lived>=120 and not feed.reason else min(30.0,max(2.0,retry_delay*1.5))
        time.sleep(retry_delay+random.uniform(0.0,1.5))

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
        cf={}
        for threshold in (1.0,2.0,3.0,4.0,5.0):
            row=con.execute("""SELECT COUNT(*) FROM decision_quality_v235
                WHERE ts_ms>=? AND ts_ms<? AND grade IN ('B+','B-')
                  AND selected_usd>=? AND half_bbo_edge>=? AND drop_l1_edge>=? AND coverage3>=?""",
                (start,end,SIM_MIN_TRADE_USD,DEPTH_QUALITY_BPLUS_HALF_EDGE,DEPTH_QUALITY_BPLUS_DROP_FLOOR,threshold)).fetchone()
            cf[f"x{int(threshold)}"]=row[0] or 0
    finally: con.close()
    perf=sorted(depth_quality_compute_us)
    def pct(q):
        if not perf: return None
        return perf[min(len(perf)-1,int((len(perf)-1)*q))]
    return {"rows":[dict(r) for r in rows],"accepted":accepted[0] or 0,"avg_selected_usd":accepted[1],"max_selected_usd":accepted[2],
            "coverage_counterfactual":cf,
            "compute":{"samples":len(perf),"p50_us":pct(.50),"p95_us":pct(.95),"p99_us":pct(.99),"max_us":max(perf) if perf else None}}

def v236_admission_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    try:
        rows=con.execute("""SELECT grade,disposition,COUNT(*) n,SUM(profiles_reserved) profiles_reserved
            FROM shadow_admissions_v236 WHERE ts_ms>=? AND ts_ms<?
            GROUP BY grade,disposition ORDER BY n DESC""",(start,end)).fetchall()
        return {"rows":[dict(r) for r in rows]}
    finally: con.close()

def v239_skip_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    try:
        rows=con.execute("""SELECT profile,reason,COUNT(*) n,AVG(book_age_ms) avg_book_age_ms,
            AVG(executable_usd) avg_executable_usd FROM shadow_skips_v239
            WHERE ts_ms>=? AND ts_ms<? GROUP BY profile,reason ORDER BY n DESC""",(start,end)).fetchall()
        return {"rows":[dict(r) for r in rows]}
    finally: con.close()


def v241_live_funnel_stats():
    start,end,_=local_day_bounds_ms(0); con=db_connect()
    try:
        rows=con.execute("""SELECT disposition,COALESCE(reason,'') reason,COUNT(*) n,
            AVG(requested_usd) avg_requested_usd FROM live_admissions_v241
            WHERE decision_ts_ms>=? AND decision_ts_ms<?
            GROUP BY disposition,COALESCE(reason,'') ORDER BY n DESC""",(start,end)).fetchall()
        recent=con.execute("""SELECT decision_ts_ms,route_id,grade,selected_usd,requested_usd,
            disposition,reason,retry_count,route_tested,attempt_id,fresh_grade,fresh_selected_usd,fresh_edge,
            first_check_delay_ms,gateway_delay_ms,route_depth_age_ms,route_depth_skew_ms,gateway_check_us,
            route_exchange_age_ms,route_exchange_skew_ms,
            retry_reasons_json
            FROM live_admissions_v241
            WHERE decision_ts_ms>=? AND decision_ts_ms<? ORDER BY decision_ts_ms DESC LIMIT 30""",(start,end)).fetchall()
        with live_lock:
            caps=sum(int(v.get("expires_ts_ms") or 0)>=now_ms() and
                _capability_matches_policy(v) for v in live_route_capabilities.values())
        return {"rows":[dict(r) for r in rows],"recent":[dict(r) for r in recent],"validated_routes":caps or 0}
    finally: con.close()


def refresh_dashboard_cache():
    """Run SQLite aggregations off the Flask request path."""
    started=now_ms()
    try:
        rows,label=daily_rows()
        data={"day":label,"rows":rows[:100],"sim":paper_status(),
              "matrix":execution_matrix() if ENABLE_ONLINE_RESEARCH_TRIALS else [],
              "raw_primary":raw_primary_status() if ENABLE_ONLINE_RESEARCH_TRIALS else
                  {"n":0,"executed":0,"wins":0,"losses":0,"pnl":0.0,"avg_exec_net":None},
              "v22":v22_stats(),"recent_decisions":recent_v22_decisions(),
              "shadows":shadow_status(),"quality":v235_quality_stats(),
              "admissions":v236_admission_stats(),"shadow_skips":v239_skip_stats(),
              "live_funnel":v241_live_funnel_stats(),"v235_capture":v234_capture_stats()}
        with dashboard_cache_lock:
            dashboard_cache.update({"updated_ms":now_ms(),"data":data,"last_error":None,
                                    "refresh_ms":now_ms()-started})
        return True
    except Exception as e:
        with dashboard_cache_lock:
            dashboard_cache["last_error"]=str(e)[:300]
        logerr(f"dashboard cache: {e}")
        return False

def dashboard_cache_worker():
    while True:
        time.sleep(DASHBOARD_CACHE_SEC)
        refresh_dashboard_cache()

def runtime_health(now):
    try:
        db_bytes=os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
        wal_path=DB_PATH+"-wal"; wal_bytes=os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        live_db_bytes=os.path.getsize(LIVE_DB_PATH) if os.path.exists(LIVE_DB_PATH) else 0
        live_wal_path=LIVE_DB_PATH+"-wal"
        live_wal_bytes=os.path.getsize(live_wal_path) if os.path.exists(live_wal_path) else 0
    except OSError:
        db_bytes=0; wal_bytes=0; live_db_bytes=0; live_wal_bytes=0
    with state_lock:
        workers=[dict(v) for _,v in sorted(state["ws_workers"].items())]
        connected=state["ws_connected"]; expected=state["ws_expected"]; symbols=list(state["symbols"])
        disconnect_times=list(state.get("ws_disconnect_times",()))
        out={"ws_connected":connected,"ws_expected":expected,"ws_disconnects":state["ws_disconnects"],
             "ws_reconnects":state["ws_reconnects"],"ws_app_pings":state["ws_app_pings"],
             "ws_app_pongs":state["ws_app_pongs"],"ws_ping_errors":state["ws_ping_errors"],
             "scan_coalesced":state["scan_coalesced"],
             "scan_queue_drops":state["scan_queue_drops"],"db_queue_drops":state["db_queue_drops"],
             "depth_gap_events":state["depth_gap_events"],"depth_resync_failures":state["depth_resync_failures"],
             "depth_resync_recoveries":state["depth_resync_recoveries"]}
    out.update({"ws_disconnects_5m":sum(t>=now-300000 for t in disconnect_times),
                "ws_disconnects_15m":sum(t>=now-900000 for t in disconnect_times),
                "ws_disconnects_60m":sum(t>=now-3600000 for t in disconnect_times)})
    ages=[max(0,now-(w.get("last_message_ms") or w.get("opened_ms") or now)) for w in workers if w.get("connected")]
    pong_ages=[max(0,now-(w.get("last_pong_ms") or w.get("opened_ms") or now)) for w in workers if w.get("connected")]
    pong_overdue=sum(1 for w in workers if w.get("connected") and w.get("last_ping_ms") and
                     w.get("last_ping_ms",0)>(w.get("last_pong_ms") or w.get("opened_ms") or 0) and
                     now-w.get("last_ping_ms",now)>MEXC_APP_PING_SEC*1000)
    with depth_lock:
        depth_ages=sorted(max(0,now-int(state["depth"].get(sym,{}).get("ts",0))) for sym in symbols
                          if state["depth"].get(sym,{}).get("ready") and state["depth"].get(sym,{}).get("ts"))
    depth_p95=depth_ages[min(len(depth_ages)-1,int((len(depth_ages)-1)*.95))] if depth_ages else None
    with depth_resync_lock:
        resync_queue=depth_resync_q.qsize(); resync_pending=len(depth_resync_pending)
    with pending_lock:
        shadow_pending=len(pending_shadow_trials)
        shadow_overdue=0; shadow_pending_by_profile={p:0 for p in SHADOW_PROFILES}; shadow_pending_by_stage={"leg1":0,"leg2":0}
        for q in pending_shadow_trials:
            stage=1 if q.get("stage",1)==1 else 2
            due_ts=q["leg1_due"] if stage==1 else q["leg2_due"]
            shadow_overdue+=int(due_ts<now)
            shadow_pending_by_profile[q.get("profile","?")]=shadow_pending_by_profile.get(q.get("profile","?"),0)+1
            shadow_pending_by_stage["leg1" if stage==1 else "leg2"]+=1
        lag=sorted(shadow_execution_lag_ms)
        research_pending=len(pending_trials); paper_pending=len(pending_paper_trials); depth_capture_pending=len(pending_depth_captures)
    with dashboard_cache_lock:
        cache_updated=dashboard_cache.get("updated_ms",0); cache_refresh_ms=dashboard_cache.get("refresh_ms")
        cache_error=dashboard_cache.get("last_error")
    def lag_pct(frac):
        if not lag: return None
        return lag[min(len(lag)-1,int((len(lag)-1)*frac))]
    out.update({"workers":workers,"stale_workers":sum(a>30000 for a in ages),
                "max_worker_message_age_ms":max(ages) if ages else None,
                "max_pong_age_ms":max(pong_ages) if pong_ages else None,
                "workers_without_pong":pong_overdue,
                "app_ping_interval_sec":MEXC_APP_PING_SEC,"app_pong_timeout_sec":MEXC_APP_PONG_TIMEOUT_SEC,
                "depth_book_age_p95_ms":depth_p95,"depth_book_age_max_ms":max(depth_ages) if depth_ages else None,
                "scan_queue":scanq.qsize(),"scan_queue_capacity":scanq.maxsize,
                "db_queue":dbq.qsize(),"db_queue_capacity":dbq.maxsize,
                "db_size_bytes":db_bytes,"db_wal_size_bytes":wal_bytes,
                "live_db_size_bytes":live_db_bytes,"live_db_wal_size_bytes":live_wal_bytes,
                "resync_queue":resync_queue,"resync_pending":resync_pending,
                "research_pending":research_pending,"paper_pending":paper_pending,
                "depth_capture_pending":depth_capture_pending,"shadow_pending":shadow_pending,
                "bot_shadow_schedule_queue":bot_shadow_schedule_q.qsize(),
                "shadow_overdue":shadow_overdue,"shadow_pending_by_profile":shadow_pending_by_profile,
                "shadow_pending_by_stage":shadow_pending_by_stage,
                "dashboard_cache_age_ms":max(0,now-cache_updated) if cache_updated else None,
                "dashboard_refresh_ms":cache_refresh_ms,"dashboard_cache_error":cache_error,
                "shadow_execution_lag":{"samples":len(lag),"p50_ms":lag_pct(.50),"p95_ms":lag_pct(.95),
                    "p99_ms":lag_pct(.99),"max_ms":max(lag) if lag else None}})
    return out

@app.get("/api/status")
def api_status():
    now=now_ms()
    with dashboard_cache_lock:
        cached=dict(dashboard_cache.get("data") or {})
    _,_,fallback_day=local_day_bounds_ms(0)
    rows=cached.get("rows",[]); label=cached.get("day",fallback_day); sim=cached.get("sim",{})
    with state_lock:
        age=now-state["last_ws_ms"] if state["last_ws_ms"] else None
        base={"version":VERSION,"symbols":len(state["symbols"]),"routes":len(state["routes"]),"ws":state["ws_connected"],
              "ws_expected":state["ws_expected"],"age_ms":age,"market_source":state["market_source"],"errors":list(state["errors"]),
              "scan_updates":state["scan_updates"],"diag":state["route_diag"],"depth_ready":state.get("depth_ready",0)}
    base.update({"day":label,"rows":rows[:100],"sim":sim,"fee_pct":FEE*100,"stables":STABLES,"min_exec_usd":MIN_EXEC_USD,
                 "max_exec_usd":MAX_EXEC_USD,"min_net_pct":MIN_NET*100,"primary_age_ms":PRIMARY_BBO_AGE_MS,"primary_latency_ms":PRIMARY_EXEC_LATENCY_MS,
                 "research_ages":RESEARCH_BBO_AGES_MS,"research_latencies":RESEARCH_LATENCIES_MS,
                 "matrix":cached.get("matrix",[]),"raw_primary":cached.get("raw_primary",{}),"rebalance_cover_mult":REBALANCE_COVER_MULT,"v22":cached.get("v22",{}),
                 "trace_window_ms":TRACE_WINDOW_MS,"decision_max_recv_age_ms":DECISION_MAX_RECV_AGE_MS,"decision_max_skew_ms":DECISION_MAX_SKEW_MS,"primary_max_skew_ms":PRIMARY_MAX_SKEW_MS,"paper_rearm_neutral_ms":PAPER_REARM_NEUTRAL_MS,"paper_hard_cooldown_ms":PAPER_HARD_COOLDOWN_MS,
                 "recent_decisions":cached.get("recent_decisions",[]),"shadows":cached.get("shadows",{}),"quality":cached.get("quality",{}),
                 "admissions":cached.get("admissions",{}),"shadow_skips":cached.get("shadow_skips",{}),"health":runtime_health(now),
                 "live":live_status(),"live_funnel":cached.get("live_funnel",{}),"public_flow":public_flow_status(),
                 "coverage":route_coverage_status(),
                 "shadow_profiles":{k:{"leg1_ms":v[0],"leg2_ms":v[1]} for k,v in SHADOW_PROFILES.items()},
                 "v235_policy":{"edge_min_pct":SHADOW_MIN_EDGE*100,"target_weights":SHADOW_TARGET_WEIGHTS,
                    "rebalance_check_min":SHADOW_REBALANCE_CHECK_SEC/60,"rebalance_max_h":SHADOW_REBALANCE_MAX_SEC/3600,
                    "early_dev_pct":SHADOW_REBALANCE_EARLY_DEV*100,"post_resync_grace_ms":POST_RESYNC_GRACE_MS,
                    "depth_capture_offsets_ms":DEPTH_CAPTURE_OFFSETS_MS,"depth_capture_levels":DEPTH_CAPTURE_LEVELS,
                    "a_fraction_pct":DEPTH_QUALITY_A_FRACTION*100,"b_fraction_pct":DEPTH_QUALITY_B_FRACTION*100,
                    "bplus_half_edge_pct":DEPTH_QUALITY_BPLUS_HALF_EDGE*100,"bplus_drop_floor_pct":DEPTH_QUALITY_BPLUS_DROP_FLOOR*100,
                    "coverage3_min":DEPTH_QUALITY_COVERAGE3_MIN,"absolute_cap_usd":SHADOW_TRADE_CAP_USD,
                    "leg2_min_edge_pct":LIVE_LEG2_MIN_EDGE*100,"fee_safety_pct":LIVE_FEE_SAFETY*100,
                    "bot_shadow_rebalance":BOT_SHADOW_REBALANCE_ENABLED,
                    "replay_min_edge_pct":REPLAY_MIN_EDGE*100,"online_research_trials":ENABLE_ONLINE_RESEARCH_TRIALS,
                    "event_neutral_close_ms":EVENT_NEUTRAL_CLOSE_MS},
                 "v235_capture":cached.get("v235_capture",{})})
    return jsonify(base)

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MEXC 2-Leg V2.4.11</title>
<style>body{background:#090d14;color:#e8edf5;font-family:system-ui;margin:0;padding:16px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.c,.panel{background:#111827;border:1px solid #273248;border-radius:10px;padding:12px}.v{font-weight:800;font-size:20px}.big{font-size:28px}.muted{color:#9aa7bd;font-size:12px}.good{color:#22d38a}.bad{color:#ff6677}.panel{margin-top:12px;overflow:auto}.latency{display:grid;grid-template-columns:repeat(3,minmax(250px,1fr));gap:10px}.latbox{background:#0c1421;border:1px solid #2a3850;border-radius:10px;padding:12px}.balance{margin-top:8px;line-height:1.7}.metricgrid{display:grid;grid-template-columns:repeat(2,minmax(95px,1fr));gap:7px;margin-top:10px}.metric{background:#111b2b;border-radius:8px;padding:8px}.tag{display:inline-block;border:1px solid #334155;border-radius:999px;padding:3px 7px;font-size:11px;color:#bac6d8}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:8px;border-bottom:1px solid #263044;text-align:right}th:first-child,td:first-child{text-align:left}@media(max-width:900px){.latency{grid-template-columns:1fr}}
h2{font-size:22px;margin:4px 0 12px}h3{font-size:15px;margin:8px 0}.panel{padding:10px}.sim-panels{display:grid;grid-template-columns:1fr 1fr;gap:12px}.sim-panel{min-width:0}.sim-panel table{font-variant-numeric:tabular-nums}.sim-panel th,.sim-panel td{padding:6px 5px;white-space:nowrap}.sim-panel .pnl{font-weight:700}.sim-description{margin:4px 0 8px}.sim-detail{margin-top:8px;border-top:1px solid #273248;padding-top:7px}.sim-detail summary{font-size:12px;color:#bac6d8}.sim-scroll{overflow:auto}.sim-caption{font-size:11px;color:#9aa7bd;margin:6px 0 0}summary{cursor:pointer;font-weight:600;line-height:1.6}summary:focus-visible{outline:2px solid #79b8ff;outline-offset:3px}details[open]>summary{margin-bottom:8px}.brief-grid{display:grid;grid-template-columns:repeat(6,minmax(90px,1fr));gap:10px}.brief-grid .v{font-size:18px}.brief-state{margin:0 0 8px}.sim-detail td{white-space:normal;min-width:65px}.sim-detail td.balances{min-width:150px}@media(max-width:1050px){.sim-panels{grid-template-columns:1fr}.brief-grid{grid-template-columns:repeat(3,minmax(90px,1fr))}}@media(max-width:600px){body{padding:10px}.cards{gap:6px;grid-template-columns:repeat(3,minmax(0,1fr))}.cards .v{font-size:16px}h2{font-size:18px}.sim-panel table{font-size:11px}.sim-panel th,.sim-panel td{padding:6px 3px}.sim-nav{display:none}.brief-grid{gap:7px}.brief-grid .v{font-size:16px}.panel{padding:9px}}
</style></head><body>
<h2>MEXC — Scanner + micro-bot Spot 2-leg V2.4.8</h2><div class=cards id=cards></div>
<div class=panel id=livebrief></div>
<div class=sim-panels><div class="panel sim-panel" id=latencies></div><div class="panel sim-panel" id=botlatencies></div></div>
<details class=panel id=publicflow-fold><summary id=publicflow-summary>Flux publics</summary><div id=publicflow></div></details>
<details class=panel><summary>Exécution LIVE — métriques et paramètres</summary><div id=live></div></details>
<details class=panel><summary>Santé du serveur et des connexions</summary><div id=health></div></details>
<details class=panel><summary>Couverture des routes</summary><div id=coverage></div></details>
<details class=panel><summary>Règles d'admission à taille réelle</summary><div id=v247></div></details>
<details class=panel><summary>Confirmation des ordres — WebSocket privé</summary><div id=privatews></div></details>
<details class=panel><summary>Admissions et motifs de refus BOT</summary><div id=livefunnel></div></details>
<details class=panel><summary>Politique de simulation</summary><div id=policy></div></details>
<details class=panel><summary>Qualité des signaux — compteurs détaillés</summary><div id=quality></div></details>
<div class=panel><h3>Décisions récentes — qualité économique à T0</h3><p>La classe A/B+ décrit la profondeur. Une donnée MEXC périmée reste enregistrée mais est refusée pour une nouvelle admission Shadow ou LIVE.</p><table><thead><tr><th>Heure</th><th>Route</th><th>Edge T0</th><th>Classe</th><th>Taille retenue</th><th>Edge −50% BBO</th><th>Edge sans L1</th><th>Réserve L1–L3</th><th>Calcul</th></tr></thead><tbody id=recent></tbody></table></div>
<div class=panel id=capture></div><div class=panel id=diag></div>
<script>
function simNumber(v,n=2){return v==null||!Number.isFinite(Number(v))?'—':Number(v).toFixed(n)}
function simLabel(n){return n.replace('BOT_','').replaceAll('_',' / ')}
function renderSimPanel(id,names,sh,title,description){
 let host=document.getElementById(id);
 if(!host.querySelector('.sim-main'))host.innerHTML='<h3 class="sim-title"></h3><p class="muted sim-description"></p><div class="sim-scroll sim-main"></div><details class="sim-detail"><summary>Soldes, coûts et détails</summary><div class="sim-scroll sim-extra"></div></details><p class="sim-caption">Simulation uniquement · aucun total entre profils indépendants.</p>';
 host.querySelector('.sim-title').textContent=title;host.querySelector('.sim-description').textContent=description;
 host.querySelector('.sim-caption').textContent='Simulation · profils indépendants · L1 et L2 mesurées '+(names[0].startsWith('BOT_')?'depuis l’admission fraîche.':'depuis T0.');
 let rows='',extra='';
 for(let n of names){let x=sh[n];if(!x)continue;let pnl=simNumber(x.profit),pc=x.profit==null?'muted':(Number(x.profit)>=0?'good':'bad');
 rows+=`<tr><td>${simLabel(n)}</td><td>${simNumber(x.leg1_ms,0)} / ${simNumber(x.leg2_ms,0)}</td><td class="pnl ${pc}">${Number(x.profit)>0?'+':''}${pnl} $</td><td>${simNumber(x.trades,0)}</td><td>${simNumber(x.wins,0)} / ${simNumber(x.losses,0)}</td><td class=sim-nav>${simNumber(x.nav)} $</td></tr>`;
 let balances=Object.entries(x.balances||{}).map(([a,v])=>`${a} ${simNumber(v)}`).join(' · ');
 extra+=`<tr><td>${simLabel(n)}</td><td class=balances>${balances||'—'}</td><td>${simNumber(x.nav)} $</td><td>${x.avg_edge==null?'—':simNumber(100*x.avg_edge,3)+'%'}</td><td>${simNumber(x.rebalances,0)} / ${simNumber(x.rebalance_cost,3)} $</td><td>${simNumber(x.skipped_balance,0)} / ${simNumber(x.skipped_flash,0)} / ${simNumber(x.open_exposures,0)}</td></tr>`;}
 host.querySelector('.sim-main').innerHTML=`<table><thead><tr><th>Profil</th><th>L1 / L2 ms</th><th>PnL</th><th>Trades</th><th>Gagnés / perdus</th><th class=sim-nav>NAV</th></tr></thead><tbody>${rows||'<tr><td colspan=6>En attente de données</td></tr>'}</tbody></table>`;
 host.querySelector('.sim-extra').innerHTML=`<table><thead><tr><th>Profil</th><th>Soldes</th><th>NAV</th><th>Edge T0 moy.</th><th>Rebal. / coût</th><th>Refus solde / L1 annulée / expositions</th></tr></thead><tbody>${extra}</tbody></table>`;
}
async function refresh(){let d=await fetch('/api/status').then(r=>r.json()),g=d.diag,p=d.v235_policy||{},cap=d.v235_capture||{},ql=d.quality||{},ad=d.admissions||{},sk=d.shadow_skips||{},he=d.health||{},lv=d.live||{},lf=d.live_funnel||{},la=lv.arm||{},cv=d.coverage||{},rc=cv.route_counts||{},tc=cv.tick_counts||{},pv=lv.prevalidation||{},pw=lv.private_order_stream||{};
let pf=d.public_flow||{};let pfr=(pf.workers||[]).map(w=>`<tr><td>${w.worker_id}</td><td>${!w.active?'reconnexion':(w.catching_up?'rattrapage':'actif')}</td><td>${w.lag_ms??'-'}</td><td>${w.queue??0} / ${Number(w.queue_age_ms||0).toFixed(0)} ms</td><td>${Number(w.messages_per_sec||0).toFixed(0)} / ${Number((w.processed||0)/Math.max(1,w.batches||0)).toFixed(1)}</td><td>${Number(w.max_callback_us||0).toFixed(0)} / ${Number(w.max_apply_us||0).toFixed(0)}</td><td>${w.recovery_reason||'-'}</td></tr>`).join('');document.getElementById('publicflow').innerHTML=`<h3>Flux publics V2.4.10</h3><p>Retard du dernier message, distinct de l'âge d'un carnet inactif. Traitement public centralisé, équitable, par lots de 32 maximum sans délai de remplissage. Rattrapage : entrées suspendues, deltas conservés. Seuils LIVE : 50 / 100 / 50 ms.</p><table><thead><tr><th>WS</th><th>État</th><th>Retard ms</th><th>File / attente</th><th>Messages/s / lot moyen</th><th>Réception / traitement max µs</th><th>Récupération</th></tr></thead><tbody>${pfr}</tbody></table>`;
document.getElementById('v247').innerHTML=`<div class=tag>V2.4.7 · ADMISSION À TAILLE RÉELLE</div><h3>${lv.entry_order_policy||'-'} · ${lv.validated_routes||0} routes compatibles / ${lv.legacy_capabilities_stored||0} capacités stockées</h3><p>Le LIVE réévalue les nouvelles versions du carnet, indépendamment du premier classement replay. A/B+ LIVE est calculé à la taille abordable, cap 20 $, frais et réserve inclus. Les anciennes validations MARKET ne valident pas les ordres FOK.</p><p>Compteurs cumulés depuis ce démarrage (observations, pas trades) :</p><table>${Object.entries(lv.v247_counts||{}).sort((a,b)=>b[1]-a[1]).slice(0,12).map(([k,n])=>`<tr><td>${k}</td><td>${n}</td></tr>`).join('')}</table><p class=muted>Les Shadows restent des scénarios de carnet : absence de preuve de remplissage FOK, commissions supposées et absence de modèle de concurrence au matching. Leurs profits ne sont pas une prédiction du LIVE. Un snapshot REST sans timestamp MEXC reste exclu du LIVE jusqu'à un delta WS cohérent. Journaux et pertes historiques sont conservés.</p>`;
document.getElementById('privatews').innerHTML=`<div class=tag>ORDRES WEBSOCKET PRIVÉ V2.4.7</div><h3 class='${pw.connected?'good':((lv.configured_mode||'shadow')==='shadow'?'':'bad')}'>Mode ${(pw.mode||'off').toUpperCase()} · ${pw.connected?'connecté':'déconnecté'}${pw.subscribed?' · abonné':''}</h3><div class=cards><div><div class=v>${pw.opens||0} / ${pw.disconnects||0}</div><div class=muted>ouvertures / chutes privées</div></div><div><div class=v>${pw.messages||0} / ${pw.order_events||0}</div><div class=muted>messages / événements ordre</div></div><div><div class=v>${pw.terminal_events||0}</div><div class=muted>événements terminaux</div></div><div><div class=v>${pw.matched_events||0} / ${pw.unmatched_events||0}</div><div class=muted>liés au bot / non liés</div></div><div><div class=v>${pw.ws_before_rest||0} / ${pw.rest_before_ws||0} / ${pw.same_ms||0}</div><div class=muted>WS avant REST / après / égal</div></div><div><div class=v>${pw.post_to_event?.p95_ms==null?'-':Number(pw.post_to_event.p95_ms).toFixed(1)+' ms'}</div><div class=muted>POST → événement privé p95</div></div><div><div class=v>${pw.ws_rest_delta?.p95_ms==null?'-':Number(pw.ws_rest_delta.p95_ms).toFixed(1)+' ms'}</div><div class=muted>delta WS − REST p95</div></div><div><div class=v>${pw.confirmations_ws||0} / ${pw.confirmations_rest||0} / ${pw.confirmations_post||0}</div><div class=muted>autorité WS / REST / POST</div></div><div><div class=v>${pw.pings||0} / ${pw.pongs||0}</div><div class=muted>PING / PONG privés</div></div><div><div class=v>${pw.listenkey_keepalive_ok||0} / ${pw.listenkey_keepalive_errors||0}</div><div class=muted>listenKey OK / erreurs</div></div><div><div class='v ${(pw.decode_errors||0)===0?'good':'bad'}'>${pw.decode_errors||0}</div><div class=muted>erreurs de décodage</div></div><div><div class=v>${pw.pending_orders||0}</div><div class=muted>ordres suivis</div></div></div><div class=muted style='margin-top:10px'>HYBRID : le premier résultat terminal correspondant exactement au clientOrderId et au symbole (WS privé ou REST) devient décisionnaire. Le contrôle REST indépendant continue après une confirmation WS afin de réconcilier et mesurer l'écart, sans retarder la jambe suivante. Une perte du WS bloque les nouvelles entrées ; le secours REST reste actif pour un ordre déjà envoyé.</div>`;
document.getElementById('cards').innerHTML=`<div class=c><div class=v>${d.symbols}</div><div class=muted>marchés WS</div></div><div class=c><div class=v>${d.routes}</div><div class=muted>routes 2-leg</div></div><div class=c><div class=v>${d.ws}/${d.ws_expected}</div><div class=muted>WebSockets</div></div><div class=c><div class=v>${d.age_ms??'-'} ms</div><div class=muted>dernier BBO reçu</div></div><div class=c><div class=v>${d.depth_ready||0}/${d.symbols}</div><div class=muted>carnets Depth prêts</div></div><div class=c><div class=v>${d.v22?.decisions||0}</div><div class=muted>décisions replay ≥${(p.replay_min_edge_pct??0.30).toFixed(2)}%</div></div><div class=c><div class=v>${d.v22?.trace_points||0}</div><div class=muted>points trajectoire</div></div>`;
let wsok=(he.ws_connected===he.ws_expected)&&(he.stale_workers||0)===0&&(he.workers_without_pong||0)===0&&(d.depth_ready===d.symbols)&&(he.resync_pending||0)===0;document.getElementById('health').innerHTML=`<div class=tag>SANTÉ TEMPS RÉEL</div><h3 class='${wsok?'good':'bad'}'>WebSockets ${he.ws_connected||0}/${he.ws_expected||0} · ${he.stale_workers||0} silencieux &gt;30 s</h3><div class=cards><div><div class=v>${he.ws_disconnects||0}</div><div class=muted>chutes WS</div></div><div><div class=v>${he.ws_reconnects||0}</div><div class=muted>reconnexions WS</div></div><div><div class=v>${he.ws_app_pings||0} / ${he.ws_app_pongs||0}</div><div class=muted>PING / PONG MEXC</div></div><div><div class=v>${he.max_pong_age_ms==null?'-':he.max_pong_age_ms+' ms'}</div><div class=muted>âge maximal PONG</div></div><div><div class=v>${he.workers_without_pong||0} / ${he.ws_ping_errors||0}</div><div class=muted>PONG en retard / erreurs ping</div></div><div><div class=v>${he.max_worker_message_age_ms==null?'-':he.max_worker_message_age_ms+' ms'}</div><div class=muted>silence maximal worker WS</div></div><div><div class=v>${he.depth_book_age_p95_ms==null?'-':he.depth_book_age_p95_ms+' ms'}</div><div class=muted>âge Depth p95</div></div><div><div class=v>${he.depth_book_age_max_ms==null?'-':he.depth_book_age_max_ms+' ms'}</div><div class=muted>âge Depth maximum prêt</div></div><div><div class=v>${he.depth_gap_events||0}</div><div class=muted>gaps Depth hors bootstrap</div></div><div><div class=v>${he.depth_resync_failures||0}</div><div class=muted>échecs resync</div></div><div><div class=v>${he.resync_queue||0}/${he.resync_pending||0}</div><div class=muted>file/pending resync</div></div><div><div class=v>${he.scan_queue||0}/${he.scan_queue_capacity||0}</div><div class=muted>file scan</div></div><div><div class=v>${he.scan_queue_drops||0}</div><div class=muted>abandons file scan</div></div><div><div class=v>${he.scan_coalesced||0}</div><div class=muted>updates fusionnées</div></div><div><div class=v>${he.db_queue||0}/${he.db_queue_capacity||0}</div><div class=muted>file DB</div></div><div><div class=v>${he.db_queue_drops||0}</div><div class=muted>abandons DB</div></div><div><div class=v>${((he.db_size_bytes||0)/1048576).toFixed(0)} / ${((he.db_wal_size_bytes||0)/1048576).toFixed(0)} MB</div><div class=muted>base / WAL</div></div><div><div class=v>${he.research_pending||0} / ${he.depth_capture_pending||0}</div><div class=muted>matrice live / captures pending</div></div><div><div class='v ${(he.shadow_overdue||0)===0?'good':'bad'}'>${he.shadow_pending||0} / ${he.shadow_overdue||0}</div><div class=muted>pending Shadow / en retard</div></div><div><div class=v>${he.shadow_execution_lag?.p95_ms==null?'-':he.shadow_execution_lag.p95_ms+' ms'}</div><div class=muted>retard worker Shadow p95</div></div><div><div class=v>${he.dashboard_cache_age_ms==null?'-':Math.round(he.dashboard_cache_age_ms/1000)+' s'}</div><div class=muted>âge statistiques page</div></div><div><div class=v>${he.dashboard_refresh_ms==null?'-':he.dashboard_refresh_ms+' ms'}</div><div class=muted>durée calcul statistiques</div></div></div>`;
document.getElementById('coverage').innerHTML=`<div class=tag>AUDIT COMPLET DES ${cv.total_routes||d.routes} ROUTES</div><h3>Couverture reçue → qualité → exécution BOT</h3><div class=cards><div><div class=v>${rc.evaluations||0}/${cv.total_routes||0}</div><div class=muted>routes évaluées aujourd'hui</div></div><div><div class=v>${rc.calculable||0}</div><div class=muted>routes avec deux carnets calculables</div></div><div><div class=v>${rc.research_fresh||0}</div><div class=muted>routes Depth frais ≤${d.primary_age_ms||150} ms</div></div><div><div class=v>${rc.live_fresh||0}</div><div class=muted>routes fraîches passerelle ≤${lv.bbo_age_ms||50} ms</div></div><div><div class=v>${rc.edge_ge_030||0}</div><div class=muted>routes ayant atteint 0,30%</div></div><div><div class=v>${rc.edge_ge_035||0}</div><div class=muted>routes ayant atteint 0,35%</div></div><div><div class=v good>${rc.quality_eligible||0}</div><div class=muted>routes classées A/B+</div></div><div><div class=v>${cv.validated_now||0}</div><div class=muted>routes /order/test valides</div></div><div><div class=v good>${rc.bot_admitted||0}</div><div class=muted>routes arrivées en Shadow BOT</div></div></div><div class=muted style='margin-top:10px'>Observations agrégées : ≥0,30% ${tc.edge_ge_030||0} · ≥0,35% ${tc.edge_ge_035||0} · A/B+ ${tc.quality_eligible||0} (A ${tc.quality_a||0} · B+ ${tc.quality_bplus||0}). Écriture fixe : ${cv.storage_rows_per_day||0} lignes/jour, mise à jour toutes les ${cv.flush_sec||60} s — aucune ligne par tick.</div>`;
let modeBrief=(lv.configured_mode||'shadow').toUpperCase();let liveBlocked=lv.circuit_open||!la.effective;
let briefCards=[[simNumber(lv.realized_pnl,4)+' $','PnL réalisé'],[simNumber(lv.completed,0),'Trades réels'],[simNumber(lv.open_exposures,0),'Expositions ouvertes'],[simNumber(lv.cap_usd,0)+' $','Cap par ordre'],[simNumber(lv.capital_limit_usd,0)+' $','Capital dédié'],[lv.api_ok?'OK':'NON','API privée']];
document.getElementById('livebrief').innerHTML=`<p class="brief-state ${liveBlocked?'bad':'good'}"><b>${modeBrief}</b> · ${lv.circuit_open?'Circuit ouvert : '+(lv.circuit_reason||'—'):(la.reason||'État non disponible')}</p><div class=brief-grid>${briefCards.map(([v,label])=>`<div><div class=v>${v}</div><div class=muted>${label}</div></div>`).join('')}</div>`;
if(lv.session?.required){
 let ss=lv.session, note=document.createElement('p');
 note.textContent=`Session LIVE limitée · PnL net ${simNumber(ss.pnl,4)} $ · ${ss.buys??'—'}/${ss.max_buys??'—'} achats · seuil supplémentaire ${ss.additional_loss_usd??'—'} $ · fin ${ss.expires_ts_ms?new Date(ss.expires_ts_ms).toLocaleTimeString():'—'} · ${ss.reason||'ouverte'} · exclus : ${(lv.excluded_assets||[]).join(', ')}`;
 document.getElementById('livebrief').appendChild(note);
}
if(Number(lv.open_exposures)>0){
 let alert=document.createElement('div');alert.className='bad';alert.style.cssText='border:2px solid #f87171;border-radius:8px;padding:12px;margin-top:10px';
 let items=(lv.open_exposure_items||[]).map(x=>`${simNumber(x.units,8)} ${x.asset}`).join(' · ');
 alert.textContent=`INTERVENTION REQUISE — ${items||lv.open_exposures+' position(s) ouverte(s)'}. Les nouvelles entrées sont bloquées. La sortie automatique est terminée ; une position peut continuer à perdre de la valeur.`;
 document.getElementById('livebrief').appendChild(alert);
}
let flowWorkers=(d.public_flow||{}).workers||[];let recovering=flowWorkers.filter(w=>!w.active).length;document.getElementById('publicflow-summary').textContent=`Flux publics · ${flowWorkers.length-recovering}/${flowWorkers.length} connectés · ${flowWorkers.filter(w=>w.catching_up&&w.active).length} en rattrapage · ${recovering} en reconnexion · décodeur ${(d.public_flow||{}).protobuf_backend||'—'}`;
let lb=Object.entries(lv.balances||{}).filter(([a])=>(lv.allowed_start_assets||[]).includes(a)).map(([a,v])=>`${a} <b>${Number(v.free||0).toFixed(4)}</b>`).join(' · '),liveok=la.effective&&!lv.circuit_open,mode=(lv.configured_mode||'shadow').toUpperCase();
let fmtMs=x=>x?.p95_ms==null?'-':Number(x.p95_ms).toFixed(1)+' ms';
let econ=lv.open_exposure_economic_pnl,realized=Number(lv.realized_pnl||0);
let liveCards=[
 [lv.api_ok?'OK':'NON','API privée / canTrade',lv.api_ok?'good':'bad'],
 [Number(lv.cap_usd||0).toFixed(0)+' $','cap par ordre initial'],
 [Number(lv.capital_limit_usd||0).toFixed(0)+' $','capital dédié maximal'],
 [Number(lv.daily_loss_limit_usd||0).toFixed(0)+' $',lv.session?.required?'seuil quotidien standard (session active ci-dessus)':'coupe-circuit sur pertes réalisées'],
 [lv.validated_routes||0,'routes directionnelles valides'],
 [lv.test_validations||0,'validations opportunité /order/test'],
 [lv.capability_reuses||0,'validations réutilisées'],
 [lv.completed||0,'trades réels terminés'],
 [lv.wins+' / '+lv.losses,'gagnés / perdus réels'],
 [(realized>=0?'+':'')+realized.toFixed(4)+' $','PnL réalisé journalisé',realized>=0?'good':'bad'],
 [lv.open_exposures||0,'expositions ouvertes — hors trades',(lv.open_exposures||0)===0?'good':'bad'],
 [econ==null?'-':(econ>=0?'+':'')+Number(econ).toFixed(4)+' $','PnL économique exposé — non réalisé',(econ==null||econ>=0)?'good':'bad'],
 [(lv.busy?'1':'0')+' / '+(lv.queued||0),'actif / en file'],
 [fmtMs(lv.first_check_latency),'T0 → premier contrôle p95'],
 [fmtMs(lv.gateway_admit_latency),'T0 → admission fraîche p95'],
 [fmtMs(lv.admission_to_post),'admission → POST jambe 1 p95'],
 [fmtMs(lv.leg1_post_http),'HTTP POST jambe 1 p95'],
 [fmtMs(lv.leg2_post_http),'HTTP POST jambe 2 p95'],
 [fmtMs(lv.leg1_confirmation),'POST → confirmation jambe 1 p95 LIVE'],
 [fmtMs(lv.leg2_confirmation),'POST → confirmation jambe 2 p95 LIVE'],
 [fmtMs(lv.prepare_journal),'journal avant ordre p95'],
 [fmtMs(lv.response_journal),'journal réponse p95'],
 [lv.gateway_check_compute?.p95_us==null?'-':Number(lv.gateway_check_compute.p95_us).toFixed(1)+' µs','contrôle gateway p95'],
 [lv.last_route_exchange_age_ms==null?'-':lv.last_route_exchange_age_ms+' ms','dernier âge MEXC contrôlé'],
 [lv.last_route_exchange_skew_ms==null?'-':lv.last_route_exchange_skew_ms+' ms','dernier écart MEXC L1/L2'],
 [Number(lv.mexc_clock_offset_ms||0).toFixed(0)+' ms','correction horloge locale → MEXC'],
 [lv.last_route_exchange_age_t0_ms==null?'-':lv.last_route_exchange_age_t0_ms+' ms','âge MEXC au T0 du dernier ordre'],
 [lv.last_route_exchange_skew_t0_ms==null?'-':lv.last_route_exchange_skew_t0_ms+' ms','écart MEXC L1/L2 au T0'],
 [lv.last_leg2_exchange_age_ms==null?'-':lv.last_leg2_exchange_age_ms+' ms','âge MEXC jambe 2 après fill L1'],
 [lv.last_leg2_exchange_skew_ms==null?'-':lv.last_leg2_exchange_skew_ms+' ms','écart fill L1 → Depth L2'],
 [fmtMs(lv.bot_shadow_schedule_lag),'copie Shadow hors chemin p95'],
 [lv.last_keepalive_rtt_ms==null?'-':Number(lv.last_keepalive_rtt_ms).toFixed(1)+' ms','dernier keep-alive HTTP'],
 [(lv.keepalive_ok||0)+' / '+(lv.keepalive_errors||0),'keep-alive OK / erreurs'],
 [lv.account_cache_age_ms==null?'-':lv.account_cache_age_ms+' ms','âge cache compte']
];
let liveCardHtml=liveCards.map(x=>"<div><div class='v "+(x[2]||'')+"'>"+x[0]+"</div><div class=muted>"+x[1]+"</div></div>").join('');
document.getElementById('live').innerHTML=`<div class=tag>PASSERELLE PRIVÉE V2.4.7</div><h3 class='${liveok?'good':((lv.configured_mode||'shadow')==='shadow'?'':'bad')}'>Mode ${mode} · ${la.reason||'-'}</h3><div class=cards>${liveCardHtml}</div><div class=muted style='margin-top:10px'>Soldes libres dédiés : ${lb||'-'} · Protection autres actifs ${lv.protected_bags?'active':'inactive'} · Rééquilibrage réel désactivé ; Shadows BOT ${lv.bot_shadow_rebalancing?'30 min / 4 h':'désactivé'} · Fraîcheur locale entrée/jambe 1 ≤${lv.bbo_age_ms||50} ms et MEXC ≤${lv.exchange_age_ms||100} ms · écart MEXC L1/L2 ≤${lv.max_exchange_skew_ms||50} ms · fraîcheur locale jambe 2 ≤${lv.leg2_bbo_age_ms||100} ms et MEXC ≤${lv.leg2_exchange_age_ms||150} ms · retard Depth L2 sur fill L1 ≤${lv.leg2_max_exchange_skew_ms||150} ms · Haircut unique ${Number(lv.fee_safety_pct||0).toFixed(2)}% · jambe 2 seulement si edge conservateur ≥${Number(lv.leg2_min_edge_pct||0).toFixed(2)}% · Commission jambe 1 différée ${lv.defer_leg1_commission?'oui':'non'} · Unwind ${lv.emergency_unwind==='prevalidated_market_without_depth_gate'?'MARKET prévalidé sans verrou Depth':'-'} · ${lv.circuit_causes?.length||0} cause(s) de circuit conservée(s) · Circuit ${lv.circuit_open?'OUVERT: '+(lv.circuit_reason||'-'):'fermé'} · journal critique séparé : ${lv.live_journal_path||'-'} · HTTP ordres gardé chaud toutes les ${lv.http_keepalive_sec||'-'} s · route testée dans les ${Number(lv.test_max_age_hours||0).toFixed(0)} h requise : ${lv.require_tested_route?'oui':'non'}. ${mode==='TEST'?'Les requêtes sont envoyées à /api/v3/order/test : aucune entrée dans le carnet et aucun fonds déplacé.':(mode==='LIVE'?'Les ordres réels exigent les trois verrous locaux.':'Passerelle inactive.')}</div>`;
document.getElementById('live').insertAdjacentHTML('beforeend',`<div class=cards style='margin-top:10px'><div><div class=v>${lv.leg1_http_queue?.p95_ms==null?'-':Number(lv.leg1_http_queue.p95_ms).toFixed(1)+' ms'}</div><div class=muted>attente verrou HTTP jambe 1 p95</div></div><div><div class=v>${lv.leg2_http_queue?.p95_ms==null?'-':Number(lv.leg2_http_queue.p95_ms).toFixed(1)+' ms'}</div><div class=muted>attente verrou HTTP jambe 2 p95</div></div><div><div class=v>${Number(lv.http_keepalive_timeout_sec||0).toFixed(2)} s</div><div class=muted>timeout keep-alive HTTP</div></div></div>`);
document.getElementById('live').insertAdjacentHTML('beforeend',`<div class=cards style='margin-top:10px'><div><div class=v>${pv.running?'ACTIVE':'OFF'}</div><div class=muted>prévalidation de fond TEST</div></div><div><div class=v>${pv.checked||0}/${cv.total_routes||0}</div><div class=muted>progression cycle courant</div></div><div><div class=v good>${pv.validated||0}</div><div class=muted>routes prévalidées depuis démarrage</div></div><div><div class=v>${pv.skipped_depth||0}/${pv.skipped_size||0}</div><div class=muted>reportées Depth / taille</div></div><div><div class='v ${(pv.errors||0)===0?'good':'bad'}'>${pv.errors||0}</div><div class=muted>erreurs de prévalidation</div></div></div><div class=muted style='margin-top:8px'>Session HTTP dédiée, une route toutes les ${Number(lv.prevalidation_interval_sec||0).toFixed(0)} s ; suspendue automatiquement en LIVE. Le carnet éventuellement ancien sert uniquement à construire la requête syntaxique /order/test : aucune admission BOT ne contourne la fraîcheur stricte de ${lv.bbo_age_ms||50} ms. Dernier état : ${pv.last_route||'-'} · ${pv.last_status||'-'}${pv.last_error?' · '+pv.last_error:''}.</div>`);
let fc={};for(let r of (lf.rows||[])){let k=r.disposition+(r.reason?': '+r.reason:'');fc[k]=(fc[k]||0)+r.n}let fr=(lf.recent||[]).map(x=>`<tr><td>${new Date(x.decision_ts_ms).toLocaleTimeString()}</td><td>${x.route_id}</td><td>${x.grade} → ${x.fresh_grade||'-'}</td><td>${Number(x.selected_usd||0).toFixed(2)} $ → ${x.fresh_selected_usd==null?'-':Number(x.fresh_selected_usd).toFixed(2)+' $'}</td><td>${x.requested_usd==null?'-':Number(x.requested_usd).toFixed(2)+' $'}</td><td>${x.disposition}</td><td>${x.reason||'-'}</td><td>${x.retry_count||0}</td><td>${x.first_check_delay_ms==null?'-':x.first_check_delay_ms+' ms'} / ${x.gateway_delay_ms==null?'-':x.gateway_delay_ms+' ms'}</td><td>${x.route_depth_age_ms==null?'-':x.route_depth_age_ms+' ms'} / ${x.route_depth_skew_ms==null?'-':x.route_depth_skew_ms+' ms'}</td></tr>`).join('');document.getElementById('livefunnel').innerHTML=`<div class=tag>ENTONNOIR BOT V2.4.7</div><h3>Classe initiale → classe fraîche réellement admise</h3><div class=cards><div><div class=v>${lv.validated_routes||0}</div><div class=muted>routes directionnelles valides</div></div><div><div class=v>${lv.bot_shadow_admissions||0}</div><div class=muted>signaux simulation BOT admis</div></div><div><div class=v>${he.ws_disconnects_5m||0} / ${he.ws_disconnects_15m||0} / ${he.ws_disconnects_60m||0}</div><div class=muted>chutes WS sur 5 / 15 / 60 min</div></div><div><div class=v>${lv.last_t0_to_leg1_submit_ms==null?'-':lv.last_t0_to_leg1_submit_ms+' ms'}</div><div class=muted>T0 → POST jambe 1</div></div><div><div class=v>${lv.last_leg1_done_to_leg2_submit_ms==null?'-':lv.last_leg1_done_to_leg2_submit_ms+' ms'}</div><div class=muted>confirmation jambe 1 → POST jambe 2</div></div><div><div class=v>${lv.last_t0_to_done_ms==null?'-':lv.last_t0_to_done_ms+' ms'}</div><div class=muted>T0 → route terminée</div></div></div><div class=muted style='margin-top:10px'>Mode TEST : jusqu'à ${lv.test_retry_max||0} relances / ${lv.test_retry_window_ms||0} ms pour découvrir une route. Mode LIVE : au plus ${lv.live_retry_max||0} relances / ${lv.live_retry_window_ms||0} ms, abandon avant tout POST si T0 dépasse ${lv.max_t0_to_leg1_post_ms||0} ms. Les capacités sont prévalidées en arrière-plan en TEST puis réutilisées pendant ${Number(lv.test_max_age_hours||0).toFixed(0)} h. Cooldown ${Number(lv.hard_cooldown_ms||0)/1000||5} s par crypto intermédiaire ; une autre crypto reste libre. Les six Shadows BOT sont planifiés hors du chemin critique.</div><table><thead><tr><th>Heure</th><th>Route</th><th>Classe T0 → fraîche</th><th>Taille T0 → fraîche</th><th>Demande bot</th><th>Disposition</th><th>Raison</th><th>Retries</th><th>1er contrôle / admission</th><th>Âge / skew Depth</th></tr></thead><tbody>${fr}</tbody></table>`;
let sh=d.shadows||{};
renderSimPanel('latencies',['FAST','TARGET','DEGRADED'],sh,'Shadows de recherche',`3 profils · capital ${simNumber(sh.FAST?.capital??1000,0)} $ par profil · cap ${simNumber(p.absolute_cap_usd||350,0)} $`);
renderSimPanel('botlatencies',['BOT_FAST','BOT_TARGET','BOT_DEGRADED','BOT_100_200','BOT_150_300','BOT_200_400'],sh,'Simulations de latence BOT',`6 profils · capital ${simNumber(sh.BOT_FAST?.capital??lv.capital_limit_usd??200,0)} $ par profil · cap ${simNumber(lv.cap_usd||20,0)} $ · rebal. ${lv.bot_shadow_rebalancing?'30 min / 4 h':'désactivé'}`);
let tw=Object.entries(p.target_weights||{}).map(([a,v])=>`${a} ${(100*v).toFixed(0)}%`).join(' · ');document.getElementById('policy').innerHTML=`<div class=tag>POLITIQUE V2.4</div><h3>Depth Quality adaptatif — cap Shadow ${(p.absolute_cap_usd||0).toFixed(0)} $</h3><div class=cards><div><div class=v>${(p.edge_min_pct??0).toFixed(2)}%</div><div class=muted>edge initial minimum</div></div><div><div class=v>A · ${(p.a_fraction_pct??0).toFixed(0)}%</div><div class=muted>capacité si suppression L1 ≥ edge min</div></div><div><div class=v>B+ · ${(p.b_fraction_pct??0).toFixed(0)}%</div><div class=muted>capacité si −50% BBO ≥ ${(p.bplus_half_edge_pct??0).toFixed(2)}%</div></div><div><div class=v>×${(p.coverage3_min??0).toFixed(0)}</div><div class=muted>réserve minimum niveaux 1–3</div></div><div><div class=v>${tw}</div><div class=muted>allocation cible Shadow</div></div><div><div class=v>${p.rebalance_check_min??'-'} min / ${p.rebalance_max_h??'-'} h</div><div class=muted>contrôle / rebalance Shadow</div></div><div><div class=v>${(p.replay_min_edge_pct??0).toFixed(2)}%</div><div class=muted>plancher dataset replay</div></div><div><div class=v>${p.online_research_trials?'ON':'OFF'}</div><div class=muted>matrice 66 essais en ligne</div></div></div><div class=muted style='margin-top:10px'>A : 50% de la capacité robuste. B+ : 33%, edge après −50% BBO ≥0,75%, edge sans niveau 1 ≥−2%, réserve L1–L3 ≥×3. B−, C et D sont refusés. Le Shadow est plafonné à ${(p.absolute_cap_usd||0).toFixed(0)} $ ; la passerelle privée reste plafonnée séparément.</div>`;
document.getElementById('policy').insertAdjacentHTML('beforeend',`<div class=muted style='margin-top:8px'>Après confirmation de la jambe 1 : haircut appliqué une seule fois à ${Number(p.fee_safety_pct||0).toFixed(2)}%, puis jambe 2 autorisée si l'edge conservateur reste ≥${Number(p.leg2_min_edge_pct||0).toFixed(2)}%. Rééquilibrage 30 min / 4 h actif dans les Shadows BOT ; rééquilibrage réel toujours désactivé.</div>`);
let qa=0,qb=0,qsmall=0,qbm=0,qcd=0;for(let r of (ql.rows||[])){if(r.grade==='A'&&r.eligible===1)qa+=r.n;if(r.grade==='B+'&&r.eligible===1)qb+=r.n;if((r.grade==='A'||r.grade==='B+')&&r.reason==='below_min_trade')qsmall+=r.n;if(r.grade==='B-')qbm+=r.n;if(r.grade==='C'||r.grade==='D')qcd+=r.n}let ac={},aprofiles=0;for(let r of (ad.rows||[])){ac[r.disposition]=(ac[r.disposition]||0)+r.n;aprofiles+=r.profiles_reserved||0}let sr={};for(let r of (sk.rows||[]))sr[r.reason]=(sr[r.reason]||0)+r.n;let aexec=(ac.executed_all_profiles||0)+(ac.executed_partial_profiles||0),finished=['FAST','TARGET','DEGRADED'].reduce((n,k)=>n+((sh[k]||{}).trades||0),0),ablock=(ac.blocked_same_event||0)+(ac.blocked_not_rearmed||0)+(ac.blocked_cooldown||0),perf=ql.compute||{},cf=ql.coverage_counterfactual||{};document.getElementById('quality').innerHTML=`<div class=tag>QUALITÉ DEPTH AUJOURD'HUI</div><h3>A / B+ admis — entonnoir d'exécution explicite</h3><div class=cards><div><div class=v good>${qa}</div><div class=muted>A admissibles</div></div><div><div class=v good>${qb}</div><div class=muted>B+ admissibles</div></div><div><div class=v>${qsmall}</div><div class=muted>A/B+ sous 10 $</div></div><div><div class=v good>${aexec}</div><div class=muted>signaux réservés</div></div><div><div class=v>${aprofiles}</div><div class=muted>exécutions profil réservées</div></div><div><div class=v>${finished}</div><div class=muted>exécutions profil terminées</div></div><div><div class='v ${(he.shadow_overdue||0)===0?'good':'bad'}'>${he.shadow_pending||0} / ${he.shadow_overdue||0}</div><div class=muted>Shadow pending / en retard</div></div><div><div class=v>${sr.depth_stale||0} / ${sr.depth_not_ready||0}</div><div class=muted>annulées Depth ancien / absent</div></div><div><div class=v>${sr.no_liquidity||0} / ${sr.below_min_fill||0}</div><div class=muted>annulées sans liquidité / sous 10 $</div></div><div><div class=v>${ablock}</div><div class=muted>bloqués event/réarmement/cooldown</div></div><div><div class=v>${ac.blocked_no_profile_balance||0}</div><div class=muted>bloqués balance</div></div><div><div class=v>${qbm}</div><div class=muted>B− refusés</div></div><div><div class=v>${qcd}</div><div class=muted>C / D refusés</div></div><div><div class=v>${cf.x1??0} / ${cf.x3??0}</div><div class=muted>B+ potentiels si couverture ×1 / ×3</div></div><div><div class=v>${ql.max_selected_usd==null?'-':ql.max_selected_usd.toFixed(2)+' $'}</div><div class=muted>taille Depth max admise</div></div><div><div class=v>${perf.p95_us==null?'-':perf.p95_us.toFixed(1)+' µs'}</div><div class=muted>temps calcul qualité p95</div></div></div>`;
let fp=v=>v==null?'-':(100*v).toFixed(3)+'%',fn=v=>v==null?'-':Number(v).toFixed(2);let rh='';for(let q of (d.recent_decisions||[])){let t=new Date(q.ts_ms),ok=q.eligible===1;rh+=`<tr><td>${t.toLocaleTimeString()}</td><td>${q.route_id}</td><td>${fp(q.decision_net)}</td><td class='${ok?'good':'muted'}'>${q.grade||q.reason||'-'}</td><td>${q.selected_usd==null?'-':q.selected_usd.toFixed(2)+' $'}</td><td>${fp(q.half_bbo_edge)}</td><td>${fp(q.drop_l1_edge)}</td><td>${q.coverage3==null?'-':'×'+fn(q.coverage3)}</td><td>${q.compute_us==null?'-':fn(q.compute_us)+' µs'}</td></tr>`}document.getElementById('recent').innerHTML=rh;
document.getElementById('capture').innerHTML=`<div class=tag>DATASET REPLAY V2.4</div><h3>Collecte jusqu'à T0 + 5 secondes</h3><div class=cards><div><div class=v>${cap.depth_decisions||0}</div><div class=muted>décisions avec Depth</div></div><div><div class=v>${cap.depth_samples||0}</div><div class=muted>snapshots multi-level</div></div><div><div class=v>${cap.exec_details||0}</div><div class=muted>exécutions VWAP détaillées</div></div><div><div class=v>${cap.stable_depth_samples||0}</div><div class=muted>snapshots Depth stablecoins</div></div></div><div class=muted style='margin-top:10px'>Aucune décision détaillée sous ${(p.replay_min_edge_pct??0.30).toFixed(2)}%. Une capture au premier franchissement de ce seuil, puis une seconde uniquement si le même événement franchit ${(p.edge_min_pct??0.35).toFixed(2)}%. Offsets Depth : ${(p.depth_capture_offsets_ms||[]).join(', ')} ms · top ${(p.depth_capture_levels||'-')} niveaux par côté.</div>`;
document.getElementById('diag').innerHTML=`<h3>Univers</h3><div class=cards><div><div class=v>${g.all_markets}</div><div class=muted>marchés découverts</div></div><div><div class=v>${g.candidate_2leg}</div><div class=muted>2-leg candidates</div></div><div><div class='v ${g.selected_2leg===g.candidate_2leg?'good':'bad'}'>${g.selected_2leg}</div><div class=muted>2-leg suivies</div></div><div><div class=v>${g.dropped_2leg||0}</div><div class=muted>2-leg exclues</div></div><div><div class=v>${g.candidate_3leg}</div><div class=muted>3-leg détectées mais ignorées</div></div><div><div class=v>${g.ws_group_size||'-'}</div><div class=muted>symboles maximum par WebSocket</div></div><div><div class=v>${g.ws_grouping_mode||'-'}</div><div class=muted>répartition des flux</div></div><div><div class=v>${g.mexc_app_ping_sec||'-'} s</div><div class=muted>PING applicatif MEXC</div></div></div>`}
refresh();setInterval(refresh,3000)
</script></body></html>
'''

@app.get("/")
def index(): return render_template_string(HTML)


def bootstrap():
    _initialize_depth_codec()
    threading.Thread(target=db_writer,daemon=True).start()
    if not db_ready.wait(10): raise RuntimeError("SQLite schema initialization timeout")
    load_paper_state(); load_shadow_state()
    boot_ts=now_ms()
    for _p in SHADOW_PROFILES:
        shadow_last_rebalance_ms[_p]=boot_ts; shadow_last_rebalance_check_ms[_p]=boot_ts
    markets=discover_markets(); chosen,routes,meta,diag=select_routes_under_symbol_budget(markets)
    bysym=defaultdict(list)
    for r in routes:
        for s in r["symbols"]: bysym[s].append(r)
    symbols=[m["symbol"] for m in chosen]
    groups,group_loads,grouping_mode=build_balanced_ws_groups(symbols)
    diag["ws_grouping_mode"]=grouping_mode
    diag["ws_groups"]=len(groups)
    diag["mexc_app_ping_sec"]=MEXC_APP_PING_SEC
    diag["mexc_app_pong_timeout_sec"]=MEXC_APP_PONG_TIMEOUT_SEC
    with state_lock:
        state["symbols"]=symbols; state["routes"]=routes; state["routes_by_symbol"]=bysym; state["market_meta"]=meta; state["route_diag"]=diag
    with state_lock: state["ws_expected"]=len(groups)
    initialize_route_coverage(routes)
    print(f"[Routes V{VERSION}] source={state['market_source']} markets={diag['all_markets']} candidates={diag['candidate_routes']} "
          f"(2L={diag['candidate_2leg']},3L={diag['candidate_3leg']}) selected_symbols={len(symbols)} selected_routes={len(routes)} "
          f"(2L={diag['selected_2leg']},3L={diag['selected_3leg']}) WS={len(groups)} group<={WS_GROUP_SIZE} "
          f"grouping={grouping_mode} app_ping={MEXC_APP_PING_SEC:g}s replay>={REPLAY_MIN_EDGE*100:.2f}% "
          f"online_matrix={'on' if ENABLE_ONLINE_RESEARCH_TRIALS else 'off'} db_batch={DB_COMMIT_BATCH}/{DB_COMMIT_MAX_MS}ms "
          f"test_retry={LIVE_RETRY_MAX}/{LIVE_RETRY_WINDOW_MS}ms live_retry={LIVE_EXEC_RETRY_MAX}/{LIVE_EXEC_RETRY_WINDOW_MS}ms "
          f"cooldown=crypto:{LIVE_HARD_COOLDOWN_MS}ms http_keepalive={LIVE_HTTP_KEEPALIVE_SEC:g}s "
          f"entry_depth<={LIVE_BBO_AGE_MS}ms leg2_depth<={LIVE_LEG2_BBO_AGE_MS}ms "
          f"entry_mexc_age<={LIVE_EXCHANGE_AGE_MS}ms mexc_skew<={LIVE_MAX_EXCHANGE_SKEW_MS}ms "
          f"leg2_mexc_age<={LIVE_LEG2_EXCHANGE_AGE_MS}ms leg2_fill_skew<={LIVE_LEG2_MAX_EXCHANGE_SKEW_MS}ms "
          f"leg2_guard>={LIVE_LEG2_MIN_EDGE*100:.2f}% haircut={LIVE_FEE_SAFETY*100:.2f}% "
          f"private_order_ws={LIVE_PRIVATE_WS_MODE} "
          f"prevalidate={'on' if LIVE_PREVALIDATE_ENABLED and TRADING_MODE=='test' else 'off'} "
          f"coverage_flush={ROUTE_COVERAGE_FLUSH_SEC:g}s")
    put_db(("meta",("version",VERSION))); put_db(("meta",("paper_rearm_neutral_ms",str(PAPER_REARM_NEUTRAL_MS)))); put_db(("meta",("paper_hard_cooldown_ms",str(PAPER_HARD_COOLDOWN_MS)))); put_db(("meta",("paper_failed_leg_policy","reverse_leg1_then_persist_open_exposure_until_liquidated"))); put_db(("meta",("ws_depth_interval","10ms"))); put_db(("meta",("ws_latency_sample_ms",str(WS_LATENCY_SAMPLE_MS)))); put_db(("meta",("route_snapshot_interval_sec",str(SNAPSHOT_INTERVAL_SEC)))); put_db(("meta",("route_snapshot_top_n",str(SNAPSHOT_TOP_N)))); put_db(("meta",("depth_snapshot_levels","100"))); put_db(("meta",("trace_window_ms",str(TRACE_WINDOW_MS)))); put_db(("meta",("market_source",state["market_source"])))
    put_db(("meta",("route_diag",json.dumps(diag,sort_keys=True))))
    put_db(("meta",("ws_grouping",grouping_mode)))
    put_db(("meta",("ws_keepalive",json.dumps({"method":"MEXC_JSON_PING","interval_sec":MEXC_APP_PING_SEC,"pong_timeout_sec":MEXC_APP_PONG_TIMEOUT_SEC},sort_keys=True))))
    put_db(("meta",("decision_sampling",f"first_{REPLAY_MIN_EDGE*100:.2f}pct_replay_crossing_then_first_{SHADOW_MIN_EDGE*100:.2f}pct_policy_crossing")))
    put_db(("meta",("shadow_execution_worker","dedicated_priority_1ms")))
    put_db(("meta",("online_research_trials",str(int(ENABLE_ONLINE_RESEARCH_TRIALS)))))
    put_db(("meta",("event_hysteresis",json.dumps({"close_net":EVENT_CLOSE_NET,"neutral_ms":EVENT_NEUTRAL_CLOSE_MS,"gap_ms":EVENT_CLOSE_GAP_MS},sort_keys=True))))
    put_db(("meta",("db_writer_batch",json.dumps({"max_items":DB_COMMIT_BATCH,"max_ms":DB_COMMIT_MAX_MS},sort_keys=True))))
    put_db(("meta",("shadow_profiles",json.dumps(SHADOW_PROFILES,sort_keys=True))))
    put_db(("meta",("v235_policy",json.dumps({"edge_min":SHADOW_MIN_EDGE,"target_weights":SHADOW_TARGET_WEIGHTS,
        "a_fraction":DEPTH_QUALITY_A_FRACTION,"b_fraction":DEPTH_QUALITY_B_FRACTION,"c_fraction":DEPTH_QUALITY_C_FRACTION,
        "bplus_half_edge":DEPTH_QUALITY_BPLUS_HALF_EDGE,"bplus_drop_floor":DEPTH_QUALITY_BPLUS_DROP_FLOOR,
        "coverage3_min":DEPTH_QUALITY_COVERAGE3_MIN,"absolute_cap_usd":SHADOW_TRADE_CAP_USD,
        "rebalance_check_sec":SHADOW_REBALANCE_CHECK_SEC,"rebalance_max_sec":SHADOW_REBALANCE_MAX_SEC,
        "early_dev":SHADOW_REBALANCE_EARLY_DEV,"post_resync_grace_ms":POST_RESYNC_GRACE_MS,
        "depth_capture_offsets_ms":DEPTH_CAPTURE_OFFSETS_MS,"depth_capture_levels":DEPTH_CAPTURE_LEVELS},sort_keys=True))))
    put_db(("meta",("v246_private_gateway",json.dumps({"mode":TRADING_MODE,"live_cap_usd":LIVE_CAP_USD,
        "capital_limit_usd":LIVE_CAPITAL_LIMIT_USD,"daily_loss_limit_usd":LIVE_DAILY_LOSS_LIMIT_USD,
        "max_concurrent":LIVE_MAX_CONCURRENT,"live_bbo_age_ms":LIVE_BBO_AGE_MS,
        "live_leg2_bbo_age_ms":LIVE_LEG2_BBO_AGE_MS,"live_max_skew_ms":LIVE_MAX_SKEW_MS,
        "live_exchange_age_ms":LIVE_EXCHANGE_AGE_MS,
        "live_leg2_exchange_age_ms":LIVE_LEG2_EXCHANGE_AGE_MS,
        "live_max_exchange_skew_ms":LIVE_MAX_EXCHANGE_SKEW_MS,
        "live_leg2_max_exchange_skew_ms":LIVE_LEG2_MAX_EXCHANGE_SKEW_MS,
        "defer_leg1_commission":LIVE_DEFER_LEG1_COMMISSION,
        "fee_safety":LIVE_FEE_SAFETY,"leg2_min_edge":LIVE_LEG2_MIN_EDGE,
        "protected_bags":PROTECT_EXISTING_BAGS,"allowed_start_assets":MEXC_ALLOWED_START_ASSETS,
        "require_tested_route":LIVE_REQUIRE_TESTED_ROUTE,"test_max_age_hours":LIVE_TEST_MAX_AGE_HOURS,
        "require_all_ws":LIVE_REQUIRE_ALL_WS,"require_route_ws":LIVE_REQUIRE_ROUTE_WS,
        "test_retry_window_ms":LIVE_RETRY_WINDOW_MS,"test_retry_interval_ms":LIVE_RETRY_INTERVAL_MS,
        "test_retry_max":LIVE_RETRY_MAX,"live_retry_window_ms":LIVE_EXEC_RETRY_WINDOW_MS,
        "live_retry_interval_ms":LIVE_EXEC_RETRY_INTERVAL_MS,"live_retry_max":LIVE_EXEC_RETRY_MAX,
        "max_t0_to_leg1_post_ms":LIVE_MAX_T0_TO_LEG1_POST_MS,
        "cooldown_ms":LIVE_HARD_COOLDOWN_MS,"cooldown_scope":"intermediate_crypto",
        "account_refresh_sec":LIVE_ACCOUNT_REFRESH_SEC,"account_cache_max_age_ms":LIVE_ACCOUNT_CACHE_MAX_AGE_MS,
        "live_journal_path":LIVE_DB_PATH,"live_journal_sync":"FULL","research_db_path":DB_PATH,
        "http_sessions":"dedicated_orders_status_account_prevalidation_user_stream","http_keepalive_sec":LIVE_HTTP_KEEPALIVE_SEC,
        "http_keepalive_timeout_sec":LIVE_HTTP_KEEPALIVE_TIMEOUT_SEC,
        "capability_reuse_hours":LIVE_TEST_MAX_AGE_HOURS,
        "background_prevalidation":LIVE_PREVALIDATE_ENABLED,
        "prevalidation_interval_sec":LIVE_PREVALIDATE_INTERVAL_SEC,
        "prevalidation_refresh_hours":LIVE_PREVALIDATE_REFRESH_HOURS,
        "prevalidation_depth_age_ms":LIVE_PREVALIDATE_DEPTH_AGE_MS,
        "route_coverage_flush_sec":ROUTE_COVERAGE_FLUSH_SEC,
        "bot_shadow_rebalance":BOT_SHADOW_REBALANCE_ENABLED,
        "admission_db_mode":"async_telemetry","critical_order_journal":"single_transaction_before_each_post",
        "bot_shadow_admission":"fresh_gateway_A_or_Bplus_off_critical_path",
        "order_fallback":["MARKET","FILL_OR_KILL","IMMEDIATE_OR_CANCEL"],
        "bot_shadow_profiles":BOT_SHADOW_PROFILES,
        "private_order_stream":{"mode":LIVE_PRIVATE_WS_MODE,"channel":"spot@private.orders.v3.api.pb",
            "listenkey_keepalive_sec":LIVE_PRIVATE_WS_KEEPALIVE_SEC,
            "ping_sec":LIVE_PRIVATE_WS_PING_SEC,"pong_timeout_sec":LIVE_PRIVATE_WS_PONG_TIMEOUT_SEC,
            "connection_recycle_sec":LIVE_PRIVATE_WS_MAX_CONNECTION_SEC,
            "first_terminal_authoritative":LIVE_PRIVATE_WS_MODE=="hybrid",
            "rest_reconciliation_continues_after_ws":True},
        "emergency_unwind":"prevalidated MARKET without local Depth freshness gate",
        "pnl_accounting":"realized pro-rata cost; open exposure marked separately",
        "real_order_gates":["TRADING_MODE=live","LIVE_ARMED=1","exact local arm file"]},sort_keys=True))))
    initialize_live_gateway()
    for _ in range(6): threading.Thread(target=scan_worker,daemon=True).start()
    threading.Thread(target=event_sweeper,daemon=True).start()
    # Start the live-like clock before lower-priority research/capture workers.
    threading.Thread(target=shadow_execution_worker,daemon=True).start()
    threading.Thread(target=execution_trial_worker,daemon=True).start()
    threading.Thread(target=open_exposure_worker,daemon=True).start()
    threading.Thread(target=shadow_exposure_worker,daemon=True).start()
    threading.Thread(target=snapshot_worker,daemon=True).start()
    threading.Thread(target=stable_quote_worker,daemon=True).start()
    threading.Thread(target=stable_depth_capture_worker,daemon=True).start()
    threading.Thread(target=shadow_periodic_rebalance_worker,daemon=True).start()
    threading.Thread(target=route_coverage_flush_worker,daemon=True).start()
    refresh_dashboard_cache()
    threading.Thread(target=dashboard_cache_worker,daemon=True).start()
    threading.Thread(target=public_dispatch_worker,name='public-dispatch',daemon=True).start()
    threading.Thread(target=public_watchdog_worker,name='public-watchdog',daemon=True).start()
    for i,g in enumerate(groups,1): threading.Thread(target=ws_worker,args=(g,i,group_loads[i-1]),daemon=True).start()
    time.sleep(1.0)
    threading.Thread(target=depth_resync_worker,daemon=True).start()
    threading.Thread(target=depth_bootstrap_worker,args=(symbols,),daemon=True).start()

if __name__=="__main__":
    bootstrap(); app.run(host=HOST,port=PORT,threaded=True,use_reloader=False)

"""Explicit, persistent LIVE configuration. Preparation never arms a scanner."""
from dataclasses import replace
from pathlib import Path
from collections import deque
import hashlib
import json
import os
import sqlite3
import threading
from .config import Policy
from .execution import Coordinator
from .journal import Journal, dumps
from .mexc import Rest, PrivateOrders, LiveAdapter
from .model import D, ZERO, Spec


def legacy_snapshot(path):
    """Read V2 through SQLite read-only; no copying synthetic test fixtures."""
    path = Path(path).resolve()
    con = sqlite3.connect(path.as_uri()+"?mode=ro",uri=True)
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN")
        attempts = [dict(r) for r in con.execute("SELECT * FROM live_attempts_v240 WHERE mode='live'")]
        exposures = [dict(r) for r in con.execute("SELECT * FROM live_open_exposures_v246 WHERE status='open'")]
        state = con.execute("SELECT * FROM live_state_v240 WHERE singleton=1").fetchone()
        if exposures or any(r["status"] not in ("completed","forced_unwind","manual_closed","aborted","rejected","failed","no_fill") for r in attempts):
            raise ValueError("Unresolved V2 journal; review it before any LIVE initialization")
        pnl = sum((D(r["realized_pnl"] or 0) for r in attempts),ZERO)
        if any(str(r["attempt_id"]).startswith("synthetic") for r in attempts):
            raise ValueError("Synthetic fixture is not a LIVE baseline")
        evidence = {"pnl":str(pnl),"attempts":attempts,"open_exposures":exposures,
                    "state":dict(state) if state else None,"source":str(path)}
        evidence["digest"] = hashlib.sha256(dumps(evidence).encode()).hexdigest()
        return evidence
    finally:
        con.close()


def scanner_processes(root):
    found = []
    for p in Path("/proc").iterdir():
        if not p.name.isdigit() or int(p.name) == os.getpid():
            continue
        try:
            if (p/"cwd").resolve() != root:
                continue
            args = (p/"cmdline").read_bytes().split(b"\0")
            if any(a == b"app.py" or a.endswith(b"/app.py") for a in args):
                found.append(int(p.name))
        except OSError:
            pass
    return found


def initialize(root, legacy_path, policy):
    root,legacy_path = Path(root),Path(legacy_path).resolve()
    if scanner_processes(legacy_path.parent):
        raise ValueError("The V2 scanner still runs; LIVE preparation does not stop it")
    baseline = legacy_snapshot(legacy_path)
    if not baseline["state"]:
        raise ValueError("Missing legacy execution state")
    root.mkdir(parents=True,exist_ok=True)
    auth = root/"live_authorization.json"
    if auth.exists() or (root/"live.db").exists():
        raise ValueError("LIVE journal/policy already exists; quotas must not be reset")
    journal = Journal(root/"live.db",mode="live")
    try:
        journal.set_meta("policy",policy.dictionary())
        journal.set_meta("legacy_baseline",baseline)
        if baseline["state"].get("circuit_open"):
            journal.stop("legacy:"+str(baseline["state"].get("circuit_reason")))
        journal.set_meta("session_id",baseline["digest"][:32])
    finally:
        journal.close()
    auth.write_text(dumps({"enabled":False,"session_id":baseline["digest"][:32],
        "policy":policy.dictionary(),"legacy_digest":baseline["digest"],
        "legacy_directory":str(legacy_path.parent),"expires_ms":None})+"\n")
    os.chmod(auth,0o600)
    (root/"STOP").touch()
    return auth


class PreparedLiveAdapter(LiveAdapter):
    def __init__(self, rest, private, permitted, service, journal):
        super().__init__(rest,private,permitted)
        self.service,self.journal = service,journal
        self.ready_routes = {}
        self.validated_fees = {}
        self.preflight_errors = {}
        self.requests = deque(maxlen=64)
        self.queued = set()
        self.queue_lock = threading.Lock()
        self.done = threading.Event()
        self.account_error = "account_not_loaded"
        self.thread = threading.Thread(target=self.maintain,daemon=True,name="live-preflight")

    def route_ready(self, route):
        now = self.rest.clock.ms()
        if self.ready_routes.get(route.id,0) > now and not self.account_error:
            return True
        with self.queue_lock:
            if route.id not in self.queued and len(self.requests) < self.requests.maxlen:
                self.requests.append(route)
                self.queued.add(route.id)
        return False

    def validated_route(self, route):
        return replace(route,buy=replace(route.buy,fee=self.validated_fees[route.buy.symbol]),
                       sell=replace(route.sell,fee=self.validated_fees[route.sell.symbol]))

    def submit(self, spec, guard):
        from .model import GuardRejected
        def checked_guard():
            if spec.purpose == "leg1" and self.ready_routes.get(spec.route_id,0) <= self.rest.clock.ms():
                raise GuardRejected("route_capability_expired")
            guard()
        return super().submit(spec,checked_guard)

    def check_account(self):
        balances = self.refresh_account()
        expected = {}
        for row in self.journal.export()["attempts"]:
            d = json.loads(row["data"])
            route = d.get("route","").split(">")
            if len(route) == 3 and d.get("remaining"):
                expected[route[1]] = expected.get(route[1],ZERO)+D(d["remaining"])
        for asset,row in self.signed_balances.items():
            total = row["free"]+row["locked"]
            if row["locked"] > 0:
                raise ValueError("locked_balance:"+asset)
            if asset not in ("USDT","USD1","USDC") and total > expected.get(asset,ZERO)+D("0.00000001"):
                raise ValueError("protected_asset:"+asset)
        return balances

    def maintain(self):
        last_account = 0
        while not self.done.wait(.2):
            if self.service.live and self.service.live.busy:
                continue
            if self.rest.clock.ms()-last_account >= 5000:
                try:
                    self.check_account()
                    self.account_error = None
                except Exception as exc:
                    self.account_error = str(exc) if isinstance(exc,ValueError) else type(exc).__name__
                last_account = self.rest.clock.ms()
            route = None
            with self.queue_lock:
                if self.requests:
                    route = self.requests.popleft()
                    self.queued.discard(route.id)
            if route is None or self.account_error:
                continue
            try:
                plan = self.service.decisions.evaluate(route,self.balances().get(route.buy.quote,ZERO))
                if plan.quantity <= 0 or plan.sell_quantity <= 0:
                    continue
                for market in (route.buy,route.sell):
                    fee = self.rest.fee(market.symbol)
                    if fee > D(self.service.policy.fee_reserve):
                        raise ValueError("fee_above_reserve")
                    self.validated_fees[market.symbol] = fee
                shapes = [(route.buy,"BUY",plan.quantity,plan.buy_limit),
                          (route.sell,"SELL",plan.sell_quantity,plan.sell_limit)]
                exits = self.service.decisions.exits(route,plan.sell_quantity)
                fallback = next((x for x in exits if x.market.symbol == route.buy.symbol),None)
                if fallback is None:
                    raise ValueError("no_current_return_depth")
                shapes.append((route.buy,"SELL",fallback.quantity,fallback.limit))
                for market,side,qty,price in shapes:
                    self.rest.test_order(Spec("v3test"+str(self.rest.clock.ms()),market.symbol,side,qty,price,"preflight"))
                self.ready_routes[route.id] = self.rest.clock.ms()+900000
                self.preflight_errors.pop(route.id,None)
            except Exception as exc:
                self.preflight_errors[route.id] = str(exc) if isinstance(exc,ValueError) else type(exc).__name__
                # No automatic order-type downgrade on rejection.
                self.done.wait(1)

    def close(self):
        self.done.set()
        self.thread.join(timeout=6)
        super().close()
        self.rest.close()


def attach(service):
    import os
    root = service.root
    auth_path = root/"live_authorization.json"
    auth = json.loads(auth_path.read_text())
    if auth.get("enabled") is not True or os.environ.get("MEXC_V3_LIVE") != "1":
        raise ValueError("LIVE requires explicit enabled policy and MEXC_V3_LIVE=1")
    if scanner_processes(Path(auth["legacy_directory"]).resolve()):
        raise ValueError("Legacy scanner process still present")
    journal = Journal(root/"live.db",mode="live")
    if journal.meta("session_id") != auth.get("session_id") or journal.meta("legacy_baseline")["digest"] != auth.get("legacy_digest"):
        journal.close()
        raise ValueError("LIVE baseline/session mismatch")
    service.bind_policy(journal,service.policy)
    if json.loads(dumps(service.policy.dictionary())) != auth["policy"]:
        journal.close()
        raise ValueError("Authorization policy changed")
    if journal.totals()["open_exposures"] or journal.meta("circuit"):
        journal.close()
        raise ValueError("Unresolved LIVE journal/circuit: review required")
    rest = Rest(service.clock,os.environ.get("MEXC_API_KEY",""),os.environ.get("MEXC_API_SECRET",""))
    rest.sync_time()
    private = PrivateOrders(rest)
    # Cache immutable authorization fingerprint; an external change blocks BUY.
    fingerprint = hashlib.sha256(auth_path.read_bytes()).hexdigest()
    def stopped():
        return service.done.is_set() or (root/"STOP").exists()
    def gate():
        try:
            same = hashlib.sha256(auth_path.read_bytes()).hexdigest() == fingerprint
        except OSError:
            same = False
        checks = ((same,"authorization_changed"),(not stopped(),"stop_file"),
            (private.ready,"private_ws_not_ready"),(not adapter.account_error,str(adapter.account_error)),
            (service.clock.ms()-adapter.balance_ms <= 10000,"account_cache_old"),
            (service.clock.ms()-service.rest.time_checked_ms <= 120000,"clock_measurement_old"),
            (service.rest.clock_uncertainty_ms <= service.policy.clock_uncertainty_ms,"clock_uncertainty"))
        return next((reason for good,reason in checks if not good),None)
    def permitted(purpose):
        return purpose != "leg1" or gate() is None
    adapter = PreparedLiveAdapter(rest,private,permitted,service,journal)
    service.live = Coordinator(service.decisions,adapter,journal,service.policy,service.clock,stopped)
    service.live_gate = gate
    private.start()
    adapter.thread.start()

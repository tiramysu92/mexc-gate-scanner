from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import json
import threading
import time
from . import __version__
from .books import Books
from .config import Policy
from .decision import Decisions, freshness
from .execution import Coordinator
from .feed import PublicFeed
from .journal import Journal, dumps
from .mexc import Rest
from .model import Clock, D, ZERO, ONE, Route
from .simulation import PaperAdapter, Latency, restore_wallet


def routes_for(markets):
    stables = {"USDT","USDC","USD1"}
    bases = {}
    for m in markets:
        if m.quote in stables and m.base not in stables:
            bases.setdefault(m.base,[]).append(m)
    routes = []
    for group in bases.values():
        for buy in group:
            for sell in group:
                if buy.symbol != sell.symbol and buy.buy_allowed and sell.sell_allowed:
                    routes.append(Route(buy,sell))
    return routes


class Marks:
    def __init__(self, books, markets, clock, rest):
        self.books,self.markets,self.clock,self.rest = books,markets,clock,rest
        self.direct = {(m.base,m.quote):m.symbol for m in markets}

    def __call__(self, asset):
        if asset == "USDT":
            return ONE,ONE
        symbol = self.direct.get((asset,"USDT")) or self.direct.get(("USDT",asset))
        if not symbol or symbol not in self.books.slots:
            return ZERO,ZERO
        b = self.books.get(symbol)
        # Valuation of stable quote balances is a separate 1-second policy;
        # this does not certify either execution book as fresh.
        if freshness(b,self.clock.ms(),self.clock.ms()+self.rest.offset_ms,1000,1000):
            return ZERO,ZERO
        bid,ask = D(b.bids[0][0]),D(b.asks[0][0])
        if self.direct.get((asset,"USDT")) != symbol:
            bid,ask = ONE/ask,ONE/bid
        # Explicit conversion reserve; neither USD1 nor USDC is assumed at par.
        return bid*D(".9995"),ask*D("1.0005")


class Funnel:
    """Signal episodes, not every tick; reject reasons are not mutually exclusive."""
    def __init__(self):
        self.lock = threading.RLock()
        self.active = {}
        self.counts = Counter()
        self.reasons = Counter()
        self.recent = deque(maxlen=100)

    def observe(self, plan, now):
        with self.lock:
            old = self.active.get(plan.route.id)
            new = old is None or now-old["seen"] > 500
            if new:
                old = {"seen":now,"admitted":False,"grade":None,"reasons":set()}
                self.active[plan.route.id] = old
                self.counts["sized_signal_episodes"] += 1
            old["seen"] = now
            if plan.grade in ("A","B+") and old["grade"] is None:
                old["grade"] = plan.grade
                self.counts[plan.grade] += 1
            if plan.eligible and not old["admitted"]:
                old["admitted"] = True
                self.counts["eligible_episodes"] += 1
            for reason in plan.reasons:
                if reason not in old["reasons"]:
                    old["reasons"].add(reason)
                    self.reasons[reason] += 1
            if new:
                self.recent.append({"route":plan.route.id,"grade":plan.grade,"edge":str(plan.edge),
                                    "size":str(plan.cost_usdt),"reasons":plan.reasons,"ts_ms":now})
            if len(self.active) > 2000:
                self.active = {k:v for k,v in self.active.items() if now-v["seen"] < 60000}

    def status(self):
        with self.lock:
            return {"counts":dict(self.counts),"reasons":dict(self.reasons),"recent":list(self.recent)}


class Service:
    def __init__(self, directory, policy=None, clock=None):
        self.root = Path(directory)
        self.root.mkdir(parents=True,exist_ok=True)
        self.policy,self.clock = policy or Policy(),clock or Clock()
        self.rest = Rest(self.clock)
        self.done = threading.Event()
        self.started = self.clock.ms()
        self.errors = deque(maxlen=50)
        self.profiles = []
        self.live = None
        self.live_future = None
        self.scan_ms = deque(maxlen=200)

    def prepare(self):
        self.rest.sync_time()
        all_markets = self.rest.markets()
        self.routes = routes_for(all_markets)
        if not self.routes:
            raise RuntimeError("No supported two-leg routes discovered; inspect exchange metadata")
        symbols = {m.symbol for r in self.routes for m in (r.buy,r.sell)}
        symbols.update(m.symbol for m in all_markets if m.base in {"USDT","USDC","USD1"} and m.quote in {"USDT","USDC","USD1"})
        self.markets = {m.symbol:m for m in all_markets if m.symbol in symbols}
        self.books = Books(sorted(symbols),self.clock)
        groups_count = max(1,(len(symbols)+24)//25)
        ordered = sorted(symbols)
        groups = [ordered[i::groups_count] for i in range(groups_count)]
        self.feed = PublicFeed(self.books,self.rest,groups,self.clock)
        marks = Marks(self.books,all_markets,self.clock,self.rest)
        self.decisions = Decisions(self.books,self.policy,self.clock,marks,
                                 lambda:self.clock.ms()+self.rest.offset_ms)
        self.funnel = Funnel()
        self.comparisons = {
            "reference":self.decisions,
            "sans_projection":Decisions(self.books,replace(self.policy,static_headroom=False),self.clock,marks,self.decisions.exchange_time),
            "fenetre_100_200":Decisions(self.books,replace(self.policy,static_headroom=False,entry_local_ms=100,
                entry_exchange_ms=200,exit_local_ms=150,exit_exchange_ms=250),self.clock,marks,self.decisions.exchange_time)}
        self.comparison_funnels = {key:Funnel() for key in self.comparisons}
        for name,multiplier in (("observed_proxy",1),("stress_x1_5",1.5),("stress_x2",2)):
            journal = Journal(self.root/(name+".db"))
            self.bind_policy(journal,self.policy)
            adapter = PaperAdapter(self.books,self.markets,self.clock,Latency(name,multiplier))
            adapter.wallet = restore_wallet(journal,adapter.wallet)
            coordinator = Coordinator(self.decisions,adapter,journal,self.policy,self.clock)
            self.profiles.append({"name":name,"coordinator":coordinator,"future":None,"last":None})
        self.pool = ThreadPoolExecutor(max_workers=4,thread_name_prefix="execution")

    @staticmethod
    def bind_policy(journal, policy):
        saved = journal.meta("policy")
        current = json.loads(dumps(policy.dictionary()))
        if saved is None:
            journal.set_meta("policy",current)
        elif saved != current:
            raise ValueError("Policy changed: retain this journal and use a new explicitly reviewed run directory")

    def start(self):
        self.feed.start()
        self.worker = threading.Thread(target=self.scan,daemon=True,name="decision-scan")
        self.worker.start()

    def scan(self):
        last_time_check = self.clock.ms()
        while not self.done.wait(.02):
            began = time.monotonic()
            try:
                if self.clock.ms()-last_time_check > 60000:
                    # Separate background refresh avoids stopping decisions on REST.
                    last_time_check = self.clock.ms()
                    threading.Thread(target=self.refresh_clock,daemon=True).start()
                bbo = {s:self.books.bbo(s) for s in self.markets}
                marks = {a:self.decisions.valuation(a) for a in ("USDT","USDC","USD1")}
                if self.live and self.live_future and self.live_future.done():
                    self.live_result = self.live_future.result()
                    self.live_future = None
                for profile in self.profiles:
                    future = profile["future"]
                    if future and future.done():
                        profile["last"] = future.result()
                        profile["coordinator"].journal.set_meta("wallet",profile["coordinator"].adapter.wallet)
                        profile["future"] = None
                for route in self.routes:
                    if self.done.is_set():
                        break
                    first,second = bbo.get(route.buy.symbol),bbo.get(route.sell.symbol)
                    if not first or not second:
                        continue
                    # Include quote conversion even in the cheap prefilter:
                    # depegged stablecoins must not create hidden exclusions.
                    start_ask,end_bid = marks[route.buy.quote][1],marks[route.sell.quote][0]
                    if start_ask <= 0 or end_bid <= 0 or D(second[0])*end_bid < D(first[1])*start_ask:
                        continue
                    cohort = self.decisions.capture(route)
                    for name,engine in self.comparisons.items():
                        plan = engine.evaluate(route,D(self.policy.cap_usdt)*2,captured=cohort)
                        self.comparison_funnels[name].observe(plan,cohort[-2])
                    for profile in self.profiles:
                        co = profile["coordinator"]
                        available = co.adapter.wallet.get(route.buy.quote,ZERO)
                        plan = co.decisions.evaluate(route,available)
                        if profile is self.profiles[0]:
                            self.funnel.observe(plan,self.clock.ms())
                        if profile["future"] is None and plan.eligible and not co.entry_block():
                            profile["future"] = self.pool.submit(co.execute,plan)
                    if self.live and self.live_future is None and not self.live.entry_block() and self.live_gate() is None:
                        if not self.live.adapter.route_ready(route):
                            continue
                        live_route = self.live.adapter.validated_route(route)
                        available = self.live.adapter.balances().get(route.buy.quote,ZERO)
                        plan = self.live.decisions.evaluate(live_route,available)
                        if plan.eligible:
                            self.live_future = self.pool.submit(self.live.execute,plan)
                self.scan_ms.append((time.monotonic()-began)*1000)
            except Exception as exc:
                self.errors.append({"ts_ms":self.clock.ms(),"error":type(exc).__name__})
                self.done.wait(.1)

    def refresh_clock(self):
        try:
            self.rest.sync_time()
            if self.live:
                self.live.adapter.rest.sync_time()
        except Exception as exc:
            self.errors.append({"error":"clock_"+type(exc).__name__})

    def status(self):
        return {"version":__version__,"mode":"live" if self.live else "observe",
            "market_rule_rejections":getattr(self.rest,"market_rejections",[]),
            "uptime_s":(self.clock.ms()-self.started)//1000,"routes":len(self.routes),
            "depth":self.books.status(),"flow":self.feed.status(),
            "clock":{"offset_ms":self.rest.offset_ms,"uncertainty_ms":self.rest.clock_uncertainty_ms,
                     "checked_ms":self.rest.time_checked_ms},
            "scan_max_ms":max(self.scan_ms,default=0),"funnel":self.funnel.status(),
            "validation_reuses":self.decisions.validation_reuses,
            "policy_comparison":{k:f.status() for k,f in self.comparison_funnels.items()},
            "profiles":[{"name":p["name"],"totals":p["coordinator"].journal.totals(),
                "busy":p["coordinator"].busy,"blocked":p["coordinator"].entry_block(),"last":p["last"]} for p in self.profiles],
            "live":{"totals":self.live.journal.totals(),"busy":self.live.busy,
                "blocked":self.live.entry_block() or self.live_gate(),
                "preflight_pending":len(self.live.adapter.requests),
                "preflight_errors":dict(self.live.adapter.preflight_errors),
                "baseline":self.live.journal.meta("legacy_baseline")} if self.live else None,
            "errors":list(self.errors)}

    def export(self, path):
        import gzip
        data = {"status":self.status(),"profiles":{p["name"]:p["coordinator"].journal.export() for p in self.profiles}}
        if self.live:
            data["live_journal"] = self.live.journal.export()
        with gzip.open(path,"wt",encoding="utf-8") as f:
            f.write(dumps(data))

    def close(self):
        self.done.set()
        if hasattr(self,"worker"):
            self.worker.join(timeout=2)
        # Existing exits can finish while public/private feeds are still running.
        self.pool.shutdown(wait=True,cancel_futures=False)
        self.export(self.root/"last_report.json.gz")
        for p in self.profiles:
            co = p["coordinator"]
            co.journal.set_meta("wallet",co.adapter.wallet)
            co.journal.close()
        if self.live:
            self.live.adapter.close()
            self.live.journal.close()
        self.feed.close()
        self.rest.close()

"""The same immutable plans and guards are used in paper and LIVE."""
from dataclasses import dataclass, field
from .model import D, ZERO, ONE, floor, ceil, Spec, GuardRejected


def walk(book, side, quantity):
    if book is None or quantity <= 0:
        return None
    left, quote, worst = quantity, ZERO, ZERO
    for price, amount in (book.asks if side == "BUY" else book.bids):
        p, available = D(price), D(amount)
        take = min(left, available)
        quote += take * p
        left -= take
        if take:
            worst = p
        if left <= 0:
            return quote, worst
    return None


def freshness(book, now, exchange_now, local_limit, exchange_limit):
    if book is None or not book.ready:
        return ["depth_not_ready"]
    reasons = []
    if not book.transport_ok:
        reasons.append("transport_disconnected")
    if book.catching_up:
        reasons.append("local_catchup")
    if book.exchange_ms <= 0:
        reasons.append("exchange_timestamp_missing")
    elif exchange_now - book.exchange_ms > exchange_limit:
        reasons.append("exchange_age")
    elif book.exchange_ms - exchange_now > 25:
        reasons.append("exchange_clock_inconsistent")
    if book.received_ms <= 0 or now - book.received_ms > local_limit:
        reasons.append("local_age")
    if book.received_ms > now + 1 or book.published_ms > now + 1:
        reasons.append("future_snapshot")
    return reasons


@dataclass(frozen=True)
class Plan:
    route: object
    created_ms: int
    quantity: object
    buy_limit: object
    sell_quantity: object
    sell_limit: object
    cost_usdt: object
    worst_net_usdt: object
    edge: object
    grade: str
    books: tuple
    marks: tuple
    reasons: tuple = ()
    metrics: dict = field(default_factory=dict)

    @property
    def eligible(self):
        return self.grade in ("A", "B+") and not self.reasons


@dataclass(frozen=True)
class Exit:
    market: object
    quantity: object
    limit: object
    net_usdt: object
    purpose: str


class Decisions:
    def __init__(self, books, policy, clock, valuation, exchange_time=None):
        self.books, self.policy, self.clock = books, policy, clock
        # Valuation returns a conservative (bid, ask) conversion to USDT.
        self.valuation = valuation
        self.exchange_time = exchange_time or clock.ms
        self.validation_reuses = 0

    def capture(self, route):
        buy, sell = self.books.get(route.buy.symbol), self.books.get(route.sell.symbol)
        start_bid, start_ask = self.valuation(route.buy.quote)
        end_bid, end_ask = self.valuation(route.sell.quote)
        now, exchange_now = self.clock.ms(), self.exchange_time()
        return buy,sell,start_bid,start_ask,end_bid,end_ask,now,exchange_now

    def evidence(self, route):
        out = {}
        for market in (route.buy,route.sell):
            b = self.books.get(market.symbol)
            out[market.symbol] = None if b is None else {
                "epoch":b.epoch,"version":b.version,"received_ms":b.received_ms,
                "exchange_ms":b.exchange_ms,"published_ms":b.published_ms,
                "ready":b.ready,"transport_ok":b.transport_ok,"catching_up":b.catching_up,
                "bids":b.bids[:100],"asks":b.asks[:100],
                "truncated":len(b.bids)>100 or len(b.asks)>100}
        return {"ts_ms":self.clock.ms(),"books":out}

    def evaluate(self, route, available, captured=None):
        p = self.policy
        buy,sell,start_bid,start_ask,end_bid,end_ask,now,exchange_now = captured or self.capture(route)
        reasons = []
        for label, b in (("buy", buy), ("sell", sell)):
            reasons.extend(f"{label}:{r}" for r in freshness(b, now, exchange_now,
                p.entry_local_ms, p.entry_exchange_ms))
        if route.buy.base in p.excluded_assets:
            reasons.append("excluded_asset")
        if start_ask <= 0 or end_bid <= 0:
            reasons.append("valuation_unavailable")
        if buy and sell and abs(buy.exchange_ms-sell.exchange_ms) > p.entry_skew_ms:
            reasons.append("exchange_skew")
        if buy is None or sell is None or start_ask <= 0 or end_bid <= 0:
            return Plan(route, now, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO,
                        "unrated", (), (), tuple(reasons))
        reserve = max(D(p.fee_reserve), route.buy.fee, route.sell.fee)
        budget = min(D(p.cap_usdt), D(available)*start_ask)
        money = budget/start_ask/(ONE+reserve)
        qty = ZERO
        for price, amount in buy.asks:
            take = min(D(amount), money/D(price))
            qty += take
            money -= take*D(price)
            if money <= 0:
                break
        qty = floor(qty, route.buy.quantity_step)
        # FOK reserves the worst limit, not the volume-weighted expected price.
        first = walk(buy, "BUY", qty)
        if first:
            limit = ceil(first[1], route.buy.price_step)
            qty = min(qty, floor(budget/start_ask/(ONE+reserve)/limit,
                                 route.buy.quantity_step))
            first = walk(buy, "BUY", qty)
        limit = ceil(first[1], route.buy.price_step) if first else ZERO
        exit_qty = floor(qty*(ONE-reserve), route.sell.quantity_step)
        second = walk(sell, "SELL", exit_qty)
        exit_limit = floor(second[1], route.sell.price_step) if second else ZERO
        # Funding reserve is not a second fee. Unspent quote reserve is not
        # charged as a loss; unsold base reserve is kept out of cash proceeds.
        cost = qty*limit*(ONE+route.buy.fee)*start_ask
        net = exit_qty*exit_limit*(ONE-route.sell.fee)*end_bid
        edge = net/cost-ONE if cost else -ONE
        grade = "A" if edge >= D(p.grade_a_edge) else "B+" if edge >= D(p.min_edge) else "below"
        if grade == "below":
            reasons.append("edge_below_entry")
        if not first or not second:
            reasons.append("insufficient_depth")
        if cost < D(p.minimum_trade_usdt):
            reasons.append("below_minimum_size")
        if not route.buy.valid(qty, limit, "BUY") or not route.sell.valid(exit_qty, exit_limit, "SELL"):
            reasons.append("order_rules")
        if p.static_headroom and sell:
            if now-sell.received_ms+p.confirmation_budget_ms > p.exit_local_ms:
                reasons.append("static_exit_local_headroom")
            if exchange_now-sell.exchange_ms+p.confirmation_budget_ms > p.exit_exchange_ms:
                reasons.append("static_exit_exchange_headroom")
        metrics = {"exchange_age_ms":max(exchange_now-buy.exchange_ms, exchange_now-sell.exchange_ms),
            "local_age_ms":max(now-buy.received_ms, now-sell.received_ms),
            "headroom_budget_ms":p.confirmation_budget_ms,
            "headroom_is_prediction":True,"funding_reserved_usdt":str(qty*limit*(ONE+reserve)*start_ask),
            "fee_rates":(str(route.buy.fee),str(route.sell.fee)),"policy":p.dictionary()}
        return Plan(route, now, qty, limit, exit_qty, exit_limit, cost, net, edge,
                    grade, (buy.identity, sell.identity), (start_ask, end_bid),
                    tuple(dict.fromkeys(reasons)), metrics)

    def guard_entry(self, plan, spec, available):
        if self.clock.ms()-plan.created_ms > self.policy.final_plan_ms:
            raise GuardRejected("decision_expired_before_dispatch")
        buy,sell = self.books.get(plan.route.buy.symbol),self.books.get(plan.route.sell.symbol)
        marks = (self.valuation(plan.route.buy.quote)[1],self.valuation(plan.route.sell.quote)[0])
        p = self.policy
        identities = (buy.identity,sell.identity) if buy and sell else ()
        reusable = (plan.eligible and identities == plan.books and marks == plan.marks
            and plan.metrics.get("policy") == p.dictionary()
            and spec.symbol == plan.route.buy.symbol and spec.side == "BUY"
            and spec.quantity == plan.quantity and spec.price == plan.buy_limit)
        if reusable:
            now,ex = self.clock.ms(),self.exchange_time()
            reasons = freshness(buy,now,ex,p.entry_local_ms,p.entry_exchange_ms)
            reasons += freshness(sell,now,ex,p.entry_local_ms,p.entry_exchange_ms)
            if abs(buy.exchange_ms-sell.exchange_ms) > p.entry_skew_ms:
                reasons.append("exchange_skew")
            if p.static_headroom and (now-sell.received_ms+p.confirmation_budget_ms > p.exit_local_ms
                    or ex-sell.exchange_ms+p.confirmation_budget_ms > p.exit_exchange_ms):
                reasons.append("static_exit_headroom")
            funding = D(plan.metrics["funding_reserved_usdt"])
            if funding > D(available)*marks[0] or funding > D(p.cap_usdt):
                reasons.append("funding_changed")
            if reasons:
                raise GuardRejected(";".join(reasons))
            self.validation_reuses += 1
            return
        new = self.evaluate(plan.route, available)
        if not new.eligible:
            raise GuardRejected(";".join(new.reasons) or "edge_below_entry")
        b = self.books.get(plan.route.buy.symbol)
        fill = walk(b, "BUY", spec.quantity)
        reserve = max(D(self.policy.fee_reserve), plan.route.buy.fee, plan.route.sell.fee)
        if not fill or fill[1] > spec.price:
            raise GuardRejected("frozen_buy_limit_unavailable")
        sell_qty = floor(spec.quantity*(ONE-reserve), plan.route.sell.quantity_step)
        out = walk(self.books.get(plan.route.sell.symbol), "SELL", sell_qty)
        funding = spec.quantity*spec.price*(ONE+reserve)*new.marks[0]
        cost = spec.quantity*spec.price*(ONE+plan.route.buy.fee)*new.marks[0]
        net = sell_qty*floor(out[1], plan.route.sell.price_step)*(ONE-plan.route.sell.fee)*new.marks[1] if out else ZERO
        if funding > D(self.policy.cap_usdt) or funding > D(available)*new.marks[0] or cost <= 0 or net/cost-ONE < D(self.policy.min_edge):
            raise GuardRejected("frozen_quantity_edge_or_budget")

    def exits(self, route, quantity):
        """Compare the same sellable inventory, after acquisition.

        The positive entry threshold does not apply here. Each permitted exit
        remains price protected; inability to sell becomes a durable exposure.
        """
        p = self.policy
        markets = (route.sell, route.buy)
        # Both MEXC precision steps are powers of ten: coarsest common step.
        q = floor(quantity, max(m.quantity_step for m in markets))
        candidates = []
        for m in markets:
            b = self.books.get(m.symbol)
            if freshness(b, self.clock.ms(), self.exchange_time(), p.exit_local_ms, p.exit_exchange_ms):
                continue
            fill = walk(b, "SELL", q)
            mark, _ = self.valuation(m.quote)
            limit = floor(fill[1], m.price_step) if fill else ZERO
            if fill and mark > 0 and m.valid(q, limit, "SELL"):
                candidates.append(Exit(m, q, limit, q*limit*(ONE-m.fee)*mark,
                    "leg2" if m.symbol == route.sell.symbol else "unwind"))
        return sorted(candidates, key=lambda x:x.net_usdt, reverse=True)

    def guard_exit(self, spec, market):
        p = self.policy
        b = self.books.get(spec.symbol)
        reasons = freshness(b, self.clock.ms(), self.exchange_time(), p.exit_local_ms, p.exit_exchange_ms)
        fill = walk(b, "SELL", spec.quantity)
        if reasons or not fill or fill[1] < spec.price or not market.valid(spec.quantity, spec.price, "SELL"):
            raise GuardRejected("exit_changed_before_dispatch:"+",".join(reasons))

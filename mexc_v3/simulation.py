"""Delayed execution through the real coordinator, with explicit model limits."""
from dataclasses import dataclass
from dataclasses import replace
from collections import deque
import random
from .model import D, ZERO, ONE, Receipt, GuardRejected, Uncertain
from .decision import walk, freshness
import json


@dataclass(frozen=True)
class Latency:
    name: str
    multiplier: float = 1.0
    extra_ms: float = 0


# Sparse, historical private-WS notification measurements, NOT network-to-match
# estimates, a fitted distribution, nor proven FOK SELL latencies.
BUY_NOTIFY_MS = (69,71,58,69)
SELL_NOTIFY_MS = (48,52,74,52)


def restore_wallet(journal, initial):
    """Reconstruct from committed effects, not a possibly late UI checkpoint."""
    saved = journal.meta("initial_wallet")
    if saved is None:
        journal.set_meta("initial_wallet",initial)
        saved = initial
    wallet = {a:D(q) for a,q in saved.items()}
    for row in journal.export()["attempts"]:
        data = json.loads(row["data"])
        for asset,amount in data.get("effects",{}).items():
            wallet[asset] = wallet.get(asset,ZERO)+D(amount)
    return wallet


class PaperAdapter:
    mode = "paper"

    def __init__(self, books, markets, clock, profile=None, balances=None, seed=1, faults=()):
        self.books,self.markets,self.clock = books,markets,clock
        self.profile = profile or Latency("observed_proxy")
        self.wallet = {k:D(v) for k,v in (balances or {"USDT":"86.76158227044","USD1":"100","USDC":".0486382"}).items()}
        self.rng = random.Random(seed)
        self.orders = {}
        self.faults = deque(faults)
        self.sent = []
        # V2's order notifications did not provide final commissions. The
        # default simulation reproduces that information delay and fee reserve.
        self.fees_at_confirmation = False

    def balances(self):
        return dict(self.wallet)

    def refresh_account(self):
        return self.balances()

    def submit(self, spec, guard):
        guard()
        if spec.client_id in self.orders:
            raise Uncertain("Duplicate simulated client id")
        self.sent.append(spec)
        began = self.clock.ms()
        sample = self.rng.choice(BUY_NOTIFY_MS if spec.side == "BUY" else SELL_NOTIFY_MS)
        delay = sample*self.profile.multiplier+self.profile.extra_ms
        self.clock.sleep(delay/1000)
        fault = self.faults.popleft() if self.faults else None
        book = self.books.get(spec.symbol)
        market = self.markets[spec.symbol]
        quote = walk(book,spec.side,spec.quantity)
        available = self.wallet.get(market.quote if spec.side == "BUY" else market.base,ZERO)
        valid = (book is not None and book.ready and book.transport_ok and not book.catching_up
                 and book.published_ms <= self.clock.ms() and book.exchange_ms > 0)
        if quote:
            valid = valid and (quote[1] <= spec.price if spec.side == "BUY" else quote[1] >= spec.price)
        else:
            valid = False
        if valid:
            required = quote[0]*(ONE+market.fee) if spec.side == "BUY" else spec.quantity
            valid = required <= available
        executed = spec.quantity if valid and fault != "cancel" else ZERO
        value = quote[0] if executed else ZERO
        if fault == "partial" and executed:
            executed /= 2
            value /= 2
        fee = value*market.fee
        if executed:
            sign = ONE if spec.side == "BUY" else -ONE
            self.wallet[market.base] = self.wallet.get(market.base,ZERO)+sign*executed
            self.wallet[market.quote] = self.wallet.get(market.quote,ZERO)-sign*value-fee
        result = Receipt(spec.client_id,spec.symbol,spec.side,
            "FILLED" if executed == spec.quantity else "CANCELED",executed,value,
            "paper-"+spec.client_id,{market.quote:fee},self.fees_at_confirmation,"simulation",
            {"confirmation_ms":self.clock.ms()-began,"sample_ms":sample,"matching_proxy":"notification_time"})
        self.orders[spec.client_id] = result
        if fault == "unknown":
            raise Uncertain("Injected missing terminal confirmation after possible fill")
        return result

    def reconcile(self, spec):
        result = self.orders.get(spec.client_id)
        if result is None:
            raise Uncertain("No simulated terminal order")
        return result

    def commissions(self, spec, receipt):
        return replace(receipt,fees_known=True)

    def release(self, specs):
        # Keep a bounded reconciliation cache. All durable receipts live in SQLite.
        if len(self.orders) > 1000:
            for key in list(self.orders)[:-500]:
                self.orders.pop(key,None)

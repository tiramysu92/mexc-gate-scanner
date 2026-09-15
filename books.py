"""Ordered delta reconstruction with connection epochs and resync generations.

Each symbol has its own lock. A REST response can publish only into the exact
generation it captured. A snapshot alone never invents an exchange timestamp.
"""
from collections import deque
from dataclasses import dataclass, replace
import math
import threading
from .model import Book, Clock


@dataclass(frozen=True)
class Delta:
    symbol: str
    first: int
    last: int
    exchange_ms: int
    received_ms: int
    bids: tuple
    asks: tuple


def checked(rows):
    out = {}
    for price, quantity in rows:
        p, q = float(price), float(quantity)
        if not math.isfinite(p) or not math.isfinite(q) or p <= 0 or q < 0:
            raise ValueError("Invalid depth level")
        out[p] = q
    return out


class Slot:
    def __init__(self, symbol):
        self.symbol = symbol
        self.lock = threading.RLock()
        self.epoch = self.generation = self.version = 0
        self.received = self.exchange = 0
        self.bids, self.asks = {}, {}
        self.best_bid = self.best_ask = None
        self.buffer = deque(maxlen=4096)
        self.ready = False
        self.connected = False
        self.catchup = False
        self.snapshot = None
        self.last_reason = "startup"


class Books:
    def __init__(self, symbols, clock=None):
        self.clock = clock or Clock()
        self.slots = {s: Slot(s) for s in symbols}

    def connection(self, symbols, epoch, connected):
        for symbol in symbols:
            s = self.slots[symbol]
            with s.lock:
                if epoch < s.epoch:
                    continue
                s.epoch = epoch
                s.generation += 1
                s.ready = False
                s.connected = connected
                s.version = 0
                s.snapshot = None
                s.buffer.clear()
                s.last_reason = "reconnect" if connected else "disconnected"

    def catching_up(self, symbols, active):
        for symbol in symbols:
            s = self.slots[symbol]
            with s.lock:
                s.catchup = active

    def token(self, symbol):
        s = self.slots[symbol]
        with s.lock:
            return s.epoch, s.generation

    def invalidate(self, symbol, reason, clear=False):
        s = self.slots[symbol]
        with s.lock:
            s.ready = False
            s.generation += 1
            s.snapshot = None
            s.last_reason = reason
            if clear:
                s.buffer.clear()

    @staticmethod
    def apply_levels(s, delta):
        for target, rows, name, choose in ((s.bids, delta.bids, "best_bid", max),
                                         (s.asks, delta.asks, "best_ask", min)):
            best = getattr(s, name)
            for p, q in checked(rows).items():
                if q:
                    target[p] = q
                    best = p if best is None else choose(best, p)
                else:
                    target.pop(p, None)
            if best not in target:
                best = choose(target) if target else None
            setattr(s, name, best)
        if s.best_bid is None or s.best_ask is None or s.best_bid >= s.best_ask:
            raise ValueError("Empty or crossed depth")
        s.version = delta.last
        s.received, s.exchange = delta.received_ms, delta.exchange_ms
        s.snapshot = None

    def delta(self, delta, epoch):
        s = self.slots[delta.symbol]
        with s.lock:
            if epoch != s.epoch or not s.connected:
                return False
            if delta.first <= 0 or delta.last < delta.first or delta.exchange_ms <= 0:
                self.invalidate(s.symbol, "bad_sequence", True)
                return False
            if not s.ready:
                if len(s.buffer) == s.buffer.maxlen:
                    self.invalidate(s.symbol, "replay_buffer_full", True)
                s.buffer.append(delta)
                return False
            if delta.last <= s.version:
                return True
            if delta.first > s.version + 1:
                self.invalidate(s.symbol, "sequence_gap", True)
                s.buffer.append(delta)
                return False
            try:
                self.apply_levels(s, delta)
            except ValueError:
                self.invalidate(s.symbol, "invalid_book", True)
                return False
            return True

    def install(self, symbol, token, response):
        candidate = Slot(symbol)
        candidate.version = int(response["lastUpdateId"])
        if candidate.version <= 0:
            raise ValueError("Invalid REST version")
        candidate.bids = {p:q for p,q in checked(response["bids"]).items() if q}
        candidate.asks = {p:q for p,q in checked(response["asks"]).items() if q}
        if not candidate.bids or not candidate.asks or max(candidate.bids) >= min(candidate.asks):
            raise ValueError("Invalid REST book")
        candidate.best_bid, candidate.best_ask = max(candidate.bids), min(candidate.asks)
        s = self.slots[symbol]
        # Never hold any other symbol's lock while replaying this symbol.
        with s.lock:
            if token != (s.epoch, s.generation) or not s.connected or s.ready:
                return False
            for delta in s.buffer:
                if delta.last <= candidate.version:
                    continue
                if delta.first > candidate.version + 1:
                    s.last_reason = "snapshot_gap"
                    return False
                self.apply_levels(candidate, delta)
            s.bids, s.asks = candidate.bids, candidate.asks
            s.best_bid, s.best_ask = candidate.best_bid, candidate.best_ask
            s.version, s.received, s.exchange = candidate.version, candidate.received, candidate.exchange
            s.ready = True
            s.snapshot = None
            s.buffer.clear()
            s.last_reason = "ready"
            return True

    def get(self, symbol):
        s = self.slots[symbol]
        with s.lock:
            if not s.ready:
                return None
            if s.snapshot is None:
                s.snapshot = Book(symbol, s.epoch, s.version, s.received, s.exchange,
                    self.clock.ms(), tuple(sorted(s.bids.items(), reverse=True)),
                    tuple(sorted(s.asks.items())))
            return replace(s.snapshot, transport_ok=s.connected, catching_up=s.catchup)

    def bbo(self, symbol):
        s = self.slots[symbol]
        with s.lock:
            if not s.ready:
                return None
            return s.best_bid, s.best_ask

    def status(self):
        missing = []
        for name, s in self.slots.items():
            with s.lock:
                if not s.ready:
                    missing.append({"symbol":name, "reason":s.last_reason})
        return {"ready":len(self.slots)-len(missing), "total":len(self.slots), "missing":missing}

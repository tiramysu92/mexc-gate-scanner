from dataclasses import dataclass, field, asdict
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import math
import time


def D(value):
    value = Decimal(str(value))
    if not value.is_finite():
        raise ValueError("Non-finite amount")
    return value


ZERO = D(0)
ONE = D(1)


def floor(value, step):
    if step <= 0:
        raise ValueError("Invalid step")
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def ceil(value, step):
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def encode(value):
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    raise TypeError(type(value).__name__)


class Clock:
    def ms(self):
        return time.time_ns() // 1_000_000

    def mono(self):
        return time.monotonic()

    def sleep(self, seconds):
        time.sleep(max(0, seconds))


@dataclass(frozen=True)
class Market:
    symbol: str
    base: str
    quote: str
    quantity_step: Decimal
    price_step: Decimal
    min_quantity: Decimal
    min_notional: Decimal
    max_notional: Decimal = D("1e20")
    fee: Decimal = D("0.0005")
    buy_allowed: bool = True
    sell_allowed: bool = True
    rules_known: bool = True
    max_quantity: Decimal = D("1e30")

    def __post_init__(self):
        for name in ("quantity_step","price_step","min_quantity","min_notional","max_notional","max_quantity"):
            if D(getattr(self,name)) <= 0:
                raise ValueError("Invalid market rule "+name)
        if not 0 <= D(self.fee) < 1 or self.max_notional < self.min_notional:
            raise ValueError("Invalid market fee/notional")
        # The current quantity coarsening uses decimal precision steps only.
        # Reject other lot grids instead of silently rounding them incorrectly.
        if self.quantity_step.normalize().as_tuple().digits != (1,):
            raise ValueError("Unsupported non-decimal quantity step")

    def valid(self, qty, price, side):
        return (self.rules_known and qty > 0 and price > 0
                and qty == floor(qty, self.quantity_step)
                and price == floor(price, self.price_step)
                and self.min_quantity <= qty <= self.max_quantity
                and self.min_notional <= qty * price <= self.max_notional
                and (self.buy_allowed if side == "BUY" else self.sell_allowed))


@dataclass(frozen=True)
class Book:
    symbol: str
    epoch: int
    version: int
    received_ms: int
    exchange_ms: int
    published_ms: int
    bids: tuple
    asks: tuple
    ready: bool = True
    transport_ok: bool = True
    catching_up: bool = False

    @property
    def identity(self):
        return self.symbol, self.epoch, self.version


@dataclass(frozen=True)
class Route:
    buy: Market
    sell: Market

    @property
    def id(self):
        return f"{self.buy.quote}>{self.buy.base}>{self.sell.quote}"


@dataclass(frozen=True)
class Spec:
    client_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    purpose: str
    order_type: str = "FILL_OR_KILL"
    route_id: str = ""


TERMINAL = {"FILLED", "CANCELED", "PARTIALLY_CANCELED", "EXPIRED", "REJECTED"}


@dataclass
class Receipt:
    client_id: str
    symbol: str
    side: str
    status: str
    executed: Decimal
    quote: Decimal
    order_id: str
    fees: dict = field(default_factory=dict)
    fees_known: bool = False
    source: str = "unknown"
    timings: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)

    def validate(self, spec):
        if (self.client_id != spec.client_id or self.symbol != spec.symbol
                or self.side != spec.side or not self.order_id
                or self.status not in TERMINAL
                or not ZERO <= D(self.executed) <= spec.quantity
                or D(self.quote) < 0
                or (self.executed > 0) != (self.quote > 0)
                or (self.status == "FILLED" and self.executed != spec.quantity)):
            raise Uncertain("Invalid or nonterminal order confirmation")
        if any(D(x) < 0 for x in self.fees.values()):
            raise Uncertain("Negative commission")
        if spec.order_type == "FILL_OR_KILL" and self.executed:
            if (spec.side == "BUY" and self.quote > self.executed*spec.price) or (
                    spec.side == "SELL" and self.quote < self.executed*spec.price):
                raise Uncertain("Fill outside the protected limit")
        return self


class Uncertain(RuntimeError):
    """A request may have reached the exchange: reconciliation, never repost."""


class GuardRejected(RuntimeError):
    """Raised only before a request has been dispatched."""

from dataclasses import dataclass, asdict
from pathlib import Path
import json
from .model import D


@dataclass(frozen=True)
class Policy:
    cap_usdt: str = "20"
    minimum_trade_usdt: str = "10"
    min_edge: str = "0.0015"
    grade_a_edge: str = "0.003"
    fee_reserve: str = "0.002"
    entry_local_ms: int = 50
    entry_exchange_ms: int = 100
    entry_skew_ms: int = 50
    exit_local_ms: int = 100
    exit_exchange_ms: int = 150
    final_plan_ms: int = 50
    # Baseline policy retained. Shadow comparisons quantify this restriction.
    static_headroom: bool = True
    confirmation_budget_ms: int = 85
    clock_uncertainty_ms: int = 25
    excluded_assets: tuple = ("CTO",)
    max_buys: int = 10
    loss_stop_usdt: str = "5"
    dust_usdt: str = "0.05"
    maximum_exit_orders: int = 2
    exit_retry_ms: int = 1500

    def __post_init__(self):
        if not 0 < D(self.minimum_trade_usdt) <= D(self.cap_usdt) <= 20:
            raise ValueError("Trade amounts must satisfy 0 < minimum <= cap <= 20")
        if not 0 < D(self.loss_stop_usdt) <= 5 or not 1 <= self.max_buys <= 10:
            raise ValueError("Quota bounds")
        if not 0 <= D(self.min_edge) <= D(self.grade_a_edge) < 1:
            raise ValueError("Edge bounds")
        if not 0 <= D(self.fee_reserve) < D("0.01"):
            raise ValueError("Fee reserve bounds")
        for name in ("entry_local_ms", "entry_exchange_ms", "entry_skew_ms",
                     "exit_local_ms", "exit_exchange_ms", "final_plan_ms",
                     "confirmation_budget_ms", "clock_uncertainty_ms", "exit_retry_ms"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(f"Invalid {name}")
        if self.maximum_exit_orders not in (1, 2):
            raise ValueError("At most two exit orders")

    @classmethod
    def read(cls, path=None):
        return cls(**json.loads(Path(path).read_text())) if path else cls()

    def dictionary(self):
        return asdict(self)

# MEXC Spot 2-leg Scanner V2.1

V2.1 is deliberately **2-leg only**. 3-leg candidates are counted for diagnostics but are not subscribed or scanned; a separate 3-leg scanner can be built later.

## What changed from V2.0

- Immediate decision at **T0** when a 2-leg opportunity crosses the configured net threshold. There is no artificial 100 ms confirmation wait.
- Every T0 decision is followed in parallel at **+25 / +50 / +100 / +150 / +200 ms**.
- Research cohorts are split by maximum BBO age at T0: **50 / 100 / 150 / 200 / 250 / 300 ms**.
- Once a decision exists, the delayed observation is recorded **positive or negative**. A trade is never removed retrospectively because the opportunity disappeared.
- Per trial the DB stores T0 net/size/BBO age/BBO skew and delayed net/size/BBO age/BBO skew/PnL.
- Dashboard exposes a daily matrix (00:00 -> now Europe/Paris) and highlights the default research scenario BBO <=200 ms / +100 ms execution.
- New DB: `mexc_routes_v21.db`; V2.0 data remains untouched.

## Important interpretation

The execution matrix is intentionally a **market-execution research PnL**, before the capital/rebalancing portfolio model. This avoids hiding latency losses behind the old V2 rebalance engine. The intelligent pre-trade rebalance rule will be backtested from these richer execution records and then applied to the portfolio simulator once its threshold is calibrated.

This scanner still uses top-of-book only. If the executable quantity available at the delayed observation is smaller than at T0, the trial uses the smaller BBO quantity. Full multi-level order-book slippage is not yet simulated.

## Defaults

- Spot only
- 0.05% taker fee per leg
- Min detected net: +0.01%
- Min BBO executable size: $10
- Stablecoins: USDT, USDC, USD1
- 600 WebSocket symbols maximum
- Decision cooldown per route: 250 ms
- Delayed BBO observation accepted up to 1500 ms only to avoid fabricating a price when data is missing; such unresolved cases are explicitly counted rather than silently discarded.

## Run

```bash
nohup python mexc-spot-routes/app.py > routes_v21_live.log 2>&1 &
```

Dashboard: port 8081.

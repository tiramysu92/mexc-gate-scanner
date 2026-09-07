# MEXC Spot 2-Leg Scanner V2.2

V2.2 keeps two views in parallel:

1. **RAW / research** — no starting capital. BookTicker 10 ms, MEXC send timestamps + VPS receive timestamps, BBO age/skew, continuous trace from T0 to +300 ms, and latency matrix. Raw PnL uses visible BBO capacity and is not a portfolio return.
2. **Paper bot $2,000** — starts each Paris day with $2,000 split equally across USDT/USDC/USD1. It reserves source capital at T0, takes at most one entry per arbitrage event, settles at the configured execution latency, keeps losses, limits size to capital and visible execution BBO, can rebalance stable buckets when economically financed, and compounds profits/losses automatically because settlement proceeds remain in the trading balances.

Default primary paper-bot rules:
- exchange BBO age <= 150 ms
- exchange skew <= 50 ms
- execution observation at +150 ms
- taker fee 0.05% per leg
- minimum trade $10
- initial capital $2,000 = $666.67 per stable bucket

The RAW box remains visible next to the $2,000 paper portfolio so the difference between signal capacity and capital-constrained performance can be observed live.

Database: `mexc_routes_v22.db`
Dashboard: port `8081`

Run:

```bash
python app.py
```

Important: this remains a research/paper simulator. It does not place real orders and BBO-only execution is still less strict than a full-depth sequential two-leg order model.

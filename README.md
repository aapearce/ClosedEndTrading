# ClosedEndTrading

CEF (Closed-End Fund) stress signal dashboard — monitors discount-to-NAV deviations across the full CEF universe, overlaid with VIX, to identify periods of peak dislocation.

## Architecture

```
ClosedEndTrading/
├── index.html               # Dashboard UI (open locally or deploy to GitHub Pages)
├── config.json              # All strategy parameters — calibratable for backtesting
├── requirements.txt
├── data/
│   ├── universe.json        # Full CEF ticker list by asset class (~500 funds)
│   ├── signals.json         # Generated signals (created by strategy/signals.py)
│   ├── sample_signals.json  # Sample data for UI preview
│   ├── vix.json             # VIX timeseries from Yahoo Finance
│   └── timeseries/          # Per-fund JSON: date, price, NAV, premium/discount
├── scripts/
│   ├── fetch_universe.py    # Scrapes CEFConnect for full ticker universe
│   ├── fetch_fund_data.py   # Downloads 5yr price/NAV/discount per fund
│   ├── fetch_vix.py         # Downloads VIX from Yahoo Finance
│   └── update_all.py        # Master orchestrator — run this daily
└── strategy/
    └── signals.py           # Signal engine: z-score + VIX → traffic light
```

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. First run — fetches full 5-year history (~30 min for full universe)
python scripts/update_all.py --full

# 3. Open the dashboard
open index.html
```

## Daily Update

```bash
python scripts/update_all.py          # Incremental (~5 min)
```

Or click the **Refresh Data** button in the dashboard UI.

## Strategy

- **Universe**: ~500 CEFs from CEFConnect, organised by: Equity, Credit, Fixed Income, FX, Commodities, Gold
- **Signal logic**: Rolling z-score of each fund's premium/discount vs its own history, combined with VIX level
- **Traffic light**:
  - 🔴 **Peak Stress** — discount at extreme z-score AND VIX above stress threshold
  - 🟡 **Elevated** — discount widening OR VIX elevated
  - 🟢 **Normal** — within historical range, VIX calm

## Configuration

All signal thresholds live in `config.json` — kept open for backtesting calibration:

```json
{
  "strategy": {
    "zscore_window_days": 252,
    "vix_elevated_threshold": 20,
    "vix_stress_threshold": 30,
    "zscore_elevated_threshold": 1.5,
    "zscore_stress_threshold": 2.5
  }
}
```

## Data Sources

| Data | Source | Method |
|------|--------|--------|
| CEF price, NAV, P/D | CEFConnect (Nuveen) | Undocumented internal API |
| VIX | Yahoo Finance | `yfinance` library |

## Dashboard

- **Tab 1 — Screener**: Filter by asset class, sort by any column, search by ticker/name, traffic-light signal column
- **Tab 2 — Backtesting**: Placeholder for next iteration (parameter sweep, P&L, drawdown analysis)

## Roadmap

- [ ] Tab 2: Backtesting engine with VIX threshold sweep and z-score window optimisation
- [ ] Position sizing and ranking signals
- [ ] Fund detail page (price/NAV chart, discount history)
- [ ] Email/alert on new Peak Stress signals

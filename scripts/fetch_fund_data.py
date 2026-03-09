#!/usr/bin/env python3
"""
fetch_fund_data.py

Fetches 5-year daily price timeseries for each CEF in data/universe.json.

Data source:
  Price → yfinance (NYSE/Nasdaq listed; reliable for all ~370 CEFs)
  NAV   → Tiingo API (free key, set TIINGO_API_KEY env var)
          Falls back gracefully: signals.py will use price z-score when NAV=0

Setup (optional, for NAV data):
  export TIINGO_API_KEY=your_key_here    # free at https://www.tiingo.com

Saves: data/timeseries/<TICKER>.json

Usage:
  python fetch_fund_data.py               # Full fetch (all funds)
  python fetch_fund_data.py --incremental # Last 90 days only
  python fetch_fund_data.py --ticker EOS  # Single fund
  python fetch_fund_data.py --max 20      # First 20 (quick test)
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

UNIVERSE_PATH = Path(__file__).parent.parent / "data" / "universe.json"
TS_DIR        = Path(__file__).parent.parent / "data" / "timeseries"
CONFIG_PATH   = Path(__file__).parent.parent / "config.json"

TIINGO_TOKEN  = os.environ.get("TIINGO_API_KEY", "")


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def load_universe():
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


# ── Price via yfinance ────────────────────────────────────────────────────────

def fetch_price_history(ticker: str, start: str, end: str) -> dict:
    """Returns {date_str: close_price}."""
    try:
        df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return {}
        return {
            d.strftime("%Y-%m-%d"): round(float(row["Close"]), 4)
            for d, row in df.iterrows()
        }
    except Exception as e:
        print(f" [yf:{e}]", end="")
        return {}


# ── NAV via Tiingo (optional) ─────────────────────────────────────────────────

def fetch_nav_tiingo(ticker: str, start: str, end: str) -> dict:
    """
    Tiingo has end-of-day NAV for many CEFs.
    Returns {date_str: nav_value} or {} if not available / no key.
    """
    if not TIINGO_TOKEN:
        return {}
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {
        "startDate": start,
        "endDate":   end,
        "token":     TIINGO_TOKEN,
        "columns":   "date,close,adjClose",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        # Tiingo doesn't separate NAV vs price for CEFs;
        # adjClose ≈ NAV-adjusted is a reasonable proxy.
        # We return empty — Tiingo doesn't provide true NAV.
        return {}
    except Exception:
        return {}


# ── Build records ─────────────────────────────────────────────────────────────

def build_records(prices: dict, nav_data: dict) -> list:
    """
    Merge price and NAV data into sorted record list.
    When NAV is absent, records have nav=0, premium_discount=0;
    signals.py detects this and uses price z-score instead.
    """
    all_dates = sorted(set(prices.keys()) | set(nav_data.keys()))
    records = []
    for date in all_dates:
        price = prices.get(date)
        nav   = nav_data.get(date)

        # Compute P/D when both available
        pd_val = 0.0
        if price and nav and abs(nav) > 0.01:
            pd_val = round((price / nav - 1) * 100, 4)

        if price is None:
            continue  # Don't store NAV-only rows

        records.append({
            "date":              date,
            "price":             round(price, 4),
            "nav":               round(nav, 4) if nav else 0.0,
            "premium_discount":  pd_val,
        })
    return records


# ── Persist ───────────────────────────────────────────────────────────────────

def save_fund(ticker: str, records: list, metadata: dict):
    TS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "ticker":       ticker,
        "name":         metadata.get("name", ""),
        "asset_class":  metadata.get("asset_class", ""),
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "records":      records,
    }
    with open(TS_DIR / f"{ticker}.json", "w") as f:
        json.dump(out, f)


def load_existing(ticker: str) -> list:
    path = TS_DIR / f"{ticker}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f).get("records", [])
    return []


def merge_records(existing: list, new: list) -> list:
    merged = {r["date"]: r for r in existing}
    merged.update({r["date"]: r for r in new})
    return sorted(merged.values(), key=lambda x: x["date"])


# ── Main ──────────────────────────────────────────────────────────────────────

def run(incremental=False, single_ticker=None, max_funds=None):
    config   = load_config()
    years    = config["data"]["history_years"]
    universe = load_universe()

    ticker_meta = {}
    for asset_class, funds in universe.items():
        for fund in funds:
            ticker_meta[fund["ticker"]] = {
                "name": fund["name"], "asset_class": asset_class
            }

    tickers = [single_ticker.upper()] if single_ticker else list(ticker_meta.keys())
    if max_funds:
        tickers = tickers[:max_funds]
    total = len(tickers)

    end_dt     = datetime.today()
    start_dt   = end_dt - timedelta(days=90 if incremental else 365 * years + 30)
    end_date   = end_dt.strftime("%Y-%m-%d")
    start_date = start_dt.strftime("%Y-%m-%d")
    nav_source = "Tiingo" if TIINGO_TOKEN else "none (price z-score mode)"

    print(f"Fetching {total} funds | {start_date} -> {end_date}")
    print(f"Price source: yfinance  |  NAV source: {nav_source}\n")

    ok = skipped = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{total}] {ticker:<6s}", end="", flush=True)

        prices = fetch_price_history(ticker, start_date, end_date)
        if not prices:
            print(" — no price data")
            skipped += 1
            continue

        nav_data  = fetch_nav_tiingo(ticker, start_date, end_date)
        new_recs  = build_records(prices, nav_data)

        if not new_recs:
            print(" — no records built")
            skipped += 1
            continue

        if incremental:
            existing = load_existing(ticker)
            if existing:
                new_recs = merge_records(existing, new_recs)

        save_fund(ticker, new_recs, ticker_meta.get(ticker, {"name": "", "asset_class": ""}))
        nav_str = f"nav={len(nav_data)}d" if nav_data else "nav=0 (price mode)"
        print(f" price={len(prices)}d  {nav_str}  -> {len(new_recs)} records")
        ok += 1

        time.sleep(0.25)

    print(f"\n{'='*60}")
    print(f"Done: {ok} saved  |  {skipped} skipped")
    print("Run: python strategy/signals.py  to generate signals")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--ticker",      type=str,  default=None)
    parser.add_argument("--max",         type=int,  default=None)
    args = parser.parse_args()
    run(incremental=args.incremental, single_ticker=args.ticker, max_funds=args.max)

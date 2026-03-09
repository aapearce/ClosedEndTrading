#!/usr/bin/env python3
"""
fetch_fund_data.py

For each ticker in data/universe.json, downloads 5 years of:
  date, price, NAV, premium/discount
from CEFConnect's pricing history API.

Saves each fund as data/timeseries/<TICKER>.json

Usage:
  python fetch_fund_data.py               # Full fetch (all funds)
  python fetch_fund_data.py --incremental # Only update last year per fund
  python fetch_fund_data.py --ticker EOS  # Single fund
"""

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import requests

UNIVERSE_PATH = Path(__file__).parent.parent / "data" / "universe.json"
TS_DIR = Path(__file__).parent.parent / "data" / "timeseries"
CONFIG_PATH = Path(__file__).parent.parent / "config.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ClosedEndTrading/1.0)",
    "Accept": "application/json",
    "Referer": "https://www.cefconnect.com/",
}


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_universe():
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


def fetch_fund_history(ticker: str, years: int = 5) -> list:
    """
    Fetches pricing history from CEFConnect API.
    Returns list of {date, price, nav, premium_discount} dicts sorted ascending.
    """
    url = f"https://www.cefconnect.com/api/v3/PricingHistory/{ticker}"
    params = {"NumberOfYears": years}

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        raw = resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP error for {ticker}: {e}")
        return []
    except Exception as e:
        print(f"    Error fetching {ticker}: {e}")
        return []

    records = []
    for item in raw:
        try:
            date_str = item.get("Date", item.get("TradeDate", ""))[:10]
            records.append({
                "date": date_str,
                "price": round(float(item.get("MarketPrice", item.get("Price", 0))), 4),
                "nav": round(float(item.get("NAV", item.get("NavPerShare", 0))), 4),
                "premium_discount": round(float(item.get("PremiumDiscount", item.get("Discount", 0))), 4),
            })
        except (TypeError, ValueError):
            continue

    records.sort(key=lambda x: x["date"])
    return records


def save_fund(ticker: str, records: list, metadata: dict):
    TS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "ticker": ticker,
        "name": metadata.get("name", ""),
        "asset_class": metadata.get("asset_class", ""),
        "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "records": records,
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
    all_records = {r["date"]: r for r in existing}
    all_records.update({r["date"]: r for r in new})
    return sorted(all_records.values(), key=lambda x: x["date"])


def run(incremental: bool = False, single_ticker: str = None):
    config = load_config()
    years = config["data"]["history_years"]
    universe = load_universe()

    ticker_meta = {}
    for asset_class, funds in universe.items():
        for fund in funds:
            ticker_meta[fund["ticker"]] = {"name": fund["name"], "asset_class": asset_class}

    tickers = [single_ticker.upper()] if single_ticker else list(ticker_meta.keys())
    total = len(tickers)

    print(f"Fetching data for {total} funds (incremental={incremental})...")

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] {ticker}", end="", flush=True)

        if incremental:
            existing = load_existing(ticker)
            fetch_years = 1
        else:
            existing = []
            fetch_years = years

        new_records = fetch_fund_history(ticker, years=fetch_years)

        if new_records:
            records = merge_records(existing, new_records) if (incremental and existing) else new_records
            save_fund(ticker, records, ticker_meta.get(ticker, {"name": "", "asset_class": ""}))
            print(f" — {len(records)} records")
        else:
            print(" — no data")

        time.sleep(0.3)  # polite rate limiting

    print(f"\nDone. Timeseries saved to {TS_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--ticker", type=str, default=None)
    args = parser.parse_args()
    run(incremental=args.incremental, single_ticker=args.ticker)

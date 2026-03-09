#!/usr/bin/env python3
"""
fetch_fund_data.py

Fetches 5-year timeseries for each CEF in data/universe.json.

Data sources (in priority order):
  1. Price history      → yfinance (reliable, works for all listed CEFs)
  2. NAV + P/D history  → CEFConnect fund page HTML scrape (/fund/{TICKER}?view=pricing)
     Falls back to computing P/D from price if NAV scrape fails.

Saves each fund as data/timeseries/<TICKER>.json

Usage:
  python fetch_fund_data.py               # Full fetch (all funds, ~15-30 min)
  python fetch_fund_data.py --incremental # Incremental update (last 60 days)
  python fetch_fund_data.py --ticker EOS  # Single fund test
  python fetch_fund_data.py --max 20      # First 20 funds only (quick test)
"""

import argparse
import json
import time
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd
from bs4 import BeautifulSoup

UNIVERSE_PATH = Path(__file__).parent.parent / "data" / "universe.json"
TS_DIR        = Path(__file__).parent.parent / "data" / "timeseries"
CONFIG_PATH   = Path(__file__).parent.parent / "config.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cefconnect.com/",
}

SESSION = requests.Session()


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def load_universe():
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


# ── Source 1: Price via yfinance ─────────────────────────────────────────────

def fetch_price_history(ticker: str, start: str, end: str) -> dict:
    """Returns {date_str: close_price} from yfinance."""
    try:
        df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=True)
        if df.empty:
            return {}
        return {
            d.strftime("%Y-%m-%d"): round(float(row["Close"]), 4)
            for d, row in df.iterrows()
        }
    except Exception as e:
        print(f" [yf error: {e}]", end="")
        return {}


# ── Source 2: NAV + P/D via CEFConnect HTML scrape ───────────────────────────

def fetch_nav_history_cefconnect(ticker: str) -> dict:
    """
    Scrapes the CEFConnect fund pricing page for NAV and premium/discount history.
    Returns {date_str: {"nav": float, "pd": float}} or {}

    CEFConnect fund page URL pattern (still works as HTML):
      https://www.cefconnect.com/fund/{TICKER}?view=pricing
    The page embeds data in a JS variable or renders a table.
    """
    url = f"https://www.cefconnect.com/fund/{ticker}"
    params = {"view": "pricing"}

    try:
        resp = SESSION.get(url, params=params, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return {}
        html = resp.text
    except Exception:
        return {}

    result = {}

    # Try to find JSON embedded in page JS (common pattern for SPA-style pages)
    # Look for array of objects with NAV/PremiumDiscount fields
    json_pattern = re.compile(
        r'\[\s*\{[^}]*"(?:NAV|NavPerShare|nav)"[^}]*\}.*?\]',
        re.DOTALL | re.IGNORECASE
    )
    matches = json_pattern.findall(html)
    for match in matches:
        try:
            data = json.loads(match)
            if isinstance(data, list) and len(data) > 5:
                for item in data:
                    date_str = (item.get("Date") or item.get("TradeDate") or "")[:10]
                    nav = item.get("NAV") or item.get("NavPerShare") or item.get("nav")
                    pd_val = item.get("PremiumDiscount") or item.get("premiumDiscount") or item.get("Discount")
                    if date_str and nav:
                        result[date_str] = {
                            "nav": round(float(nav), 4),
                            "pd": round(float(pd_val), 4) if pd_val is not None else None,
                        }
                if result:
                    return result
        except Exception:
            continue

    # Fallback: parse HTML table
    soup = BeautifulSoup(html, "lxml")
    for table in soup.find_all("table"):
        headers_row = table.find("tr")
        if not headers_row:
            continue
        cols = [th.get_text(strip=True).lower() for th in headers_row.find_all(["th", "td"])]
        
        date_col = nav_col = pd_col = None
        for i, c in enumerate(cols):
            if "date" in c: date_col = i
            elif "nav" in c: nav_col = i
            elif "premium" in c or "discount" in c or "p/d" in c: pd_col = i

        if date_col is None or nav_col is None:
            continue

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            try:
                date_str = cells[date_col].get_text(strip=True)
                # Try to parse various date formats
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                    try:
                        date_str = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue
                nav = float(cells[nav_col].get_text(strip=True).replace("$","").replace(",",""))
                pd_val = None
                if pd_col and pd_col < len(cells):
                    try:
                        pd_val = float(cells[pd_col].get_text(strip=True).replace("%","").replace(",",""))
                    except ValueError:
                        pass
                result[date_str] = {"nav": round(nav, 4), "pd": pd_val}
            except (IndexError, ValueError):
                continue

        if result:
            return result

    return result


# ── Merge price + NAV → records ──────────────────────────────────────────────

def build_records(prices: dict, nav_data: dict) -> list:
    """
    Merge price dict and nav_data dict into sorted list of records.
    premium_discount = (price/nav - 1) * 100  if not available from source.
    """
    all_dates = sorted(set(prices.keys()) | set(nav_data.keys()))
    records = []

    for date in all_dates:
        price = prices.get(date)
        nav_entry = nav_data.get(date, {})
        nav = nav_entry.get("nav") if nav_entry else None
        pd_val = nav_entry.get("pd") if nav_entry else None

        # Compute P/D if not provided
        if pd_val is None and price and nav and nav != 0:
            pd_val = round((price / nav - 1) * 100, 4)

        if price is None and nav is None:
            continue

        records.append({
            "date": date,
            "price": price or 0.0,
            "nav": nav or 0.0,
            "premium_discount": pd_val or 0.0,
        })

    return records


# ── Persist ──────────────────────────────────────────────────────────────────

def save_fund(ticker: str, records: list, metadata: dict):
    TS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        "ticker": ticker,
        "name": metadata.get("name", ""),
        "asset_class": metadata.get("asset_class", ""),
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    merged = {r["date"]: r for r in existing}
    merged.update({r["date"]: r for r in new})
    return sorted(merged.values(), key=lambda x: x["date"])


# ── Main ─────────────────────────────────────────────────────────────────────

def run(incremental: bool = False, single_ticker: str = None, max_funds: int = None):
    config = load_config()
    years  = config["data"]["history_years"]
    universe = load_universe()

    ticker_meta = {}
    for asset_class, funds in universe.items():
        for fund in funds:
            ticker_meta[fund["ticker"]] = {"name": fund["name"], "asset_class": asset_class}

    tickers = [single_ticker.upper()] if single_ticker else list(ticker_meta.keys())
    if max_funds:
        tickers = tickers[:max_funds]
    total = len(tickers)

    end_date   = datetime.today().strftime("%Y-%m-%d")
    if incremental:
        start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")
    else:
        start_date = (datetime.today() - timedelta(days=365 * years + 30)).strftime("%Y-%m-%d")

    print(f"Fetching {total} funds | {start_date} → {end_date} | incremental={incremental}")
    print("Sources: price=yfinance, NAV/P/D=CEFConnect HTML\n")

    ok = skipped = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{total}] {ticker:<6s}", end="", flush=True)

        # --- Price from yfinance ---
        prices = fetch_price_history(ticker, start_date, end_date)
        if not prices:
            print(" — no price data (yfinance)")
            skipped += 1
            continue
        print(f" price={len(prices)}d", end="", flush=True)

        # --- NAV + P/D from CEFConnect ---
        nav_data = fetch_nav_history_cefconnect(ticker)
        if nav_data:
            print(f" nav={len(nav_data)}d", end="", flush=True)
        else:
            print(f" nav=N/A (computing P/D from price)", end="", flush=True)

        # --- Build records ---
        new_records = build_records(prices, nav_data)

        if not new_records:
            print(" — no records built")
            skipped += 1
            continue

        # --- Merge with existing (incremental) ---
        if incremental:
            existing = load_existing(ticker)
            if existing:
                new_records = merge_records(existing, new_records)

        save_fund(ticker, new_records, ticker_meta.get(ticker, {"name": "", "asset_class": ""}))
        print(f" → saved {len(new_records)} records")
        ok += 1

        # Polite pacing — CEFConnect scrape needs ~0.5s gap
        time.sleep(0.5)

    print(f"\n{'='*60}")
    print(f"Done: {ok} saved, {skipped} skipped")
    print(f"Timeseries → {TS_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true",
                        help="Only fetch last 90 days and merge with existing data")
    parser.add_argument("--ticker", type=str, default=None,
                        help="Fetch a single ticker (e.g. --ticker PDI)")
    parser.add_argument("--max", type=int, default=None,
                        help="Limit to first N tickers (useful for testing)")
    args = parser.parse_args()
    run(incremental=args.incremental, single_ticker=args.ticker, max_funds=args.max)

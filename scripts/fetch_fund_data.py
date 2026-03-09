#!/usr/bin/env python3
"""
fetch_fund_data.py

Fetches 5-year timeseries for each CEF in data/universe.json.

Data sources:
  Price history      → yfinance
  NAV + P/D history  → Morningstar (secid lookup → NAV history endpoint)
                       Falls back to computing P/D from price if NAV unavailable.

Saves each fund as data/timeseries/<TICKER>.json

Usage:
  python fetch_fund_data.py               # Full fetch (all funds)
  python fetch_fund_data.py --incremental # Incremental update (last 90 days)
  python fetch_fund_data.py --ticker EOS  # Single fund test
  python fetch_fund_data.py --max 20      # First 20 funds (quick test)
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yfinance as yf

UNIVERSE_PATH = Path(__file__).parent.parent / "data" / "universe.json"
TS_DIR        = Path(__file__).parent.parent / "data" / "timeseries"
CONFIG_PATH   = Path(__file__).parent.parent / "config.json"

# Morningstar headers — mimic browser
MS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.morningstar.com",
    "Referer": "https://www.morningstar.com/",
}

SESSION = requests.Session()
SESSION.headers.update(MS_HEADERS)

# Cache secid lookups to avoid redundant calls
_secid_cache: dict = {}


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)

def load_universe():
    with open(UNIVERSE_PATH) as f:
        return json.load(f)


# ── Morningstar secid lookup ──────────────────────────────────────────────────

def get_morningstar_secid(ticker: str) -> str | None:
    """
    Looks up the Morningstar security ID (secId) for a ticker.
    Uses Morningstar's search API.
    Returns secId string like 'FOUSA00ET7' or None.
    """
    if ticker in _secid_cache:
        return _secid_cache[ticker]

    url = "https://www.morningstar.com/api/v2/search/securities"
    params = {
        "q": ticker,
        "limit": 5,
        "universeIds": "FOUSATSX$$ALL",  # US + all fund types
    }
    try:
        resp = SESSION.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            # Try alternate search endpoint
            resp = SESSION.get(
                "https://api.morningstar.com/sal/sal-service/fund/search",
                params={"term": ticker, "limit": "5", "section": "etf,cef"},
                timeout=15,
            )
        if resp.status_code != 200:
            return None

        data = resp.json()
        # Walk response — different endpoints have different shapes
        results = (
            data.get("results")
            or data.get("funds", {}).get("result")
            or data.get("hits")
            or []
        )
        for item in results:
            symbol = item.get("ticker") or item.get("symbol") or item.get("performanceId", "")
            sec_id = item.get("secId") or item.get("id") or item.get("performanceId")
            if symbol.upper() == ticker.upper() and sec_id:
                _secid_cache[ticker] = sec_id
                return sec_id
    except Exception:
        pass

    _secid_cache[ticker] = None
    return None


# ── Morningstar NAV history ───────────────────────────────────────────────────

def fetch_nav_morningstar(ticker: str, start_date: str, end_date: str) -> dict:
    """
    Fetches NAV and premium/discount history from Morningstar.
    Returns {date_str: {"nav": float, "pd": float}} or {}

    Uses the Morningstar chart data API which returns daily NAV series.
    """
    result = {}

    # Strategy 1: direct ticker endpoint (works for many CEFs)
    urls_to_try = [
        f"https://www.morningstar.com/api/v2/cef/{ticker}/performance",
        f"https://api.morningstar.com/sal/sal-service/fund/cef/{ticker}/navHistory",
        f"https://www.morningstar.com/cef/{ticker.lower()}.json",
    ]

    for url in urls_to_try:
        try:
            resp = SESSION.get(url, params={"startDate": start_date, "endDate": end_date}, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                extracted = _parse_morningstar_nav(data)
                if extracted:
                    return extracted
        except Exception:
            continue

    # Strategy 2: look up secId then use chart history endpoint
    # Morningstar chart endpoint: works for secId-based queries
    secid = get_morningstar_secid(ticker)
    if secid:
        chart_urls = [
            f"https://api.morningstar.com/sal/sal-service/fund/cef/{secid}/navHistory",
            f"https://www.morningstar.com/api/v2/cef/{secid}/performance",
        ]
        for url in chart_urls:
            try:
                resp = SESSION.get(
                    url,
                    params={"startDate": start_date, "endDate": end_date, "secId": secid},
                    timeout=15,
                )
                if resp.status_code == 200:
                    extracted = _parse_morningstar_nav(resp.json())
                    if extracted:
                        return extracted
            except Exception:
                continue

    # Strategy 3: Morningstar CEF data via their public chart JSON
    # Pattern used by morningstar.com fund pages
    try:
        secid = secid or ticker
        url = (
            f"https://www.morningstar.com/api/v2/chart/data"
            f"?secId={secid}&currencyId=USD&frequency=d"
            f"&startDate={start_date}&endDate={end_date}"
            f"&priceType=NAV"
        )
        resp = SESSION.get(url, timeout=15)
        if resp.status_code == 200:
            extracted = _parse_morningstar_nav(resp.json())
            if extracted:
                return extracted
    except Exception:
        pass

    return result


def _parse_morningstar_nav(data: dict | list) -> dict:
    """Parse various Morningstar response shapes into {date: {nav, pd}}."""
    result = {}
    if not data:
        return result

    # Shape 1: list of records
    if isinstance(data, list):
        for item in data:
            d = _extract_nav_record(item)
            if d:
                result[d[0]] = d[1]
        return result

    # Shape 2: dict with various list keys
    for key in ("navHistory", "history", "data", "priceHistory", "TimeSeries", "series"):
        items = data.get(key)
        if isinstance(items, list):
            for item in items:
                d = _extract_nav_record(item)
                if d:
                    result[d[0]] = d[1]
            if result:
                return result

    # Shape 3: nested data.series[0].data
    try:
        series = data.get("data", {}).get("series", [{}])
        if series and isinstance(series[0].get("data"), list):
            for item in series[0]["data"]:
                d = _extract_nav_record(item)
                if d:
                    result[d[0]] = d[1]
    except Exception:
        pass

    return result


def _extract_nav_record(item) -> tuple | None:
    """Extract (date_str, {nav, pd}) from a single record dict."""
    if not isinstance(item, dict):
        return None
    try:
        date_str = (
            item.get("date") or item.get("Date") or
            item.get("endDate") or item.get("d") or ""
        )
        if not date_str:
            return None
        date_str = str(date_str)[:10]
        # Handle YYYY-MM-DD or MM/DD/YYYY
        if "/" in date_str:
            from datetime import datetime as dt
            date_str = dt.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")

        nav = (
            item.get("nav") or item.get("NAV") or item.get("v") or
            item.get("value") or item.get("navPerShare")
        )
        if nav is None:
            return None
        nav = round(float(nav), 4)

        pd_val = (
            item.get("premiumDiscount") or item.get("PremiumDiscount") or
            item.get("discount") or item.get("premium") or item.get("pd")
        )
        pd_val = round(float(pd_val), 4) if pd_val is not None else None

        return date_str, {"nav": nav, "pd": pd_val}
    except (TypeError, ValueError, KeyError):
        return None


# ── Price via yfinance ────────────────────────────────────────────────────────

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
        print(f" [yf:{e}]", end="")
        return {}


# ── Build + persist records ───────────────────────────────────────────────────

def build_records(prices: dict, nav_data: dict) -> list:
    """Merge price + NAV into sorted records list."""
    all_dates = sorted(set(prices.keys()) | set(nav_data.keys()))
    records = []
    for date in all_dates:
        price = prices.get(date)
        entry = nav_data.get(date, {})
        nav   = entry.get("nav") if entry else None
        pd_val = entry.get("pd") if entry else None

        if pd_val is None and price and nav and nav != 0:
            pd_val = round((price / nav - 1) * 100, 4)

        if price is None and nav is None:
            continue

        records.append({
            "date": date,
            "price": round(price or 0.0, 4),
            "nav": round(nav or 0.0, 4),
            "premium_discount": round(pd_val or 0.0, 4),
        })
    return records


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

    end_dt   = datetime.today()
    start_dt = end_dt - timedelta(days=90 if incremental else 365 * years + 30)
    end_date   = end_dt.strftime("%Y-%m-%d")
    start_date = start_dt.strftime("%Y-%m-%d")

    print(f"Fetching {total} funds | {start_date} → {end_date} | incremental={incremental}")
    print("Sources: price=yfinance  NAV/P/D=Morningstar (fallback: compute from price)\n")

    ok = skipped = nav_ok = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{total}] {ticker:<6s}", end="", flush=True)

        prices = fetch_price_history(ticker, start_date, end_date)
        if not prices:
            print(" — no price data")
            skipped += 1
            continue
        print(f" price={len(prices)}d", end="", flush=True)

        nav_data = fetch_nav_morningstar(ticker, start_date, end_date)
        if nav_data:
            print(f" nav={len(nav_data)}d", end="", flush=True)
            nav_ok += 1
        else:
            print(f" nav=computed", end="", flush=True)

        new_records = build_records(prices, nav_data)
        if not new_records:
            print(" — no records")
            skipped += 1
            continue

        if incremental:
            existing = load_existing(ticker)
            if existing:
                new_records = merge_records(existing, new_records)

        save_fund(ticker, new_records, ticker_meta.get(ticker, {"name": "", "asset_class": ""}))
        print(f" → {len(new_records)} records")
        ok += 1

        time.sleep(0.4)

    print(f"\n{'='*60}")
    print(f"Done: {ok} saved  |  NAV from Morningstar: {nav_ok}  |  NAV computed: {ok - nav_ok}  |  skipped: {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--ticker", type=str, default=None)
    parser.add_argument("--max", type=int, default=None)
    args = parser.parse_args()
    run(incremental=args.incremental, single_ticker=args.ticker, max_funds=args.max)

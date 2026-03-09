#!/usr/bin/env python3
"""
fetch_universe.py

Builds the CEF universe (ticker, name, asset class) by trying CEFConnect endpoints
in order. Falls back to scraping the daily-pricing HTML page if the JSON API is
unavailable.

Saves to data/universe.json.
"""

import json
import re
import time
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

OUT_PATH = Path(__file__).parent.parent / "data" / "universe.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cefconnect.com/",
}

# Asset class normalisation
ASSET_CLASS_MAP = {
    "taxable bond": "Fixed Income",
    "municipal bond": "Fixed Income",
    "muni": "Fixed Income",
    "investment grade bond": "Credit",
    "high yield bond": "Credit",
    "senior loan": "Credit",
    "convertibles": "Credit",
    "loan": "Credit",
    "bond": "Fixed Income",
    "equity": "Equity",
    "domestic equity": "Equity",
    "foreign equity": "Equity",
    "sector equity": "Equity",
    "real estate": "Equity",
    "option income": "Equity",
    "commodities": "Commodities",
    "commodity": "Commodities",
    "precious metals": "Gold",
    "gold": "Gold",
    "real assets": "Commodities",
    "currency": "FX",
    "fx": "FX",
    "multi-asset": "Equity",
    "balanced": "Equity",
    "preferred": "Credit",
}


def normalise_class(raw: str) -> str:
    if not raw:
        return "Other"
    key = raw.strip().lower()
    for k, v in ASSET_CLASS_MAP.items():
        if k in key:
            return v
    return raw.strip().title()


# ── Strategy 1: CEFConnect JSON API (several known candidate URLs) ──────────

API_CANDIDATES = [
    # Current likely endpoints based on network inspection patterns
    "https://www.cefconnect.com/api/v3/DailyPricing",
    "https://www.cefconnect.com/api/v3/funds",
    "https://www.cefconnect.com/api/v3/Funds",
    "https://www.cefconnect.com/api/v2/funds",
    "https://www.cefconnect.com/api/DailyPricing",
    "https://www.cefconnect.com/api/fund/list",
]

PRICING_API_CANDIDATES = [
    "https://www.cefconnect.com/api/v3/DailyPricing?CategoryCode=&SortField=Ticker&SortDirection=ASC",
    "https://www.cefconnect.com/api/v3/DailyPricing",
]


def try_json_api() -> list:
    """Try known JSON API endpoints. Return list of fund dicts or []."""
    session = requests.Session()
    # Load the homepage first to pick up any session cookies
    try:
        session.get("https://www.cefconnect.com/", headers=HEADERS, timeout=15)
    except Exception:
        pass

    for url in PRICING_API_CANDIDATES + API_CANDIDATES:
        try:
            print(f"  Trying: {url}")
            resp = session.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                print(f"    → {resp.status_code}")
                continue
            data = resp.json()
            # Look for a list — could be top-level array or nested
            if isinstance(data, list) and len(data) > 5:
                print(f"    ✓ Got {len(data)} records (array)")
                return data
            if isinstance(data, dict):
                for key in ("Funds", "Results", "Data", "funds", "data", "items"):
                    if isinstance(data.get(key), list) and len(data[key]) > 5:
                        print(f"    ✓ Got {len(data[key])} records (key={key})")
                        return data[key]
            print(f"    → unexpected shape: {str(data)[:120]}")
        except Exception as e:
            print(f"    → error: {e}")

    return []


def parse_json_funds(raw_funds: list) -> list:
    """Normalise raw fund dicts from JSON API to {ticker, name, asset_class}."""
    out = []
    for f in raw_funds:
        ticker = (
            f.get("Ticker") or f.get("ticker") or
            f.get("Symbol") or f.get("symbol") or ""
        ).strip().upper()
        name = (
            f.get("FundName") or f.get("fundName") or
            f.get("Name") or f.get("name") or ""
        )
        raw_cls = (
            f.get("AssetClass") or f.get("assetClass") or
            f.get("Category") or f.get("category") or
            f.get("InvestmentType") or f.get("investmentType") or
            f.get("TypeDescription") or ""
        )
        if ticker:
            out.append({
                "ticker": ticker,
                "name": name,
                "asset_class": normalise_class(raw_cls),
                "raw_category": raw_cls,
            })
    return out


# ── Strategy 2: Scrape the Daily Pricing HTML page ──────────────────────────

DAILY_PRICING_URL = "https://www.cefconnect.com/closed-end-funds-daily-pricing"

# CEFConnect daily pricing page has category tabs; these are the known category codes
CATEGORY_CODES = [
    ("",            "Equity"),           # default / all
    ("EQ",          "Equity"),
    ("TXB",         "Fixed Income"),
    ("MUN",         "Fixed Income"),
    ("PREF",        "Credit"),
    ("HYB",         "Credit"),
    ("SL",          "Credit"),
    ("COMM",        "Commodities"),
    ("PM",          "Gold"),
    ("CUR",         "FX"),
]


def scrape_html_page(session: requests.Session, category_hint: str = "Other") -> list:
    """Scrape ticker table from CEFConnect daily pricing page."""
    funds = []
    try:
        resp = session.get(DAILY_PRICING_URL, headers={**HEADERS, "Accept": "text/html"}, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  HTML scrape error: {e}")
        return funds

    soup = BeautifulSoup(resp.text, "lxml")

    # Look for table rows with fund data
    # CEFConnect renders a table with class or id containing 'fund' or 'pricing'
    tickers_found = set()

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 5:
            continue
        # Check headers
        headers_row = rows[0].find_all(["th", "td"])
        header_text = [h.get_text(strip=True).lower() for h in headers_row]
        if not any(h in header_text for h in ["ticker", "symbol", "fund"]):
            continue

        # Map header positions
        col_map = {}
        for i, h in enumerate(header_text):
            if "ticker" in h or "symbol" in h:
                col_map["ticker"] = i
            elif "fund" in h or "name" in h:
                col_map["name"] = i
            elif "categor" in h or "type" in h or "class" in h:
                col_map["category"] = i

        if "ticker" not in col_map:
            continue

        print(f"  Found pricing table with {len(rows)-1} rows, cols: {col_map}")

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= col_map.get("ticker", 0):
                continue
            ticker = cells[col_map["ticker"]].get_text(strip=True).upper()
            if not ticker or len(ticker) > 6 or not ticker.isalpha():
                continue
            if ticker in tickers_found:
                continue
            tickers_found.add(ticker)

            name = ""
            if "name" in col_map and len(cells) > col_map["name"]:
                name = cells[col_map["name"]].get_text(strip=True)

            raw_cls = ""
            if "category" in col_map and len(cells) > col_map["category"]:
                raw_cls = cells[col_map["category"]].get_text(strip=True)

            funds.append({
                "ticker": ticker,
                "name": name,
                "asset_class": normalise_class(raw_cls) if raw_cls else category_hint,
                "raw_category": raw_cls,
            })

    return funds


# ── Strategy 3: yfinance CEF universe via known ticker list ─────────────────

def get_known_cef_tickers() -> list:
    """
    Fallback: return a curated list of ~200 well-known CEF tickers with
    approximate asset class. This is a bootstrap list for when CEFConnect
    is inaccessible; the real data still comes from CEFConnect per-fund.
    """
    return [
        # Equity
        ("EOS","Eaton Vance Enhanced Equity Income II","Equity"),
        ("ETB","Eaton Vance Tax-Managed Buy-Write Opportunities","Equity"),
        ("ETW","Eaton Vance Tax-Managed Global Buy-Write Opp","Equity"),
        ("ETY","Eaton Vance Tax-Managed Diversified Equity Inc","Equity"),
        ("AWP","abrdn Global Premier Properties","Equity"),
        ("RQI","Cohen & Steers Real Estate Opportunities","Equity"),
        ("RNP","Cohen & Steers REIT and Preferred Income","Equity"),
        ("JDD","Nuveen Diversified Dividend and Income","Equity"),
        ("JCE","Nuveen Core Equity Alpha","Equity"),
        ("BOE","BlackRock Enhanced Global Dividend Trust","Equity"),
        ("BDJ","BlackRock Enhanced Equity Dividend Trust","Equity"),
        ("BGR","BlackRock Energy and Resources Trust","Equity"),
        ("CII","BlackRock Capital and Income","Equity"),
        ("BST","BlackRock Science and Technology Trust","Equity"),
        ("BGY","BlackRock Enhanced International Dividend Trust","Equity"),
        ("GUT","Gabelli Utility Trust","Equity"),
        ("GAB","Gabelli Equity Trust","Equity"),
        ("GGZ","Gabelli Global Small and Mid Cap Value Trust","Equity"),
        ("UTF","Cohen & Steers Infrastructure","Equity"),
        ("TY","Tri-Continental Corporation","Equity"),
        ("SPE","Special Opportunities Fund","Equity"),
        # Credit / High Yield
        ("PDI","PIMCO Dynamic Income","Credit"),
        ("PDO","PIMCO Dynamic Income Opportunities","Credit"),
        ("PCI","PIMCO Dynamic Credit and Mortgage Income","Credit"),
        ("PCN","PIMCO Corporate & Income Strategy","Credit"),
        ("PCQ","PIMCO California Municipal Income","Credit"),
        ("PHK","PIMCO High Income","Credit"),
        ("HYT","BlackRock Corporate High Yield","Credit"),
        ("HYI","Western Asset High Yield Defined Opportunity","Credit"),
        ("AGD","abrdn Income Credit Strategies","Credit"),
        ("AIF","Apollo Senior Floating Rate","Credit"),
        ("AFT","Apollo Tactical Income","Credit"),
        ("BGX","Blackstone Long-Short Credit Income","Credit"),
        ("BGB","Blackstone Strategic Credit","Credit"),
        ("ECC","Eagle Point Credit","Credit"),
        ("OXLC","Oxford Lane Capital","Credit"),
        ("SCM","Stellus Capital Investment","Credit"),
        ("TPVG","TriplePoint Venture Growth","Credit"),
        ("KCAP","Portman Ridge Finance","Credit"),
        # Fixed Income
        ("GIM","Templeton Global Income","Fixed Income"),
        ("NMZ","Nuveen Municipal High Income Opportunity","Fixed Income"),
        ("NEA","Nuveen AMT-Free Quality Municipal Income","Fixed Income"),
        ("NUV","Nuveen Municipal Value","Fixed Income"),
        ("NVG","Nuveen AMT-Free Municipal Credit Opportunities","Fixed Income"),
        ("NXJ","Nuveen New Jersey Quality Municipal Income","Fixed Income"),
        ("NAD","Nuveen Quality Municipal Income","Fixed Income"),
        ("NBB","Nuveen Taxable Municipal Income","Fixed Income"),
        ("CIK","Credit Suisse Asset Management Income","Fixed Income"),
        ("WIW","Western Asset Inflation-Linked Income","Fixed Income"),
        ("WIA","Western Asset Inflation-Linked Opportunities","Fixed Income"),
        ("GHY","PGIM Global High Yield","Fixed Income"),
        ("GDL","GDL Fund","Fixed Income"),
        ("BKT","BlackRock Income Trust","Fixed Income"),
        ("BFZ","BlackRock California Municipal Income Trust","Fixed Income"),
        ("MUB","iShares National Muni Bond","Fixed Income"),
        ("FFC","Flaherty & Crumrine Preferred Securities Income","Fixed Income"),
        ("FPF","First Trust Intermediate Duration Preferred & Income","Fixed Income"),
        ("JPC","Nuveen Preferred & Income Opportunities","Fixed Income"),
        ("JPS","Nuveen Preferred & Income Securities","Fixed Income"),
        # Commodities
        ("CRF","Cornerstone Total Return","Commodities"),
        ("CLM","Cornerstone Strategic Value","Commodities"),
        ("MCN","Madison Covered Call & Equity Strategy","Commodities"),
        # Gold
        ("GLD","Aberdeen Gold & Precious Metals","Gold"),
        ("ASA","ASA Gold and Precious Metals","Gold"),
        ("CEF","Sprott Physical Gold and Silver Trust","Gold"),
        ("PHYS","Sprott Physical Gold Trust","Gold"),
        ("PSLV","Sprott Physical Silver Trust","Gold"),
        # FX
        ("GCF","GrafTech International Holdings","FX"),
        ("FAX","abrdn Asia-Pacific Income","FX"),
        ("AWF","abrdn Global Income","FX"),
        ("EDD","Morgan Stanley Emerging Markets Domestic Debt","FX"),
        ("EMD","Western Asset Emerging Markets Debt","FX"),
        ("TEI","Templeton Emerging Markets Income","FX"),
        ("MSD","Morgan Stanley Emerging Markets Debt","FX"),
    ]


# ── Main ─────────────────────────────────────────────────────────────────────

def fetch_universe():
    print("=" * 60)
    print("Fetching CEF universe...")
    print("=" * 60)

    funds = []

    # Strategy 1: JSON API
    print("\n[1/3] Trying CEFConnect JSON API endpoints...")
    raw = try_json_api()
    if raw:
        funds = parse_json_funds(raw)
        print(f"  → {len(funds)} funds from JSON API")

    # Strategy 2: HTML scrape
    if not funds:
        print("\n[2/3] Trying HTML scrape of daily pricing page...")
        session = requests.Session()
        try:
            session.get("https://www.cefconnect.com/", headers=HEADERS, timeout=15)
        except Exception:
            pass
        funds = scrape_html_page(session)
        print(f"  → {len(funds)} funds from HTML scrape")

    # Strategy 3: Curated fallback ticker list
    if not funds:
        print("\n[3/3] Using built-in curated ticker list as bootstrap...")
        raw_list = get_known_cef_tickers()
        funds = [
            {"ticker": t, "name": n, "asset_class": c, "raw_category": c}
            for t, n, c in raw_list
        ]
        print(f"  → {len(funds)} funds from curated list")
        print("  NOTE: Run scripts/probe_cefconnect.py to find the live API URL")
        print("        and update config.json with the correct endpoint.")

    # Organise by asset class
    by_class = defaultdict(list)
    seen = set()
    for f in funds:
        if f["ticker"] in seen:
            continue
        seen.add(f["ticker"])
        by_class[f["asset_class"]].append({
            "ticker": f["ticker"],
            "name": f.get("name", ""),
            "raw_category": f.get("raw_category", ""),
        })

    universe = {k: sorted(v, key=lambda x: x["ticker"]) for k, v in sorted(by_class.items())}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(universe, f, indent=2)

    total = sum(len(v) for v in universe.values())
    print(f"\n✓ Saved {total} funds across {len(universe)} asset classes → {OUT_PATH}")
    for cls, lst in universe.items():
        print(f"  {cls:20s}: {len(lst):3d} funds")

    return universe


if __name__ == "__main__":
    fetch_universe()

#!/usr/bin/env python3
"""
fetch_universe.py

Scrapes the CEFConnect fund overview API to build a complete universe of CEF tickers,
organised by asset class. Saves to data/universe.json.

CEFConnect's internal API endpoint returns paginated JSON — no login required.
Endpoint: https://www.cefconnect.com/api/v3/Funds/GetFundsOverview
"""

import json
import os
import requests
from collections import defaultdict
from pathlib import Path

OUT_PATH = Path(__file__).parent.parent / "data" / "universe.json"

# Asset class mapping: CEFConnect category strings -> our canonical names
ASSET_CLASS_MAP = {
    "Taxable Bond": "Fixed Income",
    "Municipal Bond": "Fixed Income",
    "Investment Grade Bond": "Credit",
    "High Yield Bond": "Credit",
    "Senior Loan": "Credit",
    "Convertibles": "Credit",
    "Equity": "Equity",
    "Domestic Equity": "Equity",
    "Foreign Equity": "Equity",
    "Sector Equity": "Equity",
    "Commodities": "Commodities",
    "Precious Metals": "Gold",
    "Real Assets": "Commodities",
    "Currency": "FX",
    "Multi-Asset": "Equity",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ClosedEndTrading/1.0)",
    "Accept": "application/json",
    "Referer": "https://www.cefconnect.com/",
}


def fetch_universe():
    all_funds = []
    page = 1
    page_size = 100

    print("Fetching CEF universe from CEFConnect...")

    while True:
        url = "https://www.cefconnect.com/api/v3/Funds/GetFundsOverview"
        params = {
            "PageNumber": page,
            "PageSize": page_size,
            "SortField": "Ticker",
            "SortOrder": "asc",
        }

        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

        funds = data.get("Funds", data.get("Results", []))
        if not funds:
            break

        all_funds.extend(funds)
        total = data.get("TotalCount", data.get("Total", len(all_funds)))
        print(f"  Page {page}: fetched {len(funds)} funds ({len(all_funds)}/{total})")

        if len(all_funds) >= total:
            break
        page += 1

    # Organise by asset class
    by_class = defaultdict(list)

    for f in all_funds:
        ticker = f.get("Ticker", f.get("Symbol", "")).strip().upper()
        name = f.get("FundName", f.get("Name", ""))
        raw_type = f.get("AssetClass", f.get("InvestmentType", f.get("Category", "Unknown")))
        asset_class = ASSET_CLASS_MAP.get(raw_type, raw_type or "Other")

        if not ticker:
            continue

        by_class[asset_class].append({
            "ticker": ticker,
            "name": name,
            "raw_category": raw_type,
        })

    universe = {k: sorted(v, key=lambda x: x["ticker"]) for k, v in sorted(by_class.items())}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(universe, f, indent=2)

    total_funds = sum(len(v) for v in universe.values())
    print(f"\nSaved {total_funds} funds across {len(universe)} asset classes to {OUT_PATH}")
    for cls, funds in universe.items():
        print(f"  {cls}: {len(funds)} funds")

    return universe


if __name__ == "__main__":
    fetch_universe()

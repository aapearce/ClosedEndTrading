#!/usr/bin/env python3
"""
probe_cefconnect.py

Run this from your Codespace to discover the live CEFConnect API endpoints.
It tries dozens of candidate URLs and prints what actually responds with fund data.

Usage:
    python scripts/probe_cefconnect.py

Then copy any working URL into config.json under data.cefconnect_funds_url
"""

import json
import requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cefconnect.com/",
    "X-Requested-With": "XMLHttpRequest",
}

CANDIDATES = [
    # v3 guesses
    "https://www.cefconnect.com/api/v3/DailyPricing",
    "https://www.cefconnect.com/api/v3/DailyPricing?CategoryCode=&SortField=Ticker&SortDirection=ASC",
    "https://www.cefconnect.com/api/v3/Funds",
    "https://www.cefconnect.com/api/v3/Funds/GetFundsOverview",
    "https://www.cefconnect.com/api/v3/Screener",
    "https://www.cefconnect.com/api/v3/ClosedEndFunds",
    "https://www.cefconnect.com/api/v3/fund/list",
    "https://www.cefconnect.com/api/v3/pricing",
    "https://www.cefconnect.com/api/v3/dailypricing",
    # v2 guesses
    "https://www.cefconnect.com/api/v2/DailyPricing",
    "https://www.cefconnect.com/api/v2/Funds",
    "https://www.cefconnect.com/api/v2/funds",
    # No version
    "https://www.cefconnect.com/api/DailyPricing",
    "https://www.cefconnect.com/api/Funds",
    "https://www.cefconnect.com/api/fund/list",
    # Old pattern found in GitHub (may redirect to https)
    "http://www.cefconnect.com/fund/AWP?view=fund",
    "https://www.cefconnect.com/fund/AWP?view=fund",
    # ASMX / WebService style
    "https://www.cefconnect.com/WebServices/CEFConnectService.asmx",
    # Screener service
    "https://www.cefconnect.com/ClosedEndFundScreener/GetFunds",
    "https://www.cefconnect.com/services/funds",
]

PRICING_PARAMS = [
    {},
    {"CategoryCode": "", "SortField": "Ticker", "SortDirection": "ASC"},
    {"pageSize": 500, "page": 1},
    {"take": 500, "skip": 0},
]


def probe():
    session = requests.Session()
    print("Loading homepage to pick up cookies...")
    try:
        r = session.get("https://www.cefconnect.com/", headers=HEADERS, timeout=15)
        print(f"  Homepage: {r.status_code}")
    except Exception as e:
        print(f"  Homepage error: {e}")

    print(f"\nProbing {len(CANDIDATES)} candidate URLs...\n")

    working = []

    for url in CANDIDATES:
        for params in PRICING_PARAMS[:2]:  # try with/without params
            try:
                resp = session.get(url, params=params, headers=HEADERS, timeout=10)
                ct = resp.headers.get("Content-Type", "")
                status = resp.status_code

                if status == 404:
                    print(f"  404  {url}")
                    break  # no point trying params if 404
                elif status != 200:
                    print(f"  {status}  {url}" + (f"?{params}" if params else ""))
                    continue

                # 200 — check if it looks like fund data
                snippet = resp.text[:300].strip()
                is_json = "json" in ct or snippet.startswith(("[", "{"))

                if is_json:
                    try:
                        data = resp.json()
                        size = len(data) if isinstance(data, list) else len(str(data))
                        print(f"  200 JSON  {url}  size={size}  keys={list(data.keys()) if isinstance(data, dict) else 'array'}")
                        if size > 100:
                            working.append((url, params, data))
                    except Exception:
                        print(f"  200 JSON-parse-fail  {url}  {snippet[:80]}")
                else:
                    print(f"  200 HTML  {url}  ({len(resp.text)} bytes)")
                    if params:
                        continue  # HTML with params — skip, already logged without params
                    # For HTML pages that look like they have data
                    if "Ticker" in resp.text or "ticker" in resp.text:
                        working.append((url, params, {"_html": True, "size": len(resp.text)}))

            except Exception as e:
                print(f"  ERR  {url}  {e}")
                break

    print("\n" + "=" * 60)
    if working:
        print(f"WORKING ENDPOINTS ({len(working)} found):")
        for url, params, data in working:
            print(f"  {url}")
            if params:
                print(f"    params: {params}")
            if isinstance(data, list) and data:
                print(f"    sample record keys: {list(data[0].keys()) if isinstance(data[0], dict) else data[0]}")
    else:
        print("No JSON endpoints found returning fund data.")
        print("Try inspecting Network tab in browser devtools on cefconnect.com")
        print("Look for XHR requests when the fund screener or daily pricing page loads.")


if __name__ == "__main__":
    probe()

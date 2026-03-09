#!/usr/bin/env python3
"""
fetch_vix.py - Downloads 5 years of VIX daily close from Yahoo Finance.
Saves to data/vix.json as {last_updated, records: [{date, close}]}
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf

OUT_PATH = Path(__file__).parent.parent / "data" / "vix.json"


def fetch_vix(years: int = 5):
    print("Fetching VIX data from Yahoo Finance...")
    end = datetime.today()
    start = end - timedelta(days=365 * years + 30)

    df = yf.Ticker("^VIX").history(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d")
    )

    if df.empty:
        print("  ERROR: No VIX data returned")
        return

    records = [
        {"date": date.strftime("%Y-%m-%d"), "close": round(float(row["Close"]), 2)}
        for date, row in df.iterrows()
    ]
    records.sort(key=lambda x: x["date"])

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump({
            "last_updated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "records": records,
        }, f)

    print(f"  Saved {len(records)} VIX records")
    print(f"  Range: {records[0]['date']} to {records[-1]['date']}")
    print(f"  Latest VIX: {records[-1]['close']}")


if __name__ == "__main__":
    fetch_vix()

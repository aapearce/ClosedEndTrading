#!/usr/bin/env python3
"""
signals.py

Generates stress signals for each CEF based on:
  1. Rolling z-score of premium/discount (if NAV data available)
     OR rolling z-score of price (price-only mode, equally valid for stress detection)
  2. Current VIX level (threshold configurable)

Signal levels:
  NORMAL       -- z-score within normal range AND VIX calm
  ELEVATED     -- z-score widening OR VIX elevated
  PEAK_STRESS  -- z-score at extreme AND VIX above stress threshold

For price-only funds:
  z-score of price captures the same stress dynamics as P/D z-score.
  A CEF trading far below its own price history = stress dislocation.
  signal_mode field in output indicates 'pd' or 'price' per fund.

All thresholds are read from config.json.
Output: data/signals.json
"""

import json
from pathlib import Path
from datetime import datetime, timezone

import numpy as np

DATA_DIR    = Path(__file__).parent.parent / "data"
TS_DIR      = DATA_DIR / "timeseries"
CONFIG_PATH = Path(__file__).parent.parent / "config.json"
OUT_PATH    = DATA_DIR / "signals.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_vix():
    path = DATA_DIR / "vix.json"
    if not path.exists():
        return {}, 0.0
    with open(path) as f:
        data = json.load(f)
    records = data.get("records", [])
    vix_map = {r["date"]: r["close"] for r in records}
    latest_vix = records[-1]["close"] if records else 0.0
    return vix_map, latest_vix


def compute_rolling_zscore(values: list, window: int) -> list:
    """
    Rolling z-score of a list of floats.
    Returns list of same length (None for early entries before window fills).
    """
    arr = np.array(values, dtype=float)
    result = [None] * len(arr)
    for i in range(window - 1, len(arr)):
        w = arr[i - window + 1 : i + 1]
        mu = np.mean(w)
        sigma = np.std(w, ddof=1)
        result[i] = round((arr[i] - mu) / sigma, 4) if sigma > 1e-9 else 0.0
    return result


def classify_signal(zscore, vix: float, params: dict) -> str:
    if zscore is None:
        return "NORMAL"
    z_abs = abs(zscore)
    if z_abs >= params["zscore_stress_threshold"] and vix >= params["vix_stress_threshold"]:
        return "PEAK_STRESS"
    if z_abs >= params["zscore_elevated_threshold"] or vix >= params["vix_elevated_threshold"]:
        return "ELEVATED"
    return "NORMAL"


def has_nav_data(records: list) -> bool:
    """Returns True if meaningful NAV data exists (not all zeros)."""
    navs = [r.get("nav", 0) for r in records]
    nonzero = sum(1 for n in navs if n and abs(n) > 0.01)
    return nonzero > len(records) * 0.5


def generate_signals():
    config = load_config()
    params = config["strategy"]
    window = params["zscore_window_days"]
    _, latest_vix = load_vix()

    ts_files = sorted(TS_DIR.glob("*.json"))
    print(f"Computing signals for {len(ts_files)} funds | window={window}d | VIX={latest_vix:.2f}")

    signals = []
    mode_counts = {"pd": 0, "price": 0}

    for path in ts_files:
        with open(path) as f:
            fund = json.load(f)

        records = fund.get("records", [])
        if len(records) < max(window, 20):
            continue

        use_nav = has_nav_data(records)

        if use_nav:
            # Use premium/discount z-score
            series = [r["premium_discount"] for r in records]
            signal_mode = "pd"
        else:
            # Price z-score: standardise so direction convention matches
            # Negative z-score (price far below own history) = stress
            series = [r["price"] for r in records]
            signal_mode = "price"

        mode_counts[signal_mode] += 1
        zscores   = compute_rolling_zscore(series, window)
        latest    = records[-1]
        latest_z  = zscores[-1]

        # For price mode, a deeply negative z-score = stress (fund sold off)
        # We negate so the convention is consistent: large +z = stress
        if signal_mode == "price" and latest_z is not None:
            latest_z = -latest_z

        pd_series = [r["premium_discount"] for r in records]
        mean_pd   = round(float(np.mean(pd_series)), 4)
        latest_pd = latest.get("premium_discount", 0.0)

        signals.append({
            "ticker":       fund["ticker"],
            "name":         fund.get("name", ""),
            "asset_class":  fund.get("asset_class", "Other"),
            "signal_mode":  signal_mode,           # 'pd' or 'price'
            "latest_date":  latest["date"],
            "latest_price": latest["price"],
            "latest_nav":   latest.get("nav", 0.0),
            "latest_pd":    latest_pd,
            "mean_pd":      mean_pd,
            "latest_zscore": latest_z,
            "signal":       classify_signal(latest_z, latest_vix, params),
            "latest_vix":   latest_vix,
            "num_records":  len(records),
        })

    # Sort: PEAK_STRESS first, then by abs z-score
    order = {"PEAK_STRESS": 0, "ELEVATED": 1, "NORMAL": 2}
    signals.sort(
        key=lambda x: (order.get(x["signal"], 3), -(abs(x["latest_zscore"] or 0)))
    )

    out = {
        "generated_at":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_vix":      latest_vix,
        "config_snapshot": params,
        "signals":         signals,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(out, f)

    counts = {
        s: sum(1 for x in signals if x["signal"] == s)
        for s in ["PEAK_STRESS", "ELEVATED", "NORMAL"]
    }
    print(f"Signals saved to {OUT_PATH}")
    print(f"  PEAK_STRESS={counts['PEAK_STRESS']}  ELEVATED={counts['ELEVATED']}  NORMAL={counts['NORMAL']}")
    print(f"  Signal mode: pd={mode_counts['pd']} funds  price={mode_counts['price']} funds")
    return out


if __name__ == "__main__":
    generate_signals()

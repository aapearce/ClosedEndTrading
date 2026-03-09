#!/usr/bin/env python3
"""
signals.py

Generates stress signals for each CEF based on:
  1. Rolling z-score of premium/discount vs its own history (window configurable)
  2. Current VIX level (threshold configurable)

Signal levels:
  NORMAL       -- z-score within normal range AND VIX calm
  ELEVATED     -- z-score widening OR VIX elevated
  PEAK_STRESS  -- z-score at extreme AND VIX above stress threshold

All thresholds are read from config.json and kept open for backtesting calibration.

Output: data/signals.json
"""

import json
from pathlib import Path
from datetime import datetime

import numpy as np

DATA_DIR = Path(__file__).parent.parent / "data"
TS_DIR = DATA_DIR / "timeseries"
CONFIG_PATH = Path(__file__).parent.parent / "config.json"
OUT_PATH = DATA_DIR / "signals.json"


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
    Rolling z-score. Returns list of same length (None where window not yet filled).
    Positive z-score = premium widening; Negative = discount widening (stress).
    """
    result = []
    for i in range(len(values)):
        if i < window - 1:
            result.append(None)
            continue
        w = values[max(0, i - window + 1): i + 1]
        mu = np.mean(w)
        sigma = np.std(w, ddof=1)
        result.append(round((values[i] - mu) / sigma, 4) if sigma > 0 else 0.0)
    return result


def classify_signal(zscore, vix: float, params: dict) -> str:
    if zscore is None:
        return "NORMAL"
    z_abs = abs(zscore)
    if z_abs >= params["zscore_stress_threshold"] and vix >= params["vix_stress_threshold"]:
        return "PEAK_STRESS"
    elif z_abs >= params["zscore_elevated_threshold"] or vix >= params["vix_elevated_threshold"]:
        return "ELEVATED"
    return "NORMAL"


def generate_signals():
    config = load_config()
    params = config["strategy"]
    window = params["zscore_window_days"]
    vix_map, latest_vix = load_vix()

    ts_files = sorted(TS_DIR.glob("*.json"))
    print(f"Computing signals for {len(ts_files)} funds | window={window}d | VIX={latest_vix}")

    signals = []

    for path in ts_files:
        with open(path) as f:
            fund = json.load(f)

        records = fund.get("records", [])
        if len(records) < 20:
            continue

        pd_values = [r["premium_discount"] for r in records]
        zscores = compute_rolling_zscore(pd_values, window)

        latest = records[-1]
        latest_z = zscores[-1]

        signals.append({
            "ticker": fund["ticker"],
            "name": fund.get("name", ""),
            "asset_class": fund.get("asset_class", "Other"),
            "latest_date": latest["date"],
            "latest_price": latest["price"],
            "latest_nav": latest["nav"],
            "latest_pd": latest["premium_discount"],
            "latest_zscore": latest_z,
            "mean_pd": round(float(np.mean(pd_values)), 4),
            "std_pd": round(float(np.std(pd_values, ddof=1)), 4) if len(pd_values) > 1 else 0.0,
            "min_pd": round(min(pd_values), 4),
            "max_pd": round(max(pd_values), 4),
            "signal": classify_signal(latest_z, latest_vix, params),
            "latest_vix": latest_vix,
            "num_records": len(records),
        })

    # Sort by absolute z-score descending (biggest dislocations first)
    signals.sort(key=lambda x: abs(x["latest_zscore"] or 0), reverse=True)

    out = {
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_vix": latest_vix,
        "config_snapshot": params,
        "signals": signals,
    }

    with open(OUT_PATH, "w") as f:
        json.dump(out, f)

    counts = {s: sum(1 for x in signals if x["signal"] == s) for s in ["PEAK_STRESS", "ELEVATED", "NORMAL"]}
    print(f"Signals saved to {OUT_PATH}")
    print(f"  PEAK_STRESS={counts['PEAK_STRESS']}  ELEVATED={counts['ELEVATED']}  NORMAL={counts['NORMAL']}")
    return out


if __name__ == "__main__":
    generate_signals()

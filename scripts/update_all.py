#!/usr/bin/env python3
"""
update_all.py - Master daily update script.

Runs:
  1. fetch_universe.py  (re-scrapes ticker list)
  2. fetch_fund_data.py (incremental by default)
  3. fetch_vix.py
  4. signals.py         (recomputes all signals)

Usage:
  python update_all.py          # Incremental (fast, ~5 min)
  python update_all.py --full   # Full re-fetch (slow, ~30 min)
"""

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).parent
STRATEGY = SCRIPTS.parent / "strategy"


def run(cmd):
    label = Path(cmd[1]).name
    print(f"\n{'='*60}\nRunning: {label}\n{'='*60}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"WARNING: {label} exited with code {result.returncode}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Full history re-fetch")
    args = parser.parse_args()
    py = sys.executable

    run([py, str(SCRIPTS / "fetch_universe.py")])

    if args.full:
        run([py, str(SCRIPTS / "fetch_fund_data.py")])
    else:
        run([py, str(SCRIPTS / "fetch_fund_data.py"), "--incremental"])

    run([py, str(SCRIPTS / "fetch_vix.py")])
    run([py, str(STRATEGY / "signals.py")])

    print("\n" + "="*60)
    print("Update complete. Open index.html to view the dashboard.")
    print("="*60)


if __name__ == "__main__":
    main()

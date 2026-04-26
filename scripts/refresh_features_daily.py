"""Boxing features refresh — placeholder.

Parses cached BFO boxing event HTML into a bouts CSV; no model training yet.
When the model lands, this script will compute per-fighter Elo / Glicko /
recent form / KO rate / sub rate (no subs in boxing — replace with TKO rate)
/ method rates / etc., matching the regional MMA template.

For now it just shells out to the regional MMA parser to produce
``bfo_moneyline_bouts.csv`` from any cached HTML. Useful as a sanity check
that the boxing capture is producing parseable event pages.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve()
REPO = HERE.parent.parent
RESEARCH_REPO = Path(os.environ.get(
    "MMA_RESEARCH_REPO", "/Users/Ryan/gt/regional_mma_predictor"
))

log = logging.getLogger("refresh_features_daily")


def _collect_raw_dirs() -> list[Path]:
    base = REPO / "data" / "live_odds"
    if not base.exists():
        return []
    return sorted(p for p in base.glob("*/raw_html") if p.is_dir())


def merge_raw_dirs_into_workspace(workspace: Path) -> int:
    workspace.mkdir(parents=True, exist_ok=True)
    count = 0
    for d in _collect_raw_dirs():
        for html in d.glob("*.html"):
            target = workspace / html.name
            if target.exists() or target.is_symlink():
                continue
            try:
                target.symlink_to(html)
            except OSError:
                target.write_bytes(html.read_bytes())
            count += 1
    return count


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Refresh boxing feature tables (placeholder)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    workspace = REPO / "data" / "processed" / "bfo_corpus"
    merged = merge_raw_dirs_into_workspace(workspace)
    log.info("merged %d new event HTML files into %s", merged, workspace)

    total_html = len(list(workspace.glob("*.html")))
    if total_html == 0:
        log.warning("no HTML files in %s — nothing to parse", workspace)
        print(f"new_html={merged} total_html=0 bouts=0 with_winner=0")
        return 0

    prices_out = REPO / "data" / "processed" / "bfo_moneyline_prices.csv"
    bouts_out = REPO / "data" / "processed" / "bfo_moneyline_bouts.csv"
    manifest_out = REPO / "data" / "processed" / "bfo_parse_manifest.csv"

    cmd = [
        sys.executable, "-m", "regional_mma_predictor.cli", "parse-bfo-dir",
        "--raw-dir", str(workspace),
        "--prices-out", str(prices_out),
        "--bouts-out", str(bouts_out),
        "--manifest-out", str(manifest_out),
    ]
    log.info("running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(RESEARCH_REPO), check=True,
                          capture_output=True, text=True)
    log.info("parse stdout: %s", proc.stdout.strip())

    bouts_df = pd.read_csv(bouts_out) if bouts_out.exists() else pd.DataFrame()
    print(f"new_html={merged} total_html={total_html} bouts={len(bouts_df)} "
          f"with_winner=0  [PLACEHOLDER — no results join yet]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

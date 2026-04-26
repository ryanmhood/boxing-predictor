"""Boxing live scorer — placeholder.

This is the **scaffold** for boxing scoring. Until a model is trained
(separate multi-day project — see roadmap notes), this script just emits an
empty picks CSV with a header so the rest of the pipeline (append, resolve,
summarize) doesn't error out.

When the model lands, replace ``score_upcoming`` with real Phase 1b-style
training + scoring (see ``regional_mma_predictor.model.walk_forward_backtest``
for the template).
"""

from __future__ import annotations

import argparse
import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parent.parent
log = logging.getLogger("score_live")

PICKS_COLUMNS = [
    "capture_date", "event_date", "event_id", "event_name", "promotion",
    "bout_id", "fighter_a", "fighter_b", "market", "book",
    "model_prob_a", "model_prob_b", "market_prob_a", "market_prob_b",
    "price_a", "price_b", "side", "fighter_side",
    "entry_odds_american", "entry_implied_prob",
    "model_prob", "edge_pct", "flag_would_bet", "notes",
]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Score upcoming boxing bouts (PLACEHOLDER)")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    as_of = (datetime.fromisoformat(args.date).replace(tzinfo=timezone.utc)
             if args.date else datetime.now(timezone.utc))
    yyyymmdd = as_of.strftime("%Y%m%d")
    out_path = REPO / "data" / "live_picks" / f"{yyyymmdd}_picks.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PICKS_COLUMNS)
        writer.writeheader()

    log.warning("boxing model not yet trained — emitted empty picks file at %s", out_path)
    print(f"rows=0 flagged=0 out={out_path}  [PLACEHOLDER — no model]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

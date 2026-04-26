"""Resolve open MMA paper bets against a settled-results CSV.

Expected results file (path: ``data/manual_results.csv`` by default) has columns:
    event_date,fighter_a,fighter_b,winner

where ``winner`` is one of: the exact fighter_a or fighter_b name, ``draw``,
or ``nc`` (no contest). Draws and NCs are resolved as push.

This is deliberately conservative: if no results row matches an open bet, the
bet stays in status=open so nothing is accidentally mis-settled.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.paper_bets.ledger import (  # noqa: E402
    DEFAULT_LEDGER_PATH,
    read_ledger,
    write_ledger_atomic,
)

log = logging.getLogger("paper_bets.resolve")

DEFAULT_RESULTS_PATH = REPO / "data" / "manual_results.csv"


def _parse_int(s):
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _parse_float(s):
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _normalize(name: str) -> str:
    return (name or "").strip().lower()


def _american_profit(odds: int, stake: float) -> float:
    if odds >= 100:
        return stake * (odds / 100.0)
    return stake * (100.0 / abs(odds))


def _load_results(path: Path) -> dict:
    """Return {(event_date, norm_a, norm_b): winner_norm_or_special}."""
    if not path.exists():
        return {}
    out: dict = {}
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ed = (row.get("event_date") or "").strip()
            a = _normalize(row.get("fighter_a"))
            b = _normalize(row.get("fighter_b"))
            w = _normalize(row.get("winner"))
            if not (ed and a and b):
                continue
            out[(ed, a, b)] = w
            out[(ed, b, a)] = w
    return out


def resolve_bet(row: dict, results: dict) -> dict | None:
    """Return an updated row if resolvable, else None."""
    if row.get("status") != "open":
        return None
    ed = (row.get("event_date") or "").strip()
    a = _normalize(row.get("fighter_a"))
    b = _normalize(row.get("fighter_b"))
    side = _normalize(row.get("side"))
    if not (ed and a and b and side):
        return None
    winner = results.get((ed, a, b))
    if winner is None:
        return None

    stake = _parse_float(row.get("stake_notional")) or 0.0
    odds = _parse_int(row.get("entry_odds_american"))
    if odds is None:
        return None

    if winner in ("draw", "nc"):
        status = "push"
        pnl = 0.0
        outcome = "push"
    elif winner == side:
        status = "won"
        pnl = _american_profit(odds, stake)
        outcome = "win"
    elif winner in (a, b):
        status = "lost"
        pnl = -stake
        outcome = "loss"
    else:
        log.warning(
            "bet_id=%s has unrecognized winner=%r; leaving open",
            row.get("bet_id"), winner,
        )
        return None

    updated = dict(row)
    updated["status"] = status
    updated["outcome_value"] = outcome
    updated["pnl_notional"] = round(pnl, 2)
    updated["winner"] = winner
    updated["resolved_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return updated


def resolve_ledger(ledger_path: Path, results_path: Path) -> tuple[int, int, int]:
    """Return (checked, resolved, still_open)."""
    rows = read_ledger(ledger_path)
    results = _load_results(results_path)
    if not rows:
        return 0, 0, 0

    resolved = 0
    still_open = 0
    out: list[dict] = []
    for row in rows:
        if row.get("status") == "open":
            updated = resolve_bet(row, results)
            if updated is not None:
                out.append(updated)
                resolved += 1
            else:
                out.append(row)
                still_open += 1
        else:
            out.append(row)

    if resolved > 0:
        write_ledger_atomic(out, ledger_path)
    return len(rows), resolved, still_open


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Resolve open MMA paper bets from a results CSV")
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH))
    parser.add_argument("--results", default=str(DEFAULT_RESULTS_PATH))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    checked, resolved, still_open = resolve_ledger(
        Path(args.ledger), Path(args.results),
    )
    print(f"checked={checked} resolved={resolved} still_open={still_open}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

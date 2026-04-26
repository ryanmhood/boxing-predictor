"""Shared ledger schema + atomic CSV I/O for regional MMA paper_bets."""

from __future__ import annotations

import csv
import os
import tempfile
from pathlib import Path

LEDGER_COLUMNS: list[str] = [
    "bet_id",
    "entry_ts",
    "capture_date",
    "event_date",
    "event_id",
    "event_name",
    "promotion",
    "bout_id",
    "fighter_a",
    "fighter_b",
    "market",
    "side",
    "book",
    "entry_odds_american",
    "entry_implied_prob",
    "model_prob",
    "edge_pct",
    "stake_notional",
    "status",
    "closing_odds_american",
    "clv_pct",
    "winner",
    "outcome_value",
    "pnl_notional",
    "resolved_at",
    "notes",
]

DEFAULT_LEDGER_PATH = Path("data/paper_bets.csv")


def read_ledger(path: Path | str = DEFAULT_LEDGER_PATH) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def write_ledger_atomic(rows: list[dict], path: Path | str = DEFAULT_LEDGER_PATH) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=p.name + ".", suffix=".tmp", dir=str(p.parent)
    )
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=LEDGER_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({col: _fmt(row.get(col)) for col in LEDGER_COLUMNS})
        os.replace(tmp_path, p)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def build_bet_id(
    event_id: str, bout_id: str, side: str, book: str = "bfo_consensus"
) -> str:
    return f"{event_id}_{bout_id}_{side}_{book}"

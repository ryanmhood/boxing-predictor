"""Append flagged picks from live_picks/{YYYYMMDD}_picks.csv to the paper ledger.

Idempotent by bet_id: re-running on the same day never duplicates rows.
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
    build_bet_id,
    read_ledger,
    write_ledger_atomic,
)

log = logging.getLogger("paper_bets.append")

PICKS_DIR = REPO / "data" / "live_picks"


def _parse_bool(s) -> bool:
    if isinstance(s, bool):
        return s
    return str(s).strip().lower() in {"true", "1", "yes"}


def _parse_int(s):
    if s is None or s == "":
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _parse_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _date_from_arg(date: str | None) -> str:
    if date is None:
        return datetime.now(timezone.utc).strftime("%Y%m%d")
    return date.replace("-", "")


def read_picks(path: Path) -> list[dict]:
    with path.open("r", newline="") as f:
        return [dict(row) for row in csv.DictReader(f)]


def pick_to_ledger_row(pick: dict, *, stake: float) -> dict | None:
    entry_odds = _parse_int(pick.get("entry_odds_american"))
    if entry_odds is None:
        return None

    # Per-pick variable stake (Kelly-sized) overrides the flat default if
    # the picks CSV carries a `stake` column. Falls back to the function
    # arg `stake` (the flat $10 default) when the column is missing/empty.
    pick_stake = _parse_float(pick.get("stake"))
    if pick_stake is not None and pick_stake > 0:
        stake = pick_stake

    market = pick.get("market", "moneyline")
    book = pick.get("book", "bfo_consensus")
    side = pick.get("side", "")
    event_id = pick.get("event_id", "")
    bout_id = pick.get("bout_id", "")

    bet_id = build_bet_id(event_id, bout_id, side, book)
    entry_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "bet_id": bet_id,
        "entry_ts": entry_ts,
        "capture_date": pick.get("capture_date", ""),
        "event_date": pick.get("event_date", ""),
        "event_id": event_id,
        "event_name": pick.get("event_name", ""),
        "promotion": pick.get("promotion", ""),
        "bout_id": bout_id,
        "fighter_a": pick.get("fighter_a", ""),
        "fighter_b": pick.get("fighter_b", ""),
        "market": market,
        "side": side,
        "book": book,
        "entry_odds_american": entry_odds,
        "entry_implied_prob": _parse_float(pick.get("entry_implied_prob")),
        "model_prob": _parse_float(pick.get("model_prob")),
        "edge_pct": _parse_float(pick.get("edge_pct")),
        "stake_notional": round(float(stake), 2),
        "status": "open",
        "closing_odds_american": None,
        "clv_pct": None,
        "winner": None,
        "outcome_value": None,
        "pnl_notional": None,
        "resolved_at": None,
        "notes": pick.get("notes", ""),
    }


def append_flagged(
    *,
    date: str | None = None,
    stake: float = 10.0,
    picks_dir: Path = PICKS_DIR,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    picks_file: Path | None = None,
) -> tuple[int, int, int]:
    """Return (flagged, appended, skipped_existing)."""
    if picks_file is not None:
        picks_path = Path(picks_file)
    else:
        date_yyyymmdd = _date_from_arg(date)
        picks_path = picks_dir / f"{date_yyyymmdd}_picks.csv"
    if not picks_path.exists():
        log.warning("no picks file at %s", picks_path)
        return 0, 0, 0

    picks = read_picks(picks_path)
    flagged = [p for p in picks if _parse_bool(p.get("flag_would_bet"))]
    log.info("%d picks read, %d flagged in %s", len(picks), len(flagged), picks_path)

    existing = read_ledger(ledger_path)
    existing_ids = {row["bet_id"] for row in existing}

    appended = 0
    skipped = 0
    new_rows: list[dict] = list(existing)
    for pick in flagged:
        row = pick_to_ledger_row(pick, stake=stake)
        if row is None:
            log.warning("skipping unusable pick (no odds): %s", pick.get("bout_id"))
            continue
        if row["bet_id"] in existing_ids:
            skipped += 1
            continue
        new_rows.append(row)
        existing_ids.add(row["bet_id"])
        appended += 1
        log.info(
            "appended bet_id=%s edge=%s odds=%s",
            row["bet_id"], row["edge_pct"], row["entry_odds_american"],
        )

    if appended > 0:
        write_ledger_atomic(new_rows, ledger_path)
    elif not ledger_path.exists():
        write_ledger_atomic([], ledger_path)

    return len(flagged), appended, skipped


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Append flagged MMA picks to paper ledger")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--stake", type=float, default=10.0, help="Notional stake per bet")
    parser.add_argument("--picks-dir", default=str(PICKS_DIR))
    parser.add_argument("--picks-file", default=None,
                        help="Explicit picks CSV path (overrides --date/--picks-dir)")
    parser.add_argument("--ledger", default=str(REPO / "data" / "paper_bets.csv"))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    flagged, appended, skipped = append_flagged(
        date=args.date,
        stake=args.stake,
        picks_dir=Path(args.picks_dir),
        ledger_path=Path(args.ledger),
        picks_file=Path(args.picks_file) if args.picks_file else None,
    )
    print(f"flagged={flagged} appended={appended} skipped_existing={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

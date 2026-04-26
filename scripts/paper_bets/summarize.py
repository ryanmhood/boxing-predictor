"""Summarize the regional MMA paper ledger (counts, ROI, CLV, recent bets)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parents[2]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.paper_bets.ledger import read_ledger  # noqa: E402

log = logging.getLogger("paper_bets.summarize")


def _num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def summarize(ledger_path: Path) -> dict:
    rows = read_ledger(ledger_path)
    total = len(rows)
    open_bets = [r for r in rows if r.get("status") == "open"]
    resolved = [r for r in rows if r.get("status") in ("won", "lost", "push", "void")]
    won = [r for r in resolved if r.get("status") == "won"]
    lost = [r for r in resolved if r.get("status") == "lost"]
    staked = sum(_num(r.get("stake_notional")) for r in resolved)
    pnl = sum(_num(r.get("pnl_notional")) for r in resolved)
    roi = (pnl / staked * 100.0) if staked > 0 else None
    win_rate = (len(won) / (len(won) + len(lost)) * 100.0) if (won or lost) else None
    clv_vals = [_num(r.get("clv_pct"), default=None)
                for r in rows if r.get("clv_pct") not in (None, "", "None")]
    clv_vals = [v for v in clv_vals if v is not None]
    mean_clv = (sum(clv_vals) / len(clv_vals)) if clv_vals else None

    return {
        "total_bets": total,
        "open": len(open_bets),
        "resolved": len(resolved),
        "won": len(won),
        "lost": len(lost),
        "total_staked": round(staked, 2),
        "total_pnl": round(pnl, 2),
        "roi_pct": (round(roi, 2) if roi is not None else None),
        "win_rate_pct": (round(win_rate, 2) if win_rate is not None else None),
        "n_with_clv": len(clv_vals),
        "mean_clv_pct": (round(mean_clv, 2) if mean_clv is not None else None),
    }


def _fmt_md(s: dict, ledger_path: Path) -> str:
    lines = ["# Regional MMA paper-ledger summary", ""]
    lines.append(f"Ledger: `{ledger_path}`")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Total bets | {s['total_bets']} |")
    lines.append(f"| Open | {s['open']} |")
    lines.append(f"| Resolved | {s['resolved']} ({s['won']}W / {s['lost']}L) |")
    lines.append(f"| Total staked | {s['total_staked']} |")
    lines.append(f"| Total P/L | {s['total_pnl']} |")
    lines.append(f"| ROI | {s['roi_pct']}% |" if s['roi_pct'] is not None else "| ROI | n/a |")
    lines.append(f"| Win rate | {s['win_rate_pct']}% |" if s['win_rate_pct'] is not None else "| Win rate | n/a |")
    lines.append(
        f"| Mean CLV | {s['mean_clv_pct']}% (n={s['n_with_clv']}) |"
        if s['mean_clv_pct'] is not None else "| Mean CLV | n/a |"
    )
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Summarize regional MMA paper ledger")
    parser.add_argument("--ledger", default=str(REPO / "data" / "paper_bets.csv"))
    parser.add_argument("--out", default=str(REPO / "data" / "paper_bets_summary.md"))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    ledger_path = Path(args.ledger)
    s = summarize(ledger_path)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_fmt_md(s, ledger_path))
    print(
        f"total={s['total_bets']} open={s['open']} resolved={s['resolved']} "
        f"roi={s['roi_pct']}% win_rate={s['win_rate_pct']}% mean_clv={s['mean_clv_pct']}%"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

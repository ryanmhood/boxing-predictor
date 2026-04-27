#!/usr/bin/env python3
"""
Validate BoxRec scrape coverage by joining to the PBO bouts archive.

For every PBO bout in data/processed/pbo_results.csv, check whether
data/raw/boxer_results_boxrec.csv (produced by scrape_boxrec_bulk.py)
contains a matching fight on the same date (+/- 2 days) with the same
sorted pair of normalised fighter names.

Outputs:
  data/reports/pbo_boxrec_join_coverage.md     -- per-year markdown report
  stdout                                        -- human-readable summary

Why this exists: the Wikipedia-fallback scraper achieved only 0.8% PBO
join coverage, which is too thin for the boxing model. We need >50%
BoxRec coverage on the PBO bout universe to justify continued model work.
This script is the gate.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PBO_RESULTS_CSV = ROOT / "data" / "processed" / "pbo_results.csv"
BOXREC_RESULTS_CSV = ROOT / "data" / "raw" / "boxer_results_boxrec.csv"
REPORT_PATH = ROOT / "data" / "reports" / "pbo_boxrec_join_coverage.md"
DATE_TOLERANCE_DAYS = 2

SUFFIXES = {"jr", "jr.", "junior", "jnr", "sr", "sr.", "senior", "snr",
            "ii", "iii", "iv"}
NICK_RE = re.compile(r'["“”‘’\'`]([^"“”‘’\'`]+)["“”‘’\'`]')
PUNCT_RE = re.compile(r"[^\w\s]")
WS_RE = re.compile(r"\s+")


def norm_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = NICK_RE.sub(" ", s)
    s = s.lower()
    s = PUNCT_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    parts = [p for p in s.split() if p not in SUFFIXES]
    return " ".join(parts)


def pair_key(a: str, b: str) -> tuple[str, ...]:
    na, nb = norm_name(a), norm_name(b)
    return tuple(sorted((na, nb)))


def parse_date(s: str):
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def load_pbo() -> list[dict]:
    if not PBO_RESULTS_CSV.exists():
        raise SystemExit(f"ERROR: {PBO_RESULTS_CSV} not found")
    out: list[dict] = []
    with PBO_RESULTS_CSV.open() as f:
        for r in csv.DictReader(f):
            d = parse_date(r.get("event_date", ""))
            if d is None:
                continue
            out.append({
                "event_id": r.get("event_id", ""),
                "event_date": d,
                "fighter_a": r.get("fighter_a", ""),
                "fighter_b": r.get("fighter_b", ""),
                "winner": r.get("winner", ""),
                "method": r.get("method", ""),
                "round": r.get("round", ""),
            })
    return out


def load_boxrec_index() -> dict[tuple[str, ...], list[dict]]:
    """Index BoxRec fights by sorted-pair key."""
    idx: dict[tuple[str, ...], list[dict]] = defaultdict(list)
    if not BOXREC_RESULTS_CSV.exists():
        print(
            f"WARNING: {BOXREC_RESULTS_CSV} not found -- coverage will be 0%. "
            "Run scrape_boxrec_bulk.py first.",
            file=sys.stderr,
        )
        return idx
    with BOXREC_RESULTS_CSV.open() as f:
        for r in csv.DictReader(f):
            d = parse_date(r.get("fight_date", ""))
            if d is None:
                continue
            key = pair_key(r.get("boxer_name", ""), r.get("opp_name", ""))
            if not all(key):
                continue
            idx[key].append({
                "date": d,
                "result": r.get("result", ""),
                "method": r.get("method", ""),
                "round": r.get("round", ""),
            })
    return idx


def join(pbo: list[dict], idx: dict) -> list[dict]:
    out: list[dict] = []
    for b in pbo:
        key = pair_key(b["fighter_a"], b["fighter_b"])
        cand = idx.get(key, [])
        match = None
        for c in cand:
            if abs((c["date"] - b["event_date"]).days) <= DATE_TOLERANCE_DAYS:
                match = c
                break
        out.append({
            **b,
            "matched": bool(match),
            "match_date": match["date"].isoformat() if match else "",
            "match_result": match["result"] if match else "",
            "match_method": match["method"] if match else "",
            "match_round": match["round"] if match else "",
        })
    return out


def render_report(joined: list[dict]) -> str:
    n = len(joined)
    n_match = sum(1 for r in joined if r["matched"])
    pct = (100.0 * n_match / n) if n else 0.0

    by_year: dict[int, dict[str, int]] = defaultdict(lambda: {"n": 0, "matched": 0})
    for r in joined:
        y = r["event_date"].year
        by_year[y]["n"] += 1
        if r["matched"]:
            by_year[y]["matched"] += 1

    lines = []
    lines.append("# PBO ↔ BoxRec join coverage")
    lines.append("")
    lines.append(
        f"_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} "
        f"by `scripts/check_boxrec_pbo_join.py`_"
    )
    lines.append("")
    lines.append(f"**Inputs**")
    lines.append(f"- PBO bouts: `{PBO_RESULTS_CSV.relative_to(ROOT)}` ({n} rows)")
    if BOXREC_RESULTS_CSV.exists():
        with BOXREC_RESULTS_CSV.open() as f:
            n_boxrec = sum(1 for _ in f) - 1
    else:
        n_boxrec = 0
    lines.append(
        f"- BoxRec fights: `{BOXREC_RESULTS_CSV.relative_to(ROOT)}` "
        f"({max(n_boxrec, 0)} rows)"
    )
    lines.append("")
    lines.append("## Headline")
    lines.append("")
    lines.append(
        f"- Matched: **{n_match} / {n}** PBO bouts (**{pct:.2f}%**)"
    )
    lines.append(
        f"- Match rule: same sorted pair of normalised names AND "
        f"|date_pbo - date_boxrec| ≤ {DATE_TOLERANCE_DAYS} days"
    )
    lines.append(
        "- Decision gate: ≥50% needed to justify continuing the boxing model "
        "(Wikipedia-only baseline was 0.8%)"
    )
    lines.append("")
    lines.append("## Per-year coverage")
    lines.append("")
    lines.append("| Year | PBO bouts | Matched | % |")
    lines.append("|---|---:|---:|---:|")
    for y in sorted(by_year):
        s = by_year[y]
        ypct = (100.0 * s["matched"] / s["n"]) if s["n"] else 0.0
        lines.append(f"| {y} | {s['n']} | {s['matched']} | {ypct:.1f}% |")
    lines.append("")
    if n_match == 0:
        lines.append(
            "> ⚠ Zero matches. Either the BoxRec scrape has not been run "
            "yet, or name-normalisation is failing. Spot-check the "
            "`boxer_results_boxrec.csv` file and re-run."
        )
    elif pct < 50.0:
        lines.append(
            f"> ⚠ Coverage {pct:.1f}% is below the 50% gate. Consider "
            "expanding the BoxRec target list, improving the name aliasing, "
            "or relaxing the date tolerance."
        )
    else:
        lines.append(
            f"> ✓ Coverage {pct:.1f}% clears the 50% gate -- the BoxRec "
            "feed is viable as the modelling source."
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)

    pbo = load_pbo()
    idx = load_boxrec_index()
    joined = join(pbo, idx)
    n = len(joined)
    n_match = sum(1 for r in joined if r["matched"])
    pct = (100.0 * n_match / n) if n else 0.0

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(render_report(joined), encoding="utf-8")

    if not args.quiet:
        print(f"PBO bouts          : {n}")
        print(f"Matched in BoxRec  : {n_match} ({pct:.2f}%)")
        print(f"Date tolerance     : ±{DATE_TOLERANCE_DAYS} days")
        print(f"Report             : {REPORT_PATH}")

        # Per-year breakdown
        by_year: Counter = Counter()
        by_year_match: Counter = Counter()
        for r in joined:
            y = r["event_date"].year
            by_year[y] += 1
            if r["matched"]:
                by_year_match[y] += 1
        print()
        print("Per-year coverage:")
        for y in sorted(by_year):
            tot = by_year[y]
            mm = by_year_match[y]
            ypct = (100.0 * mm / tot) if tot else 0.0
            print(f"  {y}: {mm:>5d}/{tot:<5d}  {ypct:5.1f}%")

    # Exit code: 0 = pass, 1 = below gate (so callers/CI can branch)
    return 0 if pct >= 50.0 else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Concatenate multiple boxer-results CSVs into one, deduplicating by
(fight_date, sorted-pair-of-normalised-names).

Used to produce a union of, e.g., Tapology + Plainte coverage to feed
into scripts/check_boxrec_pbo_join.py.

Usage:
    python3 scripts/merge_boxer_results.py \\
        --in data/raw/boxer_results_tapology.csv \\
        --in data/raw/boxer_results_plainte.csv \\
        --out data/raw/boxer_results_union.csv
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from pathlib import Path

FIELDS = [
    "fight_date", "boxer_id", "boxer_name", "opp_id", "opp_name",
    "result", "method", "round", "weight_class", "location",
]
SUFFIXES = {"jr", "jr.", "junior", "jnr", "sr", "sr.", "senior", "snr",
            "ii", "iii", "iv"}


def norm(name: str) -> str:
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[\"'`]", " ", s.lower())
    s = re.sub(r"[^\w\s]", " ", s)
    parts = [p for p in s.split() if p not in SUFFIXES]
    return " ".join(parts)


def dedupe_key(row: dict) -> tuple:
    a = norm(row.get("boxer_name", ""))
    b = norm(row.get("opp_name", ""))
    return (row.get("fight_date", "")[:10],) + tuple(sorted((a, b)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", action="append", dest="inputs", required=True,
                    type=Path, help="input CSV (repeatable)")
    ap.add_argument("--out", required=True, type=Path)
    args = ap.parse_args()

    seen: dict = {}
    total_in = 0
    for path in args.inputs:
        if not path.exists():
            print(f"[merge] WARNING: {path} not found, skipping", file=sys.stderr)
            continue
        with path.open() as f:
            for r in csv.DictReader(f):
                total_in += 1
                k = dedupe_key(r)
                if k[0] == "" or not all(k[1:]):
                    continue
                # Prefer the row with more fields filled in.
                old = seen.get(k)
                if old is None:
                    seen[k] = r
                else:
                    score_new = sum(1 for v in r.values() if v)
                    score_old = sum(1 for v in old.values() if v)
                    if score_new > score_old:
                        seen[k] = r

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in seen.values():
            w.writerow(r)

    print(f"[merge] inputs={total_in:_}  unique={len(seen):_}  out={args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Ingest Stephen Plainte's pre-scraped BoxRec dump.

This is NOT a scraper -- it is a one-shot data download from Plainte's
public GitHub repo (FuriouStyles/BeautifulSoup_Meets_BoxRec).  The repo
hosts a Beautiful Soup scrape Plainte ran in late 2019 / early 2020:

    boxers.csv       (~2.3 MB, 12k boxer profiles)
    fights.csv       (~29 MB,  90k+ fight rows -- but mostly pre-2020)
    all_bouts.csv    (~14 MB,  long-format bout list)

The dataset's date range is 1958-01 to 2020-01 (verified via probe).
PBO bouts are 2018-2025 with 86% of bouts occurring in 2020 or later, so
this source by itself can never clear the 50% PBO-join gate -- but it is
free, immediate, and useful as:
  (a) a baseline for pre-2020 PBO bouts (1183 of 8942)
  (b) a reference cross-check for live-scrape sources (Tapology, etc.)

USAGE:
    python3 scripts/ingest_plainte_dump.py        # download + convert

Outputs:
    data/raw/boxer_results_plainte.csv  (schema = boxer_results.csv)
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
CACHE_DIR = ROOT / "data" / "cache" / "plainte"
RAW_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BASE = (
    "https://raw.githubusercontent.com/"
    "FuriouStyles/BeautifulSoup_Meets_BoxRec/master"
)
RESULTS_CSV = RAW_DIR / "boxer_results_plainte.csv"

# Plainte's all_bouts.csv columns:
#   date, decision, opponent, opponent_0, opponent_0_br_id, opponent_br_id,
#   result, title_fight, venue, w-l-d
#
# - `decision`  = single-letter result from the boxer's POV (W/L/D)
# - `result`    = method short (KO / TKO / RTD / UD / SD / MD / PTS / DQ / ND)
# - `opponent`  = opposing fighter's name (the long-format dual encoding)
# - `opponent_0`= "subject" boxer (whose record the row belongs to)
# - `opponent_br_id` = BoxRec id of `opponent`
# - `opponent_0_br_id` = BoxRec id of `opponent_0`
# - `venue`     = location string
# - `w-l-d`     = career record at fight time (e.g. "21 2 1")

METHOD_TOKENS = {
    "ko":   "ko",
    "tko":  "tko",
    "ud":   "decision_unanimous",
    "md":   "decision_majority",
    "sd":   "decision_split",
    "pts":  "decision",
    "rtd":  "tko_corner",
    "tdec": "decision_technical",
    "tdraw":"draw_technical",
    "draw": "draw",
    "dq":   "dq",
    "nc":   "no_contest",
    "nd":   "no_contest",
}


def _norm_method(text: str) -> str:
    if not text:
        return ""
    base = re.split(r"[^a-z]", text.strip().lower())[0]
    return METHOD_TOKENS.get(base, base)


def download(fname: str) -> Path:
    """Download a raw GitHub file to local cache, skipping if present."""
    cached = CACHE_DIR / fname
    if cached.exists() and cached.stat().st_size > 1024:
        print(f"[plainte] cache hit: {cached.relative_to(ROOT)}"
              f" ({cached.stat().st_size:_} bytes)")
        return cached
    from urllib.request import urlopen, Request
    url = f"{BASE}/{fname}"
    print(f"[plainte] downloading {url} -> {cached.relative_to(ROOT)}")
    req = Request(url, headers={"User-Agent": "boxing-predictor/ingest"})
    with urlopen(req, timeout=120) as resp:
        cached.write_bytes(resp.read())
    print(f"[plainte] saved {cached.stat().st_size:_} bytes")
    return cached


def convert_all_bouts(src: Path) -> list[dict]:
    """Convert Plainte's all_bouts.csv into our boxer_results schema."""
    out: list[dict] = []
    with src.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            date = (r.get("date") or "").strip()
            if not date or len(date) < 8:
                continue
            boxer_id  = (r.get("opponent_0_br_id") or "").strip()
            boxer_nm  = (r.get("opponent_0") or "").strip()
            opp_id    = (r.get("opponent_br_id") or "").strip()
            opp_nm    = (r.get("opponent") or "").strip()
            # Plainte often suffixes opponent name with '*' for cf-asterisks
            opp_nm = opp_nm.rstrip("*").strip()
            result    = (r.get("decision") or "").strip().upper()
            method    = _norm_method(r.get("result") or "")
            venue     = (r.get("venue") or "").strip()

            if not (boxer_nm and opp_nm):
                continue

            out.append({
                "fight_date":   date,
                "boxer_id":     boxer_id,
                "boxer_name":   boxer_nm,
                "opp_id":       opp_id,
                "opp_name":     opp_nm,
                "result":       result if result in {"W","L","D","NC"} else "",
                "method":       method,
                "round":        "",        # Plainte's all_bouts.csv lacks rounds
                "weight_class": "",
                "location":     venue,
            })
    return out


RESULT_FIELDS = [
    "fight_date", "boxer_id", "boxer_name", "opp_id", "opp_name",
    "result", "method", "round", "weight_class", "location",
]


def main() -> int:
    src = download("all_bouts.csv")
    rows = convert_all_bouts(src)
    print(f"[plainte] converted {len(rows):_} fight rows")
    with RESULTS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[plainte] wrote -> {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

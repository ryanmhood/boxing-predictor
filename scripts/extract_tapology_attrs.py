"""Extract per-fighter attributes (height_cm, reach_cm) from cached
Tapology profile HTMLs. No HTTP — operates entirely on the cache that
scrape_tapology.py has already populated.

Output: data/processed/tapology_attrs.csv
  columns: fighter_id, height_cm, reach_cm

Stance is not reliably surfaced on Tapology profile pages (a sample of
80 cached profiles found zero "Orthodox/Southpaw" fields), so we don't
attempt to extract it.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
HTML_CACHE = REPO / "data" / "raw_html" / "tapology"
OUT_CSV = REPO / "data" / "processed" / "tapology_attrs.csv"

HEIGHT_RE = re.compile(
    r"<strong>Height:</strong>\s*(?:</div>\s*<div[^>]*>\s*)?<span[^>]*>([^<]+)</span>",
    re.I | re.S,
)
REACH_RE = re.compile(
    r"<strong>(?:\|\s*)?Reach:</strong>\s*(?:</div>\s*<div[^>]*>\s*)?<span[^>]*>([^<]+)</span>",
    re.I | re.S,
)
CM_RE = re.compile(r"\((\d{2,3})\s*cm\)")


def parse_cm(text: str) -> float | None:
    if not text or "N/A" in text:
        return None
    m = CM_RE.search(text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def main() -> int:
    rows: list[dict] = []
    fid_re = re.compile(r"^(\d+)_(.+)\.html$")
    n_total = 0
    n_height = 0
    n_reach = 0
    for p in sorted(HTML_CACHE.iterdir()):
        if not p.is_file() or p.name.startswith("search_"):
            continue
        m = fid_re.match(p.name)
        if not m:
            continue
        fid = m.group(1)
        n_total += 1
        html = p.read_text(encoding="utf-8", errors="replace")
        h = HEIGHT_RE.search(html)
        r = REACH_RE.search(html)
        height_cm = parse_cm(h.group(1)) if h else None
        reach_cm = parse_cm(r.group(1)) if r else None
        if height_cm is not None:
            n_height += 1
        if reach_cm is not None:
            n_reach += 1
        rows.append({
            "fighter_id": fid,
            "height_cm": height_cm if height_cm is not None else "",
            "reach_cm": reach_cm if reach_cm is not None else "",
        })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["fighter_id", "height_cm", "reach_cm"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Profiles scanned:   {n_total}")
    print(f"With height_cm:     {n_height}")
    print(f"With reach_cm:      {n_reach}")
    print(f"Wrote -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""One-shot harvester: parse all cached Tapology profile HTMLs into the
boxer_results_tapology.csv (no HTTP). Use this after a rate-limited scrape
to ensure every cached profile contributes rows, even if the live scraper
aborted before reaching it in target order.

Names are recovered by walking pbo_fighter_targets.csv → search cache →
profile cache, so the resulting CSV uses the same boxer_name strings as
the live scraper would have produced (which is what merge_boxer_results
de-dupes against).
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from scrape_tapology import (  # noqa: E402
    DEFAULT_TARGETS_CSV,
    FIGHTER_RE,
    HTML_CACHE,
    RESULT_FIELDS,
    RESULTS_CSV,
    parse_profile_record,
    safe_filename,
)


def main() -> int:
    targets: list[dict] = []
    with DEFAULT_TARGETS_CSV.open() as f:
        for r in csv.DictReader(f):
            if r.get("name"):
                targets.append(r)
    print(f"Targets: {len(targets):,}")

    rows: list[dict] = []
    n_search_cached = 0
    n_profile_cached = 0
    n_with_rows = 0
    for t in targets:
        name = t["name"]
        search_path = HTML_CACHE / f"search_{safe_filename(name)}.html"
        if not search_path.exists() or search_path.stat().st_size <= 1024:
            continue
        n_search_cached += 1
        body = search_path.read_text(encoding="utf-8", errors="replace")
        m = FIGHTER_RE.search(body)
        if not m:
            continue
        fid = m.group(1)
        slug = m.group(2)
        profile_path = HTML_CACHE / f"{fid}_{slug[:60]}.html"
        if not profile_path.exists() or profile_path.stat().st_size <= 1024:
            continue
        n_profile_cached += 1
        html = profile_path.read_text(encoding="utf-8", errors="replace")
        fights = parse_profile_record(html, fid, name)
        if fights:
            n_with_rows += 1
            rows.extend(fights)

    print(f"Search-cached:          {n_search_cached:,}")
    print(f"Profile-cached:         {n_profile_cached:,}")
    print(f"Profiles with rows:     {n_with_rows:,}")
    print(f"Total fight rows:       {len(rows):,}")

    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Wrote -> {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

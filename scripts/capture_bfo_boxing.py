"""Discover + fetch BestFightOdds boxing event pages.

BFO indexes both MMA and boxing under /events/. Boxing is normally EXCLUDED
in our regional MMA pipeline (via the "boxing" keyword in
``DEFAULT_EXCLUDE_KEYWORDS``); for the boxing daemon we INCLUDE it and
exclude MMA-style events instead.

Writes raw HTML snapshots under ``data/live_odds/{YYYYMMDD}/raw_html/``.

Boxing promotions covered (default queries — extend as needed):
- Top Rank
- PBC (Premier Boxing Champions)
- Matchroom
- Golden Boy
- Queensbury
- DAZN
- ESPN+ boxing cards
- World Boxing Super Series
- Misfits Boxing (off by default — controversial market)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve()
REPO = HERE.parent.parent

# Reuse the regional-MMA-predictor BFO scraping primitives. They're the same
# parsers; we just feed boxing keyword queries through and flip the include/
# exclude lists.
RESEARCH_REPO = Path(os.environ.get(
    "MMA_RESEARCH_REPO", "/Users/Ryan/gt/regional_mma_predictor"
))
if str(RESEARCH_REPO) not in sys.path:
    sys.path.insert(0, str(RESEARCH_REPO))

from regional_mma_predictor.archive import (  # noqa: E402
    ArchiveEvent,
    discover_recent_archive,
    discover_search,
    events_to_frame,
    parse_archive_links,
)
from regional_mma_predictor.bfo import fetch_event_html  # noqa: E402

BFO_BASE = "https://www.bestfightodds.com"

log = logging.getLogger("capture_bfo_boxing")

# Boxing-INCLUDE list — keywords that mark a boxing card we want.
# Deliberately strict: bare "boxing" is too noisy (caught Shooto Brazil Boxing
# and Golden Boy MMA in testing). Match on full promotion phrases / specific
# event-name patterns instead.
BOXING_INCLUDE_TOKENS = (
    "top rank",
    "matchroom",
    "queensberry",
    "queensbury",
    "dazn boxing",
    "espn boxing",
    "showtime boxing",
    "world boxing super series",
    "premier boxing",  # PBC
    "premier boxing champions",
    "golden boy promotions",  # only when explicitly labeled "promotions" — drops MMA branch
    "wbo title",
    "wba title",
    "wbc title",
    "ibf title",
    "the ring",
    "heavyweight title",
)

BOXING_INCLUDE_URL_TOKENS = (
    "top-rank-",
    "matchroom-",
    "queensberry-",
    "premier-boxing-champions-",
    "pbc-",
    "wbss-",
    "world-boxing-super-series-",
    "showtime-boxing-",
    "espn-boxing-",
    "dazn-boxing-",
)

# Per-promotion search queries — feed these to BFO's /search endpoint.
DEFAULT_QUERIES = [
    "Top Rank",
    "PBC",
    "Matchroom",
    "Golden Boy",
    "Queensberry",
    "DAZN",
    "WBSS",
    "Showtime Boxing",
    "ESPN Boxing",
    "Boxing",
]

# Exclude these from the boxing daemon (they're MMA / kickboxing / slap,
# even though some have "boxing" as a sport tag).
BOXING_EXCLUDE_URL_TOKENS = (
    "future-events-",
    "ufc-",
    "bellator-",
    "cage-warriors-",
    "lfa-",
    "cffc-",
    "ksw-",
    "rizin-",
    "oktagon-",
    "one-friday-fights-",
    "one-fight-night-",
    "pfl-",
    "power-slap-",
    "k-1-",
    "glory-",
)


def _is_boxing_event(ev) -> bool:
    name_lower = ev.event_name.lower() if ev.event_name else ""
    url_lower = ev.event_url.lower() if ev.event_url else ""
    # Hard MMA exclude (catches "Golden Boy MMA: Liddell vs Ortiz",
    # "Shooto Brazil Boxing" etc. — names with overlapping tokens).
    if any(t in url_lower for t in BOXING_EXCLUDE_URL_TOKENS):
        return False
    if "mma" in url_lower or "mma" in name_lower:
        return False
    if any(t in url_lower for t in BOXING_INCLUDE_URL_TOKENS):
        return True
    if any(t in name_lower for t in BOXING_INCLUDE_TOKENS):
        return True
    return False


def discover_homepage_boxing() -> list[ArchiveEvent]:
    """BFO homepage carries upcoming events; pick the boxing ones."""
    try:
        html = fetch_event_html(BFO_BASE + "/")
        events = parse_archive_links(html, source_query="homepage", include_excluded=True)
        return [e for e in events if _is_boxing_event(e)]
    except Exception as e:  # noqa: BLE001
        log.warning("discover_homepage_boxing failed: %s", e)
        return []


def discover(queries: list[str]) -> list:
    events = list(discover_homepage_boxing())
    # Recent archive: include even non-keyword matches, then filter.
    try:
        for ev in discover_recent_archive(include_excluded=True):
            if _is_boxing_event(ev):
                events.append(ev)
    except Exception as e:  # noqa: BLE001
        log.warning("discover_recent_archive failed: %s", e)

    for q in queries:
        try:
            for ev in discover_search(q, include_excluded=True, strict_query=False):
                if _is_boxing_event(ev):
                    events.append(ev)
        except Exception as e:  # noqa: BLE001
            log.warning("discover_search(%s) failed: %s", q, e)

    # Dedupe by URL
    seen: set[str] = set()
    unique = []
    for ev in events:
        if ev.event_url in seen:
            continue
        seen.add(ev.event_url)
        unique.append(ev)
    return unique


def fetch_all(events, raw_dir: Path, sleep_seconds: float = 1.5,
              overwrite: bool = False, historical_cache: Path | None = None) -> tuple[int, int, int]:
    import time
    raw_dir.mkdir(parents=True, exist_ok=True)
    fetched = cached = failed = 0
    for ev in events:
        slug = ev.event_url.rstrip("/").split("/")[-1]
        out_path = raw_dir / f"{slug}.html"
        if out_path.exists() and not overwrite:
            cached += 1
            continue
        if historical_cache is not None:
            cached_path = historical_cache / f"{slug}.html"
            if cached_path.exists() and not overwrite:
                cached += 1
                continue
        try:
            html = fetch_event_html(ev.event_url)
            out_path.write_text(html)
            fetched += 1
            log.info("saved %s (%d bytes)", out_path.name, len(html))
            time.sleep(sleep_seconds)
        except Exception as e:  # noqa: BLE001
            failed += 1
            log.warning("fetch failed for %s: %s", ev.event_url, e)
    return fetched, cached, failed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Discover + fetch BFO boxing event pages")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--queries", default=",".join(DEFAULT_QUERIES))
    parser.add_argument("--sleep-seconds", type=float, default=1.5)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s %(message)s",
    )

    yyyymmdd = (args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")).replace("-", "")
    day_dir = REPO / "data" / "live_odds" / yyyymmdd
    raw_dir = day_dir / "raw_html"

    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    events = discover(queries)
    if args.limit:
        events = events[: args.limit]
    log.info("discovered %d unique boxing events across %d queries",
             len(events), len(queries))

    df = events_to_frame(events)
    manifest_path = day_dir / "events_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(manifest_path, index=False)

    historical_cache = REPO / "data" / "processed" / "bfo_corpus"
    fetched, cached, failed = fetch_all(
        events, raw_dir, sleep_seconds=args.sleep_seconds,
        overwrite=args.overwrite, historical_cache=historical_cache,
    )
    print(f"events_discovered={len(events)} fetched={fetched} cached={cached} failed={failed}")
    print(f"manifest={manifest_path}")
    print(f"raw_dir={raw_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

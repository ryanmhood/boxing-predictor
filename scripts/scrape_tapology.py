#!/usr/bin/env python3
"""
Tapology bulk profile + fight-record scraper.

Tapology (tapology.com) was the strongest non-BoxRec live source out of the
seven sources surveyed by `scripts/probe_boxer_data_sources.py`:

  * No Cloudflare gate (tested via curl_cffi chrome120 impersonation).
  * Comprehensive fight history embedded directly in the profile HTML
    (Canelo's profile = 70 unique bout rows including 2025 fights).
  * Schema parses cleanly out of `<div class="result">` blocks: result
    (W/L/D), method short (DEC/KO/TKO), method long, opponent name + id,
    rounds, year + month-day, event title.

PIPELINE:
  1. Load targets from data/raw/boxer_overlap_targets.csv (sorted by
     PBO frequency by an upstream step).
  2. For each target:
       a. Search Tapology by name -> resolve to /fightcenter/fighters/<id>-<slug>
       b. Fetch profile HTML
       c. Parse all `<div class="result">` blocks (boxing-only)
  3. Cache search + profile HTML on disk so re-runs are cheap.
  4. Emit data/raw/boxer_results_tapology.csv with one row per fight,
     schema matching data/raw/boxer_results.csv:
       fight_date, boxer_id, boxer_name, opp_id, opp_name,
       result, method, round, weight_class, location

USAGE:
    python3 scripts/scrape_tapology.py --limit 5    # smoke test
    python3 scripts/scrape_tapology.py --resume     # skip cached IDs
    python3 scripts/scrape_tapology.py              # full run

POLITENESS:
  1.5s sleep between requests by default, real chrome120 UA, hard cap of
  1200 HTTP requests per invocation.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import unicodedata
import urllib.parse
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
CACHE_DIR = ROOT / "data" / "cache"
HTML_CACHE = ROOT / "data" / "raw_html" / "tapology"
HTML_CACHE.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TARGETS_CSV = RAW_DIR / "pbo_fighter_targets.csv"
RESULTS_CSV = RAW_DIR / "boxer_results_tapology.csv"

TAPOLOGY_BASE = "https://www.tapology.com"
SEARCH_URL = (
    f"{TAPOLOGY_BASE}/search"
    f"?term={{}}&mainSearchFilter=fighters"
)
FIGHTER_RE = re.compile(r"/fightcenter/fighters/(\d+)-([a-z0-9\-]+)", re.IGNORECASE)
DEFAULT_DELAY_S = 1.5
MAX_REQUESTS = 1200
TIMEOUT_S = 30

CHALLENGE_MARKERS = (
    "Just a moment",
    "cf-browser-verification",
    "cf_chl_opt",
    "challenge-platform",
    "Enable JavaScript and cookies to continue",
)


def is_challenge(body: str) -> bool:
    return bool(body) and any(m in body for m in CHALLENGE_MARKERS)


def norm_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def safe_filename(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", norm_name(s))[:80]


# --------------------------------------------------------------------------
# Polite curl_cffi session
# --------------------------------------------------------------------------
class TapologySession:
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    def __init__(self, delay_s: float = DEFAULT_DELAY_S, cap: int = MAX_REQUESTS):
        from curl_cffi import requests as cffi_requests
        self._session = cffi_requests.Session(impersonate="chrome120")
        self._delay = delay_s
        self._last_t = 0.0
        self.request_count = 0
        self.cap = cap

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_t
        if elapsed < self._delay:
            time.sleep(self._delay - elapsed)

    def get(self, url: str) -> tuple[int, str]:
        if self.request_count >= self.cap:
            raise RuntimeError(f"request cap ({self.cap}) reached")
        self._wait()
        self.request_count += 1
        try:
            resp = self._session.get(
                url,
                headers=self.HEADERS,
                timeout=TIMEOUT_S,
                allow_redirects=True,
            )
        except Exception as e:                       # noqa: BLE001
            self._last_t = time.monotonic()
            print(f"  [http-err] {url} -> {e!r}", file=sys.stderr)
            return -1, ""
        self._last_t = time.monotonic()
        return resp.status_code, resp.text or ""


# --------------------------------------------------------------------------
# Search + profile fetch (with cache)
# --------------------------------------------------------------------------
def _read_cache(path: Path) -> str:
    if path.exists() and path.stat().st_size > 1024:
        return path.read_text(encoding="utf-8", errors="replace")
    return ""


def _write_cache(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def search_fighter(
    sess: TapologySession, name: str
) -> tuple[str | None, str | None, str]:
    """Return (fighter_id, slug, error). On success, error == ''.

    A `search status=503` is the rate-limit signal -- callers should
    abort the loop on the first one rather than burn through the rest
    of the target list with the limiter still active.
    """
    cache_key = HTML_CACHE / f"search_{safe_filename(name)}.html"
    body = _read_cache(cache_key)
    if not body:
        url = SEARCH_URL.format(urllib.parse.quote_plus(name))
        status, body = sess.get(url)
        if status == 503:
            return None, None, "search status=503 (rate-limited)"
        if status != 200:
            return None, None, f"search status={status}"
        if is_challenge(body):
            return None, None, "search hit cloudflare challenge"
        _write_cache(cache_key, body)

    # Pick the first /fightcenter/fighters/<id>-<slug> hit, but skip any
    # link that lives inside a "Recent" / "Trending" sidebar block by
    # only honouring the FIRST search-result hit.
    m = FIGHTER_RE.search(body)
    if not m:
        return None, None, "no fighter link in search results"
    return m.group(1), m.group(2), ""


def fetch_profile(
    sess: TapologySession, fighter_id: str, slug: str
) -> tuple[str, str]:
    """Return (html, error)."""
    cache_path = HTML_CACHE / f"{fighter_id}_{slug[:60]}.html"
    body = _read_cache(cache_path)
    if body:
        return body, ""
    url = f"{TAPOLOGY_BASE}/fightcenter/fighters/{fighter_id}-{slug}"
    status, body = sess.get(url)
    if status != 200:
        return "", f"profile status={status}"
    if is_challenge(body):
        return "", "profile hit cloudflare challenge"
    _write_cache(cache_path, body)
    return body, ""


# --------------------------------------------------------------------------
# Parser for Tapology profile + fight record
# --------------------------------------------------------------------------
METHOD_TOKENS = {
    "ko":      "ko",
    "tko":     "tko",
    "ud":      "decision_unanimous",
    "md":      "decision_majority",
    "sd":      "decision_split",
    "dec":     "decision",
    "rtd":     "tko_corner",
    "tdraw":   "draw_technical",
    "draw":    "draw",
    "nc":      "no_contest",
    "dq":      "dq",
    "tdec":    "decision_technical",
    "pts":     "decision",
}
METHOD_LONG_TOKENS = {
    "decision · unanimous":  "decision_unanimous",
    "decision · majority":   "decision_majority",
    "decision · split":      "decision_split",
    "decision · technical":  "decision_technical",
    "ko":                    "ko",
    "tko":                   "tko",
    "submission":            "submission",  # MMA -- skipped via sport filter
    "draw · split":          "draw_split",
    "draw · majority":       "draw_majority",
    "draw":                  "draw",
    "disqualification":      "dq",
    "no contest":            "no_contest",
    "rtd":                   "tko_corner",
}

MONTHS = {m: i + 1 for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
)}


def _norm_method(short: str, long: str) -> str:
    longn = (long or "").strip().lower()
    for prefix, code in METHOD_LONG_TOKENS.items():
        if longn.startswith(prefix):
            return code
    short_clean = re.split(r"[^a-z]", (short or "").lower())[0]
    return METHOD_TOKENS.get(short_clean, short_clean or "")


def _parse_date(year: str, monday: str) -> str:
    """Year='2025', monday='Sep 13' -> '2025-09-13' (ISO)."""
    if not year or not monday:
        return ""
    parts = monday.replace(",", " ").split()
    if len(parts) < 2:
        return year  # year-only fallback
    mon_raw = parts[0][:3].lower()
    day_raw = parts[1]
    mon = MONTHS.get(mon_raw)
    try:
        day = int(re.sub(r"[^0-9]", "", day_raw))
    except ValueError:
        return year
    if not mon or not (1 <= day <= 31):
        return year
    try:
        return datetime(int(year), mon, day).strftime("%Y-%m-%d")
    except ValueError:
        return year


def _parse_country_flag(img_src: str) -> str:
    """`/assets/flags/US-...` -> 'US'."""
    if not img_src:
        return ""
    m = re.search(r"/flags/([A-Z]{2})-", img_src)
    return m.group(1) if m else ""


def parse_profile_record(
    html: str, fighter_id: str, fighter_name: str
) -> list[dict]:
    """Parse all boxing fight rows from a Tapology profile page.

    Each fight is a `<div class="result ...">` element containing:
      - W/L/D letter (first inner div)
      - method short -- DEC/KO/TKO (second inner div)
      - opponent link  /fightcenter/fighters/<id>-<slug>
      - bout link      /fightcenter/bouts/<id>-<slug> (method long text)
      - event link     /fightcenter/events/<id>-<slug> (year + month-day)
      - rounds text    "X Rounds"
      - sport tag      "Boxing" / "MMA" / "Kickboxing" -- we keep boxing only
    """
    from bs4 import BeautifulSoup
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")

    fights: list[dict] = []
    rows = soup.select("div.result")
    for row in rows:
        # ---------- Sport (filter to boxing only) ----------
        sport_span = row.find("span", class_=re.compile(r"text-tap_gold"))
        sport = sport_span.get_text(strip=True) if sport_span else ""
        if sport and sport.lower() not in ("boxing", "professional boxing"):
            continue  # drop MMA / kickboxing rows

        # ---------- Result letter ----------
        result = ""
        first_div = row.find("div", recursive=False)
        if first_div:
            txt = first_div.get_text(strip=True).upper()
            if txt in {"W", "L", "D", "NC"}:
                result = txt

        # ---------- Method short (DEC / KO / TKO ...) ----------
        method_short = ""
        # The second top-level child has the rotated method block.
        kids = [c for c in row.children if getattr(c, "name", None) == "div"]
        if len(kids) >= 2:
            txt = kids[1].get_text(" ", strip=True)
            tok = re.sub(r"[^A-Za-z]", "", txt).strip()
            if tok and len(tok) <= 6:
                method_short = tok

        # ---------- Opponent link ----------
        opp_a = row.find("a", href=FIGHTER_RE)
        if not opp_a:
            continue
        opp_href = opp_a["href"]
        m = FIGHTER_RE.search(opp_href)
        if not m:
            continue
        opp_id = m.group(1)
        opp_name = " ".join(opp_a.get_text(" ", strip=True).split())

        # ---------- Method long via bout link ----------
        bout_a = row.find("a", href=re.compile(r"/fightcenter/bouts/"))
        method_long = bout_a.get_text(" ", strip=True) if bout_a else ""

        # ---------- Rounds text ----------
        rounds = ""
        for div in row.find_all("div"):
            txt = div.get_text(" ", strip=True)
            rm = re.match(r"^(\d{1,2})\s*Rounds?$", txt)
            if rm:
                rounds = rm.group(1)
                break

        # ---------- Date (year + Mon DD) ----------
        # The row carries multiple `/fightcenter/events/` links: an event-
        # title link (text = "Canelo vs. Crawford"), and a date link
        # whose body is two <span>s (year + "Mon DD"). Walk all event
        # links and pick whichever one carries the spans.
        year, monday = "", ""
        for event_a in row.find_all("a", href=re.compile(r"/fightcenter/events/")):
            spans_text = [s.get_text(strip=True) for s in event_a.find_all("span")]
            for t in spans_text:
                if re.fullmatch(r"\d{4}", t):
                    year = t
                elif re.match(r"^[A-Za-z]{3,4}\s+\d{1,2}$", t):
                    monday = t
            if year and monday:
                break
        fight_date = _parse_date(year, monday) if year else ""

        # ---------- Country flag (proxy for location) ----------
        country = ""
        flag_img = row.find("img", src=re.compile(r"/flags/[A-Z]{2}-"))
        if flag_img:
            country = _parse_country_flag(flag_img.get("src", ""))

        method_norm = _norm_method(method_short, method_long)

        fights.append({
            "fight_date":   fight_date,
            "boxer_id":     fighter_id,
            "boxer_name":   fighter_name,
            "opp_id":       opp_id,
            "opp_name":     opp_name,
            "result":       result,
            "method":       method_norm,
            "round":        rounds,
            "weight_class": "",       # not surfaced in row HTML; need bout page
            "location":     country,  # country code; full venue needs bout page
        })

    return fights


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------
RESULT_FIELDS = [
    "fight_date", "boxer_id", "boxer_name", "opp_id", "opp_name",
    "result", "method", "round", "weight_class", "location",
]


def already_cached_keys() -> set[str]:
    cached: set[str] = set()
    for p in HTML_CACHE.glob("search_*.html"):
        if p.stat().st_size > 1024:
            cached.add(p.stem.replace("search_", ""))
    return cached


def load_targets(targets_path: Path, limit: int | None) -> list[dict]:
    if not targets_path.exists():
        raise SystemExit(
            f"ERROR: {targets_path} missing. Generate it from "
            "data/processed/pbo_results.csv first."
        )
    rows: list[dict] = []
    with targets_path.open() as f:
        for r in csv.DictReader(f):
            if not r.get("name"):
                continue
            rows.append(r)
    if limit:
        rows = rows[:limit]
    return rows


def write_results(rows: list[dict]) -> None:
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  wrote {len(rows)} fight rows -> {RESULTS_CSV}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument("--limit", type=int, default=None,
                    help="cap on number of target boxers to process")
    ap.add_argument("--resume", action="store_true",
                    help="skip targets whose search-cache already exists")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY_S,
                    help=f"min seconds between requests (default {DEFAULT_DELAY_S})")
    ap.add_argument("--single", type=str, default=None,
                    help="scrape just this one fighter name (for smoke tests)")
    ap.add_argument("--targets", type=Path, default=DEFAULT_TARGETS_CSV,
                    help="path to targets CSV (cols: name,pbo_appearances)")
    args = ap.parse_args(argv)

    sess = TapologySession(delay_s=max(args.delay, 1.0))

    if args.single:
        targets = [{"name": args.single, "boxer_id": args.single, "norm": norm_name(args.single)}]
    else:
        targets = load_targets(args.targets, args.limit)
    print(f"[tap] targets: {len(targets)} (--limit={args.limit} --resume={args.resume})")

    cached_keys = already_cached_keys() if args.resume else set()
    all_fights: list[dict] = []
    n_resolved = 0
    n_failed_search = 0
    n_failed_profile = 0
    started = time.time()

    for i, t in enumerate(targets, 1):
        name = t["name"]
        fid, slug, err = search_fighter(sess, name)
        if not fid:
            n_failed_search += 1
            print(f"[tap] {i}/{len(targets)} {name!r} search FAILED ({err})")
            if "503" in err:
                print(
                    "[tap] >> abort: Tapology returned 503 (rate-limited). "
                    "Wait 10-15 min, then re-run with --resume --delay 3.0."
                )
                break
            continue
        html, err = fetch_profile(sess, fid, slug or "")
        if not html:
            n_failed_profile += 1
            print(f"[tap] {i}/{len(targets)} {name!r} (id={fid}) profile FAILED ({err})")
            continue
        n_resolved += 1
        fights = parse_profile_record(html, fid, name)
        all_fights.extend(fights)
        if i % 10 == 0 or i == len(targets):
            elapsed = time.time() - started
            print(
                f"[tap] {i}/{len(targets)} resolved={n_resolved} "
                f"fights={len(all_fights)} req={sess.request_count} "
                f"t={elapsed:.0f}s"
            )

    write_results(all_fights)

    print()
    print("=" * 64)
    print("Tapology bulk scrape -- summary")
    print("=" * 64)
    print(f"targets attempted        : {len(targets)}")
    print(f"resolved profiles        : {n_resolved}")
    print(f"failed searches          : {n_failed_search}")
    print(f"failed profile fetches   : {n_failed_profile}")
    print(f"total fight rows written : {len(all_fights)}")
    print(f"http requests issued     : {sess.request_count}")
    print(f"output                   : {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

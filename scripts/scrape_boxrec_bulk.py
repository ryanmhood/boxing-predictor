#!/usr/bin/env python3
"""
BoxRec bulk profile + fight-record scraper using harvested CF cookies.

PIPELINE:
  1. Load cf_clearance + UA from data/cache/boxrec_cookies.json
     (produced by scripts/scrape_boxrec_playwright.py harvest)
  2. Load targets from data/raw/boxer_overlap_targets.csv (sorted by
     PBO frequency by an upstream step)
  3. For each target:
       a. Search BoxRec by name -> resolve to /en/proboxer/<id>
       b. Fetch profile HTML
       c. Parse the "Professional career record" table
  4. Cache search + profile HTML on disk so re-runs are cheap.
  5. Emit data/raw/boxer_results_boxrec.csv with one row per fight.

USAGE:
    python3 scripts/scrape_boxrec_bulk.py --limit 5     # smoke test
    python3 scripts/scrape_boxrec_bulk.py --resume      # skip cached IDs
    python3 scripts/scrape_boxrec_bulk.py               # full run

CLOUDFLARE RECOVERY:
  curl_cffi requests share the harvested cookie jar. If a request comes
  back with a CF challenge body (cookies expired mid-scrape), the script
  prints a clear message and stops -- the user re-runs the Playwright
  harvester to refresh cookies, then resumes with --resume.

POLITENESS:
  ~1.5s sleep between requests by default. Hard cap of 1500 HTTP requests
  per invocation as a safety rail.
"""
from __future__ import annotations

import argparse
import csv
import json
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
HTML_CACHE = ROOT / "data" / "raw_html" / "boxrec"
HTML_CACHE.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

COOKIES_PATH = CACHE_DIR / "boxrec_cookies.json"
TARGETS_CSV = RAW_DIR / "boxer_overlap_targets.csv"
RESULTS_CSV = RAW_DIR / "boxer_results_boxrec.csv"

BOXREC_BASE = "https://boxrec.com"
BOXREC_SEARCH_URL = (
    f"{BOXREC_BASE}/en/search"
    f"?p%5Brole%5D=proboxer&p%5Bcommon%5D="  # ?p[common]=<name>
)
PROBOXER_RE = re.compile(r"/en/proboxer/(\d+)")
CHALLENGE_MARKERS = (
    "Just a moment",
    "cf-browser-verification",
    "cf_chl_opt",
    "challenge-platform",
    "Enable JavaScript and cookies to continue",
)
DEFAULT_DELAY_S = 1.5
MAX_REQUESTS = 1500
TIMEOUT_S = 30


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def is_challenge(body: str) -> bool:
    return bool(body) and any(m in body for m in CHALLENGE_MARKERS)


def looks_real(body: str) -> bool:
    if not body or len(body) < 5000:
        return False
    needles = ("BoxRec", "proboxer", "Pro Boxer", "boxer")
    return any(n in body for n in needles) and not is_challenge(body)


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
    return re.sub(r"[^a-z0-9_]+", "_", norm_name(s))[:60]


def load_cookies() -> tuple[dict, str]:
    if not COOKIES_PATH.exists():
        raise SystemExit(
            f"ERROR: {COOKIES_PATH} not found. Run "
            f"`python3 scripts/scrape_boxrec_playwright.py harvest` first."
        )
    payload = json.loads(COOKIES_PATH.read_text())
    cookie_jar = {
        c["name"]: c["value"]
        for c in payload.get("cookies", [])
        if c.get("name") and c.get("value")
    }
    ua = payload.get("user_agent") or (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    if not cookie_jar.get("cf_clearance"):
        print(
            "[load_cookies] WARNING: no cf_clearance cookie in saved jar -- "
            "requests will likely 403."
        )
    return cookie_jar, ua


# --------------------------------------------------------------------------
# Polite curl_cffi session
# --------------------------------------------------------------------------
class BoxRecSession:
    def __init__(self, cookie_jar: dict, ua: str, delay_s: float = DEFAULT_DELAY_S):
        from curl_cffi import requests as cffi_requests
        self._session = cffi_requests.Session(impersonate="chrome120")
        self._cookies = cookie_jar
        self._ua = ua
        self._delay = delay_s
        self._last_t = 0.0
        self.request_count = 0
        self.cap = MAX_REQUESTS
        self._headers = {
            "User-Agent": ua,
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
                headers=self._headers,
                cookies=self._cookies,
                timeout=TIMEOUT_S,
                allow_redirects=True,
            )
        except Exception as e:
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


def search_boxer_id(
    sess: BoxRecSession, name: str
) -> tuple[str | None, str]:
    """Return (boxer_id, error). On success, error == ''."""
    cache_key = f"search_{safe_filename(name)}.html"
    cache_path = HTML_CACHE / cache_key
    body = _read_cache(cache_path)
    if not body:
        url = BOXREC_SEARCH_URL + urllib.parse.quote_plus(name)
        status, body = sess.get(url)
        if status != 200:
            return None, f"search status={status}"
        if is_challenge(body):
            return None, "search hit cloudflare challenge"
        _write_cache(cache_path, body)
    # First /en/proboxer/<id> link in the body is the top result.
    m = PROBOXER_RE.search(body)
    if not m:
        return None, "no proboxer link in search results"
    return m.group(1), ""


def fetch_profile(
    sess: BoxRecSession, boxer_id: str
) -> tuple[str, str]:
    """Return (html, error)."""
    cache_path = HTML_CACHE / f"{boxer_id}.html"
    body = _read_cache(cache_path)
    if body:
        return body, ""
    url = f"{BOXREC_BASE}/en/proboxer/{boxer_id}"
    status, body = sess.get(url)
    if status != 200:
        return "", f"profile status={status}"
    if is_challenge(body):
        return "", "profile hit cloudflare challenge"
    _write_cache(cache_path, body)
    return body, ""


# --------------------------------------------------------------------------
# Parser for BoxRec profile + fight record
# --------------------------------------------------------------------------
# BoxRec's record table is a wide HTML <table> where each fight is a row
# with cells along the lines of:
#   date | opponent (a -> /en/proboxer/<id>) | weight class | result_letter
#   | result_method | rounds | location | ...
# Class names have shifted over time (`responsiveTable`, `dataTable`,
# `tableContent`); we treat any wide table whose first column parses as a
# date as a candidate record table and pick the one with the most
# proboxer links.
DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
RESULT_TOKENS = {"W": "W", "L": "L", "D": "D", "NC": "NC", "ND": "NC"}
METHOD_TOKENS = {
    "ko": "ko",
    "tko": "tko",
    "ud": "decision_unanimous",
    "md": "decision_majority",
    "sd": "decision_split",
    "pts": "decision",
    "rtd": "tko_corner",
    "dq": "dq",
    "tdec": "decision_technical",
    "tdraw": "draw_technical",
    "draw": "draw",
    "nc": "no_contest",
}


def _norm_method(text: str) -> str:
    t = (text or "").lower()
    base = re.split(r"[^a-z]", t)[0] if t else ""
    return METHOD_TOKENS.get(base, base or "")


def _parse_row_cells(row) -> list[str]:
    return [
        " ".join(c.get_text(" ", strip=True).split())
        for c in row.find_all(["td", "th"])
    ]


def parse_profile_record(html: str, boxer_id: str, boxer_name: str) -> list[dict]:
    """Parse the fight record from a BoxRec profile page.

    The function is permissive: it iterates every <table>, looks for rows
    whose first cell looks like an ISO date, and treats those rows as fights.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml") if "lxml" in sys.modules or _has_lxml() else BeautifulSoup(html, "html.parser")
    fights: list[dict] = []
    best_table = None
    best_score = 0
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        score = 0
        for r in rows:
            cells = r.find_all(["td", "th"])
            if not cells:
                continue
            first = cells[0].get_text(" ", strip=True)
            if DATE_RE.search(first) and r.find("a", href=PROBOXER_RE):
                score += 1
        if score > best_score:
            best_score = score
            best_table = table
    if not best_table:
        return fights

    for tr in best_table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        first = cells[0].get_text(" ", strip=True)
        m = DATE_RE.search(first)
        if not m:
            continue
        fight_date = m.group(1)
        text_cells = _parse_row_cells(tr)

        # Resolve opponent via first /en/proboxer/<id> link in the row.
        opp_link = tr.find("a", href=PROBOXER_RE)
        if not opp_link:
            continue
        opp_href = opp_link.get("href", "")
        opp_id_match = PROBOXER_RE.search(opp_href)
        opp_id = opp_id_match.group(1) if opp_id_match else ""
        opp_name = " ".join(opp_link.get_text(" ", strip=True).split())

        # Result + method are usually in cells immediately after the
        # opponent. We scan all cells for a single-letter W/L/D/NC token
        # and a method token; this is robust to column-ordering drift.
        result = ""
        method = ""
        rnd = ""
        weight_class = ""
        location = ""
        for cell_txt in text_cells:
            up = cell_txt.upper().strip()
            if not result and up in RESULT_TOKENS:
                result = RESULT_TOKENS[up]
                continue
            if not method:
                low = cell_txt.lower().strip()
                tok = re.split(r"[^a-z]", low)[0] if low else ""
                if tok in METHOD_TOKENS:
                    method = METHOD_TOKENS[tok]
                    continue
            if not rnd:
                # standalone integer 1..15 = rounds
                rm = re.match(r"^(\d{1,2})$", cell_txt.strip())
                if rm and 1 <= int(rm.group(1)) <= 15:
                    rnd = rm.group(1)
                    continue
            if not weight_class:
                low = cell_txt.lower()
                if any(
                    w in low
                    for w in (
                        "weight", "heavy", "cruise", "light", "feather",
                        "bantam", "fly", "straw", "minimum",
                    )
                ):
                    weight_class = cell_txt
                    continue
            if not location:
                # location often contains a comma + country/state
                if "," in cell_txt and len(cell_txt) <= 80:
                    location = cell_txt
                    continue

        fights.append({
            "fight_date": fight_date,
            "boxer_id": boxer_id,
            "boxer_name": boxer_name,
            "opp_id": opp_id,
            "opp_name": opp_name,
            "result": result,
            "method": method,
            "round": rnd,
            "weight_class": weight_class,
            "location": location,
        })
    return fights


def _has_lxml() -> bool:
    try:
        import lxml  # noqa: F401
        return True
    except ImportError:
        return False


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------
RESULT_FIELDS = [
    "fight_date", "boxer_id", "boxer_name", "opp_id", "opp_name",
    "result", "method", "round", "weight_class", "location",
]


def already_cached_targets() -> set[str]:
    """Set of boxer-name keys whose search-cache hit AND profile-cache hit
    are present (so a --resume run can skip them).
    """
    cached: set[str] = set()
    for p in HTML_CACHE.glob("search_*.html"):
        if p.stat().st_size > 1024:
            cached.add(p.stem.replace("search_", ""))
    return cached


def load_targets(limit: int | None) -> list[dict]:
    if not TARGETS_CSV.exists():
        raise SystemExit(
            f"ERROR: {TARGETS_CSV} missing. Run scrape_pbo_overlap.py first "
            "to generate the target list."
        )
    rows: list[dict] = []
    with TARGETS_CSV.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
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
    args = ap.parse_args(argv)

    cookie_jar, ua = load_cookies()
    sess = BoxRecSession(cookie_jar, ua, delay_s=max(args.delay, 1.0))
    targets = load_targets(args.limit)
    print(f"[bulk] targets: {len(targets)} (--limit={args.limit} --resume={args.resume})")

    cached_keys = already_cached_targets() if args.resume else set()
    all_fights: list[dict] = []
    n_resolved = 0
    n_failed_search = 0
    n_failed_profile = 0
    started = time.time()
    EARLY_EXIT_RECOVERY_HINT = (
        "  >> Cookies appear stale or blocked. Re-run "
        "`python3 scripts/scrape_boxrec_playwright.py harvest` to refresh, "
        "then re-run this script with --resume."
    )

    for i, t in enumerate(targets, 1):
        name = t["name"]
        key = safe_filename(name)
        if args.resume and key in cached_keys:
            # Try to parse the cached profile if present
            pass
        boxer_id, err = search_boxer_id(sess, name)
        if not boxer_id:
            n_failed_search += 1
            if "cloudflare" in err:
                print(f"[bulk] {i}/{len(targets)} {name!r} CF block on search; aborting")
                print(EARLY_EXIT_RECOVERY_HINT)
                break
            print(f"[bulk] {i}/{len(targets)} {name!r} search FAILED ({err})")
            continue
        html, err = fetch_profile(sess, boxer_id)
        if not html:
            n_failed_profile += 1
            if "cloudflare" in err:
                print(f"[bulk] {i}/{len(targets)} {name!r} (id={boxer_id}) CF block on profile; aborting")
                print(EARLY_EXIT_RECOVERY_HINT)
                break
            print(f"[bulk] {i}/{len(targets)} {name!r} profile FAILED ({err})")
            continue
        n_resolved += 1
        fights = parse_profile_record(html, boxer_id, name)
        all_fights.extend(fights)
        if i % 10 == 0:
            elapsed = time.time() - started
            print(
                f"[bulk] {i}/{len(targets)} resolved={n_resolved} "
                f"fights={len(all_fights)} req={sess.request_count} "
                f"t={elapsed:.0f}s"
            )

    write_results(all_fights)

    print()
    print("=" * 64)
    print("BoxRec bulk scrape -- summary")
    print("=" * 64)
    print(f"targets attempted        : {min(len(targets), i if targets else 0)}")
    print(f"resolved profiles        : {n_resolved}")
    print(f"failed searches          : {n_failed_search}")
    print(f"failed profile fetches   : {n_failed_profile}")
    print(f"total fight rows written : {len(all_fights)}")
    print(f"http requests issued     : {sess.request_count}")
    print(f"output                   : {RESULTS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

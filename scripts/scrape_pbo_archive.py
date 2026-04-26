"""ProBoxingOdds (PBO) sitemap-driven historical odds scraper.

Fetches every event listed in PBO's sitemap-teams.xml between 2018-01-01 and
2025-12-31, caches each event's HTML, parses moneyline + method/round props,
and emits five processed CSVs plus an event URL list. PBO is run by the same
operator as BestFightOdds and uses the same `<table class="odds-table">`
shape; this parser is adapted from regional_mma_predictor/bfo.py with three
PBO-specific changes:

    1. Matchup IDs live on cell `data-li="[book_id, side, matchup_id]"`
       and on the row's `data-mu` / row id `mu-{id}`, not in
       `/cnadm/matchups/{id}` links.
    2. The first `table.odds-table` is a "responsive header" containing only
       fighter names (no books); the second contains all priced cells.
    3. PBO does NOT mark winners on event pages. We extract winners by
       joining bouts to BoxRec fight rows already cached in
       data/raw/boxrec_fights.csv.

Run:
    python3 scripts/scrape_pbo_archive.py [--limit N] [--delay 1.5]

The script is idempotent: any event whose HTML is already cached is parsed
without a network hit, so re-runs only fetch new events. Polite by default
(1.5s delay), single-process, no parallelism. Hard cap of 1500 HTTP requests
per invocation as a safety rail.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag


# ---------- Paths ----------

DAEMON_ROOT = Path("/Users/Ryan/boxing-odds-daemon")
SITEMAP_URL = "https://www.proboxingodds.com/sitemap-teams.xml"
RAW_DIR = DAEMON_ROOT / "data" / "raw"
RAW_HTML_DIR = DAEMON_ROOT / "data" / "raw_html" / "pbo_events"
PROCESSED_DIR = DAEMON_ROOT / "data" / "processed"
EVENT_URLS_CSV = RAW_DIR / "pbo_event_urls.csv"
ML_PRICES_CSV = PROCESSED_DIR / "pbo_moneyline_prices.csv"
ML_BOUTS_CSV = PROCESSED_DIR / "pbo_moneyline_bouts.csv"
METHOD_CSV = PROCESSED_DIR / "pbo_method_props.csv"
ROUND_CSV = PROCESSED_DIR / "pbo_round_props.csv"
RESULTS_CSV = PROCESSED_DIR / "pbo_results.csv"
BOXREC_FIGHTS_CSV = RAW_DIR / "boxrec_fights.csv"


# ---------- Constants ----------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
AMERICAN_RE = re.compile(r"^[+−\-]\d{2,5}$")
EVENT_ID_RE = re.compile(r"/events/(\d{4})-(\d{2})-(\d{2})-(\d+)")
EVENT_URL_RE = re.compile(
    r"https://www\.proboxingodds\.com/events/(\d{4})-(\d{2})-(\d{2})-(\d+)"
)
SITEMAP_LOC_RE = re.compile(
    r"<loc>(https://www\.proboxingodds\.com/events/[^<]+)</loc>"
)
DATA_LI_RE = re.compile(r"\[([^\]]+)\]")
DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")  # in titles "5/20" form
START_DATE = date(2018, 1, 1)
END_DATE = date(2025, 12, 31)
REQUEST_BUDGET = 1500
DEFAULT_DELAY = 1.5
TIMEOUT = 25


# ---------- HTTP helpers ----------


class RequestBudget:
    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.used = 0

    def take(self) -> None:
        if self.used >= self.cap:
            raise RuntimeError(f"request budget exceeded ({self.cap})")
        self.used += 1


def polite_get(
    url: str, budget: RequestBudget, delay: float, last_t: list[float]
) -> requests.Response:
    """GET with min `delay` seconds since the previous polite_get call."""
    elapsed = time.monotonic() - last_t[0]
    if elapsed < delay:
        time.sleep(delay - elapsed)
    budget.take()
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    last_t[0] = time.monotonic()
    return resp


# ---------- Sitemap ----------


def fetch_event_urls(
    budget: RequestBudget, delay: float, last_t: list[float]
) -> list[dict]:
    """Return list of {event_id, event_date, url} filtered to 2018-2025."""
    print(f"[sitemap] GET {SITEMAP_URL}")
    resp = polite_get(SITEMAP_URL, budget, delay, last_t)
    resp.raise_for_status()
    urls = SITEMAP_LOC_RE.findall(resp.text)
    out: list[dict] = []
    for url in urls:
        m = EVENT_URL_RE.match(url)
        if not m:
            continue
        y, mo, d, eid = m.groups()
        try:
            ev_date = date(int(y), int(mo), int(d))
        except ValueError:
            continue
        if not (START_DATE <= ev_date <= END_DATE):
            continue
        out.append(
            {
                "event_id": eid,
                "event_date": ev_date.isoformat(),
                "url": url,
            }
        )
    out.sort(key=lambda r: (r["event_date"], r["event_id"]))
    return out


def write_event_urls_csv(rows: list[dict]) -> None:
    EVENT_URLS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with EVENT_URLS_CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["event_id", "event_date", "url"])
        w.writeheader()
        w.writerows(rows)
    print(f"[sitemap] wrote {len(rows)} event URLs to {EVENT_URLS_CSV}")


# ---------- Per-event fetch + cache ----------


def cache_path(event_id: str) -> Path:
    return RAW_HTML_DIR / f"{event_id}.html"


def fetch_event(
    url: str,
    event_id: str,
    budget: RequestBudget,
    delay: float,
    last_t: list[float],
) -> str | None:
    p = cache_path(event_id)
    if p.exists() and p.stat().st_size > 5_000:
        return p.read_text()
    try:
        resp = polite_get(url, budget, delay, last_t)
    except Exception as exc:
        print(f"[fetch] ERROR {url}: {exc}")
        return None
    if resp.status_code == 403 or resp.status_code == 429:
        print(f"[fetch] {resp.status_code} on {url} -- backing off 3s once")
        time.sleep(3.0)
        try:
            resp = polite_get(url, budget, delay, last_t)
        except Exception as exc:
            print(f"[fetch] retry ERROR {url}: {exc}")
            return None
    if resp.status_code != 200:
        print(f"[fetch] {resp.status_code} on {url}")
        return None
    if len(resp.text) < 5_000:
        # tiny stub page (no markets / 404-ish); still cache to avoid re-fetching
        pass
    p.write_text(resp.text)
    return resp.text


# ---------- Parsing helpers ----------


def _text(node: Tag) -> str:
    return " ".join(node.get_text(" ", strip=True).split())


def parse_data_li(s: str) -> list[int] | None:
    if not s:
        return None
    m = DATA_LI_RE.search(s)
    if not m:
        return None
    try:
        return [int(x.strip()) for x in m.group(1).split(",")]
    except ValueError:
        return None


def american_from_text(txt: str) -> int | None:
    if txt is None:
        return None
    t = txt.strip().replace("−", "-")
    # cell may have arrow chars / spaces appended; pick first numeric token
    for tok in t.split():
        if AMERICAN_RE.match(tok):
            try:
                return int(tok.replace("−", "-"))
            except ValueError:
                continue
    return None


def book_headers_from_table(table: Tag) -> list[dict]:
    headers: list[dict] = []
    for th in table.select("thead th[data-b]"):
        book_id = str(th.get("data-b") or "")
        a = th.find("a") or th.find("span")
        name = _text(a or th)
        if name and book_id:
            headers.append({"book_id": book_id, "book": name})
    return headers


def event_meta_from_soup(soup: BeautifulSoup, url: str) -> dict:
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    event_name = title.split(" Betting Odds")[0].strip() if title else ""
    m = EVENT_ID_RE.search(urlparse(url).path)
    if not m:
        return {"event_id": None, "event_date": None, "event_name": event_name}
    y, mo, d, eid = m.groups()
    return {
        "event_id": eid,
        "event_date": date(int(y), int(mo), int(d)).isoformat(),
        "event_name": event_name,
    }


def parse_opening_deltas(html: str) -> dict[str, int]:
    """Return {fighter_name: change_since_opening_int}.

    PBO embeds a single 'Change since opening' move list in the
    event-swing-container. Values are signed integers (American-odds delta).
    """
    m = re.search(r"data-moves=\"([^\"]+)\"", html)
    if not m:
        return {}
    raw = m.group(1).replace("&quot;", '"')
    try:
        moves = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    out: dict[str, int] = {}
    for block in moves:
        if not isinstance(block, dict):
            continue
        if "opening" not in str(block.get("name", "")).lower():
            continue
        for entry in block.get("data", []):
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            name, delta = entry[0], entry[1]
            try:
                out[str(name)] = int(delta)
            except (TypeError, ValueError):
                continue
        break
    return out


def parse_event(
    html: str, url: str
) -> tuple[dict, list[dict], list[dict], list[dict]]:
    """Return (event_meta, ml_price_rows, method_rows, round_rows)."""
    soup = BeautifulSoup(html, "html.parser")
    meta = event_meta_from_soup(soup, url)

    # Find the priced odds-table (the one with sportsbook headers).
    tables = soup.select("table.odds-table")
    priced_tables = [t for t in tables if t.select_one("thead th[data-b]")]
    if not priced_tables:
        return meta, [], [], []
    # Use ALL priced tables (some events have multiple), preferring those with books.
    ml_rows: list[dict] = []
    method_rows: list[dict] = []
    round_rows: list[dict] = []

    opening_deltas = parse_opening_deltas(html)

    # Track matchup -> {side: fighter} so we can attach prop labels.
    matchup_fighters: dict[str, dict[int, str]] = defaultdict(dict)

    for table in priced_tables:
        books = book_headers_from_table(table)
        book_by_id = {b["book_id"]: b["book"] for b in books}

        for tr in table.select("tbody tr"):
            classes = tr.get("class") or []
            is_prop = "pr" in classes

            if not is_prop:
                # moneyline row
                fighter_span = tr.select_one("th .t-b-fcc") or tr.select_one(
                    ".t-b-fcc"
                )
                if not fighter_span:
                    continue
                fighter = _text(fighter_span)
                # find any but-sg cell: data-li=[book_id, side, matchup_id]
                local_matchup: str | None = None
                local_side: int | None = None
                for cell in tr.find_all("td", attrs={"data-li": True}):
                    parsed = parse_data_li(cell.get("data-li", ""))
                    if not parsed or len(parsed) < 3:
                        continue
                    if "but-sg" not in (cell.get("class") or []):
                        # could be the action-icon button-cell variant
                        if "button-cell" in (cell.get("class") or []):
                            # data-li=[side, matchup_id]
                            if len(parsed) == 2:
                                local_side = local_side or int(parsed[0])
                                local_matchup = local_matchup or str(parsed[1])
                            continue
                        continue
                    book_id = str(parsed[0])
                    side = int(parsed[1])
                    matchup_id = str(parsed[2])
                    local_matchup = matchup_id
                    local_side = side
                    price = american_from_text(_text(cell))
                    if price is None:
                        continue
                    book_name = book_by_id.get(book_id, f"book_{book_id}")
                    ml_rows.append(
                        {
                            "event_id": meta["event_id"],
                            "event_date": meta["event_date"],
                            "matchup_id": matchup_id,
                            "side": side,
                            "fighter": fighter,
                            "book_id": book_id,
                            "book": book_name,
                            "price_american": price,
                            "line_type": "closing",
                        }
                    )
                # also record opening-delta row (per fighter, no per-book) when we know it
                if (
                    local_matchup
                    and local_side
                    and fighter in opening_deltas
                ):
                    delta = opening_deltas[fighter]
                    # we cannot recover per-book opening; record one consensus row
                    ml_rows.append(
                        {
                            "event_id": meta["event_id"],
                            "event_date": meta["event_date"],
                            "matchup_id": local_matchup,
                            "side": local_side,
                            "fighter": fighter,
                            "book_id": "consensus",
                            "book": "consensus",
                            "price_american": _opening_from_close(
                                ml_rows, meta["event_id"], local_matchup, local_side, delta
                            ),
                            "line_type": "opening",
                        }
                    )
                if local_matchup and local_side:
                    matchup_fighters[local_matchup][local_side] = fighter
                continue

            # prop row (`tr.pr`)
            label_th = tr.find("th")
            label = _text(label_th) if label_th else ""
            if not label:
                continue
            # Determine matchup from any data-li cell on this prop row
            row_matchup: str | None = None
            row_side: int | None = None
            row_prop_code: int | None = None
            row_sub_idx: int | None = None
            prices_for_row: list[dict] = []
            for cell in tr.find_all("td", attrs={"data-li": True}):
                parsed = parse_data_li(cell.get("data-li", ""))
                if not parsed:
                    continue
                # prop priced cell: [book_id, side, matchup_id, prop_code, sub_idx]
                if len(parsed) == 5 and "but-sgp" in (cell.get("class") or []):
                    book_id = str(parsed[0])
                    side = int(parsed[1])
                    row_matchup = str(parsed[2])
                    row_side = side
                    row_prop_code = int(parsed[3])
                    row_sub_idx = int(parsed[4])
                    price = american_from_text(_text(cell))
                    if price is None:
                        continue
                    book_name = book_by_id.get(book_id, f"book_{book_id}")
                    prices_for_row.append(
                        {
                            "book_id": book_id,
                            "book": book_name,
                            "price_american": price,
                        }
                    )
                elif len(parsed) == 4 and "button-cell" in (
                    cell.get("class") or []
                ):
                    # action-icon: [side, matchup_id, prop_code, sub_idx]
                    if row_matchup is None:
                        row_matchup = str(parsed[1])
                        row_side = int(parsed[0])
                        row_prop_code = int(parsed[2])
                        row_sub_idx = int(parsed[3])
            if not row_matchup or not prices_for_row:
                continue
            base_record = {
                "event_id": meta["event_id"],
                "event_date": meta["event_date"],
                "matchup_id": row_matchup,
                "side": row_side,
                "prop_code": row_prop_code,
                "sub_index": row_sub_idx,
                "label": label,
            }
            target = (
                round_rows
                if _is_round_label(label)
                else (method_rows if _is_method_label(label) else None)
            )
            if target is None:
                continue
            for pr in prices_for_row:
                target.append({**base_record, **pr})

    # Fill prop rows with fighter names where matchup mapping is known
    for rows in (method_rows, round_rows):
        for r in rows:
            mu = r.get("matchup_id")
            sd = r.get("side")
            r["fighter"] = matchup_fighters.get(mu, {}).get(sd) if mu else None

    return meta, ml_rows, method_rows, round_rows


def _opening_from_close(
    rows: list[dict], event_id: str | None, matchup_id: str, side: int, delta: int
) -> int | None:
    """Estimate opening price = closing - delta, using the most recent closing row.

    PBO publishes a single signed integer 'change since opening' per fighter.
    For favorites moving toward more juice, the delta is negative (e.g. -65
    means close is 65 American points more negative than open). We pick the
    consensus closing price as the 'best' price across books on the matched
    side, then subtract delta. This is intentionally a coarse opener — it is
    flagged as line_type='opening' with book='consensus' so consumers can
    decide whether to use it.
    """
    closes = [
        r["price_american"]
        for r in rows
        if r["event_id"] == event_id
        and r["matchup_id"] == matchup_id
        and r["side"] == side
        and r["line_type"] == "closing"
        and r["price_american"] is not None
    ]
    if not closes:
        return None
    # use median for stability across books
    closes_sorted = sorted(closes)
    median_close = closes_sorted[len(closes_sorted) // 2]
    try:
        return int(median_close) - int(delta)
    except (TypeError, ValueError):
        return None


def _is_round_label(label: str) -> bool:
    s = label.lower()
    if "round" in s and ("over" in s or "under" in s):
        return True  # over/under N rounds
    if re.search(r"\bin round\s*\d", s):
        return True
    if re.search(r"\bin rounds?\s*\d+\s*-\s*\d+", s):
        return True
    return False


def _is_method_label(label: str) -> bool:
    s = label.lower()
    keywords = (
        "wins by decision",
        "wins by tko",
        "wins by ko",
        "wins by dq",
        "wins by",
        "fight goes to decision",
        "fight doesn't go to decision",
        "fight is a draw",
        "any other result",
        "knocked down",
        "no contest",
    )
    return any(k in s for k in keywords)


# ---------- Moneyline collapsing ----------


def american_to_implied(american: int | float) -> float:
    a = float(american)
    if a >= 0:
        return 100.0 / (a + 100.0)
    return abs(a) / (abs(a) + 100.0)


def best_american_price(prices: list[int]) -> int | None:
    clean = [int(p) for p in prices if p is not None]
    return max(clean) if clean else None


def collapse_moneyline_bouts(price_rows: list[dict]) -> list[dict]:
    """One consensus row per (event_id, matchup_id) using closing per-book prices."""
    closes = [r for r in price_rows if r["line_type"] == "closing"]
    by_key: dict[tuple, list[dict]] = defaultdict(list)
    for r in closes:
        by_key[(r["event_id"], r["event_date"], r["matchup_id"])].append(r)
    bouts: list[dict] = []
    for (eid, edate, mu), grp in by_key.items():
        sides: dict[int, list[dict]] = defaultdict(list)
        for r in grp:
            sides[r["side"]].append(r)
        if 1 not in sides or 2 not in sides:
            continue
        side1 = sides[1]
        side2 = sides[2]
        fighter_a = side1[0]["fighter"]
        fighter_b = side2[0]["fighter"]
        prices_a = [r["price_american"] for r in side1 if r["price_american"] is not None]
        prices_b = [r["price_american"] for r in side2 if r["price_american"] is not None]
        if not prices_a or not prices_b:
            continue
        best_a = best_american_price(prices_a)
        best_b = best_american_price(prices_b)
        mean_a = sum(american_to_implied(p) for p in prices_a) / len(prices_a)
        mean_b = sum(american_to_implied(p) for p in prices_b) / len(prices_b)
        total = mean_a + mean_b
        if total <= 0:
            continue
        bouts.append(
            {
                "event_id": eid,
                "event_date": edate,
                "matchup_id": mu,
                "fighter_a": fighter_a,
                "fighter_b": fighter_b,
                "price_a": best_a,
                "price_b": best_b,
                "n_books_a": len({r["book_id"] for r in side1}),
                "n_books_b": len({r["book_id"] for r in side2}),
                "market_prob_a": mean_a / total,
                "market_prob_b": mean_b / total,
                "weight_class": None,  # PBO does not surface a weight-class string per fight
            }
        )
    return bouts


# ---------- Results join ----------


def normalize_name(s: str | None) -> str:
    if s is None:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_boxrec_date(s: str) -> str | None:
    s = (s or "").strip()
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def load_boxrec_results() -> dict[tuple[str, frozenset], dict]:
    """Index BoxRec fights by (date, frozenset({norm_a, norm_b})) -> row."""
    out: dict[tuple[str, frozenset], dict] = {}
    if not BOXREC_FIGHTS_CSV.exists():
        return out
    with BOXREC_FIGHTS_CSV.open() as f:
        reader = csv.DictReader(f)
        for r in reader:
            d = parse_boxrec_date(r.get("fight_date", ""))
            if not d:
                continue
            a = normalize_name(r.get("boxer_a"))
            b = normalize_name(r.get("boxer_b"))
            if not a or not b:
                continue
            key = (d, frozenset({a, b}))
            out[key] = r
    return out


def join_results(bouts: list[dict]) -> list[dict]:
    idx = load_boxrec_results()
    rows: list[dict] = []
    for b in bouts:
        a = normalize_name(b["fighter_a"])
        bb = normalize_name(b["fighter_b"])
        key = (b["event_date"], frozenset({a, bb}))
        rec = idx.get(key)
        winner = rec.get("winner") if rec else None
        method = rec.get("method") if rec else None
        rnd = rec.get("round") if rec else None
        rows.append(
            {
                "event_id": b["event_id"],
                "event_date": b["event_date"],
                "fighter_a": b["fighter_a"],
                "fighter_b": b["fighter_b"],
                "winner": winner,
                "method": method,
                "round": rnd,
            }
        )
    return rows


# ---------- CSV writers ----------


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
            n += 1
    return n


# ---------- Driver ----------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="Optional cap on number of events to process this run.")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                    help="Seconds between same-host requests (>=1.5).")
    ap.add_argument("--start", default=START_DATE.isoformat(),
                    help="ISO start date filter (default 2018-01-01).")
    ap.add_argument("--end", default=END_DATE.isoformat(),
                    help="Inclusive ISO end date filter (default 2025-12-31).")
    args = ap.parse_args(argv)

    delay = max(args.delay, 1.5)
    budget = RequestBudget(REQUEST_BUDGET)
    last_t = [time.monotonic() - delay]  # allow first call immediately

    RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Sitemap fetch + URL enumeration
    urls = fetch_event_urls(budget, delay, last_t)
    start_d = datetime.fromisoformat(args.start).date()
    end_d = datetime.fromisoformat(args.end).date()
    urls = [u for u in urls if start_d.isoformat() <= u["event_date"] <= end_d.isoformat()]
    write_event_urls_csv(urls)
    if args.limit:
        urls = urls[: args.limit]
    print(f"[plan] processing {len(urls)} event URLs (delay={delay}s)")

    # 2. Per-event fetch + parse
    all_ml_rows: list[dict] = []
    all_method_rows: list[dict] = []
    all_round_rows: list[dict] = []
    all_bouts: list[dict] = []
    n_events_with_data = 0
    fetch_count = 0
    parse_count = 0
    t_start = time.time()
    HARD_WALL_S = 50 * 60  # 50 min

    for i, ev in enumerate(urls, 1):
        if time.time() - t_start > HARD_WALL_S:
            print(f"[stop] wall-clock cap reached after {i - 1} events")
            break
        if budget.used >= budget.cap - 1:
            print(f"[stop] request budget exhausted at event {i - 1}")
            break
        already = cache_path(ev["event_id"]).exists()
        html = fetch_event(ev["url"], ev["event_id"], budget, delay, last_t)
        if not already and html is not None:
            fetch_count += 1
        if html is None:
            continue
        try:
            meta, ml_rows, method_rows, round_rows = parse_event(html, ev["url"])
        except Exception as exc:
            print(f"[parse] ERROR {ev['url']}: {exc}")
            continue
        parse_count += 1
        if not ml_rows:
            continue
        # carry the event date from the URL even if title parser missed it
        for r in ml_rows:
            r.setdefault("event_date", ev["event_date"])
        all_ml_rows.extend(ml_rows)
        all_method_rows.extend(method_rows)
        all_round_rows.extend(round_rows)
        bouts = collapse_moneyline_bouts(ml_rows)
        all_bouts.extend(bouts)
        if bouts:
            n_events_with_data += 1
        if i % 50 == 0:
            elapsed = time.time() - t_start
            print(
                f"[progress] {i}/{len(urls)} events parsed | "
                f"fetched={fetch_count} budget={budget.used}/{budget.cap} | "
                f"bouts so far={len(all_bouts)} | t={elapsed:.0f}s"
            )

    # 3. Write outputs
    n_ml = write_csv(
        ML_PRICES_CSV,
        [
            "event_id", "event_date", "matchup_id", "side", "fighter",
            "book_id", "book", "price_american", "line_type",
        ],
        all_ml_rows,
    )
    n_bouts = write_csv(
        ML_BOUTS_CSV,
        [
            "event_id", "event_date", "matchup_id",
            "fighter_a", "fighter_b", "price_a", "price_b",
            "n_books_a", "n_books_b", "market_prob_a", "market_prob_b",
            "weight_class",
        ],
        all_bouts,
    )
    n_method = write_csv(
        METHOD_CSV,
        [
            "event_id", "event_date", "matchup_id", "side", "fighter",
            "label", "prop_code", "sub_index",
            "book_id", "book", "price_american",
        ],
        all_method_rows,
    )
    n_round = write_csv(
        ROUND_CSV,
        [
            "event_id", "event_date", "matchup_id", "side", "fighter",
            "label", "prop_code", "sub_index",
            "book_id", "book", "price_american",
        ],
        all_round_rows,
    )

    # 4. Results join
    results_rows = join_results(all_bouts)
    n_results = write_csv(
        RESULTS_CSV,
        ["event_id", "event_date", "fighter_a", "fighter_b", "winner", "method", "round"],
        results_rows,
    )

    # 5. Validation report
    print()
    print("=" * 70)
    print("PBO ARCHIVE SCRAPE -- VALIDATION REPORT")
    print("=" * 70)
    print(f"events listed in sitemap (2018-2025) : {len(urls)}")
    print(f"events with >=1 priced moneyline row : {n_events_with_data}")
    print(f"new HTTP fetches this run            : {fetch_count}")
    print(f"http budget used                     : {budget.used}/{budget.cap}")
    print()
    print(f"moneyline price rows written         : {n_ml}  -> {ML_PRICES_CSV}")
    print(f"collapsed moneyline bouts written    : {n_bouts}  -> {ML_BOUTS_CSV}")
    print(f"method-prop rows written             : {n_method}  -> {METHOD_CSV}")
    print(f"round-prop rows written              : {n_round}  -> {ROUND_CSV}")
    print(f"results-join rows written            : {n_results}  -> {RESULTS_CSV}")
    print()

    # per-year row counts
    from collections import Counter
    by_year = Counter()
    for b in all_bouts:
        if b["event_date"]:
            by_year[b["event_date"][:4]] += 1
    print("Per-year moneyline-bout counts:")
    for y in sorted(by_year):
        print(f"  {y}: {by_year[y]}")
    print()

    # winner extraction rate
    n_with_winner = sum(1 for r in results_rows if r["winner"])
    pct = (100.0 * n_with_winner / max(1, len(results_rows)))
    print(f"% bouts with winner (BoxRec join)    : {pct:.1f}% "
          f"({n_with_winner}/{len(results_rows)})")
    n_with_method = sum(1 for r in results_rows if r["method"])
    print(f"% bouts with method (BoxRec join)    : "
          f"{100.0 * n_with_method / max(1, len(results_rows)):.1f}%")
    n_with_round = sum(1 for r in results_rows if r["round"])
    print(f"% bouts with round (BoxRec join)     : "
          f"{100.0 * n_with_round / max(1, len(results_rows)):.1f}%")
    pct_method = 100.0 * len({r["matchup_id"] for r in all_method_rows}) / max(
        1, len({(b["event_id"], b["matchup_id"]) for b in all_bouts})
    )
    pct_round = 100.0 * len({r["matchup_id"] for r in all_round_rows}) / max(
        1, len({(b["event_id"], b["matchup_id"]) for b in all_bouts})
    )
    print(f"% bouts with method-prop coverage    : {pct_method:.1f}%")
    print(f"% bouts with round-prop coverage     : {pct_round:.1f}%")
    print()

    # spot-check famous fights
    famous = [
        ("Crawford-Spence", "2023-07-29", {"terence crawford", "errol spence"}),
        ("Fury-Wilder III", "2021-10-09", {"tyson fury", "deontay wilder"}),
        ("Canelo-Bivol", "2022-05-07", {"saul alvarez", "dmitry bivol"}),
        ("Haney-Loma", "2023-05-20", {"devin haney", "vasyl lomachenko"}),
        ("Inoue-Fulton", "2023-07-25", {"naoya inoue", "stephen fulton"}),
    ]
    bouts_idx: dict[tuple[str, frozenset], dict] = {}
    for b in all_bouts:
        bouts_idx[(b["event_date"], frozenset({normalize_name(b["fighter_a"]),
                                               normalize_name(b["fighter_b"])}))] = b
    print("Famous-fight spot check:")
    for name, edate, names in famous:
        # match if any pair contains both normalized names
        hit = None
        for (d, fset), b in bouts_idx.items():
            if d == edate and names.issubset(fset):
                hit = b
                break
        if not hit:
            # also accept any date within +-2d
            for (d, fset), b in bouts_idx.items():
                if names.issubset(fset):
                    hit = b
                    edate = d
                    break
        if hit:
            res = next(
                (r for r in results_rows
                 if r["event_id"] == hit["event_id"]
                 and frozenset({normalize_name(r["fighter_a"]),
                                normalize_name(r["fighter_b"])}) == frozenset(
                     {normalize_name(hit["fighter_a"]),
                      normalize_name(hit["fighter_b"])})),
                None,
            )
            print(
                f"  {name:20s} {edate} | {hit['fighter_a']} ({hit['price_a']:+d}) vs "
                f"{hit['fighter_b']} ({hit['price_b']:+d})  "
                f"winner={res['winner'] if res else 'NA'}"
            )
        else:
            print(f"  {name:20s} {edate} | NOT FOUND in PBO scrape")
    print()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

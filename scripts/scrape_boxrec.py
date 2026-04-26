#!/usr/bin/env python3
"""
Boxer profile + fight record scraper for boxing-odds-daemon.

PRIMARY SOURCE: Wikipedia.
  BoxRec is the canonical DB but is hard-blocked behind a Cloudflare JS
  challenge (every request from this environment returns the
  "Just a moment..." 403 page). Solving that requires a real headless
  browser with cf-clearance cookies (e.g. curl_cffi impersonation +
  Playwright) which is out of scope for the initial scaffold.

Wikipedia infoboxes + "Professional boxing record" tables on individual
boxer pages give us almost every BoxRec field we need:
    DOB, height, reach, stance, weight class, total record (W-L-D, KOs)
    and a per-fight log: date, opponent, result, method, round, location

This module fetches:
  fetch_active_boxers()    -> roster CSV from "List of current world
                              boxing champions"
  fetch_profile(slug)      -> dict with profile fields
  fetch_fight_record(slug) -> list of fight dicts

All requests go through a polite session: 5s+ between hits, retry on
429/503, descriptive UA, on-disk HTML cache so re-runs are free.

The script kept its original name (scrape_boxrec.py) because the
upstream pipeline references it, even though the scraper now talks to
Wikipedia. See BOXING_DATA_GAPS.md for the BoxRec story.
"""
from __future__ import annotations

import csv
import os
import random
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
HTML_CACHE = ROOT / "data" / "raw_html" / "boxrec"
HTML_CACHE.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "boxing-odds-daemon/0.1 (research; contact: krameitbullington@gmail.com) "
    "python-requests"
)
WIKI_BASE = "https://en.wikipedia.org"
MIN_DELAY_S = 5.0          # polite floor between network hits
MAX_DELAY_S = 7.5          # jitter ceiling
MAX_RETRIES = 4
RETRY_BACKOFF_S = (8, 20, 45, 90)

CHAMP_LIST_URL = (
    f"{WIKI_BASE}/wiki/List_of_current_world_boxing_champions"
)
P4P_URL = f"{WIKI_BASE}/wiki/Boxing_pound_for_pound_rankings"
ROSTER_URLS = [
    # Curated roster: each page lists 20-100+ active pro boxers.
    f"{WIKI_BASE}/wiki/List_of_male_boxers",
    f"{WIKI_BASE}/wiki/List_of_female_boxers",
]

# Weight class normalisation -- map Wikipedia phrasing to canonical names
# the model side will use.
WEIGHT_CLASS_CANON = {
    "minimumweight": "minimumweight",
    "strawweight": "minimumweight",
    "light flyweight": "light_flyweight",
    "junior flyweight": "light_flyweight",
    "flyweight": "flyweight",
    "super flyweight": "super_flyweight",
    "junior bantamweight": "super_flyweight",
    "bantamweight": "bantamweight",
    "super bantamweight": "super_bantamweight",
    "junior featherweight": "super_bantamweight",
    "featherweight": "featherweight",
    "super featherweight": "super_featherweight",
    "junior lightweight": "super_featherweight",
    "lightweight": "lightweight",
    "super lightweight": "super_lightweight",
    "junior welterweight": "super_lightweight",
    "welterweight": "welterweight",
    "super welterweight": "super_welterweight",
    "junior middleweight": "super_welterweight",
    "light middleweight": "super_welterweight",
    "middleweight": "middleweight",
    "super middleweight": "super_middleweight",
    "light heavyweight": "light_heavyweight",
    "cruiserweight": "cruiserweight",
    "bridgerweight": "bridgerweight",
    "heavyweight": "heavyweight",
}

METHOD_CANON = {
    "ud": "decision_unanimous",
    "md": "decision_majority",
    "sd": "decision_split",
    "tko": "tko",
    "ko": "ko",
    "rtd": "tko_corner",
    "dq": "dq",
    "tdraw": "draw_technical",
    "draw": "draw",
    "nc": "no_contest",
}


# --------------------------------------------------------------------------
# HTTP session
# --------------------------------------------------------------------------
@dataclass
class PoliteSession:
    """requests session with delay floor + retry/backoff + on-disk cache."""

    session: requests.Session = field(default_factory=requests.Session)
    last_hit: float = 0.0
    request_count: int = 0
    request_cap: int = 50  # hard ceiling for a single run

    def __post_init__(self) -> None:
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _wait(self) -> None:
        elapsed = time.time() - self.last_hit
        delay = random.uniform(MIN_DELAY_S, MAX_DELAY_S)
        if elapsed < delay:
            time.sleep(delay - elapsed)

    def get(self, url: str, cache_key: str) -> str:
        cache_path = HTML_CACHE / f"{cache_key}.html"
        if cache_path.exists() and cache_path.stat().st_size > 1024:
            return cache_path.read_text(encoding="utf-8", errors="replace")

        if self.request_count >= self.request_cap:
            raise RuntimeError(
                f"Request cap ({self.request_cap}) hit; refusing more network."
            )

        for attempt in range(MAX_RETRIES):
            self._wait()
            self.last_hit = time.time()
            self.request_count += 1
            try:
                resp = self.session.get(url, timeout=25)
            except requests.RequestException as e:
                print(f"  [warn] {url} -> {e}", file=sys.stderr)
                time.sleep(RETRY_BACKOFF_S[min(attempt, len(RETRY_BACKOFF_S) - 1)])
                continue
            if resp.status_code == 200:
                cache_path.write_text(resp.text, encoding="utf-8")
                return resp.text
            if resp.status_code in (429, 503):
                wait = RETRY_BACKOFF_S[min(attempt, len(RETRY_BACKOFF_S) - 1)]
                print(f"  [retry] {resp.status_code} on {url}; sleep {wait}s",
                      file=sys.stderr)
                time.sleep(wait)
                continue
            print(f"  [error] {resp.status_code} on {url}", file=sys.stderr)
            return ""
        return ""


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------
HEIGHT_CM_RE = re.compile(r"\((\d+(?:\.\d+)?)\s*cm\)")
REACH_CM_RE = re.compile(r"\((\d+(?:\.\d+)?)\s*cm\)")
DOB_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")
RECORD_RE = re.compile(r"(\d+)\s*[–\-]\s*(\d+)(?:\s*[–\-]\s*(\d+))?")


def _slug_from_href(href: str) -> str:
    return urllib.parse.unquote(href.split("/wiki/")[-1])


def _norm_weight_class(text: str) -> str:
    t = text.lower().strip()
    for key, val in WEIGHT_CLASS_CANON.items():
        if key in t:
            return val
    return ""


def _norm_method(text: str) -> str:
    t = text.lower().strip()
    # Wikipedia uses "UD", "TKO", "KO", "MD", "SD", "RTD", "PTS", etc.
    base = re.split(r"[ ,()]", t)[0]
    return METHOD_CANON.get(base, base or "")


def _to_int(text: str) -> int | None:
    m = re.search(r"\d+", text or "")
    return int(m.group()) if m else None


# --------------------------------------------------------------------------
# Champion list -> active boxer roster
# --------------------------------------------------------------------------
SKIP_LINK_TOKENS = (
    ":", "List_", "_(magazine)", "_(boxing)", "Boxing_", "World_Boxing",
    "_title", "Pound_for_pound", "Champion", "International_Boxing",
)


def fetch_active_boxers(sess: PoliteSession, limit: int = 80) -> list[dict]:
    """Pull current world champions list and parse boxer rows.

    Each row in the page's wikitables looks like:
        <weight class header> | Champion | Title | Reign began | Defenses
    """
    html = sess.get(CHAMP_LIST_URL, "_wiki_champs")
    soup = BeautifulSoup(html, "lxml")
    boxers: dict[str, dict] = {}

    # Champion list page uses many wikitables, one per weight class block.
    # Strategy: walk every row; first cell-with-link is a boxer; nearest
    # preceding header (h2/h3) gives the weight class.
    current_wc = ""
    for el in soup.find_all(["h2", "h3", "table"]):
        if el.name in ("h2", "h3"):
            txt = el.get_text(" ", strip=True)
            wc = _norm_weight_class(txt)
            if wc:
                current_wc = wc
            continue
        if "wikitable" not in (el.get("class") or []):
            continue
        for tr in el.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue
            for td in tds:
                a = td.find("a", href=True)
                if not a:
                    continue
                href = a["href"]
                if not href.startswith("/wiki/"):
                    continue
                if any(tok in href for tok in SKIP_LINK_TOKENS):
                    continue
                name = a.get_text(strip=True)
                slug = _slug_from_href(href)
                if not name or len(name) < 3:
                    continue
                if slug in boxers:
                    continue
                boxers[slug] = {
                    "boxer_id": slug,
                    "name": name,
                    "wiki_url": WIKI_BASE + href,
                    "weight_class_primary": current_wc,
                    "discovery_source": "wiki_current_champions",
                }
            if len(boxers) >= limit:
                break
        if len(boxers) >= limit:
            break
    # 2nd pass: pound-for-pound rankings page (already cached after this hit)
    if len(boxers) < limit:
        try:
            html = sess.get(P4P_URL, "_wiki_p4p")
            soup = BeautifulSoup(html, "lxml")
            for table in soup.find_all("table", class_="wikitable"):
                for tr in table.find_all("tr"):
                    for td in tr.find_all(["td", "th"]):
                        a = td.find("a", href=True)
                        if not a:
                            continue
                        href = a["href"]
                        if not href.startswith("/wiki/"):
                            continue
                        if any(tok in href for tok in SKIP_LINK_TOKENS):
                            continue
                        slug = _slug_from_href(href)
                        name = a.get_text(strip=True)
                        if not name or len(name) < 3 or slug in boxers:
                            continue
                        boxers[slug] = {
                            "boxer_id": slug, "name": name,
                            "wiki_url": WIKI_BASE + href,
                            "weight_class_primary": "",
                            "discovery_source": "wiki_p4p",
                        }
                    if len(boxers) >= limit:
                        break
                if len(boxers) >= limit:
                    break
        except Exception as e:
            print(f"  [warn] p4p discovery failed: {e}", file=sys.stderr)
    return list(boxers.values())


# --------------------------------------------------------------------------
# Boxer profile + fight record
# --------------------------------------------------------------------------
def _parse_infobox(infobox) -> dict:
    out: dict[str, str | int | None] = {
        "dob": None, "height_cm": None, "reach_cm": None, "stance": None,
        "country": None, "gym": None, "total_fights": None,
        "w": None, "l": None, "d": None, "ko_wins": None, "ko_losses": None,
    }
    if not infobox:
        return out
    # Wikipedia tags birthdays with class="bday" microformat -- most reliable.
    bday = infobox.find(class_="bday")
    if bday and re.match(r"\d{4}-\d{2}-\d{2}", bday.get_text(strip=True)):
        out["dob"] = bday.get_text(strip=True)
    for tr in infobox.find_all("tr"):
        th = tr.find("th"); td = tr.find("td")
        if not (th and td):
            continue
        label = th.get_text(" ", strip=True).lower().replace(" ", " ")
        val = td.get_text(" ", strip=True).replace(" ", " ")
        if "born" in label:
            m = DOB_RE.search(val)
            if m:
                out["dob"] = m.group(1)
            # Country = last comma-token of birth line, after age
            after_age = val.split(")", 1)[-1]
            country = after_age.split(",")[-1].strip()
            if country and len(country) <= 40:
                out["country"] = country
        elif "height" in label:
            m = HEIGHT_CM_RE.search(val)
            if m:
                out["height_cm"] = float(m.group(1))
        elif "reach" in label:
            m = REACH_CM_RE.search(val)
            if m:
                out["reach_cm"] = float(m.group(1))
        elif "stance" in label:
            out["stance"] = val.split("[")[0].strip().lower()
        elif "nationality" in label and not out["country"]:
            out["country"] = val.split("[")[0].strip()
        elif label.startswith("total"):
            out["total_fights"] = _to_int(val)
        elif label == "wins":
            out["w"] = _to_int(val)
        elif label == "losses":
            out["l"] = _to_int(val)
        elif label == "draws":
            out["d"] = _to_int(val)
        elif "wins by" in label and "knockout" in label:
            out["ko_wins"] = _to_int(val)
        elif "losses by" in label and ("knockout" in label or "ko" in label):
            out["ko_losses"] = _to_int(val)
        elif label in ("trainer", "trained by", "team", "gym"):
            out["gym"] = val.split("[")[0].strip()[:60]
    return out


def fetch_profile(sess: PoliteSession, slug: str) -> dict:
    url = f"{WIKI_BASE}/wiki/{slug}"
    html = sess.get(url, f"profile_{slug}")
    if not html:
        return {}
    soup = BeautifulSoup(html, "lxml")
    infobox = soup.find("table", class_="infobox")
    return _parse_infobox(infobox)


def fetch_fight_record(sess: PoliteSession, slug: str, name: str) -> list[dict]:
    """Parse the 'Professional boxing record' table on a boxer page.

    Standard column layout:
        No. | Result | Record | Opponent | Type | Round, time | Date
            | Location | Notes
    """
    url = f"{WIKI_BASE}/wiki/{slug}"
    html = sess.get(url, f"profile_{slug}")
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    fights: list[dict] = []
    for table in soup.find_all("table", class_="wikitable"):
        head_text = " ".join(
            th.get_text(" ", strip=True)
            for th in table.find_all("th")[:10]
        )
        if not ("Result" in head_text and "Opponent" in head_text):
            continue
        # Build header index
        header_row = table.find("tr")
        headers = [
            th.get_text(" ", strip=True).lower()
            for th in header_row.find_all(["th", "td"])
        ]
        def col(name_part: str) -> int:
            for i, h in enumerate(headers):
                if name_part in h:
                    return i
            return -1
        i_result = col("result")
        i_opp = col("opponent")
        i_type = col("type")
        i_round = col("round")
        i_date = col("date")
        i_loc = col("location")
        i_notes = col("note")
        for tr in table.find_all("tr")[1:]:
            tds = tr.find_all(["td", "th"])
            if len(tds) < max(i_result, i_opp, i_type, i_date) + 1:
                continue
            def get(idx: int) -> str:
                if idx < 0 or idx >= len(tds):
                    return ""
                return tds[idx].get_text(" ", strip=True).replace(" ", " ")
            result = get(i_result).lower()
            opp = get(i_opp)
            method = _norm_method(get(i_type))
            rnd_text = get(i_round)
            rnd = _to_int(rnd_text)
            date_raw = get(i_date)
            location = get(i_loc)
            notes = get(i_notes)
            if result == "win":
                winner = name
            elif result == "loss":
                winner = opp
            else:
                winner = ""  # draw / NC
            fights.append({
                "fight_date": date_raw,
                "boxer_a": name,
                "boxer_b": opp,
                "weight_class": "",  # fight-level WC isn't reliably in table
                "scheduled_rounds": "",  # not in table; would need fight page
                "winner": winner,
                "method": method,
                "round": rnd if rnd is not None else "",
                "location": location,
                "promoter": "",  # not in record table
                "notes": notes[:120],
            })
        break  # only the first record table; later tables are amateur etc.
    return fights


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------
BOXER_FIELDS = [
    "boxer_id", "name", "dob", "height_cm", "reach_cm", "stance",
    "weight_class_primary", "gym", "country",
    "total_fights", "w", "l", "d", "ko_wins", "ko_losses",
    "wiki_url", "discovery_source",
]
FIGHT_FIELDS = [
    "fight_date", "boxer_a", "boxer_b", "weight_class", "scheduled_rounds",
    "winner", "method", "round", "location", "promoter", "notes",
]
DISCOVERY_FIELDS = [
    "boxer_id", "name", "weight_class_primary", "wiki_url", "discovery_source",
]


def write_csv(path: Path, rows: Iterable[dict], fields: list[str]) -> int:
    rows = list(rows)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)


def main(deep_scrape_n: int = 12) -> None:
    sess = PoliteSession(request_cap=50)
    print(f"[scrape_boxrec] discovering active boxers via {CHAMP_LIST_URL}")
    roster = fetch_active_boxers(sess, limit=500)
    print(f"  -> {len(roster)} boxers discovered")

    discovery_path = RAW_DIR / "boxrec_discovery.csv"
    n = write_csv(discovery_path, roster, DISCOVERY_FIELDS)
    print(f"  wrote {n} rows -> {discovery_path}")

    # Pick the first N for the deep scrape; one Wikipedia page provides
    # both profile and fight record.
    deep_targets = roster[:deep_scrape_n]
    boxers_out: list[dict] = []
    fights_out: list[dict] = []
    for i, b in enumerate(deep_targets, 1):
        print(f"  [{i}/{len(deep_targets)}] {b['name']}")
        prof = fetch_profile(sess, b["boxer_id"])
        if prof:
            row = {**b, **prof}
            boxers_out.append(row)
        fights = fetch_fight_record(sess, b["boxer_id"], b["name"])
        fights_out.extend(fights)
        print(f"     profile_fields={sum(1 for v in prof.values() if v)}"
              f" fights={len(fights)}")

    nb = write_csv(RAW_DIR / "boxrec_boxers.csv", boxers_out, BOXER_FIELDS)
    nf = write_csv(RAW_DIR / "boxrec_fights.csv", fights_out, FIGHT_FIELDS)
    print(f"\nwrote {nb} boxers -> data/raw/boxrec_boxers.csv")
    print(f"wrote {nf} fights  -> data/raw/boxrec_fights.csv")
    print(f"network requests issued this run: {sess.request_count}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    main(deep_scrape_n=n)

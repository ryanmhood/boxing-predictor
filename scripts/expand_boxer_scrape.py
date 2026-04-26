#!/usr/bin/env python3
"""
Expand the Wikipedia boxer scrape from ~12 boxers to ~200 (capped by
network budget), producing a large fight-results table we can join to the
PBO odds archive (8,942 bouts, 2018-2025) for backtesting.

Strategy
--------
1. Build the discovery roster from MULTIPLE Wikipedia aggregator pages:
     * List_of_current_world_boxing_champions   (already cached)
     * Boxing_pound_for_pound_rankings           (already cached)
     * 2018..2025 _in_boxing                     (8 yearly summary pages)
     * List_of_male_boxers                       (large alphabetical list)
   PLUS prioritise fighters who actually appear in PBO bouts (top by
   appearance count) -- these are the fighters whose records will pay
   off most for the join.

2. Slug-normalise PBO fighter names so we can hit their Wikipedia pages
   directly (Wikipedia fuzzy-redirects most "Firstname_Lastname" guesses).

3. Deep-scrape each boxer's Wikipedia page for the
   "Professional boxing record" table; reuse the existing parser from
   scrape_boxrec.py.

4. Append (don't overwrite) to data/raw/boxrec_fights.csv and update
   boxrec_boxers.csv. Write data/raw/boxer_roster.csv (the merged
   roster) and data/raw/boxer_name_aliases.json (normalised-name map).

5. Validation join: load PBO results, inner-join on
   (event_date +/- 1 day, normalised names) and report % of bouts that
   now have a winner -- this is the backtest-readiness number.

Budget
------
Hard ceiling: 200 Wikipedia requests this session (the cache short-
circuits prior hits, so a re-run is free). Cache lives in
data/raw_html/boxrec/ alongside the existing files (same naming scheme).
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
import unicodedata
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup

# Reuse the existing scraper's session + parsers.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
import scrape_boxrec as sb  # type: ignore  # noqa: E402

WIKI_BASE = sb.WIKI_BASE
RAW_DIR = ROOT / "data" / "raw"
WIKI_HTML_LINK = ROOT / "data" / "raw_html" / "wikipedia"
WIKI_HTML_LINK.mkdir(parents=True, exist_ok=True)

# Discovery URLs beyond what scrape_boxrec already touches.
YEAR_PAGES = [
    f"{WIKI_BASE}/wiki/{y}_in_boxing"
    for y in range(2018, 2026)
]
EXTRA_LIST_PAGES = [
    f"{WIKI_BASE}/wiki/List_of_male_boxers",
    f"{WIKI_BASE}/wiki/List_of_female_boxers",
]

REQUEST_CAP = 200  # raised from 50, per the task brief
PROFILE_TARGET = 200  # how many distinct boxer pages to deep-scrape

# Tokens whose presence in a Wikipedia link almost always means it isn't
# a personal boxer page (organisation, list, magazine, weight class, etc.)
SKIP_TOKENS = (
    ":", "List_", "_(magazine)", "_(boxing_magazine)", "_(promoter)",
    "Boxing_at_", "World_Boxing", "_title", "Pound_for_pound",
    "Champion", "International_Boxing", "Welterweight", "Heavyweight",
    "Middleweight", "Lightweight", "Featherweight", "Bantamweight",
    "Flyweight", "Cruiserweight", "Strawweight", "_Federation",
    "_(boxing)", "_(sport)", "_Championship", "_Championships",
    "Olympic", "AIBA", "WBC_", "WBA_", "WBO_", "IBF_", "WBSS",
    "Top_Rank", "Matchroom", "DAZN", "Showtime", "Sky_Sports",
    "ESPN", "Premier_Boxing", "_arena", "_Arena", "Garden",
    "Madison_Square", "T-Mobile", "MGM_", "Wikipedia",
)


# --------------------------------------------------------------------------
# Name normalisation -- shared with the join step
# --------------------------------------------------------------------------
NICKNAME_RE = re.compile(r"[\"'‘’“”].+?[\"'‘’“”]")
NON_ALNUM = re.compile(r"[^a-z0-9 ]+")
SUFFIXES = (" jr", " jnr", " sr", " ii", " iii", " iv")


def normalize_name(name: str) -> str:
    """Lowercase, strip accents, drop nicknames in quotes, remove punctuation,
    collapse whitespace. 'Saúl "Canelo" Álvarez' -> 'saul alvarez'."""
    if not name:
        return ""
    s = name.strip()
    # Remove ASCII or smart-quoted nickname segments.
    s = NICKNAME_RE.sub(" ", s)
    # Strip diacritics.
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = NON_ALNUM.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    for suf in SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s


# Known boxer slugs whose Wikipedia title diverges from "Firstname_Lastname".
# These are high-PBO-volume fighters; mapping them by hand saves wasted requests.
WIKI_SLUG_OVERRIDES: dict[str, str] = {
    # Names normalised via normalize_name(...)
    "saul alvarez": "Canelo_Álvarez",
    "canelo alvarez": "Canelo_Álvarez",
    "vasiliy lomachenko": "Vasiliy_Lomachenko",
    "tyson fury": "Tyson_Fury",
    "anthony joshua": "Anthony_Joshua",
    "deontay wilder": "Deontay_Wilder",
    "naoya inoue": "Naoya_Inoue",
    "terence crawford": "Terence_Crawford",
    "errol spence": "Errol_Spence_Jr.",
    "errol spence jr": "Errol_Spence_Jr.",
    "gennadiy golovkin": "Gennadiy_Golovkin",
    "gennady golovkin": "Gennadiy_Golovkin",
    "manny pacquiao": "Manny_Pacquiao",
    "tank davis": "Gervonta_Davis",
    "gervonta davis": "Gervonta_Davis",
    "ryan garcia": "Ryan_Garcia",
    "devin haney": "Devin_Haney",
    "shakur stevenson": "Shakur_Stevenson",
    "teofimo lopez": "Teófimo_López",
    "katie taylor": "Katie_Taylor",
    "amanda serrano": "Amanda_Serrano",
    "claressa shields": "Claressa_Shields",
    "tim tszyu": "Tim_Tszyu",
    "jaime munguia": "Jaime_Munguía",
    "david benavidez": "David_Benavidez",
    "caleb plant": "Caleb_Plant",
    "demetrius andrade": "Demetrius_Andrade",
    "jermall charlo": "Jermall_Charlo",
    "jermell charlo": "Jermell_Charlo",
    "regis prograis": "Regis_Prograis",
    "josh taylor": "Josh_Taylor_(boxer)",
    "tony bellew": "Tony_Bellew",
    "joe joyce": "Joe_Joyce_(boxer)",
    "daniel dubois": "Daniel_Dubois_(boxer)",
    "joseph parker": "Joseph_Parker_(boxer)",
    "filip hrgovic": "Filip_Hrgović",
    "zhilei zhang": "Zhilei_Zhang",
    "andy ruiz": "Andy_Ruiz_Jr.",
    "andy ruiz jr": "Andy_Ruiz_Jr.",
    "luis ortiz": "Luis_Ortiz_(boxer)",
    "dillian whyte": "Dillian_Whyte",
    "derek chisora": "Derek_Chisora",
    "frank sanchez": "Frank_Sánchez_(boxer)",
    "michael conlan": "Michael_Conlan",
    "leigh wood": "Leigh_Wood",
    "luis nery": "Luis_Nery",
    "stephen fulton": "Stephen_Fulton_Jr.",
    "rey vargas": "Rey_Vargas",
    "mauricio lara": "Mauricio_Lara",
    "leo santa cruz": "Leo_Santa_Cruz",
    "carl frampton": "Carl_Frampton",
    "josh warrington": "Josh_Warrington",
    "kid galahad": "Kid_Galahad",
    "lee selby": "Lee_Selby",
    "scott quigg": "Scott_Quigg",
    "kell brook": "Kell_Brook",
    "amir khan": "Amir_Khan_(boxer)",
    "kell brook": "Kell_Brook",
    "conor benn": "Conor_Benn",
    "chris eubank jr": "Chris_Eubank_Jr.",
    "liam smith": "Liam_Smith_(boxer)",
    "callum smith": "Callum_Smith",
    "billy joe saunders": "Billy_Joe_Saunders",
    "carl froch": "Carl_Froch",
    "george groves": "George_Groves",
    "james degale": "James_DeGale",
    "anthony yarde": "Anthony_Yarde",
    "joshua buatsi": "Joshua_Buatsi",
    "callum johnson": "Callum_Johnson_(boxer)",
    "lawrence okolie": "Lawrence_Okolie",
    "richard riakporhe": "Richard_Riakporhe",
    "chris billam smith": "Chris_Billam-Smith",
    "moses itauma": "Moses_Itauma",
    "fabio wardley": "Fabio_Wardley",
    "david adeleye": "David_Adeleye",
    "frazer clarke": "Frazer_Clarke",
    "johnny fisher": "Johnny_Fisher_(boxer)",
    "katie taylor": "Katie_Taylor",
    "chantelle cameron": "Chantelle_Cameron",
    "savannah marshall": "Savannah_Marshall",
    "natasha jonas": "Natasha_Jonas",
    "alycia baumgardner": "Alycia_Baumgardner",
    "mikaela mayer": "Mikaela_Mayer",
    "jessica mccaskill": "Jessica_McCaskill",
    "cecilia braekhus": "Cecilia_Brækhus",
    "skye nicolson": "Skye_Nicolson",
    "ellie scotney": "Ellie_Scotney",
    "lauren price": "Lauren_Price",
    "sandy ryan": "Sandy_Ryan",
    "jaron ennis": "Jaron_Ennis",
    "vergil ortiz jr": "Vergil_Ortiz_Jr.",
    "vergil ortiz": "Vergil_Ortiz_Jr.",
    "eimantas stanionis": "Eimantas_Stanionis",
    "yordenis ugas": "Yordenis_Ugás",
    "keith thurman": "Keith_Thurman",
    "shawn porter": "Shawn_Porter",
    "danny garcia": "Danny_Garcia",
    "adrien broner": "Adrien_Broner",
    "mikey garcia": "Mikey_Garcia",
    "lamont roach jr": "Lamont_Roach_Jr.",
    "isaac cruz": "Isaac_Cruz",
    "william zepeda": "William_Zepeda",
    "frank martin": "Frank_Martin_(boxer)",
    "raymond muratalla": "Raymond_Muratalla",
    "keyshawn davis": "Keyshawn_Davis",
    "andy cruz": "Andy_Cruz",
    "abdullah mason": "Abdullah_Mason",
    "richardson hitchins": "Richardson_Hitchins",
    "subriel matias": "Subriel_Matías",
    "jose ramirez": "José_Ramírez_(boxer)",
    "alberto puello": "Alberto_Puello",
    "rolando romero": "Rolando_Romero",
    "jose pedraza": "José_Pedraza",
    "george kambosos jr": "George_Kambosos_Jr.",
    "george kambosos": "George_Kambosos_Jr.",
    "raymond ford": "Raymond_Ford_(boxer)",
    "nick ball": "Nick_Ball_(boxer)",
    "luis alberto lopez": "Luis_Alberto_López",
    "joet gonzalez": "Joet_González",
    "robeisy ramirez": "Robeisy_Ramírez",
    "rafael espinoza": "Rafael_Espinoza_(boxer)",
    "bruce carrington": "Bruce_Carrington",
    "abraham nova": "Abraham_Nova",
    "emanuel navarrete": "Emanuel_Navarrete",
    "oscar valdez": "Óscar_Valdez",
    "miguel berchelt": "Miguel_Berchelt",
    "joe cordina": "Joe_Cordina",
    "lamont roach": "Lamont_Roach_Jr.",
    "sam goodman": "Sam_Goodman_(boxer)",
    "jason moloney": "Jason_Moloney",
    "andrew moloney": "Andrew_Moloney",
    "junto nakatani": "Junto_Nakatani",
    "kazuto ioka": "Kazuto_Ioka",
    "kosei tanaka": "Kosei_Tanaka",
    "jesse rodriguez": "Jesse_Rodríguez_(boxer)",
    "juan francisco estrada": "Juan_Francisco_Estrada",
    "roman gonzalez": "Roman_Gonzalez_(boxer)",
    "srisaket sor rungvisai": "Srisaket_Sor_Rungvisai",
    "carlos cuadras": "Carlos_Cuadras",
    "kenshiro teraji": "Kenshiro_Teraji",
    "hiroto kyoguchi": "Hiroto_Kyoguchi",
    "moruti mthalane": "Moruti_Mthalane",
    "artur beterbiev": "Artur_Beterbiev",
    "dmitry bivol": "Dmitry_Bivol",
    "callum smith": "Callum_Smith",
    "joe smith jr": "Joe_Smith_Jr.",
    "anthony yarde": "Anthony_Yarde",
    "sergey kovalev": "Sergey_Kovalev",
    "jean pascal": "Jean_Pascal",
    "marcus browne": "Marcus_Browne",
    "badou jack": "Badou_Jack",
    "michał cieślak": "Michał_Cieślak",
    "michal cieslak": "Michał_Cieślak",
    "mairis briedis": "Mairis_Briedis",
    "yuniel dorticos": "Yuniel_Dorticós",
    "jai opetaia": "Jai_Opetaia",
    "lawrence okolie": "Lawrence_Okolie",
    "nuno costa": "Nuno_Costa_(boxer)",
    "noel mikaelian": "Noel_Mikaelian",
    "agit kabayel": "Agit_Kabayel",
    "filip hrgovic": "Filip_Hrgović",
    "demsey mckean": "Demsey_McKean",
    "joshua buatsi": "Joshua_Buatsi",
    "willy hutchinson": "Willy_Hutchinson",
    "edgar berlanga": "Edgar_Berlanga",
    "diego pacheco": "Diego_Pacheco",
    "christian mbilli": "Christian_Mbilli",
    "munguia jaime": "Jaime_Munguía",
    "carlos adames": "Carlos_Adames",
    "janibek alimkhanuly": "Janibek_Alimkhanuly",
    "erislandy lara": "Erislandy_Lara",
    "vladimir hernandez": "Vladimir_Hernández",
    "michael zerafa": "Michael_Zerafa",
    "danny dignum": "Danny_Dignum",
    "felix cash": "Felix_Cash",
    "denzel bentley": "Denzel_Bentley",
    "hamzah sheeraz": "Hamzah_Sheeraz",
}


def slugify_for_wiki(name: str) -> str:
    """Build the most likely Wikipedia slug from a free-text boxer name.

    Uses an override map for well-known boxers whose Wikipedia title
    diverges from a naive 'Firstname_Lastname' guess, then falls back
    to the naive slug.
    """
    norm = normalize_name(name)
    if norm in WIKI_SLUG_OVERRIDES:
        return WIKI_SLUG_OVERRIDES[norm]
    s = name.strip()
    s = NICKNAME_RE.sub(" ", s)
    s = re.sub(r"\s+", "_", s.strip())
    return s


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------
def _harvest_links(html: str) -> list[tuple[str, str]]:
    """Pull (slug, displayed_name) candidates from a Wikipedia page body."""
    soup = BeautifulSoup(html, "lxml")
    out: list[tuple[str, str]] = []
    body = soup.find("div", id="mw-content-text") or soup
    for a in body.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/wiki/"):
            continue
        if any(tok in href for tok in SKIP_TOKENS):
            continue
        slug = urllib.parse.unquote(href.split("/wiki/")[-1].split("#")[0])
        name = a.get_text(strip=True)
        if not name or len(name) < 4 or " " not in name:
            continue
        # Drop dates, years, single-token tags
        if re.fullmatch(r"\d{4}", name):
            continue
        out.append((slug, name))
    return out


def discover_from_year_pages(sess: sb.PoliteSession) -> dict[str, dict]:
    """Hit each '<year>_in_boxing' page and collect linked boxers.

    Each year page has many fight rows whose 'Winner' / 'Loser' columns
    deep-link to boxer pages -- the densest single source of relevant
    fighters for the 2018-2025 window.
    """
    found: dict[str, dict] = {}
    for url in YEAR_PAGES:
        year = url.rsplit("/", 1)[-1].split("_")[0]
        try:
            html = sess.get(url, f"_wiki_year_{year}")
        except RuntimeError as e:
            print(f"  [cap] {e}", file=sys.stderr)
            return found
        if not html:
            continue
        for slug, name in _harvest_links(html):
            if slug in found:
                continue
            found[slug] = {
                "boxer_id": slug,
                "name": name,
                "wiki_url": f"{WIKI_BASE}/wiki/{slug}",
                "weight_class_primary": "",
                "discovery_source": f"wiki_year_{year}",
            }
        print(f"  year {year}: {len(found)} cumulative roster entries")
    return found


def discover_from_extra_lists(sess: sb.PoliteSession) -> dict[str, dict]:
    """Pull extra candidates from the alphabetical 'List_of_*_boxers' pages."""
    found: dict[str, dict] = {}
    for url in EXTRA_LIST_PAGES:
        key = "_wiki_" + url.rsplit("/", 1)[-1].lower()
        try:
            html = sess.get(url, key)
        except RuntimeError as e:
            print(f"  [cap] {e}", file=sys.stderr)
            return found
        if not html:
            continue
        for slug, name in _harvest_links(html):
            if slug in found:
                continue
            found[slug] = {
                "boxer_id": slug,
                "name": name,
                "wiki_url": f"{WIKI_BASE}/wiki/{slug}",
                "weight_class_primary": "",
                "discovery_source": f"wiki_{url.rsplit('/', 1)[-1]}",
            }
    return found


# --------------------------------------------------------------------------
# Fighter prioritisation -- aim for boxers actually in PBO bouts
# --------------------------------------------------------------------------
def pbo_fighter_appearances(pbo_csv: Path) -> list[tuple[str, int]]:
    p = pd.read_csv(pbo_csv)
    counter: Counter[str] = Counter()
    for col in ("fighter_a", "fighter_b"):
        for n in p[col].dropna():
            counter[str(n)] += 1
    return counter.most_common()


# --------------------------------------------------------------------------
# Roster merge + deep-scrape pipeline
# --------------------------------------------------------------------------
def build_roster(sess: sb.PoliteSession) -> dict[str, dict]:
    """Merge multiple discovery sources into a single roster keyed by slug."""
    roster: dict[str, dict] = {}

    # 1) Existing curated roster from the original scraper -- already cached.
    for r in sb.fetch_active_boxers(sess, limit=500):
        roster[r["boxer_id"]] = r
    print(f"  champions/p4p source -> {len(roster)} boxers")

    # 2) Year-in-boxing pages 2018..2025 -- highest-relevance for our PBO window.
    year_found = discover_from_year_pages(sess)
    for slug, r in year_found.items():
        roster.setdefault(slug, r)
    print(f"  + year_in_boxing pages -> {len(roster)} boxers cumulative")

    # 3) Alphabetical list pages -- only if budget allows; skip for now.
    # (Each is a big page that adds thousands of low-relevance links.)

    # 4) Synthesise candidates from PBO top fighters who aren't yet in
    # the roster -- guess the slug; the fetch will simply 404 silently
    # if Wikipedia doesn't have a page (and we won't pay a request twice
    # because of caching).
    pbo_csv = ROOT / "data" / "processed" / "pbo_results.csv"
    if pbo_csv.exists():
        appearances = pbo_fighter_appearances(pbo_csv)
        existing_slugs = set(roster.keys())
        existing_names_norm = {
            normalize_name(r["name"]) for r in roster.values()
        }
        added = 0
        for name, _count in appearances:
            n_norm = normalize_name(name)
            if n_norm in existing_names_norm:
                continue
            slug = slugify_for_wiki(name)
            if slug in existing_slugs:
                continue
            roster[slug] = {
                "boxer_id": slug,
                "name": name,
                "wiki_url": f"{WIKI_BASE}/wiki/{slug}",
                "weight_class_primary": "",
                "discovery_source": "pbo_top_fighter",
            }
            existing_slugs.add(slug)
            existing_names_norm.add(n_norm)
            added += 1
            if added >= 1500:
                break
        print(f"  + pbo_top_fighter synth -> +{added} candidates "
              f"(roster total {len(roster)})")
    return roster


def prioritise(roster: dict[str, dict],
               appearances: dict[str, int]) -> list[dict]:
    """Sort roster: PBO top-appearance fighters first, then champions,
    then year-page discoveries, then synthesised tails."""
    def key(r: dict) -> tuple[int, int, str]:
        ncount = appearances.get(normalize_name(r["name"]), 0)
        # Higher PBO appearance = scrape sooner. Champions get a small bonus.
        src_bonus = {
            "wiki_current_champions": 5,
            "wiki_p4p": 3,
        }.get(r.get("discovery_source", ""), 0)
        # negate so largest sorts first
        return (-(ncount + src_bonus), -src_bonus, r["name"])
    return sorted(roster.values(), key=key)


def deep_scrape(sess: sb.PoliteSession,
                ordered_roster: list[dict],
                target_n: int) -> tuple[list[dict], list[dict], list[str]]:
    boxers_out: list[dict] = []
    fights_out: list[dict] = []
    skipped: list[str] = []
    scraped = 0
    for r in ordered_roster:
        if scraped >= target_n:
            break
        if sess.request_count >= sess.request_cap - 1:
            print(f"  [stop] approaching request cap "
                  f"({sess.request_count}/{sess.request_cap})",
                  file=sys.stderr)
            break
        slug = r["boxer_id"]
        name = r["name"]
        try:
            prof = sb.fetch_profile(sess, slug)
            fights = sb.fetch_fight_record(sess, slug, name)
        except RuntimeError as e:
            print(f"  [cap] {e}", file=sys.stderr)
            break
        if not prof and not fights:
            skipped.append(slug)
            continue
        if prof:
            boxers_out.append({**r, **prof})
        # Tag each fight with source slug for the join debug trail.
        for f in fights:
            f["source_url"] = f"{WIKI_BASE}/wiki/{slug}"
        fights_out.extend(fights)
        scraped += 1
        if scraped % 10 == 0:
            print(f"    [{scraped}/{target_n}] reqs={sess.request_count} "
                  f"fights_so_far={len(fights_out)}")
    return boxers_out, fights_out, skipped


# --------------------------------------------------------------------------
# Date parsing for Wikipedia fight-record cells
# --------------------------------------------------------------------------
DATE_FORMATS = (
    "%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y", "%Y-%m-%d",
)


def parse_date(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Wikipedia sometimes prefixes with weird tokens like "(age 27)" -- strip.
    cleaned = re.sub(r"\(.*?\)", "", s).strip()
    if cleaned != s:
        return parse_date(cleaned)
    # Last resort: pull a YYYY-MM-DD substring.
    m = re.search(r"\d{4}-\d{2}-\d{2}", s)
    return m.group(0) if m else ""


# --------------------------------------------------------------------------
# Outputs
# --------------------------------------------------------------------------
ROSTER_FIELDS = [
    "boxer_id", "name", "weight_class_primary",
    "wiki_url", "discovery_source",
]


def write_csv(path: Path, rows, fields: list[str]) -> int:
    rows = list(rows)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)


def append_csv(path: Path, rows, fields: list[str]) -> int:
    rows = list(rows)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)


# --------------------------------------------------------------------------
# Validation join
# --------------------------------------------------------------------------
def join_validation(fights_csv: Path, pbo_csv: Path) -> dict:
    pbo = pd.read_csv(pbo_csv)
    fights = pd.read_csv(fights_csv)

    # Normalise PBO names + dates.
    pbo["fa_norm"] = pbo["fighter_a"].fillna("").map(normalize_name)
    pbo["fb_norm"] = pbo["fighter_b"].fillna("").map(normalize_name)
    pbo["event_date_dt"] = pd.to_datetime(pbo["event_date"], errors="coerce")

    # Normalise scraped fights.
    fights["a_norm"] = fights["boxer_a"].fillna("").map(normalize_name)
    fights["b_norm"] = fights["boxer_b"].fillna("").map(normalize_name)
    fights["fight_date_iso"] = fights["fight_date"].fillna("").map(parse_date)
    fights["fight_dt"] = pd.to_datetime(
        fights["fight_date_iso"], errors="coerce"
    )
    fights = fights.dropna(subset=["fight_dt"])
    fights = fights[(fights["a_norm"] != "") & (fights["b_norm"] != "")]

    # Index fights by canonical pair-key (sorted) -> list of (date, winner).
    pair_index: dict[tuple[str, str], list[tuple[pd.Timestamp, str]]] = defaultdict(list)
    for _, row in fights.iterrows():
        a, b = row["a_norm"], row["b_norm"]
        pair = tuple(sorted((a, b)))
        pair_index[pair].append((row["fight_dt"], row.get("winner") or ""))

    matched = 0
    matched_with_winner = 0
    for _, row in pbo.iterrows():
        if pd.isna(row["event_date_dt"]) or not row["fa_norm"] or not row["fb_norm"]:
            continue
        pair = tuple(sorted((row["fa_norm"], row["fb_norm"])))
        cands = pair_index.get(pair)
        if not cands:
            continue
        for fdt, winner in cands:
            if abs((fdt - row["event_date_dt"]).days) <= 1:
                matched += 1
                if winner:
                    matched_with_winner += 1
                break

    return {
        "pbo_total": int(len(pbo)),
        "matched": matched,
        "matched_with_winner": matched_with_winner,
        "match_pct": round(100 * matched / max(1, len(pbo)), 2),
        "winner_pct": round(100 * matched_with_winner / max(1, len(pbo)), 2),
        "scraped_fights_total": int(len(fights)),
    }


def coverage_misses(fights_csv: Path, pbo_csv: Path, top_k: int = 25) -> list[tuple[str, int]]:
    pbo = pd.read_csv(pbo_csv)
    fights = pd.read_csv(fights_csv)
    seen = set()
    for col in ("boxer_a", "boxer_b"):
        for n in fights[col].dropna():
            seen.add(normalize_name(str(n)))
    counter: Counter[str] = Counter()
    for col in ("fighter_a", "fighter_b"):
        for n in pbo[col].dropna():
            counter[str(n)] += 1
    misses = [(n, c) for n, c in counter.most_common()
              if normalize_name(n) not in seen]
    return misses[:top_k]


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main() -> None:
    t0 = time.time()
    sess = sb.PoliteSession(request_cap=REQUEST_CAP)

    # Pre-cached HTML from the prior 50-cap run will short-circuit
    # all the existing _wiki_* and profile_* hits, so we burn the budget
    # on NEW boxer pages.
    print(f"[expand] starting; request_cap={sess.request_cap}, "
          f"profile_target={PROFILE_TARGET}")

    # Build appearance index for prioritisation.
    pbo_csv = ROOT / "data" / "processed" / "pbo_results.csv"
    appear_norm: dict[str, int] = {}
    if pbo_csv.exists():
        for name, count in pbo_fighter_appearances(pbo_csv):
            n = normalize_name(name)
            if n:
                appear_norm[n] = max(appear_norm.get(n, 0), count)

    # Discovery
    roster = build_roster(sess)
    print(f"[expand] roster size after discovery: {len(roster)}")
    print(f"[expand] requests used in discovery: {sess.request_count}")

    # Persist the roster up front so we have an artefact even if the
    # deep scrape blows the budget.
    roster_path = RAW_DIR / "boxer_roster.csv"
    write_csv(roster_path, roster.values(), ROSTER_FIELDS)
    print(f"  wrote roster -> {roster_path} ({len(roster)} rows)")

    # Prioritise + deep scrape
    ordered = prioritise(roster, appear_norm)
    print(f"[expand] beginning deep scrape (target={PROFILE_TARGET})")
    boxers_out, fights_out, skipped = deep_scrape(sess, ordered, PROFILE_TARGET)
    print(f"[expand] deep scrape produced "
          f"{len(boxers_out)} boxers, {len(fights_out)} fights, "
          f"skipped {len(skipped)} empty pages")
    print(f"[expand] total requests this session: {sess.request_count}")

    # Date-normalise scraped fights before we write out
    for f in fights_out:
        iso = parse_date(f.get("fight_date", ""))
        if iso:
            f["fight_date"] = iso

    # ---- Outputs ----
    boxers_path = RAW_DIR / "boxrec_boxers.csv"
    fights_path = RAW_DIR / "boxrec_fights.csv"
    results_path = RAW_DIR / "boxer_results.csv"
    aliases_path = RAW_DIR / "boxer_name_aliases.json"

    # Merge boxer profiles (existing + new), de-dup by boxer_id
    existing_boxers = {}
    if boxers_path.exists():
        with boxers_path.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing_boxers[row["boxer_id"]] = row
    for r in boxers_out:
        existing_boxers[r["boxer_id"]] = {
            **existing_boxers.get(r["boxer_id"], {}), **r
        }
    write_csv(boxers_path, existing_boxers.values(), sb.BOXER_FIELDS)
    print(f"  wrote {len(existing_boxers)} boxers -> {boxers_path}")

    # Append fights to existing log (no de-dup -- a re-run would duplicate,
    # but we cache HTML so that's an explicit choice).
    # Better: rebuild from cache to avoid duplication. We'll compute a
    # de-dup key (date, sorted-pair, method) and merge.
    existing_fights: list[dict] = []
    if fights_path.exists():
        with fights_path.open(encoding="utf-8") as f:
            existing_fights = list(csv.DictReader(f))
    seen_keys = set()
    merged_fights: list[dict] = []
    for f in existing_fights + fights_out:
        a = normalize_name(f.get("boxer_a", ""))
        b = normalize_name(f.get("boxer_b", ""))
        d = parse_date(f.get("fight_date", "")) or f.get("fight_date", "")
        key = (d, tuple(sorted((a, b))), f.get("method", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        merged_fights.append(f)
    write_csv(fights_path, merged_fights, sb.FIGHT_FIELDS)
    print(f"  wrote {len(merged_fights)} merged fights -> {fights_path}")

    # boxer_results.csv -- the joined-ready schema requested by the brief
    results_fields = [
        "fight_date", "boxer_a", "boxer_b", "winner", "method", "round",
        "weight_class", "location", "source_url",
    ]
    write_csv(results_path, merged_fights, results_fields)
    print(f"  wrote {len(merged_fights)} rows -> {results_path}")

    # Name aliases JSON
    aliases: dict[str, list[str]] = defaultdict(list)
    for r in existing_boxers.values():
        norm = normalize_name(r["name"])
        if norm:
            aliases[norm].append(r["name"])
            slug = r["boxer_id"].replace("_", " ")
            slug_norm = normalize_name(slug)
            if slug_norm and slug_norm != norm:
                aliases[norm].append(slug)
    aliases_serialisable = {k: sorted(set(v)) for k, v in aliases.items()}
    aliases_path.write_text(
        json.dumps(aliases_serialisable, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"  wrote {len(aliases_serialisable)} aliases -> {aliases_path}")

    # ---- Validation join ----
    print("\n[validation] joining boxer_results.csv to PBO archive...")
    stats = join_validation(results_path, pbo_csv)
    print(json.dumps(stats, indent=2))

    misses = coverage_misses(results_path, pbo_csv, top_k=15)
    print("\n[validation] top fighters NOT yet in scraped record table:")
    for n, c in misses:
        print(f"  {c:3d}  {n}")

    elapsed = time.time() - t0
    print(f"\n[expand] done in {elapsed:.1f}s; "
          f"network requests: {sess.request_count}/{sess.request_cap}")


if __name__ == "__main__":
    main()

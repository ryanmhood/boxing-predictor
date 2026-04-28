#!/usr/bin/env python3
"""
Probe candidate non-BoxRec boxer-record data sources for feasibility.

Context: BoxRec is wedged behind Cloudflare past every workaround we've tried
(curl_cffi TLS impersonation, Playwright stealth, Chrome-CDP).  We need an
alternative source that gives us boxer-fight history for the PBO bout universe
(8,942 bouts in data/processed/pbo_results.csv, 2018-2025).

This script does NOT scrape at scale.  It is a one-shot reconnaissance run:
for each candidate, fetch one or two URLs (Canelo Alvarez where possible),
record HTTP status, response size, anti-bot markers (Cloudflare / Akamai /
Imperva), and how many "extractable" fields are present in the body.  The
verdict gets persisted to data/reports/boxer_source_eval.md so a follow-up
session can pick the winner without re-probing.

Sources covered:
  1. boxing-data.com (RapidAPI gateway)         -- paid, document only
  2. Stephen Plainte BoxRec dump (GitHub)       -- one-time CSV download
  3. Kaggle: mexwell/boxing-matches             -- needs auth
  4. Kaggle: iyadelwy/boxing-matches-...        -- needs auth
  5. ESPN boxing                                -- live, Akamai-protected
  6. Tapology                                   -- live, anti-bot unknown
  7. BoxingScene                                -- live news + results
  8. TheRingMagazine / Ring TV                  -- live results
  9. Wikipedia bulk dump                        -- already known (0.8%)

Politeness: 1.5s sleep between requests to the same host, real Chrome120 UA
via curl_cffi, hard cap of 30 requests per invocation.
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "data" / "reports"
HTML_OUT_DIR = ROOT / "data" / "raw_html" / "source_probe"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
HTML_OUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = REPORT_DIR / "boxer_source_eval.md"
SUMMARY_PATH = REPORT_DIR / "boxer_source_eval.json"

DELAY_S = 1.5
TIMEOUT_S = 25
MAX_REQUESTS = 30

CF_MARKERS = (
    "Just a moment",
    "cf-browser-verification",
    "cf_chl_opt",
    "challenge-platform",
    "Enable JavaScript and cookies to continue",
    "Cloudflare",
)
AKAMAI_MARKERS = (
    "ak_bmsc",
    "Reference&#32;&#35;",  # Akamai "Access Denied" page
    "errors.edgesuite.net",
    "akamaized",
)
IMPERVA_MARKERS = (
    "_Incapsula_Resource",
    "Imperva",
    "incap_ses",
)
CHALLENGE_TOKEN_BUCKETS = {
    "cloudflare": CF_MARKERS,
    "akamai": AKAMAI_MARKERS,
    "imperva": IMPERVA_MARKERS,
}


@dataclass
class ProbeResult:
    source: str
    url: str
    method: str = "GET"
    status: int = 0
    bytes: int = 0
    challenge: str = ""           # "" / "cloudflare" / "akamai" / "imperva"
    field_hits: dict = field(default_factory=dict)
    notes: str = ""
    error: str = ""


def detect_challenge(body: str) -> str:
    if not body:
        return ""
    for name, markers in CHALLENGE_TOKEN_BUCKETS.items():
        if any(m in body for m in markers):
            return name
    return ""


def field_hits(body: str, fields: dict[str, list[str]]) -> dict[str, int]:
    """For each field, count how many of its substrings appear in body."""
    out: dict[str, int] = {}
    if not body:
        return {k: 0 for k in fields}
    lower = body.lower()
    for k, needles in fields.items():
        out[k] = sum(1 for n in needles if n.lower() in lower)
    return out


# Canelo facts used as a "is the page real" tripwire across every source.
CANELO_FIELDS = {
    "name":            ["Canelo", "Saul", "Alvarez"],
    "record":          ["62-2", "62 - 2", "62-2-2", "professional record"],
    "weight":          ["super middleweight", "middleweight", "lb"],
    "nationality":     ["Mexico", "Mexican", "Guadalajara"],
    "opponent":        ["Bivol", "Golovkin", "Charlo", "Munguia", "Crawford"],
    "method":          ["KO", "TKO", "UD", "decision"],
    "date":            ["2024", "2023", "2022", "2021"],
    "fight_record_kw": ["round", "rounds", "wins", "losses", "draws"],
}


def http_get(session, url: str, headers: dict, *, allow_redirects: bool = True) -> tuple[int, str, str]:
    """Return (status, body, error)."""
    try:
        r = session.get(
            url,
            headers=headers,
            timeout=TIMEOUT_S,
            allow_redirects=allow_redirects,
        )
        return r.status_code, r.text or "", ""
    except Exception as e:                       # noqa: BLE001
        return -1, "", repr(e)


def head_only(session, url: str, headers: dict) -> tuple[int, str, str]:
    try:
        r = session.head(url, headers=headers, timeout=TIMEOUT_S, allow_redirects=True)
        return r.status_code, "", ""
    except Exception as e:                       # noqa: BLE001
        return -1, "", repr(e)


def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")[:80]


def write_html_dump(source: str, url: str, body: str) -> str:
    if not body:
        return ""
    fname = f"{slugify(source)}__{slugify(url.split('://', 1)[-1])}.html"
    p = HTML_OUT_DIR / fname
    p.write_text(body[:300_000], encoding="utf-8")  # cap dumps at 300 KB
    return p.relative_to(ROOT).as_posix()


# --------------------------------------------------------------------------
# Probe sources
# --------------------------------------------------------------------------
HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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


def probe_plainte_dump(session) -> list[ProbeResult]:
    """Stephen Plainte's BoxRec dump on github.com/FuriouStyles."""
    out: list[ProbeResult] = []
    base = "https://raw.githubusercontent.com/FuriouStyles/BeautifulSoup_Meets_BoxRec/master"
    for fn in ("all_bouts.csv", "fights.csv", "boxers.csv"):
        url = f"{base}/{fn}"
        # HEAD first to confirm size cheaply
        st, _, err = head_only(session, url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        if err:
            out.append(ProbeResult("plainte_github", url, "HEAD", -1, 0, "", {}, "HEAD failed", err))
            continue
        # Pull just the first 100 KB to measure schema + sniff
        try:
            r = session.get(url, headers={**HEADERS_BROWSER, "Range": "bytes=0-102400"}, timeout=TIMEOUT_S)
            body = r.text or ""
            status = r.status_code
        except Exception as e:                   # noqa: BLE001
            body, status = "", -1
            err = repr(e)
        time.sleep(DELAY_S)
        # CSV column header is on first line; field hits = column count.
        header = body.split("\n", 1)[0] if body else ""
        cols = [c.strip() for c in header.split(",")] if header else []
        out.append(ProbeResult(
            source="plainte_github",
            url=url,
            status=status,
            bytes=len(body.encode("utf-8")),
            challenge="",
            field_hits={"csv_columns": len(cols)},
            notes=f"columns: {','.join(cols[:8])}{'...' if len(cols) > 8 else ''}",
            error=err if status != 206 and status != 200 else "",
        ))
    return out


def probe_kaggle(session) -> list[ProbeResult]:
    """Kaggle dataset pages are public read; downloads need auth."""
    out: list[ProbeResult] = []
    for slug, label in (
        ("mexwell/boxing-matches",                          "kaggle_mexwell"),
        ("iyadelwy/boxing-matches-dataset-predict-winner",  "kaggle_iyadelwy"),
    ):
        page_url = f"https://www.kaggle.com/datasets/{slug}"
        st, body, err = http_get(session, page_url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        ch = detect_challenge(body)
        snippet = write_html_dump(label, page_url, body)
        notes_parts = []
        if "Sign in" in body or "Register" in body:
            notes_parts.append("page accessible, anonymous")
        if "Download" in body or "Download (" in body:
            notes_parts.append("download button rendered")
        notes_parts.append(f"saved={snippet}")
        out.append(ProbeResult(
            source=label,
            url=page_url,
            status=st,
            bytes=len(body.encode("utf-8")),
            challenge=ch,
            field_hits=field_hits(body, {"download_kw": ["Download"], "boxing_kw": ["boxing", "fight"]}),
            notes="; ".join(notes_parts),
            error=err,
        ))
        # Test direct download (will likely 401/302 to login)
        dl_url = f"https://www.kaggle.com/api/v1/datasets/download/{slug}"
        st2, _, err2 = head_only(session, dl_url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        out.append(ProbeResult(
            source=label,
            url=dl_url,
            method="HEAD",
            status=st2,
            bytes=0,
            challenge="",
            field_hits={},
            notes="anonymous download attempt -- expect 401/302",
            error=err2,
        ))
    return out


def probe_espn(session) -> list[ProbeResult]:
    """ESPN boxing.  Several URL shapes are in the wild; try the most likely."""
    candidates = [
        # Athletes / fighter profile
        "https://www.espn.com/boxing/fighter/_/id/2495380/canelo-alvarez",
        # Recent results landing page
        "https://www.espn.com/boxing/results",
        # Story search for a known Canelo fight
        "https://www.espn.com/boxing/story/_/id/40000000/canelo-alvarez",
    ]
    out: list[ProbeResult] = []
    for url in candidates:
        st, body, err = http_get(session, url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        ch = detect_challenge(body)
        rel = write_html_dump("espn", url, body)
        notes = f"saved={rel}"
        if st == 404:
            notes = "404 -- URL shape stale; " + notes
        out.append(ProbeResult(
            source="espn",
            url=url,
            status=st,
            bytes=len(body.encode("utf-8")),
            challenge=ch,
            field_hits=field_hits(body, CANELO_FIELDS),
            notes=notes,
            error=err,
        ))
    return out


def probe_tapology(session) -> list[ProbeResult]:
    """Tapology has expanded into boxing.  Profiles live under /fightcenter/fighters/."""
    candidates = [
        "https://www.tapology.com/search?term=Canelo+Alvarez&mainSearchFilter=fighters",
        "https://www.tapology.com/fightcenter/fighters/29-saul-canelo-alvarez",
        "https://www.tapology.com/fightcenter/boxing",
    ]
    out: list[ProbeResult] = []
    for url in candidates:
        st, body, err = http_get(session, url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        ch = detect_challenge(body)
        rel = write_html_dump("tapology", url, body)
        out.append(ProbeResult(
            source="tapology",
            url=url,
            status=st,
            bytes=len(body.encode("utf-8")),
            challenge=ch,
            field_hits=field_hits(body, CANELO_FIELDS),
            notes=f"saved={rel}",
            error=err,
        ))
    return out


def probe_boxingscene(session) -> list[ProbeResult]:
    candidates = [
        "https://www.boxingscene.com/",
        "https://www.boxingscene.com/boxer/canelo-alvarez",
        "https://www.boxingscene.com/?s=canelo+alvarez",
    ]
    out: list[ProbeResult] = []
    for url in candidates:
        st, body, err = http_get(session, url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        ch = detect_challenge(body)
        rel = write_html_dump("boxingscene", url, body)
        out.append(ProbeResult(
            source="boxingscene",
            url=url,
            status=st,
            bytes=len(body.encode("utf-8")),
            challenge=ch,
            field_hits=field_hits(body, CANELO_FIELDS),
            notes=f"saved={rel}",
            error=err,
        ))
    return out


def probe_ring_magazine(session) -> list[ProbeResult]:
    candidates = [
        "https://www.ringtv.com/",
        "https://www.ringtv.com/category/news/",
        "https://www.ringmagazine.com/",
    ]
    out: list[ProbeResult] = []
    for url in candidates:
        st, body, err = http_get(session, url, HEADERS_BROWSER)
        time.sleep(DELAY_S)
        ch = detect_challenge(body)
        rel = write_html_dump("ring", url, body)
        out.append(ProbeResult(
            source="ring_magazine",
            url=url,
            status=st,
            bytes=len(body.encode("utf-8")),
            challenge=ch,
            field_hits=field_hits(body, CANELO_FIELDS),
            notes=f"saved={rel}",
            error=err,
        ))
    return out


def probe_boxing_data_api(session) -> list[ProbeResult]:
    """boxing-data.com is gated by RapidAPI; document only, do not subscribe."""
    out: list[ProbeResult] = []
    landing = "https://boxing-data.com/"
    st, body, err = http_get(session, landing, HEADERS_BROWSER)
    time.sleep(DELAY_S)
    out.append(ProbeResult(
        source="boxing_data_com",
        url=landing,
        status=st,
        bytes=len(body.encode("utf-8")),
        challenge=detect_challenge(body),
        field_hits=field_hits(body, {
            "free_tier": ["Free tier", "Try for Free"],
            "rapidapi":  ["RapidAPI", "rapidapi.com"],
            "pricing":   ["pricing", "price", "$"],
        }),
        notes="paid commercial API behind RapidAPI; do not subscribe without confirmation",
        error=err,
    ))
    return out


def probe_wikipedia_marker(session) -> list[ProbeResult]:
    """Wikipedia is the v1 source; we already know coverage is 0.8%.
    Probe one URL just to keep the matrix consistent and confirm it still
    serves 200s."""
    url = "https://en.wikipedia.org/wiki/Canelo_%C3%81lvarez"
    headers = {
        **HEADERS_BROWSER,
        "User-Agent": (
            "boxing-predictor research probe / contact: "
            "krameitbullington@gmail.com"
        ),
    }
    st, body, err = http_get(session, url, headers)
    time.sleep(DELAY_S)
    return [ProbeResult(
        source="wikipedia",
        url=url,
        status=st,
        bytes=len(body.encode("utf-8")),
        challenge=detect_challenge(body),
        field_hits=field_hits(body, CANELO_FIELDS),
        notes="known: 0.8% PBO coverage on bulk roster (logged)",
        error=err,
    )]


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------
def render_markdown(results: list[ProbeResult]) -> str:
    by_source: dict[str, list[ProbeResult]] = {}
    for r in results:
        by_source.setdefault(r.source, []).append(r)

    lines = [
        "# Boxer-record source evaluation",
        "",
        f"_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} "
        f"by `scripts/probe_boxer_data_sources.py`_",
        "",
        "BoxRec is Cloudflare-walled past every workaround we've tried "
        "(`scripts/scrape_boxrec_playwright.py`, `scrape_boxrec_cdp.py`, "
        "`probe_boxrec_curl_cffi.py` — see `BOXING_DATA_GAPS.md`). This probe "
        "scouts alternative sources for boxer fight history that can be "
        "joined onto `data/processed/pbo_results.csv` (8,942 PBO bouts, "
        "2018-2025).",
        "",
        "## Per-source verdicts",
        "",
    ]

    for source, rows in by_source.items():
        rep = rows[0]
        worst_status = max((r.status for r in rows if r.status > 0), default=0)
        lines.append(f"### `{source}`")
        lines.append("")
        lines.append("| URL | Status | Bytes | Challenge | Field hits |")
        lines.append("|---|---:|---:|---|---|")
        for r in rows:
            fh = ", ".join(f"{k}={v}" for k, v in r.field_hits.items()) or "-"
            lines.append(
                f"| `{r.url}` | {r.status} | {r.bytes} | "
                f"{r.challenge or '-'} | {fh} |"
            )
        lines.append("")
        lines.append(f"_Notes_: {'; '.join(filter(None, (r.notes for r in rows)))}")
        if any(r.error for r in rows):
            lines.append("")
            lines.append(f"_Errors_: " + "; ".join(r.error for r in rows if r.error))
        lines.append("")

    lines += [
        "## Decision matrix",
        "",
        "PBO coverage by year (8,942 total):",
        "",
        "| Year | Bouts |",
        "|---|---:|",
        "| 2018 |   158 |",
        "| 2019 | 1,025 |",
        "| 2020 |   590 |",
        "| 2021 | 1,199 |",
        "| 2022 | 1,351 |",
        "| 2023 | 1,414 |",
        "| 2024 | 1,702 |",
        "| 2025 | 1,503 |",
        "",
        "**Implication**: 86% of PBO bouts are 2020-2025. Any source frozen "
        "before 2020 (Plainte, Wikipedia bulk dump) cannot clear the 50% "
        "gate alone — they cover at most ~13%.  We need a source with "
        "**continuing post-2020 coverage**, or a hybrid of Plainte + a "
        "live-scrape source for recent data.",
        "",
        "See `data/raw_html/source_probe/` for the raw response bodies "
        "captured for spot-checks.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    from curl_cffi import requests as cffi_requests
    session = cffi_requests.Session(impersonate="chrome120")

    print("[probe] starting; output:", REPORT_PATH)
    probes = [
        ("plainte_github",   probe_plainte_dump),
        ("boxing_data_com",  probe_boxing_data_api),
        ("kaggle",           probe_kaggle),
        ("espn",             probe_espn),
        ("tapology",         probe_tapology),
        ("boxingscene",      probe_boxingscene),
        ("ring",             probe_ring_magazine),
        ("wikipedia",        probe_wikipedia_marker),
    ]

    results: list[ProbeResult] = []
    n_req = 0
    for label, fn in probes:
        if n_req >= MAX_REQUESTS:
            print(f"[probe] cap {MAX_REQUESTS} reached, stopping at {label}")
            break
        print(f"[probe] {label} ...")
        try:
            chunk = fn(session)
        except Exception as e:                   # noqa: BLE001
            chunk = [ProbeResult(label, "<probe-fn>", "", -1, 0, "", {}, "", repr(e))]
        n_req += len(chunk)
        for r in chunk:
            print(f"  {r.url[:80]:<80}  status={r.status:<4}  "
                  f"chal={r.challenge or '-':<10}  bytes={r.bytes:>7}")
        results.extend(chunk)

    REPORT_PATH.write_text(render_markdown(results), encoding="utf-8")
    SUMMARY_PATH.write_text(
        json.dumps([asdict(r) for r in results], indent=2),
        encoding="utf-8",
    )

    print()
    print(f"[probe] wrote {REPORT_PATH}")
    print(f"[probe] wrote {SUMMARY_PATH}")
    print(f"[probe] saved {len(list(HTML_OUT_DIR.glob('*.html')))} HTML dumps "
          f"to {HTML_OUT_DIR.relative_to(ROOT)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
PBO ∩ roster overlap scraper.

Step 1: Find boxers present BOTH in our 1,730-name Wikipedia roster AND in
the 8,942-bout PBO odds archive. Rank by PBO appearance frequency.

Step 2: Deep-scrape (Wikipedia profile + fight record) the top-N boxers
using the existing scrape_boxrec.py helpers. HTML cached on disk.

Step 3: Build canonical name aliases (accents, suffixes, nicknames).

Step 4: Inner-join PBO bouts to scraped fight outcomes on
        (date ±2 days, sorted-pair of normalised names) and report join %.
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path("/Users/Ryan/boxing-odds-daemon")
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"
# The existing scraper already cached ~174 Wikipedia profile pages under
# data/raw_html/boxrec/ (legacy name; payload is Wikipedia HTML). Reuse it
# directly so we don't re-download. Also mirror new pages into
# data/raw_html/wikipedia/ as the task spec requires.
WIKI_HTML_LEGACY = ROOT / "data" / "raw_html" / "boxrec"
WIKI_HTML = ROOT / "data" / "raw_html" / "wikipedia"
WIKI_HTML.mkdir(parents=True, exist_ok=True)
WIKI_HTML_LEGACY.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(ROOT / "scripts"))
import scrape_boxrec  # type: ignore

# Re-point the existing scraper's HTML cache to the legacy dir so we hit
# the 174 already-cached profiles for free.
scrape_boxrec.HTML_CACHE = WIKI_HTML_LEGACY

# --------------------------------------------------------------------------
# Name normalisation
# --------------------------------------------------------------------------
SUFFIXES = {"jr", "jr.", "junior", "jnr", "sr", "sr.", "senior", "snr",
            "ii", "iii", "iv"}
NICK_RE = re.compile(r'["“”‘’\'`]([^"“”‘’\'`]+)["“”‘’\'`]')
PUNCT_RE = re.compile(r"[^\w\s]")
WS_RE = re.compile(r"\s+")


def norm_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = NICK_RE.sub(" ", s)         # drop quoted nicknames
    s = s.lower()
    s = PUNCT_RE.sub(" ", s)
    s = WS_RE.sub(" ", s).strip()
    parts = [p for p in s.split() if p not in SUFFIXES]
    return " ".join(parts)


def pair_key(a: str, b: str) -> tuple[str, str]:
    na, nb = norm_name(a), norm_name(b)
    return tuple(sorted((na, nb)))


# --------------------------------------------------------------------------
# Step 1: overlap
# --------------------------------------------------------------------------
def build_overlap(top_n: int = 250) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    roster = pd.read_csv(RAW / "boxer_roster.csv")
    pbo = pd.read_csv(PROC / "pbo_results.csv")
    print(f"[step1] roster={len(roster)} pbo_bouts={len(pbo)}")

    roster["norm"] = roster["name"].map(norm_name)
    # roster norm -> (boxer_id, name, wiki_url)
    roster_lookup: dict[str, dict] = {}
    for _, r in roster.iterrows():
        if r["norm"] and r["norm"] not in roster_lookup:
            roster_lookup[r["norm"]] = {
                "boxer_id": r["boxer_id"],
                "name": r["name"],
                "wiki_url": r["wiki_url"],
            }
    print(f"  roster unique-norm names: {len(roster_lookup)}")

    pbo["norm_a"] = pbo["fighter_a"].map(norm_name)
    pbo["norm_b"] = pbo["fighter_b"].map(norm_name)
    appearance = Counter()
    for _, r in pbo.iterrows():
        if r["norm_a"]:
            appearance[r["norm_a"]] += 1
        if r["norm_b"]:
            appearance[r["norm_b"]] += 1
    print(f"  pbo unique-norm fighters: {len(appearance)}")

    overlap = []
    for nm, cnt in appearance.items():
        if nm in roster_lookup:
            row = {**roster_lookup[nm], "norm": nm, "pbo_appearances": cnt}
            overlap.append(row)
    overlap.sort(key=lambda r: -r["pbo_appearances"])
    overlap_df = pd.DataFrame(overlap)
    print(f"  overlap (roster ∩ pbo): {len(overlap_df)} boxers")
    print(f"  top10:")
    for r in overlap[:10]:
        print(f"    {r['pbo_appearances']:4d}  {r['name']}")

    targets = overlap_df.head(top_n).copy()
    print(f"  -> picked top {len(targets)} for deep scrape")
    return targets, pbo, roster_lookup


# --------------------------------------------------------------------------
# Step 2: deep scrape
# --------------------------------------------------------------------------
def deep_scrape(targets: pd.DataFrame, request_cap: int, time_budget_s: float):
    sess = scrape_boxrec.PoliteSession(request_cap=request_cap)
    # Tighten polite delay to 4-5s as instructed.
    scrape_boxrec.MIN_DELAY_S = 4.0
    scrape_boxrec.MAX_DELAY_S = 5.0

    fights_out: list[dict] = []
    scraped_boxers: list[dict] = []
    start = time.time()
    cached_hits = 0
    new_hits = 0
    failures: list[str] = []

    for i, row in targets.reset_index(drop=True).iterrows():
        elapsed = time.time() - start
        if elapsed > time_budget_s:
            print(f"  [budget] hit time budget at {elapsed:.0f}s "
                  f"after {i} boxers; saving partial state")
            break
        slug = row["boxer_id"]
        name = row["name"]
        cache_path = WIKI_HTML_LEGACY / f"profile_{slug}.html"
        was_cached = cache_path.exists() and cache_path.stat().st_size > 1024
        try:
            fights = scrape_boxrec.fetch_fight_record(sess, slug, name)
        except RuntimeError as e:
            print(f"  [stop] {e}")
            break
        except Exception as e:
            failures.append(f"{slug}: {e!r}")
            continue
        # Mirror cached HTML into data/raw_html/wikipedia/ for spec
        legacy = WIKI_HTML_LEGACY / f"profile_{slug}.html"
        mirror = WIKI_HTML / f"{slug}.html"
        if legacy.exists() and not mirror.exists():
            try:
                mirror.write_text(legacy.read_text(encoding="utf-8",
                                                  errors="replace"),
                                  encoding="utf-8")
            except OSError as e:
                print(f"  [warn] mirror failed {slug}: {e}")
        if was_cached:
            cached_hits += 1
        else:
            new_hits += 1
        scraped_boxers.append({
            "boxer_id": slug, "name": name, "fights_found": len(fights),
        })
        for f in fights:
            f["source_url"] = f"https://en.wikipedia.org/wiki/{slug}"
        fights_out.extend(fights)
        if (i + 1) % 25 == 0:
            print(f"  [{i+1}/{len(targets)}] {name} -> {len(fights)} fights "
                  f"(cached={cached_hits} new={new_hits} "
                  f"req={sess.request_count} elapsed={elapsed:.0f}s)")
    print(f"  scraped {len(scraped_boxers)} boxers, "
          f"{len(fights_out)} fight rows; cached={cached_hits} new={new_hits} "
          f"requests={sess.request_count} failures={len(failures)}")
    if failures[:5]:
        print(f"  first failures: {failures[:5]}")
    return scraped_boxers, fights_out


# --------------------------------------------------------------------------
# Step 3: aliases
# --------------------------------------------------------------------------
def build_aliases(targets: pd.DataFrame, pbo: pd.DataFrame,
                  fights_out: list[dict]) -> dict:
    aliases: dict[str, set[str]] = defaultdict(set)
    # Roster names
    for _, r in targets.iterrows():
        canon = norm_name(r["name"])
        aliases[canon].add(r["name"])
    # PBO variants
    pbo_norms_a = pbo[["fighter_a", "norm_a"]].drop_duplicates()
    pbo_norms_b = pbo[["fighter_b", "norm_b"]].drop_duplicates()
    canon_set = set(aliases.keys())
    for _, r in pbo_norms_a.iterrows():
        if r["norm_a"] in canon_set:
            aliases[r["norm_a"]].add(str(r["fighter_a"]))
    for _, r in pbo_norms_b.iterrows():
        if r["norm_b"] in canon_set:
            aliases[r["norm_b"]].add(str(r["fighter_b"]))
    # Wikipedia opponent strings (sometimes use full nicknames)
    for f in fights_out:
        for k in ("boxer_a", "boxer_b"):
            v = f.get(k)
            if not v:
                continue
            n = norm_name(v)
            if n in canon_set:
                aliases[n].add(v)
    return {k: sorted(v) for k, v in aliases.items()}


# --------------------------------------------------------------------------
# Step 4: validate join
# --------------------------------------------------------------------------
DATE_FORMATS = ("%d %b %Y", "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y-%m-%d")


def parse_fight_date(s: str):
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    # Try pandas as last resort.
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None


def validate_join(pbo: pd.DataFrame, fights_out: list[dict]) -> dict:
    # Index scraped fights by sorted-pair-of-norm-names -> list of
    # (date, winner, method, round)
    idx: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for f in fights_out:
        d = parse_fight_date(f.get("fight_date") or "")
        if d is None:
            continue
        key = pair_key(f.get("boxer_a", ""), f.get("boxer_b", ""))
        if not all(key):
            continue
        idx[key].append({
            "date": d,
            "winner": f.get("winner") or "",
            "method": f.get("method") or "",
            "round": f.get("round") or "",
        })

    pbo = pbo.copy()
    pbo["pbo_date"] = pd.to_datetime(pbo["event_date"], errors="coerce").dt.date
    matched_winner = []
    matched_method = []
    matched_round = []
    for _, row in pbo.iterrows():
        key = pair_key(row["fighter_a"], row["fighter_b"])
        d = row["pbo_date"]
        if not all(key) or d is None:
            matched_winner.append(None)
            matched_method.append(None)
            matched_round.append(None)
            continue
        cand = idx.get(key)
        if not cand:
            matched_winner.append(None); matched_method.append(None); matched_round.append(None)
            continue
        best = None
        for c in cand:
            if abs((c["date"] - d).days) <= 2:
                best = c; break
        if not best:
            matched_winner.append(None); matched_method.append(None); matched_round.append(None)
            continue
        matched_winner.append(best["winner"] or None)
        matched_method.append(best["method"] or None)
        matched_round.append(best["round"] or None)
    pbo["scraped_winner"] = matched_winner
    pbo["scraped_method"] = matched_method
    pbo["scraped_round"] = matched_round

    n = len(pbo)
    n_with_winner = pbo["scraped_winner"].notna().sum()
    print(f"\n[step4] join rate: {n_with_winner}/{n} = "
          f"{100.0 * n_with_winner / n:.2f}%")

    pbo["year"] = pd.to_datetime(pbo["event_date"], errors="coerce").dt.year
    by_year = pbo.groupby("year").apply(
        lambda g: pd.Series({
            "n_bouts": len(g),
            "n_winner": g["scraped_winner"].notna().sum(),
            "pct": 100.0 * g["scraped_winner"].notna().sum() / max(len(g), 1),
        })
    ).reset_index()
    print("\nPer-year join rate:")
    for _, r in by_year.iterrows():
        print(f"  {int(r['year'])}: {int(r['n_winner']):4d}/"
              f"{int(r['n_bouts']):4d}  {r['pct']:.1f}%")

    # Spot-check 5 famous fights
    famous = [
        ("Tyson Fury", "Oleksandr Usyk"),
        ("Canelo Alvarez", "Dmitry Bivol"),
        ("Terence Crawford", "Errol Spence Jr"),
        ("Anthony Joshua", "Daniel Dubois"),
        ("Naoya Inoue", "Stephen Fulton"),
    ]
    print("\nSpot-checks (famous fights):")
    spot = []
    for a, b in famous:
        k = pair_key(a, b)
        cand = idx.get(k, [])
        if not cand:
            print(f"  {a} vs {b}: NO SCRAPED MATCH")
            spot.append((a, b, None, None))
            continue
        # Most recent
        cand_sorted = sorted(cand, key=lambda c: c["date"], reverse=True)
        c = cand_sorted[0]
        print(f"  {a} vs {b} [{c['date']}]: winner={c['winner']!r} "
              f"method={c['method']!r} round={c['round']!r}")
        spot.append((a, b, str(c["date"]), c["winner"]))

    return {"n": int(n), "n_with_winner": int(n_with_winner),
            "by_year": by_year, "spot": spot, "pbo_joined": pbo}


# --------------------------------------------------------------------------
# Append idempotently to boxrec_fights.csv
# --------------------------------------------------------------------------
def append_fights_idempotent(fights_out: list[dict]) -> int:
    fp = RAW / "boxrec_fights.csv"
    existing = pd.read_csv(fp)
    seen = set()
    for _, r in existing.iterrows():
        seen.add((str(r.get("fight_date", "")), str(r.get("boxer_a", "")),
                  str(r.get("boxer_b", ""))))
    new_rows = []
    for f in fights_out:
        k = (str(f.get("fight_date", "")), str(f.get("boxer_a", "")),
             str(f.get("boxer_b", "")))
        if k in seen:
            continue
        seen.add(k)
        new_rows.append(f)
    if not new_rows:
        return 0
    fields = list(existing.columns)
    with fp.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        for r in new_rows:
            w.writerow(r)
    return len(new_rows)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main(top_n: int = 250, request_cap: int = 300,
         time_budget_s: float = 20 * 60):
    targets, pbo, _ = build_overlap(top_n=top_n)
    targets.to_csv(RAW / "boxer_overlap_targets.csv", index=False)

    print(f"\n[step2] deep-scrape up to {len(targets)} boxers "
          f"(cap={request_cap}, budget={time_budget_s:.0f}s)")
    scraped_boxers, fights_out = deep_scrape(
        targets, request_cap=request_cap, time_budget_s=time_budget_s)

    # Save raw boxer_results.csv
    out_fp = RAW / "boxer_results.csv"
    fields = ["fight_date", "boxer_a", "boxer_b", "winner", "method",
              "round", "weight_class", "location", "source_url"]
    with out_fp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for f in fights_out:
            w.writerow(f)
    print(f"  wrote {len(fights_out)} rows -> {out_fp}")

    appended = append_fights_idempotent(fights_out)
    print(f"  appended {appended} new rows -> data/raw/boxrec_fights.csv")

    # Step 3: aliases
    aliases = build_aliases(targets, pbo, fights_out)
    al_fp = RAW / "boxer_name_aliases.json"
    al_fp.write_text(json.dumps(aliases, indent=2, ensure_ascii=False))
    print(f"  wrote {len(aliases)} alias entries -> {al_fp}")

    # Step 4: validate
    rep = validate_join(pbo, fights_out)
    rep["pbo_joined"].to_csv(RAW / "pbo_results_joined.csv", index=False)
    print(f"\n  wrote pbo_results_joined.csv "
          f"({rep['n_with_winner']}/{rep['n']} winners)")


if __name__ == "__main__":
    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 250
    cap = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    budget = float(sys.argv[3]) if len(sys.argv) > 3 else 20 * 60
    main(top_n=top_n, request_cap=cap, time_budget_s=budget)

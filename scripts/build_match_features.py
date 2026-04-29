"""Pair per-boxer features into per-match diff features.

Two outputs:
  1. data/processed/boxer_match_features.parquet
       - Training/eval set: every union bout, exactly once per (date, pair).
       - Schema: fight_date, year, a_id, b_id, a_wins (label), <diff features>,
         and both fighters' raw features.
       - Canonical ordering: a = lower boxer_id, b = higher.
  2. data/processed/pbo_scoring_features.parquet
       - PBO bouts joined via name+date to union, with same diff features.
       - For backtest. Includes price_a, price_b, market_prob_a, winner_side.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
UNION_CSV = REPO / "data" / "raw" / "boxer_results_union.csv"
FEAT_CSV = REPO / "data" / "processed" / "boxer_features.csv"
PBO_BOUTS_CSV = REPO / "data" / "processed" / "pbo_moneyline_bouts.csv"
OUT_TRAIN = REPO / "data" / "processed" / "boxer_match_features.parquet"
OUT_SCORE = REPO / "data" / "processed" / "pbo_scoring_features.parquet"


FEATURE_COLS_NUMERIC = [
    "career_wins", "career_losses", "career_draws", "career_nc", "career_fights",
    "ko_win_pct", "tko_loss_pct", "dec_win_pct",
    "r5_w_pct", "r10_w_pct", "r5_ko_pct", "r10_ko_pct",
    "days_since_last", "fights_last_365d",
    "glicko_mu", "glicko_phi", "glicko_sigma",
    "avg_opp_glicko_last5",
    "is_debut",
]


def norm_name(s) -> str:
    if pd.isna(s):
        return ""
    s = re.sub(r"[^a-z]+", " ", str(s).lower()).strip()
    return re.sub(r"\s+", " ", s)


def main() -> int:
    print("Loading inputs ...")
    feat = pd.read_csv(FEAT_CSV, parse_dates=["fight_date"], low_memory=False)
    feat["boxer_id"] = feat["boxer_id"].astype("int64")
    # Some boxers have duplicate (boxer_id, fight_date) — keep last (latest state)
    feat = feat.drop_duplicates(subset=["boxer_id", "fight_date"], keep="last")
    print(f"  feat: {len(feat):,} unique (boxer, date) keys")

    union = pd.read_csv(UNION_CSV, parse_dates=["fight_date"], low_memory=False)
    print(f"  union: {len(union):,}")

    # ---------- 1. Training pair-set from union ----------
    print("\nBuilding training pairs from union ...")
    u = union.dropna(subset=["fight_date", "boxer_id", "opp_id"]).copy()
    u["boxer_id"] = pd.to_numeric(u["boxer_id"], errors="coerce")
    u["opp_id"] = pd.to_numeric(u["opp_id"], errors="coerce")
    u = u.dropna(subset=["boxer_id", "opp_id"]).copy()
    u["boxer_id"] = u["boxer_id"].astype("int64")
    u["opp_id"] = u["opp_id"].astype("int64")
    u["result"] = u["result"].astype(str).str.upper()
    u = u[u["result"].isin(["W", "L"])].copy()

    u["a_id"] = u[["boxer_id", "opp_id"]].min(axis=1).astype("int64")
    u["b_id"] = u[["boxer_id", "opp_id"]].max(axis=1).astype("int64")
    # a_wins from THIS row's perspective: this row's boxer is "a" iff boxer_id == a_id
    u["a_wins"] = ((u["boxer_id"] == u["a_id"]) & (u["result"] == "W")) | \
                  ((u["boxer_id"] == u["b_id"]) & (u["result"] == "L"))
    u["a_wins"] = u["a_wins"].astype(int)
    u = u.drop_duplicates(subset=["fight_date", "a_id", "b_id"]).copy()
    u = u[["fight_date", "a_id", "b_id", "a_wins"]]
    print(f"  unique decisive bouts: {len(u):,}")

    # Merge in features for a and b via two left joins
    feat_keys = ["boxer_id", "fight_date"]
    feat_a = feat.rename(columns={c: f"{c}_a" for c in FEATURE_COLS_NUMERIC} | {"boxer_id": "a_id"})
    feat_a = feat_a[["a_id", "fight_date"] + [f"{c}_a" for c in FEATURE_COLS_NUMERIC]]
    feat_b = feat.rename(columns={c: f"{c}_b" for c in FEATURE_COLS_NUMERIC} | {"boxer_id": "b_id"})
    feat_b = feat_b[["b_id", "fight_date"] + [f"{c}_b" for c in FEATURE_COLS_NUMERIC]]

    train = u.merge(feat_a, on=["a_id", "fight_date"], how="left")
    train = train.merge(feat_b, on=["b_id", "fight_date"], how="left")
    before = len(train)
    train = train.dropna(subset=[f"glicko_mu_a", f"glicko_mu_b"]).copy()
    print(f"  joined with features: {len(train):,}  dropped (missing one side): {before - len(train):,}")

    # Diff columns
    for c in FEATURE_COLS_NUMERIC:
        train[f"{c}_diff"] = train[f"{c}_a"].astype(float) - train[f"{c}_b"].astype(float)

    train["year"] = train["fight_date"].dt.year
    cols = ["fight_date", "year", "a_id", "b_id", "a_wins"] + \
           [f"{c}_a" for c in FEATURE_COLS_NUMERIC] + \
           [f"{c}_b" for c in FEATURE_COLS_NUMERIC] + \
           [f"{c}_diff" for c in FEATURE_COLS_NUMERIC]
    train = train[cols]

    OUT_TRAIN.parent.mkdir(parents=True, exist_ok=True)
    train.to_parquet(OUT_TRAIN, index=False)
    print(f"  wrote {len(train):,} rows -> {OUT_TRAIN}")
    print(f"  per-year row counts (last 10y):")
    yc = train.groupby("year").size().tail(15)
    for y, n in yc.items():
        print(f"    {y}: {n:,}")
    print(f"  base rate a_wins: {train['a_wins'].mean():.3f}")

    # ---------- 2. PBO scoring set ----------
    print("\nBuilding PBO scoring set ...")
    pbo = pd.read_csv(PBO_BOUTS_CSV, parse_dates=["event_date"], low_memory=False)
    pbo = pbo.dropna(subset=["price_a", "price_b"]).copy()
    print(f"  PBO bouts with prices: {len(pbo):,}")

    union2 = union.dropna(subset=["fight_date", "boxer_id"]).copy()
    union2["boxer_id"] = pd.to_numeric(union2["boxer_id"], errors="coerce")
    union2["opp_id"] = pd.to_numeric(union2["opp_id"], errors="coerce")
    union2 = union2.dropna(subset=["boxer_id"]).copy()
    union2["boxer_id"] = union2["boxer_id"].astype("int64")
    union2["name_n"] = union2["boxer_name"].map(norm_name)
    union2["opp_name_n"] = union2["opp_name"].map(norm_name)
    # Build name -> id from BOTH primary boxer rows AND opponent rows
    name_id_pairs = pd.concat([
        union2[["name_n", "boxer_id"]].rename(columns={"name_n": "name_n", "boxer_id": "id"}),
        union2.dropna(subset=["opp_id"])[["opp_name_n", "opp_id"]].rename(
            columns={"opp_name_n": "name_n", "opp_id": "id"}).astype({"id": "int64"}),
    ], ignore_index=True)
    name_id_pairs = name_id_pairs[name_id_pairs["name_n"] != ""]
    name_id_map = name_id_pairs.groupby("name_n")["id"].agg(
        lambda s: s.value_counts().index[0]).to_dict()
    # build winner lookup: sorted-pair → list of (date, winner_n)
    win_rows = union2[union2["result"].astype(str).str.upper() == "W"].copy()
    win_lookup: dict[tuple, list] = {}
    for r in win_rows.itertuples(index=False):
        key = tuple(sorted([r.name_n, r.opp_name_n]))
        win_lookup.setdefault(key, []).append((r.fight_date, r.name_n))

    score_rows = []
    for r in pbo.itertuples(index=False):
        a_n = norm_name(r.fighter_a)
        b_n = norm_name(r.fighter_b)
        if not a_n or not b_n:
            continue
        key = tuple(sorted([a_n, b_n]))
        cands = win_lookup.get(key, [])
        winner_n = None
        bout_date = None
        for fd, wn in cands:
            if abs((fd - r.event_date).days) <= 2:
                winner_n = wn
                bout_date = fd
                break
        if winner_n is None:
            continue
        a_id = name_id_map.get(a_n)
        b_id = name_id_map.get(b_n)
        if a_id is None or b_id is None:
            continue
        winner_side = "a" if winner_n == a_n else "b"
        score_rows.append({
            "event_id": r.event_id, "event_date": r.event_date, "fight_date": bout_date,
            "year": pd.Timestamp(r.event_date).year,
            "fighter_a": r.fighter_a, "fighter_b": r.fighter_b,
            "a_id": int(a_id), "b_id": int(b_id),
            "price_a": r.price_a, "price_b": r.price_b,
            "market_prob_a": r.market_prob_a, "market_prob_b": r.market_prob_b,
            "winner_side": winner_side,
        })

    sdf = pd.DataFrame(score_rows)
    print(f"  resolved name+winner: {len(sdf):,} of {len(pbo):,}")

    # Look up features for both sides at fight_date
    if len(sdf):
        feat_a_sc = feat.rename(columns={c: f"{c}_a" for c in FEATURE_COLS_NUMERIC} | {"boxer_id": "a_id"})
        feat_a_sc = feat_a_sc[["a_id", "fight_date"] + [f"{c}_a" for c in FEATURE_COLS_NUMERIC]]
        feat_b_sc = feat.rename(columns={c: f"{c}_b" for c in FEATURE_COLS_NUMERIC} | {"boxer_id": "b_id"})
        feat_b_sc = feat_b_sc[["b_id", "fight_date"] + [f"{c}_b" for c in FEATURE_COLS_NUMERIC]]

        sdf = sdf.merge(feat_a_sc, on=["a_id", "fight_date"], how="left")
        sdf = sdf.merge(feat_b_sc, on=["b_id", "fight_date"], how="left")
        before = len(sdf)
        sdf = sdf.dropna(subset=["glicko_mu_a", "glicko_mu_b"]).copy()
        print(f"  joined features both sides: {len(sdf):,}  dropped {before - len(sdf):,}")

        for c in FEATURE_COLS_NUMERIC:
            sdf[f"{c}_diff"] = sdf[f"{c}_a"].astype(float) - sdf[f"{c}_b"].astype(float)

    sdf.to_parquet(OUT_SCORE, index=False)
    print(f"  wrote {len(sdf):,} rows -> {OUT_SCORE}")
    if len(sdf):
        per_year = sdf.groupby("year").size()
        print("\nScoring rows per year:")
        for y, n in per_year.items():
            print(f"  {y}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

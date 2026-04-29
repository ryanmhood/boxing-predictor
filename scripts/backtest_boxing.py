"""Backtest per-year LightGBM boxing models against PBO closing prices.

For every PBO bout in pbo_scoring_features.parquet:
  1. Load the year-Y model (trained on [2010..Y-1]) and platt params.
  2. Compute model_p_a (calibrated).
  3. Devig PBO consensus prices to get market_p_a (subtract overround
     proportionally — symmetric devig).
  4. edge_a = model_p_a - market_p_a.
  5. Gate: |edge| >= 3pp AND market_p_a in [0.10, 0.90] for the side with
     positive edge (asymmetric since boxing has heavy favorites — gate
     applies to whichever side we'd bet on).
  6. PnL: $1 stake, payout from American odds. Computes ROI at PBO median
     and at PBO median * 1.05 (5% vig haircut for realism).

Outputs:
  - data/processed/boxing_backtest_picks.csv (every bout, with model probs,
    edge, decision, outcome, PnL)
  - per-year ROI table -> stdout + data/reports/boxing_backtest_summary.csv
  - per-tier ROI table (heavy-fav market_p > 0.65, mid 0.35..0.65, dog < 0.35)
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb

REPO = Path(__file__).resolve().parent.parent
SCORE_PARQUET = REPO / "data" / "processed" / "pbo_scoring_features.parquet"
MODELS_DIR = REPO / "data" / "models" / "boxing"
PICKS_CSV = REPO / "data" / "processed" / "boxing_backtest_picks.csv"
SUMMARY_CSV = REPO / "data" / "reports" / "boxing_backtest_summary.csv"


def american_to_implied(american: float) -> float:
    a = float(american)
    return 100.0 / (a + 100.0) if a > 0 else abs(a) / (abs(a) + 100.0)


def american_payout(american: float) -> float:
    """Return profit per $1 stake for a winning bet."""
    a = float(american)
    return a / 100.0 if a > 0 else 100.0 / abs(a)


def haircut_payout(american: float, haircut: float) -> float:
    """Apply a vig haircut by reducing the payout multiplier by (1 - haircut)."""
    return american_payout(american) * (1.0 - haircut)


def devig(p_a: float, p_b: float) -> tuple[float, float]:
    """Symmetric devig: divide by the sum so probs sum to 1."""
    s = p_a + p_b
    if s <= 0:
        return float("nan"), float("nan")
    return p_a / s, p_b / s


def apply_platt(p_raw: np.ndarray, a: float, b: float) -> np.ndarray:
    eps = 1e-6
    p_clip = np.clip(p_raw, eps, 1 - eps)
    logit = np.log(p_clip / (1 - p_clip))
    z = a * logit + b
    return 1.0 / (1.0 + np.exp(-z))


def main() -> int:
    print(f"Loading {SCORE_PARQUET.name} ...")
    df = pd.read_parquet(SCORE_PARQUET)
    print(f"  rows: {len(df):,}")

    out_rows = []
    by_year_models: dict[int, dict] = {}

    for year, gdf in df.groupby("year"):
        ydir = MODELS_DIR / f"year={int(year)}"
        if not (ydir / "model.txt").exists():
            print(f"  SKIP year={year}: no model trained")
            continue
        booster = lgb.Booster(model_file=str(ydir / "model.txt"))
        platt = json.loads((ydir / "platt.json").read_text())
        feat_list = json.loads((ydir / "feature_list.json").read_text())
        by_year_models[int(year)] = {"booster": booster, "platt": platt, "feats": feat_list}

        Xy = gdf[feat_list].astype("float32").values
        p_raw = booster.predict(Xy)
        p_cal = apply_platt(p_raw, platt["a"], platt["b"])
        gdf = gdf.copy()
        gdf["model_p_a_raw"] = p_raw
        gdf["model_p_a"] = p_cal
        # Devig PBO prices
        imp_a = gdf["price_a"].apply(american_to_implied).astype(float)
        imp_b = gdf["price_b"].apply(american_to_implied).astype(float)
        s = imp_a + imp_b
        gdf["market_p_a"] = (imp_a / s).astype(float)
        gdf["market_p_b"] = (imp_b / s).astype(float)
        gdf["overround"] = (s - 1.0).astype(float)

        gdf["edge_a"] = gdf["model_p_a"] - gdf["market_p_a"]
        out_rows.append(gdf)

    if not out_rows:
        print("No models / no scoring rows. Aborting.")
        return 1

    full = pd.concat(out_rows, ignore_index=True)
    print(f"Scored bouts: {len(full):,}")
    print(f"  model_p_a range: [{full['model_p_a'].min():.3f}, {full['model_p_a'].max():.3f}]")
    print(f"  edge_a range:    [{full['edge_a'].min():+.3f}, {full['edge_a'].max():+.3f}]")
    print(f"  overround mean:  {full['overround'].mean():.3f}")

    # --------- Decision: pick side with positive edge, gate by edge mag + market range ---------
    EDGE_THRESH = 0.03
    LO, HI = 0.10, 0.90

    def decide(row):
        ea = row["edge_a"]
        if ea > 0:
            side = "a"
            edge = ea
            market_p = row["market_p_a"]
            price = row["price_a"]
        else:
            side = "b"
            edge = -ea
            market_p = row["market_p_b"]
            price = row["price_b"]
        if edge < EDGE_THRESH:
            return pd.Series({"bet_side": "", "edge_abs": edge, "market_p_bet": market_p,
                              "price_bet": price, "bet": False})
        if market_p < LO or market_p > HI:
            return pd.Series({"bet_side": "", "edge_abs": edge, "market_p_bet": market_p,
                              "price_bet": price, "bet": False})
        return pd.Series({"bet_side": side, "edge_abs": edge, "market_p_bet": market_p,
                          "price_bet": price, "bet": True})

    decisions = full.apply(decide, axis=1)
    full = pd.concat([full, decisions], axis=1)
    n_bets = int(full["bet"].sum())
    print(f"  bets placed: {n_bets} / {len(full)} ({n_bets/len(full)*100:.1f}%)")

    # --------- PnL ---------
    def pnl(row, haircut: float):
        if not row["bet"]:
            return 0.0
        won = row["winner_side"] == row["bet_side"]
        if haircut > 0:
            return haircut_payout(row["price_bet"], haircut) if won else -1.0
        return american_payout(row["price_bet"]) if won else -1.0

    full["pnl"] = full.apply(lambda r: pnl(r, 0.0), axis=1)
    full["pnl_haircut5"] = full.apply(lambda r: pnl(r, 0.05), axis=1)

    # --------- Per-year summary ---------
    def roi_block(d: pd.DataFrame, label: str = "") -> dict:
        b = d[d["bet"]]
        n = len(b)
        if n == 0:
            return {"label": label, "n_bouts": len(d), "n_bets": 0,
                    "hit_rate": float("nan"), "roi_pct": float("nan"),
                    "ci95_pp": float("nan"), "roi_haircut5_pct": float("nan"),
                    "brier_market": float("nan"), "brier_model": float("nan"),
                    "brier_imp_pp": float("nan")}
        hits = ((b["winner_side"] == b["bet_side"])).mean()
        roi = b["pnl"].sum() / n * 100.0
        roi_h = b["pnl_haircut5"].sum() / n * 100.0
        ci = 1.96 * b["pnl"].std(ddof=1) / math.sqrt(n) * 100.0 if n > 1 else float("nan")
        # Brier on FULL set (not just bets) using a-perspective
        y_a = (d["winner_side"] == "a").astype(int)
        brier_market = ((d["market_p_a"] - y_a) ** 2).mean()
        brier_model = ((d["model_p_a"] - y_a) ** 2).mean()
        return {"label": label, "n_bouts": len(d), "n_bets": n,
                "hit_rate": hits, "roi_pct": roi, "ci95_pp": ci,
                "roi_haircut5_pct": roi_h,
                "brier_market": brier_market, "brier_model": brier_model,
                "brier_imp_pp": (brier_market - brier_model) * 100.0}

    print("\n=== Per-year ROI ===")
    print(f"{'year':>6} {'bouts':>6} {'bets':>5} {'hit%':>7} {'ROI%':>9} {'±CI95':>8} "
          f"{'ROIh5%':>9} {'BrierM':>8} {'BrierMd':>9} {'ΔBpp':>7}")
    rows_summary = []
    for year, g in full.groupby("year"):
        s = roi_block(g, label=str(year))
        rows_summary.append(s)
        print(f"{s['label']:>6} {s['n_bouts']:>6} {s['n_bets']:>5} "
              f"{s['hit_rate']*100 if not math.isnan(s['hit_rate']) else float('nan'):>6.1f}% "
              f"{s['roi_pct']:>+8.2f}% ±{s['ci95_pp']:>5.2f}% "
              f"{s['roi_haircut5_pct']:>+8.2f}% "
              f"{s['brier_market']:>8.4f} {s['brier_model']:>9.4f} "
              f"{s['brier_imp_pp']:>+6.2f}")
    s_all = roi_block(full, label="ALL")
    rows_summary.append(s_all)
    print(f"{'ALL':>6} {s_all['n_bouts']:>6} {s_all['n_bets']:>5} "
          f"{s_all['hit_rate']*100:>6.1f}% "
          f"{s_all['roi_pct']:>+8.2f}% ±{s_all['ci95_pp']:>5.2f}% "
          f"{s_all['roi_haircut5_pct']:>+8.2f}% "
          f"{s_all['brier_market']:>8.4f} {s_all['brier_model']:>9.4f} "
          f"{s_all['brier_imp_pp']:>+6.2f}")

    # 2024-25 holdout block
    h = full[full["year"].isin([2024, 2025])]
    s_h = roi_block(h, label="2024-25 holdout")
    rows_summary.append(s_h)
    print(f"\n2024-25 holdout: bouts={s_h['n_bouts']} bets={s_h['n_bets']} "
          f"ROI={s_h['roi_pct']:+.2f}% ±{s_h['ci95_pp']:.2f}pp  "
          f"ROIh5={s_h['roi_haircut5_pct']:+.2f}%  "
          f"BrierΔ={s_h['brier_imp_pp']:+.2f}pp")

    # --------- Per-tier on full set ---------
    print("\n=== Per-tier ROI (across all years) ===")
    tiers = [("heavy-fav (mp>=0.65)", full[full["market_p_a"] >= 0.65].copy()),
             ("mid (0.35..0.65)", full[(full["market_p_a"] >= 0.35) & (full["market_p_a"] < 0.65)].copy()),
             ("dog (mp<0.35)", full[full["market_p_a"] < 0.35].copy())]
    for label, g in tiers:
        s = roi_block(g, label=label)
        rows_summary.append(s)
        n_bets = s["n_bets"]
        if n_bets:
            print(f"  {label:>22}: bouts={s['n_bouts']:>4} bets={n_bets:>3} hit={s['hit_rate']*100:>5.1f}% "
                  f"ROI={s['roi_pct']:+.2f}% ±{s['ci95_pp']:.2f}pp  ROIh5={s['roi_haircut5_pct']:+.2f}%")
        else:
            print(f"  {label:>22}: bouts={s['n_bouts']:>4} bets=0 (no bets passed gate)")

    # Always-favorite naive baseline reference
    fav_side = np.where(full["market_p_a"] >= full["market_p_b"], "a", "b")
    fav_won = (fav_side == full["winner_side"])
    fav_price = np.where(fav_side == "a", full["price_a"], full["price_b"])
    fav_pnl = np.array([american_payout(p) if w else -1.0 for p, w in zip(fav_price, fav_won)])
    print(f"\nNaive always-favorite (this exact subset): n={len(full)} hit={fav_won.mean()*100:.1f}% "
          f"ROI={fav_pnl.mean()*100:+.2f}%")

    # Save artefacts
    PICKS_CSV.parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(PICKS_CSV, index=False)
    SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_summary).to_csv(SUMMARY_CSV, index=False)
    print(f"\nWrote: {PICKS_CSV}")
    print(f"Wrote: {SUMMARY_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Backtest per-year LightGBM boxing models against PBO closing prices.

For every PBO bout in pbo_scoring_features.parquet:
  1. Load the year-Y model (trained on [2010..Y-1]) and platt params.
  2. Compute model_p_a (calibrated). G4 (bx-zz7): the calibrated prob
     uses a per-tier Platt scaler routed by the raw model_p (heavy /
     mid / light).
  3. Devig PBO consensus prices to get market_p_a (symmetric devig).
  4. edge_a = model_p_a - market_p_a.
  5. Three strategies evaluated in parallel (G4 part D):
       - Global:       |edge| >= 3pp, market_p in [0.10, 0.90]
       - Mid-tier:     |edge| >= 3pp, market_p in [0.35, 0.65]
       - Heavy-fav:    market_p > 0.85, bet ML if model agrees within 5pp
                       (sanity check vs the naive "always-favorite" line)
  6. PnL: $1 stake, payout from American odds. Computes ROI at PBO median
     and at PBO median * 0.95 (5% vig haircut for realism).

Outputs:
  - data/processed/boxing_backtest_picks.csv (every bout, with model probs,
    edge, decision-per-strategy, outcome, PnL)
  - per-year + per-strategy ROI tables -> stdout + data/reports/boxing_backtest_summary.csv
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


def tier_label(p_raw: float) -> str:
    if p_raw >= 0.85 or p_raw <= 0.15:
        return "heavy"
    if 0.35 <= p_raw <= 0.65:
        return "mid"
    return "light"


def apply_platt_per_tier(
    p_raw: np.ndarray, tier_platts: dict[str, dict]
) -> np.ndarray:
    """Route each row to its tier's Platt scaler based on raw p."""
    out = np.zeros_like(p_raw)
    for i, p in enumerate(p_raw):
        t = tier_label(float(p))
        a = tier_platts[t]["a"]
        b = tier_platts[t]["b"]
        out[i] = float(apply_platt(np.array([float(p)]), a, b)[0])
    return out


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
        # G4: per-tier Platt scalers (graceful fallback to global if missing)
        tier_platts: dict[str, dict] = {}
        for t in ("heavy", "mid", "light"):
            p_path = ydir / f"platt_{t}.json"
            if p_path.exists():
                tier_platts[t] = json.loads(p_path.read_text())
            else:
                tier_platts[t] = platt
        by_year_models[int(year)] = {
            "booster": booster, "platt": platt, "feats": feat_list,
            "tier_platts": tier_platts,
        }

        Xy = gdf[feat_list].astype("float32").values
        p_raw = booster.predict(Xy)
        p_cal_global = apply_platt(p_raw, platt["a"], platt["b"])
        p_cal_tier = apply_platt_per_tier(p_raw, tier_platts)
        gdf = gdf.copy()
        gdf["model_p_a_raw"] = p_raw
        gdf["model_p_a_global"] = p_cal_global
        gdf["model_p_a"] = p_cal_tier  # default to per-tier (G4)
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

    # --------- Three strategies (G4 part D) ---------
    EDGE_THRESH = 0.03

    def positive_edge_side(row) -> tuple[str, float, float, float]:
        """Return (side, edge_abs, market_p_bet, price_bet) for whichever
        side has positive model edge over market.
        """
        ea = row["edge_a"]
        if ea > 0:
            return "a", float(ea), float(row["market_p_a"]), float(row["price_a"])
        return "b", float(-ea), float(row["market_p_b"]), float(row["price_b"])

    def decide_global(row):
        side, edge, mp, price = positive_edge_side(row)
        bet = (edge >= EDGE_THRESH) and (0.10 <= mp <= 0.90)
        return pd.Series({"side_global": side if bet else "", "edge_abs_global": edge,
                          "mp_global": mp, "price_global": price, "bet_global": bool(bet)})

    def decide_mid(row):
        side, edge, mp, price = positive_edge_side(row)
        bet = (edge >= EDGE_THRESH) and (0.35 <= mp <= 0.65)
        return pd.Series({"side_mid": side if bet else "", "edge_abs_mid": edge,
                          "mp_mid": mp, "price_mid": price, "bet_mid": bool(bet)})

    def decide_heavy(row):
        # Pick the heavier favorite (mp > 0.85). Bet ML if model agrees within 5pp.
        ma = float(row["market_p_a"])
        mb = float(row["market_p_b"])
        if ma > 0.85:
            fav_side, fav_mp, fav_price = "a", ma, float(row["price_a"])
            model_p = float(row["model_p_a"])
        elif mb > 0.85:
            fav_side, fav_mp, fav_price = "b", mb, float(row["price_b"])
            model_p = 1.0 - float(row["model_p_a"])
        else:
            return pd.Series({"side_heavy": "", "mp_heavy": float("nan"),
                              "price_heavy": float("nan"), "bet_heavy": False})
        # "Model agrees within 5pp": model thinks the favorite is at least
        # market_p - 0.05 (we're not betting against the favorite if the
        # model strongly disagrees). This is the "sanity-check vs naive"
        # strategy from the bead.
        agree = model_p >= (fav_mp - 0.05)
        return pd.Series({"side_heavy": fav_side if agree else "",
                          "mp_heavy": fav_mp, "price_heavy": fav_price,
                          "bet_heavy": bool(agree)})

    full = pd.concat([full,
                      full.apply(decide_global, axis=1),
                      full.apply(decide_mid, axis=1),
                      full.apply(decide_heavy, axis=1)], axis=1)

    for name in ("global", "mid", "heavy"):
        n = int(full[f"bet_{name}"].sum())
        print(f"  bets ({name}): {n} / {len(full)} ({n/len(full)*100:.1f}%)")

    # --------- PnL per strategy ---------
    def pnl_for(row, name: str, haircut: float) -> float:
        if not row[f"bet_{name}"]:
            return 0.0
        won = row["winner_side"] == row[f"side_{name}"]
        price = row[f"price_{name}"]
        if haircut > 0:
            return haircut_payout(price, haircut) if won else -1.0
        return american_payout(price) if won else -1.0

    for name in ("global", "mid", "heavy"):
        full[f"pnl_{name}"] = full.apply(lambda r, n=name: pnl_for(r, n, 0.0), axis=1)
        full[f"pnl_{name}_h5"] = full.apply(lambda r, n=name: pnl_for(r, n, 0.05), axis=1)

    # Back-compat columns for downstream report tooling
    full["bet_side"] = full["side_global"]
    full["bet"] = full["bet_global"]
    full["price_bet"] = full["price_global"]
    full["pnl"] = full["pnl_global"]
    full["pnl_haircut5"] = full["pnl_global_h5"]

    # --------- Summary helpers ---------
    def roi_block(d: pd.DataFrame, strategy: str, label: str = "") -> dict:
        bet_col = f"bet_{strategy}"
        side_col = f"side_{strategy}"
        pnl_col = f"pnl_{strategy}"
        pnl_h_col = f"pnl_{strategy}_h5"
        b = d[d[bet_col]]
        n = len(b)
        if n == 0:
            return {"strategy": strategy, "label": label, "n_bouts": len(d), "n_bets": 0,
                    "hit_rate": float("nan"), "roi_pct": float("nan"),
                    "ci95_pp": float("nan"), "roi_haircut5_pct": float("nan"),
                    "brier_market": float("nan"), "brier_model": float("nan"),
                    "brier_imp_pp": float("nan")}
        hits = ((b["winner_side"] == b[side_col])).mean()
        roi = b[pnl_col].sum() / n * 100.0
        roi_h = b[pnl_h_col].sum() / n * 100.0
        ci = 1.96 * b[pnl_col].std(ddof=1) / math.sqrt(n) * 100.0 if n > 1 else float("nan")
        # Brier on the BETS subset (G4: report calibration on bouts where the
        # strategy actually places a bet — that's where the Brier criterion
        # matters for paper-betting)
        y_a = (b["winner_side"] == "a").astype(int)
        brier_market = ((b["market_p_a"] - y_a) ** 2).mean()
        brier_model = ((b["model_p_a"] - y_a) ** 2).mean()
        return {"strategy": strategy, "label": label, "n_bouts": len(d), "n_bets": n,
                "hit_rate": hits, "roi_pct": roi, "ci95_pp": ci,
                "roi_haircut5_pct": roi_h,
                "brier_market": brier_market, "brier_model": brier_model,
                "brier_imp_pp": (brier_market - brier_model) * 100.0}

    rows_summary: list[dict] = []
    holdout = full[full["year"].isin([2024, 2025])]

    for strategy in ("global", "mid", "heavy"):
        print(f"\n=== Strategy: {strategy.upper()} — per-year ROI ===")
        print(f"{'year':>6} {'bouts':>6} {'bets':>5} {'hit%':>7} {'ROI%':>9} {'±CI95':>8} "
              f"{'ROIh5%':>9} {'BrierM':>8} {'BrierMd':>9} {'ΔBpp':>7}")
        for year, g in full.groupby("year"):
            s = roi_block(g, strategy, label=str(year))
            rows_summary.append(s)
            hit_str = f"{s['hit_rate']*100:>6.1f}%" if s["n_bets"] else "    -- "
            roi_str = f"{s['roi_pct']:>+8.2f}%" if s["n_bets"] else "      --"
            ci_str = f"±{s['ci95_pp']:>5.2f}%" if s["n_bets"] and not math.isnan(s["ci95_pp"]) else "       "
            roih_str = f"{s['roi_haircut5_pct']:>+8.2f}%" if s["n_bets"] else "      --"
            print(f"{s['label']:>6} {s['n_bouts']:>6} {s['n_bets']:>5} "
                  f"{hit_str} {roi_str} {ci_str} {roih_str} "
                  f"{s['brier_market']:>8.4f} {s['brier_model']:>9.4f} "
                  f"{s['brier_imp_pp']:>+6.2f}")
        s_all = roi_block(full, strategy, label="ALL")
        rows_summary.append(s_all)
        s_h = roi_block(holdout, strategy, label="2024-25 holdout")
        rows_summary.append(s_h)
        print(f"  ALL:            bets={s_all['n_bets']:>3} ROI={s_all['roi_pct']:+.2f}% "
              f"±{s_all['ci95_pp']:.2f}pp ROIh5={s_all['roi_haircut5_pct']:+.2f}% "
              f"BrierΔ={s_all['brier_imp_pp']:+.2f}pp")
        print(f"  2024-25 holdout: bouts={s_h['n_bouts']} bets={s_h['n_bets']} "
              f"ROI={s_h['roi_pct']:+.2f}% ±{s_h['ci95_pp']:.2f}pp "
              f"ROIh5={s_h['roi_haircut5_pct']:+.2f}% "
              f"BrierΔ={s_h['brier_imp_pp']:+.2f}pp")

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

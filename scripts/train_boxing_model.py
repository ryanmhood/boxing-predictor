"""Walk-forward LightGBM boxing model.

Train on [2010..year-1], eval on year, for year in 2018..2025. Per year:
  - 80/20 random split of the training window for Platt calibration
  - LightGBM binary on 80% (early-stop on holdout)
  - Platt scaling fit on holdout's raw probs
  - Save model.txt + platt.json + feature_list.json under data/models/boxing/year={Y}/

Per-year metrics emitted: train_n, eval_n, base_rate, raw_brier, cal_brier,
brier_baseline (predict base_rate), brier_improvement, raw_logloss,
cal_logloss, auc.

The walk-forward evaluation reuses match features (paired bouts), so the
"eval" rows are union bouts (NOT PBO bouts). PBO backtest comes later
via scripts/backtest_boxing.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.model_selection import train_test_split

REPO = Path(__file__).resolve().parent.parent
TRAIN_PARQUET = REPO / "data" / "processed" / "boxer_match_features.parquet"
MODELS_DIR = REPO / "data" / "models" / "boxing"
METRICS_CSV = MODELS_DIR / "walk_forward_metrics.csv"

TRAIN_YEAR_MIN = 2010
EVAL_YEARS = list(range(2018, 2026))

FEATURE_COLS_NUMERIC = [
    "career_wins", "career_losses", "career_draws", "career_nc", "career_fights",
    "ko_win_pct", "tko_loss_pct", "dec_win_pct",
    "r5_w_pct", "r10_w_pct", "r5_ko_pct", "r10_ko_pct",
    "days_since_last", "fights_last_365d",
    "glicko_mu", "glicko_phi", "glicko_sigma",
    "avg_opp_glicko_last5",
    "is_debut",
]


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for c in FEATURE_COLS_NUMERIC:
        cols.extend([f"{c}_a", f"{c}_b", f"{c}_diff"])
    return [c for c in cols if c in df.columns]


def fit_platt(p_raw: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """Return (a, b) such that p_cal = sigmoid(a * logit(p_raw) + b).

    Implements the standard Platt sklearn-style calibration on a logit feature.
    """
    eps = 1e-6
    p_clip = np.clip(p_raw, eps, 1 - eps)
    logit = np.log(p_clip / (1 - p_clip))
    lr = LogisticRegression(C=1e6, solver="lbfgs")
    lr.fit(logit.reshape(-1, 1), y)
    return float(lr.coef_[0, 0]), float(lr.intercept_[0])


def apply_platt(p_raw: np.ndarray, a: float, b: float) -> np.ndarray:
    eps = 1e-6
    p_clip = np.clip(p_raw, eps, 1 - eps)
    logit = np.log(p_clip / (1 - p_clip))
    z = a * logit + b
    return 1.0 / (1.0 + np.exp(-z))


def main() -> int:
    print(f"Loading {TRAIN_PARQUET.name} ...")
    df = pd.read_parquet(TRAIN_PARQUET)
    print(f"  rows: {len(df):,}")
    feat_cols = get_feature_columns(df)
    print(f"  feature columns: {len(feat_cols)}")

    df = df[df["year"] >= TRAIN_YEAR_MIN].copy()
    print(f"  rows in [{TRAIN_YEAR_MIN}, ...): {len(df):,}")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    metrics = []
    for year in EVAL_YEARS:
        train = df[df["year"] < year].copy()
        evald = df[df["year"] == year].copy()
        if len(train) < 500 or len(evald) < 50:
            print(f"\nYEAR {year}: skip (train={len(train)}, eval={len(evald)})")
            continue
        X_train_full = train[feat_cols].astype("float32").values
        y_train_full = train["a_wins"].astype("int8").values
        X_eval = evald[feat_cols].astype("float32").values
        y_eval = evald["a_wins"].astype("int8").values

        # 80/20 split for Platt
        X_tr, X_holdout, y_tr, y_holdout = train_test_split(
            X_train_full, y_train_full, test_size=0.20, random_state=42, stratify=y_train_full)
        d_tr = lgb.Dataset(X_tr, label=y_tr, feature_name=feat_cols)
        d_ho = lgb.Dataset(X_holdout, label=y_holdout, feature_name=feat_cols, reference=d_tr)

        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "learning_rate": 0.04,
            "num_leaves": 31,
            "min_data_in_leaf": 50,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.9,
            "bagging_freq": 5,
            "verbose": -1,
            "seed": 42,
        }
        model = lgb.train(
            params, d_tr, num_boost_round=600,
            valid_sets=[d_ho],
            callbacks=[lgb.early_stopping(30, verbose=False),
                       lgb.log_evaluation(0)],
        )

        p_holdout_raw = model.predict(X_holdout)
        a_platt, b_platt = fit_platt(p_holdout_raw, y_holdout)
        p_eval_raw = model.predict(X_eval)
        p_eval_cal = apply_platt(p_eval_raw, a_platt, b_platt)

        base_rate = float(y_train_full.mean())
        brier_base = brier_score_loss(y_eval, np.full_like(y_eval, base_rate, dtype=float))
        brier_raw = brier_score_loss(y_eval, p_eval_raw)
        brier_cal = brier_score_loss(y_eval, p_eval_cal)
        ll_raw = log_loss(y_eval, np.clip(p_eval_raw, 1e-6, 1 - 1e-6))
        ll_cal = log_loss(y_eval, np.clip(p_eval_cal, 1e-6, 1 - 1e-6))
        auc = roc_auc_score(y_eval, p_eval_raw) if len(set(y_eval)) > 1 else float("nan")

        # Save artefacts
        ydir = MODELS_DIR / f"year={year}"
        ydir.mkdir(parents=True, exist_ok=True)
        model.save_model(str(ydir / "model.txt"))
        with open(ydir / "platt.json", "w") as f:
            json.dump({"a": a_platt, "b": b_platt, "base_rate": base_rate}, f)
        with open(ydir / "feature_list.json", "w") as f:
            json.dump(feat_cols, f)

        row = {
            "year": year,
            "train_n": len(train),
            "eval_n": len(evald),
            "base_rate": base_rate,
            "brier_baseline": brier_base,
            "brier_raw": brier_raw,
            "brier_cal": brier_cal,
            "brier_improvement_vs_base": brier_base - brier_cal,
            "logloss_raw": ll_raw,
            "logloss_cal": ll_cal,
            "auc": auc,
            "platt_a": a_platt,
            "platt_b": b_platt,
        }
        metrics.append(row)
        print(f"YEAR {year}: train={len(train):,} eval={len(evald):,} base={base_rate:.3f}  "
              f"brier_base={brier_base:.4f} brier_cal={brier_cal:.4f} (Δ={brier_base-brier_cal:+.4f})  "
              f"auc={auc:.3f}  platt(a,b)=({a_platt:.3f},{b_platt:.3f})")

    pd.DataFrame(metrics).to_csv(METRICS_CSV, index=False)
    print(f"\nWrote walk-forward metrics -> {METRICS_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

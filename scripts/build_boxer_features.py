"""Per-(boxer, bout_date) lag-1-safe feature builder.

For every UNIQUE BOUT in the union feed (deduped by date+sorted-pair), emit
TWO feature rows — one per fighter — with state computed from that fighter's
prior fights. State is maintained for ALL fighters seen as either boxer_id
or opp_id, so bouts where only one fighter's profile was scraped still
produce feature rows for both sides (with whatever signal we have on the
opponent-only fighter, which always includes Glicko + cross-derived W/L
counts from the perspective rows that DO exist).

Features per (boxer_id, bout_date):
  career_wins, career_losses, career_draws, career_nc, career_fights
  ko_win_pct, tko_loss_pct, dec_win_pct (over career wins/losses; nan if 0)
  r5_w_pct, r10_w_pct, r5_ko_pct, r10_ko_pct
  days_since_last, fights_last_365d
  glicko_mu, glicko_phi, glicko_sigma   (Glicko-2 BEFORE this fight, real units)
  avg_opp_glicko_last5
  is_debut

  -- G4 expansion (bx-zz7) --
  height_cm, reach_cm, reach_to_height_ratio   (from data/processed/tapology_attrs.csv;
                                                NaN where Tapology profile not scraped)
  ko_win_rate_10, tko_loss_rate_10, dec_rate_10  (method dist over last 10 decisive fights)
  avg_scheduled_rounds_10                        (mean of scheduled-round count over last 10)
  inactive_180d_flag, inactive_365d_flag         (binary; based on days_since_last)
  opp_glicko_min_last5, opp_glicko_std_last5     (schedule-strength dispersion)

Output: data/processed/boxer_features.csv

Glicko-2: mu0=1500, phi0=350, sigma0=0.06, tau=0.5; time decay between bouts
sets phi := sqrt(phi^2 + sigma^2 * periods) where periods = max(1, days/30).
"""

from __future__ import annotations

import math
import sys
from collections import deque
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
UNION_CSV = REPO / "data" / "raw" / "boxer_results_union.csv"
ATTRS_CSV = REPO / "data" / "processed" / "tapology_attrs.csv"
OUT_CSV = REPO / "data" / "processed" / "boxer_features.csv"

GLICKO_SCALE = 173.7178
GLICKO_MU0 = 1500.0
GLICKO_PHI0 = 350.0
GLICKO_SIGMA0 = 0.06
GLICKO_TAU = 0.5
GLICKO_EPS = 1e-6

KO_TOKENS = {"ko", "tko", "tko_corner"}
DEC_TOKENS = {"decision", "decision_unanimous", "decision_majority", "decision_split", "decision_technical", "td"}
NC_TOKENS = {"no_contest", "nc"}


def _norm_method(m) -> str:
    if m is None:
        return ""
    s = str(m).strip().lower() if not (isinstance(m, float) and math.isnan(m)) else ""
    return s


def _g(phi: float) -> float:
    return 1.0 / math.sqrt(1 + 3 * phi * phi / (math.pi * math.pi))


def _expected(mu: float, mu_j: float, phi_j: float) -> float:
    return 1.0 / (1.0 + math.exp(-_g(phi_j) * (mu - mu_j)))


def glicko_update(mu: float, phi: float, sigma: float,
                  mu_opp: float, phi_opp: float, score: float,
                  tau: float = GLICKO_TAU) -> tuple[float, float, float]:
    g = _g(phi_opp)
    e = _expected(mu, mu_opp, phi_opp)
    v = 1.0 / max(g * g * e * (1 - e), 1e-12)
    delta = v * g * (score - e)
    a = math.log(sigma * sigma)

    def f(x: float) -> float:
        ex = math.exp(x)
        num = ex * (delta * delta - phi * phi - v - ex)
        den = 2 * (phi * phi + v + ex) ** 2
        return num / den - (x - a) / (tau * tau)

    A = a
    if delta * delta > phi * phi + v:
        B = math.log(delta * delta - phi * phi - v)
    else:
        k = 1
        while f(a - k * tau) < 0:
            k += 1
            if k > 100:
                break
        B = a - k * tau

    fA = f(A); fB = f(B)
    iters = 0
    while abs(B - A) > GLICKO_EPS and iters < 100:
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB <= 0:
            A, fA = B, fB
        else:
            fA = fA / 2.0
        B, fB = C, fC
        iters += 1

    new_sigma = math.exp(A / 2.0)
    phi_star = math.sqrt(phi * phi + new_sigma * new_sigma)
    new_phi = 1.0 / math.sqrt(1.0 / (phi_star * phi_star) + 1.0 / v)
    new_mu = mu + new_phi * new_phi * g * (score - e)
    return new_mu, new_phi, new_sigma


def inflate_phi(phi: float, sigma: float, days: float) -> float:
    if days <= 0:
        return phi
    periods = max(1.0, days / 30.0)
    new_phi = math.sqrt(phi * phi + sigma * sigma * periods)
    return min(new_phi, GLICKO_PHI0 / GLICKO_SCALE)


def load_attrs() -> dict[int, dict]:
    """Load fighter attributes from tapology_attrs.csv. Returns
    {fighter_id (int): {"height_cm": float|nan, "reach_cm": float|nan,
                        "ratio": float|nan}}.
    """
    if not ATTRS_CSV.exists():
        print(f"  WARN: {ATTRS_CSV.name} not found; height/reach features = NaN")
        return {}
    a = pd.read_csv(ATTRS_CSV, low_memory=False)
    a["fighter_id"] = pd.to_numeric(a["fighter_id"], errors="coerce")
    a = a.dropna(subset=["fighter_id"]).copy()
    a["fighter_id"] = a["fighter_id"].astype("int64")
    a["height_cm"] = pd.to_numeric(a["height_cm"], errors="coerce")
    a["reach_cm"] = pd.to_numeric(a["reach_cm"], errors="coerce")
    out: dict[int, dict] = {}
    for r in a.itertuples(index=False):
        h = float(r.height_cm) if r.height_cm == r.height_cm else float("nan")
        rc = float(r.reach_cm) if r.reach_cm == r.reach_cm else float("nan")
        ratio = (rc / h) if (rc == rc and h == h and h > 0) else float("nan")
        out[int(r.fighter_id)] = {"height_cm": h, "reach_cm": rc, "ratio": ratio}
    print(f"  attrs loaded for {len(out):,} fighters "
          f"(height={a['height_cm'].notna().sum():,}, "
          f"reach={a['reach_cm'].notna().sum():,})")
    return out


def main() -> int:
    print(f"Loading {UNION_CSV.name} ...")
    df = pd.read_csv(UNION_CSV, parse_dates=["fight_date"], low_memory=False)
    print(f"  rows: {len(df):,}")
    attrs = load_attrs()

    df = df.dropna(subset=["fight_date", "boxer_id", "opp_id"]).copy()
    df["boxer_id"] = pd.to_numeric(df["boxer_id"], errors="coerce")
    df["opp_id"] = pd.to_numeric(df["opp_id"], errors="coerce")
    df = df.dropna(subset=["boxer_id", "opp_id"]).copy()
    df["boxer_id"] = df["boxer_id"].astype("int64")
    df["opp_id"] = df["opp_id"].astype("int64")
    df["result"] = df["result"].astype(str).str.upper().replace({"NAN": ""})
    df["method_norm"] = df["method"].map(_norm_method)
    df["sched_rounds"] = pd.to_numeric(df["round"], errors="coerce")

    # Build canonical bouts: one row per (date, sorted-pair).
    df["a_id"] = df[["boxer_id", "opp_id"]].min(axis=1).astype("int64")
    df["b_id"] = df[["boxer_id", "opp_id"]].max(axis=1).astype("int64")
    df["a_persp"] = (df["boxer_id"] == df["a_id"])  # this row is from a's perspective

    # Determine a-side result. From a's-perspective row, result is a's. From b's-perspective, invert.
    def a_result(row):
        r = row["result"]
        if row["a_persp"]:
            return r
        # invert from b's perspective: B's W = A's L
        return {"W": "L", "L": "W", "D": "D", "NC": "NC"}.get(r, "")
    df["a_result"] = df.apply(a_result, axis=1)

    # Group by bout key, picking a row with a known result if any
    df["bout_key"] = list(zip(df["fight_date"], df["a_id"], df["b_id"]))
    df = df.sort_values(["bout_key", "a_persp"], ascending=[True, False])
    bouts = df.groupby("bout_key", sort=False).first().reset_index()
    bouts = bouts.sort_values("fight_date", kind="stable").reset_index(drop=True)
    print(f"  unique bouts: {len(bouts):,}")
    print(f"  date range: {bouts['fight_date'].min().date()} .. {bouts['fight_date'].max().date()}")
    print(f"  unique fighter ids: {pd.concat([bouts['a_id'], bouts['b_id']]).nunique():,}")

    # State: per fighter
    state: dict[int, dict] = {}
    glicko: dict[int, dict] = {}

    def get_state(fid: int) -> dict:
        if fid not in state:
            state[fid] = {
                "career_wins": 0, "career_losses": 0, "career_draws": 0, "career_nc": 0,
                "career_ko_wins": 0, "career_tko_wins": 0, "career_dec_wins": 0,
                "career_tko_losses": 0,
                # rolling-10 (date, result_letter, method_norm, sched_rounds)
                "results": deque(maxlen=10),
                "all_dates": [],
                "opp_glicko_last5": deque(maxlen=5),
            }
        return state[fid]

    def get_glicko(fid: int, fdate) -> dict:
        if fid not in glicko:
            glicko[fid] = {"mu": 0.0, "phi": GLICKO_PHI0 / GLICKO_SCALE,
                           "sigma": GLICKO_SIGMA0, "last_date": None}
        else:
            last = glicko[fid]["last_date"]
            if last is not None:
                days = (fdate - last).days
                glicko[fid]["phi"] = inflate_phi(glicko[fid]["phi"], glicko[fid]["sigma"], days)
        return glicko[fid]

    def emit_features(fid: int, fdate, opp_id: int) -> dict:
        s = get_state(fid)
        g = get_glicko(fid, fdate)
        cw = s["career_wins"]; cl = s["career_losses"]; cd = s["career_draws"]; cnc = s["career_nc"]
        cf = cw + cl + cd + cnc
        ko_pct = (s["career_ko_wins"] + s["career_tko_wins"]) / cw if cw else float("nan")
        tko_loss_pct = s["career_tko_losses"] / cl if cl else float("nan")
        dec_pct = s["career_dec_wins"] / cw if cw else float("nan")

        last10 = list(s["results"])
        last5 = last10[-5:]

        def winpct(buf):
            if not buf: return float("nan")
            wins = sum(1 for _, r, _, _ in buf if r == "W")
            denom = sum(1 for _, r, _, _ in buf if r in ("W", "L", "D"))
            return wins / denom if denom else float("nan")

        def kopct(buf):
            if not buf: return float("nan")
            kos = sum(1 for _, r, m, _ in buf if r == "W" and m in KO_TOKENS)
            denom = sum(1 for _, r, _, _ in buf if r == "W")
            return kos / denom if denom else float("nan")

        # Method-distribution rates over last 10 (denominator = decisive fights in window)
        decisive10 = [(_d, r, m) for _d, r, m, _sr in last10 if r in ("W", "L")]
        denom10 = len(decisive10)
        if denom10:
            ko_w_rate10 = sum(1 for _d, r, m in decisive10 if r == "W" and m in KO_TOKENS) / denom10
            tko_l_rate10 = sum(1 for _d, r, m in decisive10 if r == "L" and m in KO_TOKENS) / denom10
            dec_rate10 = sum(1 for _d, r, m in decisive10 if m in DEC_TOKENS) / denom10
        else:
            ko_w_rate10 = tko_l_rate10 = dec_rate10 = float("nan")

        # Average scheduled rounds over last 10 fights with a round value
        sr_vals = [sr for _d, _r, _m, sr in last10 if sr is not None and not math.isnan(sr)]
        avg_sched_rounds10 = (sum(sr_vals) / len(sr_vals)) if sr_vals else float("nan")

        all_dates = s["all_dates"]
        days_since = (fdate - all_dates[-1]).days if all_dates else float("nan")
        cutoff = fdate - pd.Timedelta(days=365)
        idx = 0
        for d in reversed(all_dates):
            if d >= cutoff:
                idx += 1
            else:
                break
        # Inactivity flags (binary; 0 if first fight)
        if math.isnan(days_since) if isinstance(days_since, float) else False:
            inactive_180 = 0
            inactive_365 = 0
        elif all_dates:
            inactive_180 = int(days_since > 180)
            inactive_365 = int(days_since > 365)
        else:
            inactive_180 = 0
            inactive_365 = 0

        opp_glicko_buf = list(s["opp_glicko_last5"])
        if opp_glicko_buf:
            avg_opp_g = sum(opp_glicko_buf) / len(opp_glicko_buf)
            min_opp_g = min(opp_glicko_buf)
            if len(opp_glicko_buf) > 1:
                mean_o = avg_opp_g
                var_o = sum((x - mean_o) ** 2 for x in opp_glicko_buf) / (len(opp_glicko_buf) - 1)
                std_opp_g = math.sqrt(var_o)
            else:
                std_opp_g = 0.0
        else:
            avg_opp_g = float("nan")
            min_opp_g = float("nan")
            std_opp_g = float("nan")

        # Stylistic from tapology_attrs
        a = attrs.get(fid, None)
        height_cm = a["height_cm"] if a else float("nan")
        reach_cm = a["reach_cm"] if a else float("nan")
        ratio = a["ratio"] if a else float("nan")

        return {
            "fight_date": fdate, "boxer_id": fid, "opp_id": opp_id,
            "career_wins": cw, "career_losses": cl, "career_draws": cd, "career_nc": cnc,
            "career_fights": cf, "ko_win_pct": ko_pct, "tko_loss_pct": tko_loss_pct,
            "dec_win_pct": dec_pct, "r5_w_pct": winpct(last5), "r10_w_pct": winpct(last10),
            "r5_ko_pct": kopct(last5), "r10_ko_pct": kopct(last10),
            "days_since_last": days_since, "fights_last_365d": idx,
            "glicko_mu": g["mu"] * GLICKO_SCALE + GLICKO_MU0,
            "glicko_phi": g["phi"] * GLICKO_SCALE,
            "glicko_sigma": g["sigma"], "avg_opp_glicko_last5": avg_opp_g,
            "is_debut": int(cf == 0),
            # G4 expansion
            "height_cm": height_cm, "reach_cm": reach_cm,
            "reach_to_height_ratio": ratio,
            "ko_win_rate_10": ko_w_rate10,
            "tko_loss_rate_10": tko_l_rate10,
            "dec_rate_10": dec_rate10,
            "avg_scheduled_rounds_10": avg_sched_rounds10,
            "inactive_180d_flag": inactive_180,
            "inactive_365d_flag": inactive_365,
            "opp_glicko_min_last5": min_opp_g,
            "opp_glicko_std_last5": std_opp_g,
        }

    out_rows = []
    print("Iterating bouts ...")
    for i, b in enumerate(bouts.itertuples(index=False)):
        if i % 20000 == 0:
            print(f"  {i:,}/{len(bouts):,}")
        a_id = int(b.a_id); b_id = int(b.b_id)
        fdate = b.fight_date
        a_res = b.a_result
        method = b.method_norm
        sched_r = float(b.sched_rounds) if (
            b.sched_rounds == b.sched_rounds  # not nan
        ) else float("nan")

        # Emit pre-fight features for both
        out_rows.append(emit_features(a_id, fdate, b_id))
        out_rows.append(emit_features(b_id, fdate, a_id))

        # Track opp Glicko at time of bout (pre-fight, post-inflation already applied)
        sa = get_state(a_id); sb = get_state(b_id)
        ga_pre = glicko[a_id]["mu"] * GLICKO_SCALE + GLICKO_MU0
        gb_pre = glicko[b_id]["mu"] * GLICKO_SCALE + GLICKO_MU0
        sa["opp_glicko_last5"].append(gb_pre)
        sb["opp_glicko_last5"].append(ga_pre)

        # Update career counters (lookup by perspective)
        # a's result determines BOTH sides' increments
        a_method = method  # method recorded; applies symmetrically (a's KO win = b's KO loss)
        if a_res == "W":
            sa["career_wins"] += 1
            sb["career_losses"] += 1
            if a_method == "ko":
                sa["career_ko_wins"] += 1
                sb["career_tko_losses"] += 1
            elif a_method in ("tko", "tko_corner"):
                sa["career_tko_wins"] += 1
                sb["career_tko_losses"] += 1
            elif a_method in DEC_TOKENS:
                sa["career_dec_wins"] += 1
        elif a_res == "L":
            sa["career_losses"] += 1
            sb["career_wins"] += 1
            if a_method == "ko":
                sb["career_ko_wins"] += 1
                sa["career_tko_losses"] += 1
            elif a_method in ("tko", "tko_corner"):
                sb["career_tko_wins"] += 1
                sa["career_tko_losses"] += 1
            elif a_method in DEC_TOKENS:
                sb["career_dec_wins"] += 1
        elif a_res == "D":
            sa["career_draws"] += 1
            sb["career_draws"] += 1
        elif a_res == "NC" or a_method in NC_TOKENS:
            sa["career_nc"] += 1
            sb["career_nc"] += 1

        if a_res in ("W", "L", "D"):
            sa["results"].append((fdate, a_res, a_method, sched_r))
            b_res = {"W": "L", "L": "W", "D": "D"}[a_res]
            sb["results"].append((fdate, b_res, a_method, sched_r))
        sa["all_dates"].append(fdate)
        sb["all_dates"].append(fdate)

        # Glicko update (skip if non-decisive)
        if a_res in ("W", "L", "D"):
            score_a = {"W": 1.0, "L": 0.0, "D": 0.5}[a_res]
            ga = glicko[a_id]; gb = glicko[b_id]
            mu_a, phi_a, sig_a = glicko_update(ga["mu"], ga["phi"], ga["sigma"],
                                               gb["mu"], gb["phi"], score_a)
            mu_b, phi_b, sig_b = glicko_update(gb["mu"], gb["phi"], gb["sigma"],
                                               ga["mu"], ga["phi"], 1.0 - score_a)
            glicko[a_id] = {"mu": mu_a, "phi": phi_a, "sigma": sig_a, "last_date": fdate}
            glicko[b_id] = {"mu": mu_b, "phi": phi_b, "sigma": sig_b, "last_date": fdate}

    out = pd.DataFrame(out_rows)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"\nWrote {len(out):,} rows ({len(bouts):,} bouts × 2) -> {OUT_CSV}")
    print(f"  unique (boxer_id, fight_date) keys: {out[['boxer_id','fight_date']].drop_duplicates().shape[0]:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

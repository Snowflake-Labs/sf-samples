#!/usr/bin/env python3
"""Reference implementation of the trailer-signal opening-weekend model.

A distributional regressor with strict walk-forward temporal validation:
  * two base learners (CatBoost + ElasticNet) blended via a residual mixture,
  * a full predictive distribution per film -> HDR50 band, Bayes point, P78 upside,
  * a calibrated demand-forward flag (P(opening >= $50M)) that lifts confident large films,
  * pedigree already gated behind demand inside the OW_FEATURES view,
  * judged on an asymmetric flop-safety loss.

Source-neutral: reads whatever you loaded into {DB}.RESEARCH.OW_FEATURES. See
docs/07_model_architecture.md for the narrative. This is a starting point -- tune it.

Run against your data:
    python model/train_ow_model.py --connection my_sandbox --database MY_SANDBOX_DB

Run with no data at all (synthetic, ~10 min, proves your environment works):
    python model/train_ow_model.py --sample

Deps: pip install -r model/requirements.txt
"""
import argparse, json, warnings
import numpy as np, pandas as pd
warnings.filterwarnings("ignore")
from sklearn.linear_model import ElasticNetCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.calibration import CalibratedClassifierCV
from catboost import CatBoostRegressor

# Columns that are keys/targets, not features
NON_FEATURES = {"MOVIE_ID", "MOVIE_TITLE", "RELEASE_DATE", "DAYS_OUT",
                "OPENING_WEEKEND", "LOG_OPENING_WEEKEND", "THEATER_COUNT"}
LARGE = 50e6          # LARGE+ tier threshold ($50M)
HALFLIFE_MONTHS = 24  # time-decay half-life for sample weights

# Where the reference build landed on 122 films under strict walk-forward validation. This is a
# comparison point, not a specification -- a different film set or better features could
# legitimately land elsewhere. Note that MAPE is a mean over the whole set and says nothing about
# any single film: a good model will land some films almost exactly and miss others badly, and
# that spread is the normal shape of this problem. See docs/06.
REFERENCE_MAPE = 0.38
REFERENCE_MAPE_RANGE = (0.30, 0.50)
# A gap this far below the reference is usually leakage rather than a modeling gain, because the
# as-of filtering is the easiest thing here to get wrong. It is a prompt to check, not a verdict.
CHECK_MAPE_BELOW = 0.25
EXPECTED_HDR50_COVERAGE = (0.42, 0.58)


def load_features(conn_name, database):
    import snowflake.connector
    con = snowflake.connector.connect(connection_name=conn_name)
    df = pd.read_sql(f"SELECT * FROM {database}.RESEARCH.OW_FEATURES", con)
    con.close()
    df.columns = [c.upper() for c in df.columns]
    df["RELEASE_DATE"] = pd.to_datetime(df["RELEASE_DATE"])
    return df


def prepare_matrix(df, feats):
    """Impute missing features with a MISSINGness flag instead of zero-filling.

    An earlier version of this script did `df.fillna(0)`. That is worse than it looks:
    every feature here is a percentile or a rank-like quantity, so 0 does not mean
    "unknown", it means "bottom of the entire field". A film whose search pull failed was
    therefore handed to the model as the least-demanded film in the set -- an actively
    wrong signal rather than a neutral one. Median-fill plus an explicit _MISSING
    indicator lets the model learn "this was unobserved" as its own fact.

    The median is computed on the full column rather than per fold. That is a small,
    deliberate simplification: it is a leak of feature-distribution information (not of
    the label) and it keeps the fold loop readable. If you want it strictly clean, move
    the median computation inside the fold and fit it on train rows only.
    """
    X = df[feats].copy()
    miss = X.isna()
    flag_cols = [c for c in feats if miss[c].any()]
    X = X.fillna(X.median(numeric_only=True))
    X = X.fillna(0.0)   # a column that is entirely NULL has no median; it carries no info
    for c in flag_cols:
        X[f"{c}__MISSING"] = miss[c].astype(float)
    return X.values.astype(float), list(X.columns)


def cbr():
    return CatBoostRegressor(loss_function="RMSE", verbose=0, random_seed=42,
                             iterations=500, depth=5, learning_rate=0.03,
                             l2_leaf_reg=10, thread_count=-1)


def walk_forward_splits(df, n_blocks=8):
    """Sort films by release date; first 50% = base train; predict each of the next
    n_blocks using only earlier films. Returns list of (train_idx, test_idx) over rows.

    Splitting on FILM, not row, is essential: each film contributes 4 horizon rows and
    they must never straddle the train/test boundary or the model sees the same film's
    own -3 row while predicting its -21 row."""
    first_date = df.groupby("MOVIE_ID")["RELEASE_DATE"].min().sort_values()
    films = first_date.index.values
    base = int(0.5 * len(films))
    rows = {f: np.where(df["MOVIE_ID"].values == f)[0] for f in films}
    blocks = np.array_split(films[base:], n_blocks)
    splits = []
    for i, blk in enumerate(blocks):
        train_films = np.concatenate([films[:base]] + [blocks[j] for j in range(i)]) if i else films[:base]
        tr = np.concatenate([rows[f] for f in train_films])
        te = np.concatenate([rows[f] for f in blk])
        splits.append((tr, te))
    return splits


def decay_weights(dates, cutoff):
    age_months = (cutoff - dates).dt.days.values / 30.44
    return 0.5 ** (age_months / HALFLIFE_MONTHS)


def hdr_triple(cb_point, lin_point, cb_res, lin_res):
    """Residual-mixture distribution -> (lo, hi, hdr50_mean, bayes q1/3, q0.55, p78)."""
    samples = np.exp(np.concatenate([cb_point + cb_res, lin_point + lin_res]))
    s = np.sort(samples); n = len(s); k = int(np.floor(0.5 * n))
    j = int(np.argmin(s[k:] - s[:n - k]))          # narrowest 50% window = HDR50
    lo, hi = s[j], s[j + k]
    hdr_mean = s[(s >= lo) & (s <= hi)].mean()
    return lo, hi, hdr_mean, np.quantile(samples, 1/3), np.quantile(samples, 0.55), np.quantile(samples, 0.78)


def aloss(pred, actual, r=2.0):
    lr = np.log(pred / actual)
    return float(np.mean(r * np.maximum(lr, 0) + np.maximum(-lr, 0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--connection", help="named Snowflake connection")
    ap.add_argument("--database", help="sandbox database holding RESEARCH.OW_FEATURES")
    ap.add_argument("--sample", action="store_true",
                    help="run on synthetic data (no Snowflake needed) to verify the setup")
    ap.add_argument("--out", default="model/oof_predictions.json")
    args = ap.parse_args()

    if args.sample:
        from sample_data import make_sample
        df = make_sample()
        print("*** SYNTHETIC DATA. These numbers describe a random-number generator, "
              "not films. Do not quote them. ***\n")
    else:
        if not (args.connection and args.database):
            ap.error("--connection and --database are required unless you pass --sample")
        df = load_features(args.connection, args.database)

    feats = [c for c in df.columns if c not in NON_FEATURES and pd.api.types.is_numeric_dtype(df[c])]
    X, used = prepare_matrix(df, feats)
    yln = df["LOG_OPENING_WEEKEND"].values
    ow = df["OPENING_WEEKEND"].values
    dates = df["RELEASE_DATE"]
    horizon = df["DAYS_OUT"].values
    tier_large = (ow >= LARGE).astype(int)
    n = len(df)

    cb = np.full(n, np.nan); lin = np.full(n, np.nan); p_large = np.full(n, np.nan)
    for tr, te in walk_forward_splits(df):
        w = decay_weights(dates.iloc[tr], dates.iloc[tr].max())
        m = cbr(); m.fit(X[tr], yln[tr], sample_weight=w); cb[te] = m.predict(X[te])
        sc = StandardScaler().fit(X[tr])
        # NOTE: ElasticNetCV's internal cv=5 is random k-fold over the TRAINING block only.
        # That is hyperparameter selection inside an already-temporal fold, not evaluation,
        # so it does not leak the future into any reported metric. If you want it strictly
        # temporal, pass a TimeSeriesSplit here instead.
        en = ElasticNetCV(l1_ratio=[.1, .5, .9], cv=5, max_iter=8000, random_state=42)
        en.fit(sc.transform(X[tr]), yln[tr], sample_weight=w); lin[te] = en.predict(sc.transform(X[te]))
        # demand-forward flag: P(opening >= $50M), calibrated
        if len(np.unique(tier_large[tr])) > 1:
            clf = CalibratedClassifierCV(
                RandomForestClassifier(n_estimators=400, min_samples_leaf=2, random_state=42, n_jobs=-1),
                method="isotonic", cv=3)
            clf.fit(X[tr], tier_large[tr])
            p_large[te] = clf.predict_proba(X[te])[:, list(clf.classes_).index(1)]
        else:
            p_large[te] = 0.0

    cov = ~np.isnan(cb)

    # HORIZON-SCOPED residual pools. A 21-days-out prediction is genuinely more uncertain
    # than a 3-days-out one, so pooling all residuals together (as an earlier version did)
    # borrows the tight late-horizon spread to size the wide early-horizon band and
    # understates uncertainty exactly where the user most needs it widened. Fall back to
    # the global pool when a horizon has too few residuals to characterize.
    res_by_h = {}
    for h in np.unique(horizon):
        sel = cov & (horizon == h)
        if sel.sum() >= 30:
            res_by_h[h] = ((yln - cb)[sel], (yln - lin)[sel])
    global_res = ((yln - cb)[cov], (yln - lin)[cov])

    # one row per film at its latest available horizon
    last = df[cov].sort_values("DAYS_OUT").groupby("MOVIE_ID").tail(1)
    recs = []
    for i in last.index:
        cb_res, lin_res = res_by_h.get(df.at[i, "DAYS_OUT"], global_res)
        lo, hi, hdr_mean, bayes, q55, p78 = hdr_triple(cb[i], lin[i], cb_res, lin_res)
        pl = float(p_large[i])
        point = max(hdr_mean, q55) if pl >= 0.4 else hdr_mean   # Track B lift for confident large films
        recs.append({"movie_title": df.at[i, "MOVIE_TITLE"], "actual_ow_m": round(ow[i]/1e6, 2),
                     "predicted_ow_m": round(point/1e6, 2), "bayes_ow_m": round(bayes/1e6, 2),
                     "hdr_lo_m": round(lo/1e6, 2), "hdr_hi_m": round(hi/1e6, 2),
                     "upside_p78_m": round(p78/1e6, 2), "p_large": round(pl, 3),
                     "days_out": int(df.at[i, "DAYS_OUT"])})

    a = np.array([r["actual_ow_m"] for r in recs]) * 1e6
    p = np.array([r["predicted_ow_m"] for r in recs]) * 1e6
    ape = np.abs(p - a) / a
    inband = np.mean([(a[k] >= recs[k]["hdr_lo_m"]*1e6) & (a[k] <= recs[k]["hdr_hi_m"]*1e6)
                      for k in range(len(recs))])
    big = a >= 60e6
    mape = float(ape.mean())
    print(f"n_films            {len(recs)}")
    print(f"MAPE               {mape*100:5.1f}%")
    print(f"median APE         {np.median(ape)*100:5.1f}%")
    print(f"aLoss (flop-safe)  {aloss(p, a):.3f}")
    print(f"HDR50 coverage     {inband*100:5.1f}%   (target ~50%)")
    if big.sum():
        print(f">=$60M signed-log {np.mean(np.log(p[big]/a[big])):+.3f}   (neg = under-predict)")

    # ── How your numbers compare to the reference build ───────────────────────────
    print("\n-- vs reference build (MAPE ~38% on 122 films) --")
    if mape < CHECK_MAPE_BELOW:
        print(f"   Your MAPE {mape*100:.1f}% is well below the reference. That may be a real gain on a")
        print("   different film set -- but leakage is the most common cause and it is cheap to rule")
        print("   out. Worth checking, in this order:")
        print("   1. Are comment features horizon-filtered? (sql/10 tripwire 1 -- comment")
        print("      volume must GROW from -21 to -3.)")
        print("   2. Is COMMENT_DATE populated in TRAILER_COMMENTS_SCORED?")
        print("   3. Did a post-release feature (screen count, live popularity score, a")
        print("      final-gross-derived column) reach OW_FEATURES?")
        print("   4. Do any of a film's own rows appear in both train and test?")
        print("   Note: individual films landing near-exact is normal and is NOT a symptom of")
        print("   anything. This check is about the aggregate only.")
    elif not (REFERENCE_MAPE_RANGE[0] <= mape <= REFERENCE_MAPE_RANGE[1]):
        print(f"   Your MAPE {mape*100:.1f}% sits outside the reference range "
              f"{REFERENCE_MAPE_RANGE[0]*100:.0f}-{REFERENCE_MAPE_RANGE[1]*100:.0f}%.")
        print("   High side is usually thin coverage (check SEARCH_OBS_COUNT in sql/05) or a")
        print("   film set including limited releases. Not necessarily wrong -- worth a look.")
    else:
        print(f"   Your MAPE {mape*100:.1f}% is in the same range as the reference build.")
    if not (EXPECTED_HDR50_COVERAGE[0] <= inband <= EXPECTED_HDR50_COVERAGE[1]):
        print(f"   HDR50 coverage {inband*100:.1f}% is off ~50%: the band is "
              f"{'too narrow (overconfident)' if inband < 0.5 else 'too wide (underconfident)'}.")
    print("   Reminder: report the intent classifier's macro-F1 (sql/30_intent_eval.sql)")
    print("   alongside these numbers. An unmeasured input classifier is an unmeasured error bar.")

    json.dump(recs, open(args.out, "w"), indent=1)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    main()

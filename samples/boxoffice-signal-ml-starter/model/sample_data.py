#!/usr/bin/env python3
"""Synthetic OW_FEATURES generator — lets you run the model before you have any data.

WHY THIS EXISTS
Standing up the real pipeline means five separate data-access decisions, each with its
own terms of service, keys, and rate limits. That is the honest cost of the project, but
it is a terrible first experience: you cannot tell whether your environment works until
after all five succeed. This module fabricates a feature table with the same column
names, grain, and rough correlation structure as the real one, so you can run
`train_ow_model.py --sample` in ten minutes, watch the metric block print, and then swap
in real data with confidence that the machinery is fine.

WHAT IT IS NOT
These are not films. The numbers come from a generative model tuned to look plausible, so:
  * Do NOT quote any accuracy you get from --sample. It measures nothing about reality.
  * The effect sizes are deliberately close to the published ones (volume carries most of
    the signal, net intent is weak and nearly orthogonal) so the metric block lands in a
    believable range -- which also means an unexpectedly LOW error on sample data points at a
    bug in the pipeline rather than an improvement, since there is no real signal here to find.
"""
import numpy as np
import pandas as pd

HORIZONS = [-21, -14, -7, -3]


def make_sample(n_films: int = 180, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # A latent "how big is this film" factor. Everything observable is a noisy read on it.
    size = rng.normal(0, 1, n_films)

    # Release dates spread over ~4 years so walk-forward validation has something to bite.
    start = np.datetime64("2022-01-07")
    offsets = np.sort(rng.integers(0, 1460, n_films))
    release = start + offsets.astype("timedelta64[D]")

    # Comment volume: strongly tied to size (this is the workhorse feature).
    log_vol_final = 6.0 + 1.05 * size + rng.normal(0, 0.72, n_films)

    # Net intent: nearly independent of size, small true effect. Matches the finding that
    # intent stacks additively rather than echoing volume.
    net_intent = rng.normal(0, 12, n_films) + 1.5 * size

    # Search / pageview demand: another noisy read on size, correlated with volume.
    search_latent = 0.85 * size + rng.normal(0, 0.6, n_films)
    page_latent = 0.80 * size + rng.normal(0, 0.7, n_films)

    budget_log = 17.2 + 0.75 * size + rng.normal(0, 0.5, n_films)
    star = np.clip(0.5 + 0.25 * size + rng.normal(0, 0.3, n_films), 0, 2)
    ip_high = (size + rng.normal(0, 0.7, n_films) > 0.8).astype(int)
    predow_log = np.where(ip_high == 1, 17.0 + 0.6 * size + rng.normal(0, 0.6, n_films), 0.0)
    genre_action = (rng.random(n_films) < 0.30).astype(int)
    genre_horror = (rng.random(n_films) < 0.18).astype(int)
    month = ((offsets % 365) // 30 + 1).clip(1, 12)

    # The label. Volume does most of the work; net intent adds a small independent lift.
    log_ow = (
        17.05
        + 0.62 * (log_vol_final - 6.0)
        + 0.0055 * net_intent
        + 0.18 * search_latent
        + rng.normal(0, 0.45, n_films)
    )
    ow = np.exp(log_ow)

    def pctile(x):
        return pd.Series(x).rank(pct=True).values

    rows = []
    for h in HORIZONS:
        # Comment threads GROW toward release: an earlier horizon sees fewer comments.
        # This is what the as-of filter in sql/10 produces, and reproducing it here means
        # the sample data exercises the same shape as the real thing.
        frac = {-21: 0.45, -14: 0.62, -7: 0.82, -3: 1.00}[h]
        vol = np.maximum(5, np.round(np.exp(log_vol_final) * frac)).astype(int)

        # Demand also ramps, and is noisier further out.
        ramp = {-21: 0.70, -14: 0.82, -7: 0.93, -3: 1.00}[h]
        s = search_latent * ramp + rng.normal(0, 0.25 * (1 - ramp) + 0.05, n_films)
        p = page_latent * ramp + rng.normal(0, 0.25 * (1 - ramp) + 0.05, n_films)
        ni = net_intent + rng.normal(0, 3, n_films)

        s_r7, s_pk = pctile(s), pctile(s + rng.normal(0, 0.2, n_films))
        p_pk, p_r7 = pctile(p + rng.normal(0, 0.2, n_films)), pctile(p)
        qmult = 1 / (1 + np.exp(-0.2 * ni))

        rows.append(pd.DataFrame({
            "MOVIE_ID": np.arange(1, n_films + 1),
            "MOVIE_TITLE": [f"Sample Film {i:03d}" for i in range(1, n_films + 1)],
            "RELEASE_DATE": release,
            "DAYS_OUT": h,
            "OPENING_WEEKEND": ow,
            "LOG_OPENING_WEEKEND": log_ow,
            "THEATER_COUNT": np.clip((2200 + 900 * size + rng.normal(0, 300, n_films)), 1000, 4500).astype(int),
            "YT_COMMENTS": vol,
            "LOG_YT": np.log1p(vol),
            "AVG_SENT": np.clip(0.05 + 0.02 * ni / 12 + rng.normal(0, 0.2, n_films), -1, 1),
            "PCT_THEA": np.clip(8 + ni / 2 + rng.normal(0, 3, n_films), 0, 60),
            "PCT_PASS": np.clip(8 - ni / 2 + rng.normal(0, 3, n_films), 0, 60),
            "PCT_POS": np.clip(45 + ni / 2 + rng.normal(0, 8, n_films), 0, 100),
            "PCT_NEG": np.clip(20 - ni / 3 + rng.normal(0, 6, n_films), 0, 100),
            "NET_INTENT_PCT": ni,
            "SEARCH_ROLLING_3D_PCTILE": pctile(s + rng.normal(0, 0.15, n_films)),
            "SEARCH_ROLLING_7D_PCTILE": s_r7,
            "SEARCH_ROLLING_14D_PCTILE": pctile(s + rng.normal(0, 0.25, n_films)),
            "SEARCH_PEAK_PCTILE": s_pk,
            "SEARCH_VEL_PCTILE": pctile(rng.normal(0, 1, n_films)),
            "SEARCH_SLOPE_PCTILE": pctile(0.3 * size + rng.normal(0, 1, n_films)),
            "PAGEVIEW_R7D_PCTILE": p_r7,
            "PAGEVIEW_PEAK_PCTILE": p_pk,
            "PAGEVIEW_CUM_PCTILE": pctile(p + rng.normal(0, 0.2, n_films)),
            "PAGEVIEW_VEL_PCTILE": pctile(rng.normal(0, 1, n_films)),
            "RELEASE_MONTH": month,
            "IS_PEAK_SEASON": np.isin(month, [5, 6, 7, 11, 12]).astype(int),
            "RUNTIME": np.clip(110 + 12 * size + rng.normal(0, 12, n_films), 80, 190).astype(int),
            "GENRE_ACTION_FRANCHISE": genre_action,
            "GENRE_HORROR": genre_horror,
            "GENRE_ANIMATION_FAMILY": np.zeros(n_films, int),
            "GENRE_ORIGINAL": (1 - ip_high),
            "RATING_G": np.zeros(n_films, int),
            "RATING_PG": (rng.random(n_films) < 0.2).astype(int),
            "RATING_PG13": (rng.random(n_films) < 0.5).astype(int),
            "RATING_R": (rng.random(n_films) < 0.3).astype(int),
            "QMULT": qmult,
            "QADJ_SEARCH": s_r7 * qmult,
            "QADJ_PAGEVIEW": p_pk * qmult,
            "SEARCH7_X_STAR": s_r7 * star,
            "SEARCH7_X_IP_HIGH": s_r7 * ip_high,
            "SEARCH7_X_PREDOW": s_r7 * predow_log,
            "SEARCH7_X_ACTION": s_r7 * genre_action,
            "SEARCH7_X_HORROR": s_r7 * genre_horror,
            "PAGEVIEWPK_X_STAR": p_pk * star,
            "SENT_X_SEARCH7": (0.05 + 0.02 * ni / 12) * s_r7,
            "THEA_X_SEARCH7": np.clip(8 + ni / 2, 0, 60) * s_r7,
            "NET_X_SEARCH7": ni * s_r7,
            "NET_X_PAGEVIEWPK": ni * p_pk,
            "SENT_X_PAGEVIEWPK": (0.05 + 0.02 * ni / 12) * p_pk,
            "THEA_X_PAGEVIEWPK": np.clip(8 + ni / 2, 0, 60) * p_pk,
        }))

    df = pd.concat(rows, ignore_index=True)

    # Punch realistic holes in the demand columns. Real pulls fail; the model must cope,
    # and imputing these to zero (a common shortcut) is actively wrong -- see the
    # impute-with-flag handling in train_ow_model.py.
    for col in ["SEARCH_VEL_PCTILE", "PAGEVIEW_VEL_PCTILE", "SEARCH_SLOPE_PCTILE"]:
        mask = rng.random(len(df)) < 0.08
        df.loc[mask, col] = np.nan

    return df.sort_values(["RELEASE_DATE", "MOVIE_ID", "DAYS_OUT"]).reset_index(drop=True)


if __name__ == "__main__":
    d = make_sample()
    d.to_csv("model/sample_features.csv", index=False)
    print(f"wrote model/sample_features.csv  rows={len(d)}  films={d.MOVIE_ID.nunique()}")

# 07 — Model architecture

This is the framework that produced the results in `docs/06`. It's a **distributional
regressor**, not a tier classifier — the switch away from tiering is what removed the
blockbuster ceiling. Everything below runs on the `OW_FEATURES` view from
`sql/10_feature_view.sql`. A runnable reference implementation lives in
`model/train_ow_model.py`; this page explains what it does and why.

## 1. What it predicts
For each film it emits a **full predictive distribution** of domestic opening weekend, not a
single number — then derives a point estimate, a confidence band, and an upside figure from
that distribution. Predicting a distribution (rather than assigning a size tier and
regressing within it) is what lets the biggest films score high without a per-tier ceiling.

## 2. Validation: strict walk-forward in time
Never random k-fold — that leaks the future. Instead:
- Sort films by release date.
- Use the earliest **~50%** as a base training block.
- Split the later ~50% into **8 sequential blocks**; predict each block using **only films
  released before it** (base + earlier blocks).
- **Time-decay sample weights:** older films count less, `weight = 0.5 ** (age_months / 24)`
  (a 24-month half-life relative to the fold's cutoff date).
- **Split on film, not row.** Each film contributes four horizon rows; if they straddle the
  boundary the model sees a film's own −3 row while predicting its −21 row.

Note that only the later ~50% of films ever get an out-of-fold prediction, so your reported
`n_films` is about half your set. That is the cost of doing it honestly.

One acknowledged inconsistency: `ElasticNetCV` uses random 5-fold internally for
hyperparameter selection *within* each temporal training block. That does not leak the future
into any reported metric — it never touches test rows — but if you want it strictly temporal,
pass a `TimeSeriesSplit`.

Every accuracy number is out-of-fold under this scheme. When you compare two model variants,
hold the films, features, and splits identical and change only the one thing you're testing.

## 3. Two base learners (blended)
Trained on `LOG_OPENING_WEEKEND` inside each fold, with the time-decay weights:
- **Gradient-boosted trees** — CatBoost regressor, `loss=RMSE, iterations=500, depth=5,
  learning_rate=0.03, l2_leaf_reg=10`. Captures non-linear interactions.
- **Regularized linear** — ElasticNetCV (`l1_ratio ∈ {.1,.5,.9}`, 5-fold) on standardized
  features. A stable, smooth baseline the trees blend against.

Keep the **out-of-fold residuals** of each learner — they define the spread in the next step.

## 4. From two points to a distribution (residual mixture)
For a film, take each learner's point prediction in log space and add back the pooled OOF
residuals of that learner, forming a sample of plausible outcomes; pool both learners'
samples and exponentiate to dollars. From that empirical distribution compute:
- **HDR50** — the *highest-density region* covering 50% of the mass (the narrowest interval
  containing half the samples): gives a `[lo, hi]` band and an HDR50 mean.
- **Bayes point** — the lower-third quantile (`q = 1/3`), a flop-safe central estimate.
- **Upside (P78)** — the 0.78 quantile, reported as a credible high-end.

The band matters as much as the point: it's how the model expresses confidence, and how it
handles blockbusters without inflating the point estimate.

**Pool residuals PER HORIZON.** A 21-days-out prediction is genuinely more uncertain than a
3-days-out one. Pooling all residuals into one bucket borrows the tight late-horizon spread
to size the wide early-horizon band, so the model reports its most confident-looking
intervals exactly where it knows least. `train_ow_model.py` scopes the residual pool by
`DAYS_OUT` and falls back to the global pool only when a horizon has fewer than 30 residuals
to characterize.

## 4b. Missing features: impute with a flag, never with zero
Every demand feature here is a percentile. Zero-filling a missing one does not say
"unknown" — it says *"least-demanded film in the entire set"*, which is an actively wrong
signal rather than a neutral one, and it is the difference between a film whose search pull
failed and a film nobody is searching for. Fill with the column median and add an explicit
`<COL>__MISSING` indicator so the model can learn unobservedness as its own fact. `sql/05`
deliberately leaves un-computable percentiles NULL rather than coalescing them for the same
reason.

## 5. Demand-forward flag + point-lift (Tracks B and C)
A separate **calibrated classifier** estimates `p_large = P(opening ≥ $50M)` (e.g. a
random forest wrapped in isotonic `CalibratedClassifierCV`, cv=3), trained on the same
walk-forward folds.
- **Track B (point-lift for confident large films):** if `p_large ≥ 0.4`, the reported point
  is `max(HDR50 mean, Q55)` — nudged toward the demand-implied level; otherwise it's the
  HDR50 mean. This lifts genuine event films **without** raising the flop over-prediction
  rate on look-alikes.
- **Track C (upside):** report the P78 quantile as the credible high-end.

A **demand-quality gate** (net intent × demand, already materialized as `QADJ_*`/`NET_X_*`
in `OW_FEATURES`) is what separates true event films from high-demand hype-flops.

## 6. Feature discipline (baked into OW_FEATURES)
- **Pedigree is gated behind demand.** Budget, star power, predecessor gross, and IP tier are
  never standalone features — they enter only through demand interactions (`SEARCH7_X_STAR`,
  `SEARCH7_X_IP_HIGH`, `SEARCH7_X_PREDOW`, …). If the crowd isn't showing up, pedigree can't
  rescue the prediction.
- **No leakage features.** Any signal contaminated by post-announcement/post-release activity
  (e.g. a live third-party "popularity" score) is excluded — it inflates offline metrics and
  collapses under walk-forward validation.

## 7. Objective & metrics — optimize for flop-safety
Over-predicting a flop is worse than under-predicting a hit, so judge on an **asymmetric loss**:

```
aLoss = mean( r * max(log(pred/actual), 0)  +  max(-log(pred/actual), 0) ),  r = 2
```

(over-prediction penalized 2×). Report alongside it:
- **MAPE** and **median APE** (typical error),
- **low-band over-prediction rate** (share of small films predicted > 1.5× actual),
- **HDR50 coverage** (share of actuals landing inside the band — should be ≈ 50%),
- **large-film signed log-error** (bias on actual ≥ $60M).

## 8. Build it with CoCo
First, prove the machinery runs with no data at all:
```bash
python model/train_ow_model.py --sample     # synthetic; expect MAPE ~40%
```
Then point it at your own features:
> "Read `docs/07_model_architecture.md` and `model/train_ow_model.py`. Point it at my
> `{{SANDBOX_DB}}.RESEARCH.OW_FEATURES` view, run the walk-forward backtest, and report MAPE,
> median APE, aLoss, HDR50 coverage, and large-film bias. Then compare it against a simple
> tier-classifier baseline on the same films and splits."

If your MAPE comes in much better than that reference, or improves sharply after a change to the
feature pipeline rather than the model, walk the checklist at the bottom of
`sql/10_feature_view.sql` before concluding it is a gain — leakage is the most common cause and
the cheapest thing to rule out. (Individual films landing near-exact is normal and means nothing
about leakage; it is the aggregate that is worth interrogating.)

Reference implementation: **`model/train_ow_model.py`** (source-neutral; reads `OW_FEATURES`).

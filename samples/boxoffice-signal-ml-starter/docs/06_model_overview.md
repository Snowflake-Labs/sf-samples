# 06 — Model overview

This repo gets you to a research-ready feature set. How you model it is up to you, but here
is the approach that held up under rigorous, out-of-sample validation — and, just as
important, the traps that didn't.

## The two signals that carry the weight

- **Comment volume** (Source B): how much organic conversation a trailer generates. This is
  the single strongest input — it does most of the predictive work on its own.
- **Net intent** (Source B, via AISQL): the direction of the conversation
  (theatrical-leaning minus skip-leaning). Weak in isolation, but nearly **independent** of
  volume — so the little it carries **stacks additively** on top of volume rather than
  echoing it.

Add **demand percentiles** (Sources A + C) — search interest and consumer research pageviews —
and you have a demand-forward feature set that doesn't lean on gameable marketing numbers.

### Two things to know about volume before you get excited about it

**1. Test it against paid exposure, not against budget.** The objection to any conversation
metric is that it might just be a receipt for the media spend. The control that answers this is
not production budget — a film's negative cost is only loosely coupled to what is spent on
marketing — it is the trailer's own **view count**, which is the closest public proxy for what a
studio spent on the platform. On the reference set (116 films with both):

| Quantity | r |
|---|---|
| log views ~ log comment volume | **-0.002** |
| log views ~ log opening weekend | -0.06 |
| log comments ~ log opening weekend | 0.68 |
| log comments ~ log OW, **controlling for views** | **0.68** (unchanged) |
| log views ~ log OW, controlling for comments | -0.09 |
| comments-per-view ~ log OW | 0.57 |

Paid view volume and comment volume are orthogonal, so controlling for paid exposure is
mathematically inert — the relationship goes 0.682 to 0.683. A production-budget control does
bite, softening 0.68 to 0.54, which is reasonable: a bigger film is a bigger cultural event
before any media is bought.

**Do not control for theater count.** It is tempting as a "release scale" variable and it is the
wrong choice, because exhibitors allocate screens using demand research and presales. Screen
count already embeds the market's read on demand, so partialling it out subtracts the very
quantity you are trying to measure and reports the remainder as your finding. This is
over-controlling on a variable downstream of the signal, and it will make an honest result look
weaker than it is.

Two caveats if you replicate this. View counts in the reference set are lifetime totals from a
single snapshot rather than pre-release figures — that contamination should bias *toward* a
positive views/OW correlation, so a near-zero result survives it, but it is not a clean
measurement. And "public view count = paid impressions" is an assumption, not something measured
here.

**2. Count volume as-of the horizon, but know how small the correction is.** The conversation
under a trailer is heavily front-loaded, which is what makes this signal a pre-release
measurement almost by default. On the reference corpus of 3.5M comments: **78% arrive within
five days** of the trailer going up, 88% within thirty days, and **94% land before the film
opens** — leaving ~6% post-release.

So the as-of filter in `sql/10` is good hygiene rather than a rescue. It matters for two
practical reasons and not much else: your own horizon features (−21 vs −3) should differ from
each other or the horizon grain is meaningless, and if you scrape a film's thread months after
release you will collect that post-release tail, which is partly a *consequence* of the opening.
Filter on `COMMENT_DATE` and the question does not arise. Just don't expect removing 6% of
comments to move a correlation much — on the reference set it moved the budget-controlled figure
by about a point.

## Measure the intent classifier, or the rest is decoration

Net intent is only as good as the classifier producing it, and classifier quality is the
most commonly skipped measurement in this kind of work. Hand-label 150–300 comments and run
`sql/30_intent_eval.sql` for a per-class confusion matrix and macro-F1.

What the reference build found when it finally did this, which is worth knowing before you
trust your own first pass:

- The production scorer — the one that actually built the model's features — scored
  **macro-F1 0.65** on a 299-comment gold set. A revised single-call prompt scored **0.85**
  on the same labels. That is a large amount of headroom sitting in the prompt, not the model.
- The failure was concentrated in one class: **PASS precision was 0.31**. Two thirds of the
  comments the classifier called "skip" were actually neutral — mostly general negativity
  ('this looks awful') being scored as stated avoidance. Since net intent is
  theatrical *minus* pass, a noisy PASS side corrupts the feature while leaving it looking
  stable.
- The single biggest fix was structural: consolidating several independent yes/no calls into
  **one call that must choose among four labels**. Independent per-label flags over-fire.

Report macro-F1 alongside any model accuracy you publish. And note which number you are
quoting: accuracy on an intent-stratified gold set and accuracy over the whole corpus differ
enormously, because ~90% of real comments are neutral and a classifier that answers NEUTRAL
to everything looks excellent on the second measure.

## What "good" looks like

You will build this, get a number, and have nothing to compare it to. For reference, here is
where the reference build landed on 122 films under strict walk-forward validation. Read these
as a comparison point, not a specification — a different film set, a longer history, or better
features could legitimately land elsewhere.

| Metric | Reference build | Reading |
|---|---|---|
| MAPE | ~38% | Aggregate error across films |
| Median APE | ~35% | Typical film; usually a few points under MAPE |
| HDR50 coverage | 45–55% | Far under = overconfident bands |
| ≥$60M signed log-error | slightly negative | Under-predicting the biggest films is normal |
| Intent macro-F1 | 0.75+ | Below ~0.65 and net intent is mostly noise |

**On individual predictions versus the aggregate.** MAPE is a mean over the whole film set, and
it tells you nothing about any single film. A well-built model on this data will land some films
almost exactly — in the reference build one tentpole came in within half a percent — while
missing others badly. That spread is the normal shape of the problem, not evidence of anything
wrong. Do not read a near-exact hit as suspicious.

**What is worth a second look is a large, unexplained move in the aggregate.** Leakage is the
most common cause of an out-of-fold MAPE that lands well below a comparable published baseline,
and it is also the easiest thing to do by accident here — comment threads and search curves keep
growing right up to release, so an aggregate that is filtered wrong looks like a modeling win.
So if your number comes in much better than the reference above, or improves sharply after a
change to the feature pipeline rather than the model, run the checklist in
`sql/10_feature_view.sql` before you draw a conclusion from it. It might be a real gain. The
point is that you cannot tell from the number alone, and the check costs a minute.

`model/train_ow_model.py` prints your metrics next to this reference and points at the checklist
when the gap is large.

Also worth knowing what you are *not* beating. Published industry tracking ranges for wide
releases commonly miss by 20–30% themselves, so a ~38% MAPE model is in the same
neighborhood as the incumbent, not obviously ahead of it. If you want to make a comparative
claim, collect tracking ranges for your own film set and score both on the same films. This
repo does not do that for you, and you should not assume the win.

## Validate out-of-sample: walk-forward in time

Do **not** report accuracy from random k-fold cross-validation on all films — it lets the
model peek at the future. Use **temporal (walk-forward) validation**: sort films by release
date, train only on films released *before* each one, and predict forward. It's harder and
lower-scoring, but it's the only number that reflects real prediction-time performance.

> When you compare two models, hold **everything** constant except the thing you're testing
> — same films, same features, same temporal splits. Comparing a model on an easy film set
> against another on a hard one produces a flattering lie.

## Architecture note: prefer a distributional regressor over a tier classifier

An early instinct is to classify films into size tiers (small / mid / large) and regress
within each. That framing **caps the top** — the biggest films get pulled back toward the
pack and systematically under-predicted. A single model that predicts the opening directly
and reports a **confidence range** (a distribution, not one number) avoids the ceiling and
handles blockbusters through the upper band rather than a fragile point estimate.

On matched films under identical walk-forward validation, moving from the tier-classifier
framing to a distributional one cut average error (MAPE) by a few points — concentrated in
the **tail** (fewer large misses), with the typical/median error roughly unchanged.

This is the short version. The full framework — the two blended learners, the
residual-mixture distribution (HDR band / Bayes point / P78 upside), the demand-forward flag
and point-lift, and the exact validation and flop-safety loss — is in
**`docs/07_model_architecture.md`**, with a runnable reference implementation in
**`model/train_ow_model.py`**.

## Pedigree belongs behind demand

Static "pedigree" features (budget, star power, franchise history, a predecessor's gross)
feel predictive but encourage the model to believe hype. The version that generalized
**dropped standalone pedigree entirely** and let it re-enter only **gated behind demand**
(e.g., demand-percentile × star power). If the crowd isn't showing up in the signals,
pedigree shouldn't rescue the prediction.

## Leakage watch

Exclude any feature contaminated by post-announcement or post-release activity — notably
things like screen count, which can change up to opening weekend and reflects the
expectations of buyers and distributors that may or may not be borne out. The same applies
to a live third-party popularity score (Source E), and to any comment aggregate that is not
filtered to the horizon.

A useful habit: for every feature, ask *"could this value have been different if the film had
opened better?"* If yes, it is out. 

## Build it with CoCo

The full architecture and a copy-paste build/backtest prompt live in
**`docs/07_model_architecture.md`** (reference implementation: `model/train_ow_model.py`).
In short: point CoCo at `{{SANDBOX_DB}}.RESEARCH.OW_FEATURES`, run the walk-forward backtest,
and compare the distributional regressor against a tier-classifier baseline on the **same**
films and splits.

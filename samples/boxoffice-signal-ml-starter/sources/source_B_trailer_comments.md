# Source B — Trailer conversation (volume + AI-scored intent)

This is the **core signal** of the project: the organic conversation under a film's trailer.
Two things matter — **how much** conversation there is (volume) and **which way** it leans
(intent).

## The signal
Public comments posted under official trailer videos: short free text, usually with an
author handle and a like/vote count, often thousands of them for a big title.

## Options CoCo can help you choose from
The conversation lives on the major online video platforms and, to a lesser extent, social
and forum sites. Ask CoCo for the options and it will compare access paths — an official
platform **data API** (preferred, with your own key) vs. rendering the page and reading the
comment section from the DOM — along with their rate limits and terms. Comments typically
load dynamically, so a raw HTML fetch won't contain them; you'll use the API or a rendered
page.

## The processing that makes it useful (Snowflake Cortex AISQL)
Raw comments alone are weak and noisy — the value comes from **classifying** them. A single
AISQL pass scores each comment into:
- **sentiment** (a score and a bucket: positive / neutral / negative), and
- **intent**: `THEATRICAL` ("opening night", "seeing this in IMAX"), `STREAMING` ("wait for
  streaming"), or `PASS` ("hard pass").

From the scored table you derive, per film: comment **volume**, **net intent %**
(theatrical-leaning minus skip-leaning), and sentiment mix. See `docs/06_model_overview.md`.

## Why intent beats raw sentiment (and resists gaming)
Planted hype is generic ("can't wait!!"), and generic praise is the *least* predictive text
in the data. Manipulators also push in one direction, inflating blunt counts while leaving
the decomposed intent signal intact underneath. Classifying *intent* rather than counting
*positivity* is what makes the signal hard to fake.

## Access & etiquette
- Prefer an **official API** with your own key. If you read from a rendered page instead,
  respect the platform's terms, `robots.txt`, and rate limits — don't hammer it.
- **Pseudonymize author handles** on ingest (hash them). Store only what the model needs.

## Known failure modes (as of 2026-07)

- **Comments are disabled on a large share of family and animated trailers.** This is the
  biggest surprise in the whole pipeline and it is a structural absence, not a low count:
  those films have *no* Source B signal, ever. Handle it by excluding on **comment
  availability**, never on the genre flag — a family film whose trailer has a real comment
  thread belongs in your set, and a non-family film with comments switched off does not.
  Filtering by an `ANIMATION_FAMILY` flag instead is the easy mistake and it silently discards
  films that carry usable signal. There is a copy-paste exclusion query at the bottom of
  `sql/00_schema.sql`.
- **Forum and aggregator archive endpoints are unreliable.** The community mirrors people reach
  for when a platform's own API is inconvenient are, as of this writing, variously returning
  400s, 429s, 403s, and timeouts under light load. If your design depends on one of them, it
  will break. Prefer an official API with your own key; treat archives as a bonus, not a
  foundation.
- **A raw HTML fetch will not contain the comments.** They load dynamically. Either use the API
  or render the page.
- **The conversation is front-loaded, which works in your favor.** On the reference corpus of
  3.5M comments, 78% arrive within five days of the trailer going up and 94% land before the film
  opens. That is why this signal is essentially a pre-release measurement by the shape of the
  behavior rather than by force. Still store `COMMENT_DATE` on every row and aggregate as-of each
  horizon (`sql/10_feature_view.sql` does): if you scrape a thread months after release you pick
  up the ~6% post-release tail, which is partly a *consequence* of the opening, and your horizon
  features are meaningless if they don't differ from each other. Just don't expect the filter to
  swing a correlation — on the reference set it moved things by about a point.
- **Measure the classifier.** See `sql/30_intent_eval.sql`. The reference build's production
  scorer turned out to be at macro-F1 0.65 while a revised prompt hit 0.85 on the same labels,
  with almost all of the damage in one class. You will not discover that by reading output.

## Feeds these columns
- `TRAILER_COMMENTS_RAW` (text, pseudonymized author, like count, video/movie id, **comment date**).
- `TRAILER_COMMENTS_SCORED` (+ `COMMENT_DATE`, `SENTIMENT_SCORE`, `SENTIMENT_BUCKET`,
  `THEATRICAL_INTENT`, `STREAMING_INTENT`, `PASS_INTENT`).
- `INTENT_GOLD` (your hand labels, for `sql/30_intent_eval.sql`).

## Ask CoCo
> "Read `sources/source_B_trailer_comments.md`. What are my options for pulling public
> trailer comments, and which official API should I use with my own key? Show me how to
> ingest comments for one trailer, pseudonymize handles, load to
> `{{SANDBOX_DB}}.RESEARCH.TRAILER_COMMENTS_RAW`, and score them with Cortex AISQL into
> sentiment + intent."

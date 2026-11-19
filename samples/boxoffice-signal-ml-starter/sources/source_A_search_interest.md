# Source A — Pre-release search & attention demand

## The signal
How much the public is actively searching for or looking up a film in the weeks before it
opens. A rising interest curve ahead of release is one of the strongest, hardest-to-game
demand proxies you can get.

## Options CoCo can help you choose from
Several public and commercial services expose pre-release interest/attention data. Ask CoCo
and it will lay out the common choices with their trade-offs — free vs. paid, an official
API vs. a community client library, geographic coverage, history depth, and rate limits —
then help you pick one that fits your access and budget. Most solo research projects land on
a widely-used free interest index; paid attention-data vendors are alternatives when you
need higher resolution or a guaranteed SLA.

## Things to sort out with whichever you pick
- **Comparability.** Some interest sources return a *relative* index (rescaled per request)
  rather than absolute counts, so two separate pulls aren't on the same scale. The usual fix
  is to include a stable, high-volume reference term in each pull and normalize against a
  standalone baseline of that reference — CoCo will tailor the exact method to your source.
- **Disambiguation.** Track the *film*, not every search that happens to share its title.
  Most sources offer a stable topic/entity ID or an exact-match mode; prefer that over
  free-text, which pulls in unrelated results.
- **Access.** May need an API key plus a locale/timezone config; expect throttling, so build
  in backoff and validate a new title's pull against the source's own UI before trusting it.

## Known failure modes (as of 2026-07)
This is the signal that breaks most often, and the failures are not graceful. Budget real
time here — it is where the reference build lost the most hours.

- **The obvious free community client library for the best-known free interest index is
  currently broken**, not merely rate-limited. Expect hard HTTP 429s on nearly every request,
  and separately an incompatibility with modern HTTP-client library versions that surfaces as
  an import or connection-pool error rather than an honest "too many requests". Do not spend a
  day assuming you misconfigured it. Ask CoCo to check the library's current issue tracker
  *before* you build against it.
- **When the API path is unavailable, a manual export path still works.** The provider's own UI
  will hand you a CSV for a small set of terms over a date range. Tedious, doesn't scale to
  hundreds of films, but reliable — and for a research set you refresh weekly it is entirely
  workable. Plan for a hybrid: bulk history by hand once, incremental pulls automated if and
  when the API cooperates.
- **Silent scale changes.** A relative index can rescale between pulls with no error raised.
  Always re-pull the anchor term alongside each film, and alert on any week where the anchor's
  own baseline moves more than you'd expect. An unnoticed rescale corrupts the whole batch.
- **A NULL is not a zero.** If the anchor floors out, the normalized value is undefined — store
  NULL. Coalescing to 0 tells the model this was the least-searched film in the set, which is a
  wrong answer masquerading as a neutral one.

None of this is in the provider's documentation. It's in issue trackers and other people's blog
posts — tell CoCo to look there, not just at the API reference.

## Feeds these columns
- `SEARCH_INTEREST`, `SEARCH_ANCHOR_BASELINE`, `ENTITY_IDS`
- Downstream: rolling / peak / velocity **demand percentiles by horizon** (−21/−14/−7/−3 days)
  in `DEMAND_PERCENTILES`, built by `sql/05_demand_percentiles.sql`.

## Ask CoCo
> "Read `sources/source_A_search_interest.md`. What are my options for a pre-release
> search/attention demand signal? Compare a couple of free and paid choices, recommend one
> for a solo research project, explain how to normalize it and disambiguate the title, and
> help me pull a validated sample for one film."

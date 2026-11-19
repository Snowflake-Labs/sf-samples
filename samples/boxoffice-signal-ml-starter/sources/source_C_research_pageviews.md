# Source C — Consumer research pageview activity

## The signal
How many people are pulling up a film's information page to research it in the run-up to
release — pageview activity as a demand proxy. Like search interest, a rising curve is a
clean, hard-to-game signal, and these counts are usually **absolute**, so they compare
directly across films and dates.

## Options CoCo can help you choose from
Several public sources publish **page-traffic / pageview APIs** for the pages people visit
to research a title. Ask CoCo for the options and it will point you to one with an open API
and daily granularity, and note the etiquette each expects.

## Things to sort out with whichever you pick
- **Exact matching.** Pages have canonical titles with quirks (spacing, disambiguation
  suffixes, release-year variants) and redirects — resolve to the canonical page first.
- **Turn counts into features.** Pull daily views across the pre-release window, align to
  days-out horizons (−21/−14/−7/−3), then convert to **percentiles across your film set**
  (cumulative, rolling, peak, velocity) so the huge dynamic range doesn't dominate.
- **Etiquette.** These APIs are often keyless but ask for a descriptive contact string and
  polite request rates. History doesn't change, so cache aggressively.

## Feeds these columns
- `PAGEVIEW_DEMAND` (per movie, per date)
- Downstream: demand percentiles by horizon in `DEMAND_PERCENTILES`.

## Ask CoCo
> "Read `sources/source_C_research_pageviews.md`. What are my options for a consumer-research
> pageview demand signal with an open API? Help me resolve one film to its canonical page,
> pull daily views for the pre-release window, and load them to
> `{{SANDBOX_DB}}.RESEARCH.PAGEVIEW_DEMAND`."

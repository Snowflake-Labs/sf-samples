# Data source dossiers

The files in this folder describe each signal the pipeline uses **by its characteristics**,
rather than prescribing one provider. Teams differ in the data access, budget, and terms
they have, and some providers restrict how their data may be accessed — so the kit stays
provider-neutral and helps you research the option that fits, under your own access.

Each dossier covers **what the signal is**, how it's typically accessed, the kinds of
providers that offer it, the gotchas that matter, and the schema columns it feeds. That's
enough for Cortex Code to lay out your options when you ask — and then to help you set up the
one you choose against **your own** access.

## The research pattern

Open the repo in Cortex Code and ask, for any dossier:

> "Read `sources/source_A_search_interest.md`. Based on these characteristics, what public
> data sources and Python client libraries could I use? Compare the options, then help me
> obtain access and do the smallest first pull to validate the one I pick."

CoCo will suggest the likely options, the access method, and the client, then walk you
through setup. Repeat per source. The full prompt set is in `../prompts/coco_prompt_library.md`.

## Why dossiers instead of a fixed list?

- It keeps the kit **provider-neutral** — you choose the source that fits your access,
  budget, and terms, and nothing here implies an endorsement of any provider.
- It builds in a moment of due diligence: when CoCo suggests a source, **you** confirm its
  current Terms of Service and access rules before ingesting. Signals age; ToS change.

## The five signals

| Dossier | Signal | In the model? |
|---|---|---|
| `source_A_search_interest.md` | Normalized search-interest index per title | Yes (demand) |
| `source_B_trailer_comments.md` | Public trailer-comment volume + AI-scored intent | Yes (the core signal) |
| `source_C_research_pageviews.md` | Consumer research pageview demand | Yes (demand) |
| `source_D_box_office_history.md` | Historical grosses + opening weekends (the label) | Yes (target + history) |
| `source_E_metadata_popularity.md` | Third-party metadata popularity score | **No — excluded as leakage** |

Read `docs/03_identify_your_sources.md` for the guided flow.

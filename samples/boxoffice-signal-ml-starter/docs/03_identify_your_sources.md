# 03 — Research and choose your data sources (with CoCo)

The `sources/` dossiers describe each signal by its characteristics rather than prescribing
one provider (see `sources/README.md`), because the right source depends on your access,
budget, and terms. This page is the guided flow to research the best-fitting source for each
signal and set it up — using Cortex Code as your research partner.

## The loop, per source

1. Open the dossier (`sources/source_X_*.md`) in Cortex Code.
2. Ask CoCo to **research which provider(s) fit**, from the signal's characteristics.
3. **You confirm** the current Terms of Service and access rules before ingesting.
4. Ask CoCo to help you do the **smallest validated first pull**, then load it.

## Research prompts (copy-paste)

**Source A — search & attention demand**
> "Read `sources/source_A_search_interest.md`. What are my options for a pre-release
> search/attention demand signal — free and paid? Recommend one for a solo project, explain
> how to normalize it and disambiguate the title, and help me do one validated pull."

**Source B — trailer conversation**
> "Read `sources/source_B_trailer_comments.md`. What are my options for pulling public
> trailer comments, and which official API should I use with my own key? Show me how to pull
> one trailer, pseudonymize handles, and load to `{{SANDBOX_DB}}.RESEARCH.TRAILER_COMMENTS_RAW`."

**Source C — consumer research pageview demand**
> "Read `sources/source_C_research_pageviews.md`. What are my options for a consumer-research pageview
> demand signal with an open API? Help me resolve one film to its canonical page and pull the
> pre-release window."

**Source D — box office (the label)**
> "Read `sources/source_D_box_office_history.md`. What are my options for opening-weekend
> data, and which offer official or licensed access? Help me set one up and a
> release-date/OW validation check against a second reference."

**Source E — title metadata (use with care)**
> "Read `sources/source_E_metadata_popularity.md`. What are my options for a title-metadata
> source? I only want static fields (budget, runtime, genre). Explain why a live popularity
> score is leakage and keep it out of my feature view."

## Your due-diligence checklist (do this, don't skip)

- [ ] Confirm the source you chose actually fits your access and terms.
- [ ] Read that provider's **current** Terms of Service and API/scraping policy.
- [ ] Use **official or licensed** access wherever offered (required for Source D).
- [ ] Get your **own** key; store it in `.env` (never in git, never in a skill file).
- [ ] Pseudonymize any user handles (Source B).
- [ ] Do one small validated pull before bulk ingesting.

Once all sources are identified and access is sorted, go to **`docs/04_stand_up_the_pipeline.md`**.

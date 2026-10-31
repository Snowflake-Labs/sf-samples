-- 00_schema.sql — source-agnostic schema for the trailer-signal pipeline.
-- No data. Replace {{SANDBOX_DB}} with your sandbox database (see docs/02).
-- Run in Cortex Code or: snow sql --connection my_sandbox -f sql/00_schema.sql
--
-- Naming is deliberately provider-neutral: SEARCH_* = search/attention demand (Source A),
-- PAGEVIEW_* = consumer-research pageview demand (Source C). Nothing here is a live
-- popularity score — that's excluded as leakage (see sources/source_E_metadata_popularity.md).

USE DATABASE {{SANDBOX_DB}};
CREATE SCHEMA IF NOT EXISTS RESEARCH;
USE SCHEMA RESEARCH;

-- ---------------------------------------------------------------------------
-- Identity / spine
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MOVIE_MAP (
    MOVIE_ID        NUMBER        PRIMARY KEY,
    MOVIE_TITLE     STRING        NOT NULL          -- Title Case; keep consistent everywhere
);

CREATE TABLE IF NOT EXISTS RELEASE_DATES (
    MOVIE_ID        NUMBER        PRIMARY KEY,
    RELEASE_DATE    DATE          NOT NULL           -- VALIDATE against a 2nd reference (Source D)
);

-- ---------------------------------------------------------------------------
-- Source D — box office (target + history)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BOX_OFFICE (
    MOVIE_ID        NUMBER        PRIMARY KEY,
    MOVIE_TITLE     STRING,
    OPENING_WEEKEND FLOAT,                            -- domestic OW in dollars (the label)
    THEATER_COUNT   NUMBER                            -- for wide-release filtering
);

-- ---------------------------------------------------------------------------
-- Source A — search / attention interest (relative index) + normalization scaffolding
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ENTITY_IDS (
    MOVIE_ID        NUMBER        PRIMARY KEY,
    MOVIE_TITLE     STRING,
    RELEASE_DATE    DATE,
    ENTITY_ID       STRING,                           -- stable topic/entity id (NOT free text)
    ENTITY_NAME     STRING,
    MATCH_STATUS    STRING
);

CREATE TABLE IF NOT EXISTS SEARCH_INTEREST (
    MOVIE_ID        NUMBER,
    OBS_DATE        DATE,
    INTEREST        FLOAT,                            -- movie interest, co-scaled with anchor
    ANCHOR_INTEREST FLOAT,                            -- anchor term, same-scale within the pull
    PRIMARY KEY (MOVIE_ID, OBS_DATE)
);

CREATE TABLE IF NOT EXISTS SEARCH_ANCHOR_BASELINE (
    OBS_DATE        DATE          PRIMARY KEY,
    ANCHOR_NORM     FLOAT                             -- normalized continuous anchor timeline
);

-- ---------------------------------------------------------------------------
-- Source B — trailer comments (raw + AI-scored)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS TRAILER_COMMENTS_RAW (
    MOVIE_ID        NUMBER,
    VIDEO_ID        STRING,
    COMMENT_ID      STRING,
    AUTHOR_HASH     STRING,                           -- PSEUDONYMIZED handle (hash, never raw)
    COMMENT_TEXT    STRING,
    LIKE_COUNT      NUMBER,
    COMMENT_DATE    DATE
);

CREATE TABLE IF NOT EXISTS TRAILER_COMMENTS_SCORED (
    MOVIE_ID          NUMBER,
    COMMENT_ID        STRING,
    COMMENT_TEXT      STRING,
    LIKE_COUNT        NUMBER,
    COMMENT_DATE      DATE,                           -- CARRY THIS THROUGH. Without it you
                                                      -- cannot build horizon-correct comment
                                                      -- features and your backtest will leak.
    SENTIMENT_SCORE   FLOAT,                          -- -1..1 (mapped from AI_SENTIMENT label)
    SENTIMENT_BUCKET  STRING,                         -- 'positive' | 'neutral' | 'negative' | 'mixed'
    THEATRICAL_INTENT NUMBER,                         -- 1/0
    STREAMING_INTENT  NUMBER,                         -- 1/0
    PASS_INTENT       NUMBER                          -- 1/0  (NEUTRAL comment = all three 0)
);

-- ---------------------------------------------------------------------------
-- Source C — consumer research pageview activity
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS PAGEVIEW_DEMAND (
    MOVIE_ID        NUMBER,
    OBS_DATE        DATE,
    VIEWS           NUMBER,                           -- absolute daily views
    PRIMARY KEY (MOVIE_ID, OBS_DATE)
);

-- ---------------------------------------------------------------------------
-- Static attributes & pedigree (per film)
-- Your own catalog and/or Source E static fields. NOTE: intentionally NO live
-- popularity score — that co-moves with the outcome and leaks (see source_E).
-- In the model, the raw pedigree columns (budget/star/predecessor/IP) are NOT used
-- standalone — they enter ONLY through demand-gated interactions in OW_FEATURES.
--
-- NO SKILL FILLS THIS TABLE. It is deliberately hand-built from your own catalog plus
-- Source E static fields, because star power and IP tier are judgement calls you should
-- make explicitly rather than inherit from a vendor score. Two consequences to know:
--   * If you leave it empty, sql/10 still runs (LEFT JOIN) but every demand-gated
--     pedigree interaction is NULL and the model loses that whole family. That is a
--     legitimate starting configuration — the demand signals carry most of the weight.
--   * GENRE_PRESTIGE is used by sql/10 to scope out awards-season films. If you never
--     populate it, nothing is filtered and your set will include limited-release
--     prestige titles whose openings follow completely different mechanics.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MOVIE_METADATA (
    MOVIE_ID              NUMBER PRIMARY KEY,
    RUNTIME               NUMBER,
    BUDGET                FLOAT,
    BUDGET_LOG            FLOAT,
    -- star power (from your own cast/history source)
    MAX_STAR_POWER        FLOAT,
    TOP2_STAR_POWER       FLOAT,
    AVG_STAR_POWER        FLOAT,
    NUM_STARS_WITH_HISTORY NUMBER,
    -- franchise / prior title
    PREDECESSOR_OW        FLOAT,
    PREDECESSOR_OW_LOG    FLOAT,
    -- IP tier flags (one-hot)
    KNOWN_IP_TIER         NUMBER,
    IP_HIGH_PROFILE       NUMBER,
    IP_MODERATE           NUMBER,
    IP_NICHE              NUMBER,
    IP_ORIGINAL           NUMBER,
    IS_MAJOR_STUDIO       NUMBER,
    -- genre flags (one-hot)
    GENRE_ACTION_FRANCHISE NUMBER,
    GENRE_HORROR          NUMBER,
    GENRE_ANIMATION_FAMILY NUMBER,
    GENRE_ORIGINAL        NUMBER,
    GENRE_PRESTIGE        NUMBER,
    -- rating flags (one-hot)
    RATING_G              NUMBER,
    RATING_PG             NUMBER,
    RATING_PG13           NUMBER,
    RATING_R              NUMBER
);

-- ---------------------------------------------------------------------------
-- Gold labels for evaluating the intent classifier (see sql/30_intent_eval.sql).
-- The intent classification IS the method. If you do not measure it, you are
-- trusting a prompt. Hand-label 100-300 comments here before you believe any
-- downstream accuracy number.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS INTENT_GOLD (
    COMMENT_ID      STRING        PRIMARY KEY,
    COMMENT_TEXT    STRING,
    GOLD_INTENT     STRING,                           -- THEATRICAL | STREAMING | PASS | NEUTRAL
    LABELED_BY      STRING,
    LABELED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- Derived demand percentiles by pre-release horizon (from Source A + Source C).
-- One row per (MOVIE_ID, DAYS_OUT); DAYS_OUT in (-21, -14, -7, -3).
--
-- NOT CREATED HERE. This is an auto-computing VIEW built by sql/05_demand_percentiles.sql,
-- because the as-of windowing that keeps each horizon honest is the whole substance of
-- it -- a bare table invites hand-populating it wrong. Run sql/05 after loading Sources
-- A and C, and before sql/10_feature_view.sql.
-- ---------------------------------------------------------------------------

-- Films to exclude from the model. WRITE A REASON EVERY TIME -- the exclusion policy is
-- a modeling decision, not housekeeping, and an undocumented one is indistinguishable
-- from fitting the film set to the answer you wanted.
--
-- The exclusion that matters most, and that surprises everyone: COMMENT AVAILABILITY.
-- Comments are disabled on a large share of family/animated trailers, so those films
-- have no Source B signal at all -- not a low signal, an absent one. Exclude on the
-- ABSENCE OF COMMENTS, never on the genre flag:
--   * a family film whose trailer has a real comment thread SHOULD be in the set;
--   * a non-family film with comments disabled MUST be out.
-- Excluding by GENRE_ANIMATION_FAMILY instead is the easy mistake and it silently drops
-- films that carry usable signal. See sources/source_B_trailer_comments.md.
--
-- Suggested REASON vocabulary: 'comments_disabled', 're_release', 'limited_release',
-- 'bad_release_date', 'no_search_entity', 'day_and_date_streaming'.
CREATE TABLE IF NOT EXISTS REMOVE_FROM_MODEL (
    MOVIE_ID        NUMBER        PRIMARY KEY,
    REASON          STRING        NOT NULL
);

-- Populate the comment-availability exclusions once Source B is loaded:
--   INSERT INTO REMOVE_FROM_MODEL (MOVIE_ID, REASON)
--   SELECT m.MOVIE_ID, 'comments_disabled'
--   FROM MOVIE_MAP m
--   LEFT JOIN (SELECT MOVIE_ID, COUNT(*) n FROM TRAILER_COMMENTS_RAW GROUP BY 1) c
--          ON c.MOVIE_ID = m.MOVIE_ID
--   WHERE COALESCE(c.n, 0) < 25
--     AND m.MOVIE_ID NOT IN (SELECT MOVIE_ID FROM REMOVE_FROM_MODEL);

-- 10_feature_view.sql — assemble the modeling matrix, one row per (MOVIE_ID, DAYS_OUT).
-- Replace {{SANDBOX_DB}}. This is an illustrative-but-faithful reconstruction of the
-- demand-forward feature set — NOT a 1:1 copy of any production view.
--
-- Design choices that mirror the reference model:
--  * Grain = one row per film per pre-release horizon (DAYS_OUT in -21/-14/-7/-3), so the
--    demand signals AND the comment signals are read as-of each horizon.
--  * COMMENT FEATURES ARE AS-OF, NOT WHOLE-THREAD. Each horizon counts only comments
--    posted on or before that horizon's date. Two reasons, neither dramatic: the horizon
--    grain is meaningless if a film's -21 and -3 rows carry identical comment counts, and a
--    thread scraped long after release includes post-release comments that are partly a
--    consequence of the opening. The correction is small in practice -- on the reference
--    corpus 78% of comments arrive within 5 days of the trailer and 94% before release, so
--    only ~6% is post-release. Tripwire 1 below confirms the filter is biting.
--  * Feature families: trailer conversation (volume + decomposed intent + sentiment),
--    search & pageview DEMAND percentiles, genre/rating/runtime/season, quality-adjusted
--    demand, and demand-gated pedigree interactions.
--  * PEDIGREE IS GATED: budget / star power / predecessor gross / IP tier are NOT used as
--    standalone features. They enter ONLY multiplied by demand (e.g. SEARCH7_X_STAR), so the
--    model can't lean on hype the crowd isn't backing. See docs/06_model_overview.md.
--  * NO leakage feature (a live popularity score) — deliberately absent.

USE SCHEMA {{SANDBOX_DB}}.RESEARCH;

CREATE OR REPLACE VIEW OW_FEATURES AS
WITH comment_agg AS (
    -- One aggregate per (film, horizon), using ONLY comments posted on or before that
    -- horizon's as-of date. The COMMENT_DATE predicate is the leakage guard -- do not
    -- remove it to "get more data", that is exactly the trade that invalidates the test.
    SELECT
        dp.MOVIE_ID,
        dp.DAYS_OUT,
        COUNT(*)                                            AS YT_COMMENTS,
        LN(1 + COUNT(*))                                    AS LOG_YT,
        AVG(s.SENTIMENT_SCORE)                              AS AVG_SENT,
        100.0*AVG(s.THEATRICAL_INTENT)                      AS PCT_THEA,
        100.0*AVG(s.PASS_INTENT)                            AS PCT_PASS,
        100.0*AVG(IFF(s.SENTIMENT_BUCKET='positive',1,0))   AS PCT_POS,
        100.0*AVG(IFF(s.SENTIMENT_BUCKET='negative',1,0))   AS PCT_NEG,
        100.0*(AVG(s.THEATRICAL_INTENT) - AVG(s.PASS_INTENT)) AS NET_INTENT_PCT
    FROM DEMAND_PERCENTILES dp
    JOIN RELEASE_DATES rd0          ON rd0.MOVIE_ID = dp.MOVIE_ID
    JOIN TRAILER_COMMENTS_SCORED s  ON s.MOVIE_ID = dp.MOVIE_ID
                                   AND s.COMMENT_DATE <= DATEADD(day, dp.DAYS_OUT, rd0.RELEASE_DATE)
    GROUP BY dp.MOVIE_ID, dp.DAYS_OUT
)
SELECT
    -- keys / grain
    dp.MOVIE_ID,
    m.MOVIE_TITLE,
    rd.RELEASE_DATE,
    dp.DAYS_OUT,
    -- target
    bo.OPENING_WEEKEND,
    LN(bo.OPENING_WEEKEND)                                          AS LOG_OPENING_WEEKEND,
    bo.THEATER_COUNT,

    -- ── Source B: the core signal (volume + decomposed intent + sentiment) ──
    c.YT_COMMENTS,
    c.LOG_YT,
    c.AVG_SENT,
    c.PCT_THEA, c.PCT_PASS, c.PCT_POS, c.PCT_NEG,
    c.NET_INTENT_PCT,

    -- ── Sources A + C: demand percentiles as of this horizon ──
    dp.SEARCH_ROLLING_3D_PCTILE, dp.SEARCH_ROLLING_7D_PCTILE, dp.SEARCH_ROLLING_14D_PCTILE,
    dp.SEARCH_PEAK_PCTILE, dp.SEARCH_VEL_PCTILE, dp.SEARCH_SLOPE_PCTILE,
    dp.PAGEVIEW_R7D_PCTILE, dp.PAGEVIEW_PEAK_PCTILE, dp.PAGEVIEW_CUM_PCTILE, dp.PAGEVIEW_VEL_PCTILE,

    -- ── calendar / content (standalone is fine for these) ──
    MONTH(rd.RELEASE_DATE)                                         AS RELEASE_MONTH,
    IFF(MONTH(rd.RELEASE_DATE) IN (5,6,7,11,12), 1, 0)             AS IS_PEAK_SEASON,
    m2.RUNTIME,
    m2.GENRE_ACTION_FRANCHISE, m2.GENRE_HORROR, m2.GENRE_ANIMATION_FAMILY, m2.GENRE_ORIGINAL,
    m2.RATING_G, m2.RATING_PG, m2.RATING_PG13, m2.RATING_R,

    -- ── quality-adjusted demand: demand weighted by how the intent leans ──
    (1/(1+EXP(-0.2*c.NET_INTENT_PCT)))                             AS QMULT,
    dp.SEARCH_ROLLING_7D_PCTILE * (1/(1+EXP(-0.2*c.NET_INTENT_PCT))) AS QADJ_SEARCH,
    dp.PAGEVIEW_PEAK_PCTILE     * (1/(1+EXP(-0.2*c.NET_INTENT_PCT))) AS QADJ_PAGEVIEW,

    -- ── demand-gated PEDIGREE interactions (pedigree only counts when demand backs it) ──
    dp.SEARCH_ROLLING_7D_PCTILE * m2.MAX_STAR_POWER                AS SEARCH7_X_STAR,
    dp.SEARCH_ROLLING_7D_PCTILE * m2.IP_HIGH_PROFILE               AS SEARCH7_X_IP_HIGH,
    dp.SEARCH_ROLLING_7D_PCTILE * m2.PREDECESSOR_OW_LOG            AS SEARCH7_X_PREDOW,
    dp.SEARCH_ROLLING_7D_PCTILE * m2.GENRE_ACTION_FRANCHISE        AS SEARCH7_X_ACTION,
    dp.SEARCH_ROLLING_7D_PCTILE * m2.GENRE_HORROR                  AS SEARCH7_X_HORROR,
    dp.PAGEVIEW_PEAK_PCTILE     * m2.MAX_STAR_POWER                AS PAGEVIEWPK_X_STAR,

    -- ── intent × demand interactions ──
    c.AVG_SENT       * dp.SEARCH_ROLLING_7D_PCTILE                 AS SENT_X_SEARCH7,
    c.PCT_THEA       * dp.SEARCH_ROLLING_7D_PCTILE                 AS THEA_X_SEARCH7,
    c.NET_INTENT_PCT * dp.SEARCH_ROLLING_7D_PCTILE                 AS NET_X_SEARCH7,
    c.NET_INTENT_PCT * dp.PAGEVIEW_PEAK_PCTILE                     AS NET_X_PAGEVIEWPK,
    c.AVG_SENT       * dp.PAGEVIEW_PEAK_PCTILE                     AS SENT_X_PAGEVIEWPK,
    c.PCT_THEA       * dp.PAGEVIEW_PEAK_PCTILE                     AS THEA_X_PAGEVIEWPK

FROM DEMAND_PERCENTILES dp
JOIN MOVIE_MAP m                 ON dp.MOVIE_ID = m.MOVIE_ID
JOIN RELEASE_DATES rd            ON dp.MOVIE_ID = rd.MOVIE_ID
JOIN BOX_OFFICE bo               ON dp.MOVIE_ID = bo.MOVIE_ID
LEFT JOIN comment_agg c          ON dp.MOVIE_ID = c.MOVIE_ID AND dp.DAYS_OUT = c.DAYS_OUT
LEFT JOIN MOVIE_METADATA m2      ON dp.MOVIE_ID = m2.MOVIE_ID
WHERE dp.MOVIE_ID NOT IN (SELECT MOVIE_ID FROM REMOVE_FROM_MODEL)
  AND bo.OPENING_WEEKEND IS NOT NULL
  AND (bo.THEATER_COUNT >= 1000 OR bo.THEATER_COUNT IS NULL)
  AND COALESCE(m2.GENRE_PRESTIGE, 0) = 0;      -- reference model scopes out awards-season prestige


-- ---------------------------------------------------------------------------
-- Leakage tripwires — run all three. They cost seconds and they are the difference
-- between a research result and a self-deception.
-- ---------------------------------------------------------------------------
-- 1. Comment volume must GROW with the horizon. If YT_COMMENTS is flat across
--    -21/-14/-7/-3 for a film, the as-of filter is not biting (usually because
--    COMMENT_DATE is NULL in TRAILER_COMMENTS_SCORED).
--    SELECT DAYS_OUT, COUNT(*) AS films, ROUND(AVG(YT_COMMENTS)) AS avg_comments
--    FROM OW_FEATURES GROUP BY 1 ORDER BY 1;
--    Expect avg_comments to rise monotonically from -21 to -3.
--
-- 2. No NULL COMMENT_DATE.
--    SELECT COUNT_IF(COMMENT_DATE IS NULL) FROM TRAILER_COMMENTS_SCORED;   -- expect 0
--
-- 3. Feature-label correlations should be in a plausible range. These are noisy real-world
--    signals; a near-perfect correlation with the label means you are looking at something
--    downstream of the outcome, not at demand.
--    SELECT CORR(LOG_YT, LOG_OPENING_WEEKEND)            AS r_volume,
--           CORR(SEARCH_ROLLING_7D_PCTILE, LOG_OPENING_WEEKEND) AS r_search,
--           CORR(NET_INTENT_PCT, LOG_OPENING_WEEKEND)    AS r_intent
--    FROM OW_FEATURES WHERE DAYS_OUT = -3;
--    The reference build lands near r_volume 0.74 (0.54 once production budget is controlled;
--    unchanged when trailer views are controlled) and r_intent ~0.2. Yours will differ with your film set. A r_volume of 0.95,
--    though, is not a better dataset -- it means post-release comments are in the aggregate.

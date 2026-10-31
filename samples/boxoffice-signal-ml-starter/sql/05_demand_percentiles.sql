-- 05_demand_percentiles.sql — build DEMAND_PERCENTILES from Source A (search interest)
-- and Source C (research pageviews). Replace {{SANDBOX_DB}}.
--
-- THIS IS THE TRANSFORM THAT MAKES THE FEATURE VIEW WORK. Run it after you have loaded
-- SEARCH_INTEREST / SEARCH_ANCHOR_BASELINE and PAGEVIEW_DEMAND, and before sql/10.
--
-- What it does, and why each choice matters:
--
--  1. AS-OF DISCIPLINE. One row per (MOVIE_ID, DAYS_OUT) for DAYS_OUT in (-21,-14,-7,-3).
--     Every aggregate for a horizon uses ONLY observations dated on or before that
--     horizon's as-of date. This is the whole point: a -21 row must not be able to see
--     day -4. If you break this, your backtest will look excellent and mean nothing.
--
--  2. NORMALIZATION. If your search source returns a per-request relative index, each
--     pull has its own scale. SCALED puts every film on one timeline by dividing by the
--     anchor term that rode along in the same pull, then multiplying by that anchor's
--     standalone baseline. If your source returns absolute counts, set
--     USE_ANCHOR_NORMALIZATION to FALSE below and INTEREST is used as-is.
--
--  3. PERCENTILES, NOT LEVELS. Raw demand spans several orders of magnitude and drifts
--     with platform-wide seasonality. PERCENT_RANK within a horizon compresses that to
--     0..1 and makes films comparable across years.
--
--     ⚠ KNOWN CAVEAT — read this. PERCENT_RANK is computed across your WHOLE film set,
--     which means a 2023 film's percentile is influenced by films released in 2025.
--     That is a mild look-ahead. It is acceptable for research (the ranking is over a
--     demand distribution, not over the label) and it is what the reference build does,
--     but it is NOT strictly clean. If you want the strict version, see the
--     EXPANDING-WINDOW variant at the bottom of this file and use that instead.

USE SCHEMA {{SANDBOX_DB}}.RESEARCH;

SET USE_ANCHOR_NORMALIZATION = TRUE;   -- FALSE if your Source A returns absolute counts
SET LOOKBACK_DAYS = 90;                -- window for peak / cumulative aggregates

CREATE OR REPLACE VIEW DEMAND_PERCENTILES AS
WITH horizons AS (
    SELECT -21 AS DAYS_OUT UNION ALL SELECT -14 UNION ALL SELECT -7 UNION ALL SELECT -3
),
film_horizon AS (
    SELECT rd.MOVIE_ID,
           rd.RELEASE_DATE,
           h.DAYS_OUT,
           DATEADD(day, h.DAYS_OUT, rd.RELEASE_DATE) AS ASOF_DATE
    FROM RELEASE_DATES rd
    CROSS JOIN horizons h
    WHERE rd.MOVIE_ID NOT IN (SELECT MOVIE_ID FROM REMOVE_FROM_MODEL)
),
-- ── Source A: put every film's relative index on one common scale ──────────────
search_norm AS (
    SELECT s.MOVIE_ID,
           s.OBS_DATE,
           CASE
             WHEN NOT $USE_ANCHOR_NORMALIZATION THEN s.INTEREST
             WHEN s.ANCHOR_INTEREST > 0
               THEN s.INTEREST / s.ANCHOR_INTEREST * COALESCE(b.ANCHOR_NORM, 100)
             ELSE NULL            -- anchor floored out: unusable, do NOT coalesce to 0
           END AS SCALED
    FROM SEARCH_INTEREST s
    LEFT JOIN SEARCH_ANCHOR_BASELINE b ON b.OBS_DATE = s.OBS_DATE
),
search_agg AS (
    SELECT f.MOVIE_ID,
           f.DAYS_OUT,
           AVG(IFF(sn.OBS_DATE >  DATEADD(day,  -3, f.ASOF_DATE), sn.SCALED, NULL)) AS S_R3,
           AVG(IFF(sn.OBS_DATE >  DATEADD(day,  -7, f.ASOF_DATE), sn.SCALED, NULL)) AS S_R7,
           AVG(IFF(sn.OBS_DATE >  DATEADD(day, -14, f.ASOF_DATE), sn.SCALED, NULL)) AS S_R14,
           AVG(IFF(sn.OBS_DATE <= DATEADD(day,  -7, f.ASOF_DATE)
               AND  sn.OBS_DATE >  DATEADD(day, -14, f.ASOF_DATE), sn.SCALED, NULL)) AS S_PRIOR7,
           MAX(sn.SCALED)                                                            AS S_PEAK,
           COUNT(sn.SCALED)                                                          AS S_OBS
    FROM film_horizon f
    JOIN search_norm sn
      ON sn.MOVIE_ID = f.MOVIE_ID
     AND sn.OBS_DATE <= f.ASOF_DATE                                    -- ← as-of guard
     AND sn.OBS_DATE >  DATEADD(day, -$LOOKBACK_DAYS, f.ASOF_DATE)
    GROUP BY 1, 2
),
-- ── Source C: absolute daily pageviews ────────────────────────────────────────
pageview_agg AS (
    SELECT f.MOVIE_ID,
           f.DAYS_OUT,
           AVG(IFF(p.OBS_DATE >  DATEADD(day,  -7, f.ASOF_DATE), p.VIEWS, NULL))  AS P_R7,
           AVG(IFF(p.OBS_DATE <= DATEADD(day,  -7, f.ASOF_DATE)
               AND  p.OBS_DATE >  DATEADD(day, -14, f.ASOF_DATE), p.VIEWS, NULL))  AS P_PRIOR7,
           MAX(p.VIEWS)                                                            AS P_PEAK,
           SUM(p.VIEWS)                                                            AS P_CUM,
           COUNT(p.VIEWS)                                                          AS P_OBS
    FROM film_horizon f
    JOIN PAGEVIEW_DEMAND p
      ON p.MOVIE_ID = f.MOVIE_ID
     AND p.OBS_DATE <= f.ASOF_DATE                                     -- ← as-of guard
     AND p.OBS_DATE >  DATEADD(day, -$LOOKBACK_DAYS, f.ASOF_DATE)
    GROUP BY 1, 2
),
-- ── derived shape metrics (velocity = short vs prior window, slope = log trend) ──
metrics AS (
    SELECT f.MOVIE_ID,
           f.DAYS_OUT,
           s.S_R3, s.S_R7, s.S_R14, s.S_PEAK,
           IFF(s.S_PRIOR7 > 0, s.S_R7 / s.S_PRIOR7 - 1, NULL)                 AS S_VEL,
           IFF(s.S_R14 > 0 AND s.S_R3 > 0, LN(s.S_R3) - LN(s.S_R14), NULL)     AS S_SLOPE,
           p.P_R7, p.P_PEAK, p.P_CUM,
           IFF(p.P_PRIOR7 > 0, p.P_R7 / p.P_PRIOR7 - 1, NULL)                 AS P_VEL,
           COALESCE(s.S_OBS, 0) AS S_OBS,
           COALESCE(p.P_OBS, 0) AS P_OBS
    FROM film_horizon f
    LEFT JOIN search_agg   s ON s.MOVIE_ID = f.MOVIE_ID AND s.DAYS_OUT = f.DAYS_OUT
    LEFT JOIN pageview_agg p ON p.MOVIE_ID = f.MOVIE_ID AND p.DAYS_OUT = f.DAYS_OUT
)
SELECT
    MOVIE_ID,
    DAYS_OUT,
    -- Source A percentiles. NULL metrics stay NULL — PERCENT_RANK would rank a missing
    -- value as the bottom of the field, which is a wrong signal, not a neutral one.
    IFF(S_R3    IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_R3))    AS SEARCH_ROLLING_3D_PCTILE,
    IFF(S_R7    IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_R7))    AS SEARCH_ROLLING_7D_PCTILE,
    IFF(S_R14   IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_R14))   AS SEARCH_ROLLING_14D_PCTILE,
    IFF(S_PEAK  IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_PEAK))  AS SEARCH_PEAK_PCTILE,
    IFF(S_VEL   IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_VEL))   AS SEARCH_VEL_PCTILE,
    IFF(S_SLOPE IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY S_SLOPE)) AS SEARCH_SLOPE_PCTILE,
    -- Source C percentiles
    IFF(P_R7    IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY P_R7))    AS PAGEVIEW_R7D_PCTILE,
    IFF(P_PEAK  IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY P_PEAK))  AS PAGEVIEW_PEAK_PCTILE,
    IFF(P_CUM   IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY P_CUM))   AS PAGEVIEW_CUM_PCTILE,
    IFF(P_VEL   IS NULL, NULL, PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ORDER BY P_VEL))   AS PAGEVIEW_VEL_PCTILE,
    -- coverage counters: keep these, they are how you catch a film with a broken pull
    S_OBS AS SEARCH_OBS_COUNT,
    P_OBS AS PAGEVIEW_OBS_COUNT
FROM metrics;


-- ---------------------------------------------------------------------------
-- Validation — run these before you trust the output.
-- ---------------------------------------------------------------------------
-- 1. Every film should have exactly 4 horizon rows.
--    SELECT DAYS_OUT, COUNT(*) FROM DEMAND_PERCENTILES GROUP BY 1 ORDER BY 1;
--
-- 2. Coverage: films with thin pulls will quietly poison the percentile field.
--    SELECT COUNT_IF(SEARCH_OBS_COUNT < 14) AS thin_search,
--           COUNT_IF(PAGEVIEW_OBS_COUNT < 14) AS thin_pageview
--    FROM DEMAND_PERCENTILES WHERE DAYS_OUT = -3;
--
-- 3. AS-OF PROOF (the important one). A film's -21 percentile must not move when you
--    later load data from its release week. Snapshot, reload, compare:
--    CREATE TEMP TABLE _asof_check AS
--      SELECT MOVIE_ID, SEARCH_ROLLING_7D_PCTILE FROM DEMAND_PERCENTILES WHERE DAYS_OUT = -21;
--    -- ...refresh Source A for recent films, then:
--    SELECT COUNT(*) AS rows_that_moved
--    FROM _asof_check a JOIN DEMAND_PERCENTILES d USING (MOVIE_ID)
--    WHERE d.DAYS_OUT = -21
--      AND ABS(COALESCE(a.SEARCH_ROLLING_7D_PCTILE,-1) - COALESCE(d.SEARCH_ROLLING_7D_PCTILE,-1)) > 0.02;
--    Expect 0 from newly-arrived post-horizon observations. A non-zero count from films
--    whose OWN pre-horizon history was backfilled is fine; anything else is a leak.


-- ---------------------------------------------------------------------------
-- STRICT VARIANT (optional) — expanding-window percentiles.
-- Ranks each film only against films released BEFORE it, removing the look-ahead
-- described in the header. Slower and noisier for early films (a film ranked against
-- 20 predecessors has a coarse percentile), which is why it is not the default.
-- Swap the PERCENT_RANK() OVER (PARTITION BY DAYS_OUT ...) calls above for this shape:
--
--   , ranked AS (
--       SELECT m.*, rd.RELEASE_DATE
--       FROM metrics m JOIN RELEASE_DATES rd USING (MOVIE_ID)
--   )
--   SELECT MOVIE_ID, DAYS_OUT,
--          (SELECT AVG(IFF(p.S_R7 < r.S_R7, 1.0, 0.0))
--             FROM ranked p
--            WHERE p.DAYS_OUT = r.DAYS_OUT
--              AND p.RELEASE_DATE < r.RELEASE_DATE) AS SEARCH_ROLLING_7D_PCTILE
--   FROM ranked r;
--
-- Ask CoCo: "rewrite sql/05 using the expanding-window variant and rerun the backtest —
-- tell me how much MAPE moves." If it moves a lot, the look-ahead was doing work.

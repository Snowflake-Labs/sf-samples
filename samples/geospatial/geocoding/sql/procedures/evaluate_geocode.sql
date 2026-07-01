-- ============================================================================
-- EVALUATE_GEOCODE — deterministic accuracy metrics for a geocoded result set
-- ============================================================================
-- Given a table that holds BOTH an actual (ground-truth) location and a
-- geocoded location as GEOGRAPHY columns, returns a small set of quality
-- metrics: match rate, how many results fall within a distance threshold of
-- the ground truth, and average deviation. Mirrors the quality checks in the
-- sf-guide geocoding notebook (hit-rate, ST_DISTANCE deviation).
--
-- Params:
--   RESULT_TABLE          fully-qualified table with the two point columns
--   ACTUAL_GEOG_COLUMN    GEOGRAPHY column with the ground-truth location
--   GEOCODED_GEOG_COLUMN  GEOGRAPHY column with the geocoded location
--                         (NULL for rows that could not be geocoded)
--   THRESHOLD_METERS      distance under which a match counts as "accurate"
--                         (default 200)
--
-- Returns a TABLE(metric VARCHAR, value FLOAT):
--   total_rows, matched, unmatched, match_rate_pct, threshold_meters,
--   within_threshold_count, within_threshold_pct (of matched), avg_distance_m
--
-- Tip: FORWARD_GEOCODE_TABLE and REVERSE_GEOCODE_TABLE now emit a GEOGRAPHY
-- result_geog column, so you can pass it straight in with no wrapping:
--   CALL EVALUATE_GEOCODE('MY_DB.PUBLIC.GEOCODED', 'ACTUAL_GEOG', 'RESULT_GEOG');
-- For a legacy table that still stores lat/lon as separate FLOAT columns, wrap
-- them into GEOGRAPHY first (ST_POINT takes lon, lat):
--   SELECT ST_POINT(actual_lon, actual_lat)   AS actual_g,
--          ST_POINT(result_lon, result_lat)   AS geocoded_g
--   FROM my_geocoded_table;
-- and pass that view/table into this procedure.
--
-- Example:
--   CALL GEOCODING.PUBLIC.EVALUATE_GEOCODE(
--     'MY_DB.PUBLIC.GEOCODED', 'ACTUAL_LOCATION', 'GEOCODED_LOCATION', 200);
-- ============================================================================

CREATE OR REPLACE PROCEDURE GEOCODING.PUBLIC.EVALUATE_GEOCODE(
    RESULT_TABLE         VARCHAR,
    ACTUAL_GEOG_COLUMN   VARCHAR,
    GEOCODED_GEOG_COLUMN VARCHAR,
    THRESHOLD_METERS     FLOAT DEFAULT 200)
RETURNS TABLE(metric VARCHAR, value FLOAT)
LANGUAGE SQL
COMMENT = 'oss-geocoding'
EXECUTE AS OWNER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    res := (EXECUTE IMMEDIATE '
        WITH base AS (
            SELECT
                ' || ACTUAL_GEOG_COLUMN   || ' AS actual_g,
                ' || GEOCODED_GEOG_COLUMN || ' AS geocoded_g
            FROM ' || RESULT_TABLE || '
        ),
        agg AS (
            SELECT
                COUNT(*)                     AS total,
                COUNT(geocoded_g)            AS matched,
                COUNT(*) - COUNT(geocoded_g) AS unmatched,
                COUNT_IF(geocoded_g IS NOT NULL AND actual_g IS NOT NULL
                    AND ST_DISTANCE(actual_g, geocoded_g) <= ' || THRESHOLD_METERS || ') AS within_thr,
                AVG(IFF(geocoded_g IS NOT NULL AND actual_g IS NOT NULL,
                        ST_DISTANCE(actual_g, geocoded_g), NULL))                          AS avg_dist_m
            FROM base
        )
        SELECT ''total_rows''::VARCHAR AS metric, total::FLOAT AS value FROM agg
        UNION ALL SELECT ''matched'',                matched::FLOAT              FROM agg
        UNION ALL SELECT ''unmatched'',              unmatched::FLOAT            FROM agg
        UNION ALL SELECT ''match_rate_pct'',         ROUND(100.0 * matched / NULLIF(total, 0), 2)     FROM agg
        UNION ALL SELECT ''threshold_meters'',       ' || THRESHOLD_METERS || '::FLOAT               FROM agg
        UNION ALL SELECT ''within_threshold_count'', within_thr::FLOAT          FROM agg
        UNION ALL SELECT ''within_threshold_pct'',   ROUND(100.0 * within_thr / NULLIF(matched, 0), 2) FROM agg
        UNION ALL SELECT ''avg_distance_m'',         ROUND(avg_dist_m, 1)       FROM agg');

    RETURN TABLE(res);
END;
$$;

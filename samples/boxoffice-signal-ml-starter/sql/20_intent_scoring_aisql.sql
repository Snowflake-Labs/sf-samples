-- 20_intent_scoring_aisql.sql — score trailer comments with Snowflake Cortex AISQL.
-- Turns raw comment text into SENTIMENT (via AI_SENTIMENT) + VIEWING INTENT
-- (theatrical / streaming / pass / neutral) via a single structured TRY_COMPLETE call.
-- Replace {{SANDBOX_DB}}. Requires access to Cortex AISQL functions (see docs/02).
--
-- This is a TEMPLATE. Model names and AISQL signatures evolve — ask CoCo:
-- "update this to the current Cortex AISQL syntax and a good available model."
--
-- Two lessons baked in from real scoring runs:
--  * Classify intent with ONE consolidated call into 4 labels — not several yes/no calls.
--    Independent per-label flags over-fire; a single call is markedly more accurate.
--  * Use the messages-array form with a `response_format` enum, and prefer TRY_COMPLETE:
--    it returns NULL on a bad row instead of failing the whole batch. NULL is treated as
--    NEUTRAL below, so the scored table has no missing intents.
--  * CARRY COMMENT_DATE THROUGH. The feature view needs it to compute comment features
--    as-of each pre-release horizon. Dropping it forces you to aggregate the whole
--    thread into every horizon, which leaks release-week conversation into a -21 row.
--
-- MEASURE THIS BEFORE YOU TRUST IT: run sql/30_intent_eval.sql against hand-labeled gold
-- comments and look at macro-F1 per class. The prompt below is the result of several
-- rounds of exactly that loop; yours will need its own.

USE SCHEMA {{SANDBOX_DB}}.RESEARCH;

-- Score each raw comment: sentiment label (AI_SENTIMENT) + one intent label (TRY_COMPLETE),
-- then map to the numeric score + 1/0 intent flags the feature view expects.
INSERT INTO TRAILER_COMMENTS_SCORED
    (MOVIE_ID, COMMENT_ID, COMMENT_TEXT, LIKE_COUNT, COMMENT_DATE,
     SENTIMENT_SCORE, SENTIMENT_BUCKET, THEATRICAL_INTENT, STREAMING_INTENT, PASS_INTENT)
WITH scored AS (
    SELECT
        r.MOVIE_ID,
        r.COMMENT_ID,
        r.COMMENT_TEXT,
        r.LIKE_COUNT,
        r.COMMENT_DATE,
        AI_SENTIMENT(r.COMMENT_TEXT):categories[0]:sentiment::STRING            AS sent_label,
        SNOWFLAKE.CORTEX.TRY_COMPLETE(
            'claude-4-sonnet',
            [
              {'role':'system','content':
                'Label a single movie-trailer comment with the COMMENTER''S OWN viewing intent, as exactly one of four labels. '
                || 'THEATRICAL = explicit first-person intent to see it in a cinema (buying tickets, opening night, going to see it). '
                || 'STREAMING = explicit intent to watch at home — stream, rent, buy the disc, etc. ("wait for streaming", "catch it on Prime", "Blu-ray day one"). '
                || 'PASS = explicit intent to AVOID seeing it ("hard pass", "not wasting my money", "saves me a trip"). '
                || 'NEUTRAL = everything else. Hype/excitement, naming a format ("only in theaters", "IMAX", "8K TV"), and general praise or criticism with NO stated personal viewing intent are all NEUTRAL. '
                || 'General negativity ("bad movie", "looks awful", "woke garbage") is NEUTRAL, not PASS — tone is scored separately and PASS must not double-count it. '
                || 'When in doubt, choose NEUTRAL.'},
              {'role':'user','content': r.COMMENT_TEXT}
            ],
            {'response_format':{'type':'json','schema':{'type':'object',
              'properties':{'intent':{'type':'string','enum':['THEATRICAL','STREAMING','PASS','NEUTRAL']}},
              'required':['intent']}}}
        ):structured_output[0]:raw_message:intent::STRING                       AS intent_label
    FROM TRAILER_COMMENTS_RAW r
    WHERE r.COMMENT_TEXT IS NOT NULL
)
SELECT
    MOVIE_ID,
    COMMENT_ID,
    COMMENT_TEXT,
    LIKE_COUNT,
    COMMENT_DATE,
    -- AI_SENTIMENT returns a label; map it to a numeric score the feature view can average
    CASE sent_label WHEN 'positive' THEN 1 WHEN 'negative' THEN -1 ELSE 0 END   AS SENTIMENT_SCORE,
    sent_label                                                                  AS SENTIMENT_BUCKET,
    -- NEUTRAL and any NULL (unparseable / TRY_COMPLETE failure) collapse to all-zero
    IFF(intent_label = 'THEATRICAL', 1, 0)                                      AS THEATRICAL_INTENT,
    IFF(intent_label = 'STREAMING',  1, 0)                                      AS STREAMING_INTENT,
    IFF(intent_label = 'PASS',       1, 0)                                      AS PASS_INTENT
FROM scored;


--          100.0*AVG(PASS_INTENT)                               AS pct_pass,
--          100.0*(AVG(THEATRICAL_INTENT) - AVG(PASS_INTENT))    AS net_intent_pct
--   FROM TRAILER_COMMENTS_SCORED GROUP BY MOVIE_ID;

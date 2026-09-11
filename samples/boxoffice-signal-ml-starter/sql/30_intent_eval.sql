-- 30_intent_eval.sql — measure the intent classifier against hand-labeled gold comments.
-- Replace {{SANDBOX_DB}}. Run this BEFORE you believe any model result.
--
-- WHY THIS FILE EXISTS
-- The whole method rests on one claim: an LLM can read a comment and tell you whether
-- the person intends to buy a ticket, wait for streaming, or skip. If you never measure
-- that, you are not doing research on audience intent -- you are doing research on a
-- prompt. A classifier that systematically mistakes generic hype for THEATRICAL will
-- produce a net-intent feature that mostly encodes "how excited does this thread sound",
-- which is the gameable signal you were trying to get away from.
--
-- Everything here is provider-neutral and needs no data beyond your own labels.

USE SCHEMA {{SANDBOX_DB}}.RESEARCH;

-- ---------------------------------------------------------------------------
-- Step 1 — Build a gold set (do this by hand; there is no shortcut)
-- ---------------------------------------------------------------------------
-- Sample 150-300 comments STRATIFIED by predicted label, so you get enough of the rare
-- classes to say anything about them. A random sample will be ~85% NEUTRAL and tell you
-- almost nothing about PASS.
--
--   CREATE OR REPLACE TABLE INTENT_GOLD_QUEUE AS
--   SELECT COMMENT_ID, COMMENT_TEXT,
--          CASE WHEN THEATRICAL_INTENT = 1 THEN 'THEATRICAL'
--               WHEN STREAMING_INTENT  = 1 THEN 'STREAMING'
--               WHEN PASS_INTENT       = 1 THEN 'PASS'
--               ELSE 'NEUTRAL' END AS PREDICTED_INTENT
--   FROM TRAILER_COMMENTS_SCORED
--   QUALIFY ROW_NUMBER() OVER (PARTITION BY PREDICTED_INTENT ORDER BY RANDOM()) <= 60;
--
-- Then label GOLD_INTENT yourself and insert into INTENT_GOLD. Two rules that keep the
-- exercise honest:
--   * Label from the text alone. Do not look at the model's guess first -- you will anchor.
--   * Write the edge-case rules down as you go and apply them consistently. The ones that
--     cost the most accuracy in practice:
--       - Naming a format ("IMAX", "only in theaters") is NOT theatrical intent unless the
--         commenter says they are going.
--       - General negativity ("this looks awful") is NEUTRAL, not PASS. PASS requires
--         stated avoidance. Tone is scored separately; letting it double-count into PASS
--         is the single most common way net intent gets corrupted.
--       - Questions ("is this a remake?") are NEUTRAL.

-- ---------------------------------------------------------------------------
-- Step 2 — Confusion matrix
-- ---------------------------------------------------------------------------
WITH pred AS (
    SELECT s.COMMENT_ID,
           CASE WHEN s.THEATRICAL_INTENT = 1 THEN 'THEATRICAL'
                WHEN s.STREAMING_INTENT  = 1 THEN 'STREAMING'
                WHEN s.PASS_INTENT       = 1 THEN 'PASS'
                ELSE 'NEUTRAL' END AS PREDICTED_INTENT
    FROM TRAILER_COMMENTS_SCORED s
)
SELECT g.GOLD_INTENT,
       p.PREDICTED_INTENT,
       COUNT(*) AS n
FROM INTENT_GOLD g
JOIN pred p USING (COMMENT_ID)
GROUP BY 1, 2
ORDER BY 1, 2;

-- ---------------------------------------------------------------------------
-- Step 3 — Per-class precision / recall / F1 and MACRO-F1
-- Macro-F1 (unweighted mean across classes) is the number to report. Plain accuracy is
-- misleading here because NEUTRAL dominates: a classifier that answers NEUTRAL to
-- everything scores ~85% accuracy and is worthless.
-- ---------------------------------------------------------------------------
WITH pred AS (
    SELECT s.COMMENT_ID,
           CASE WHEN s.THEATRICAL_INTENT = 1 THEN 'THEATRICAL'
                WHEN s.STREAMING_INTENT  = 1 THEN 'STREAMING'
                WHEN s.PASS_INTENT       = 1 THEN 'PASS'
                ELSE 'NEUTRAL' END AS PREDICTED_INTENT
    FROM TRAILER_COMMENTS_SCORED s
),
joined AS (
    SELECT g.GOLD_INTENT, p.PREDICTED_INTENT
    FROM INTENT_GOLD g JOIN pred p USING (COMMENT_ID)
),
classes AS (
    SELECT 'THEATRICAL' AS CLS UNION ALL SELECT 'STREAMING'
    UNION ALL SELECT 'PASS'    UNION ALL SELECT 'NEUTRAL'
),
per_class AS (
    SELECT c.CLS,
           COUNT_IF(j.GOLD_INTENT = c.CLS)                                   AS support,
           COUNT_IF(j.PREDICTED_INTENT = c.CLS AND j.GOLD_INTENT = c.CLS)    AS tp,
           COUNT_IF(j.PREDICTED_INTENT = c.CLS AND j.GOLD_INTENT <> c.CLS)   AS fp,
           COUNT_IF(j.PREDICTED_INTENT <> c.CLS AND j.GOLD_INTENT = c.CLS)   AS fn
    FROM classes c CROSS JOIN joined j
    GROUP BY c.CLS
),
scored AS (
    SELECT CLS, support, tp, fp, fn,
           IFF(tp + fp = 0, NULL, tp / (tp + fp)) AS precision,
           IFF(tp + fn = 0, NULL, tp / (tp + fn)) AS recall
    FROM per_class
),
f1 AS (
    SELECT *,
           IFF(COALESCE(precision,0) + COALESCE(recall,0) = 0, 0,
               2 * precision * recall / (precision + recall)) AS f1
    FROM scored
)
SELECT CLS                          AS intent_class,
       support,
       ROUND(precision, 3)          AS precision,
       ROUND(recall, 3)             AS recall,
       ROUND(f1, 3)                 AS f1,
       ROUND((SELECT AVG(f1) FROM f1), 3)                       AS macro_f1,
       (SELECT SUM(support) FROM f1)                            AS gold_n,
       ROUND((SELECT SUM(tp) FROM f1) / (SELECT SUM(support) FROM f1), 3) AS accuracy
FROM f1
ORDER BY support DESC;

-- ---------------------------------------------------------------------------
-- HOW TO READ THE RESULT
-- ---------------------------------------------------------------------------
--  * Macro-F1 in the low 0.8s on a few-hundred-comment gold set is a good, usable
--    classifier for this task. Below ~0.65, your net-intent feature is mostly noise and
--    any lift you see downstream is probably volume leaking through it.
--  * Look at PASS recall specifically. It is the hardest class, it is half of net intent,
--    and it is where prompts fail quietly.
--  * The single biggest accuracy jump in the reference build came from consolidating
--    several yes/no calls into ONE call that must choose among four labels. Independent
--    per-label flags over-fire and inflate both THEATRICAL and PASS at once, which can
--    leave net intent looking stable while both components are wrong.
--
-- Report the macro-F1 alongside any model accuracy you publish. A prediction pipeline
-- whose input classifier is unmeasured has an unmeasured error bar.
--
-- Ask CoCo:
--   "Run sql/30_intent_eval.sql. If macro-F1 is under 0.75, propose three specific prompt
--    changes based on which classes are confused in the matrix, rescore a 500-comment
--    sample with each, and show me the macro-F1 for each variant before I rescore
--    everything."

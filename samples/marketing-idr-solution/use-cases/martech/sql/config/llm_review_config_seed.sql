-- ============================================================================
-- Martech: LLM Review Config Seed
-- Inserts/updates martech LLM-review thresholds and prompt parameters into
-- <DB>.CONFIG.IDR_ML_AI_EVALUATION_CONFIG (DDL created in U1
-- sql/idr_core/idr_ml_ai_evaluation_config.sql).
--
-- Idempotent via MERGE.
-- Mirrors SSP threshold band (0.55 ≤ ML score < 0.85) per Q12b-2 = A.
-- ============================================================================

MERGE INTO IDENTIFIER('&{deployment_db}.CONFIG.IDR_ML_AI_EVALUATION_CONFIG') AS t
USING (
    SELECT column1 AS CONFIG_KEY, column2 AS CONFIG_VALUE, column3 AS DESCRIPTION
    FROM VALUES
        -- ─── LLM tier (existing) ─────────────────────────────────────────
        ('LLM_REVIEW_SCORE_LOW',     '0.55',
         'Lower ML score bound for LLM review (mirrors SSP)'),
        ('LLM_REVIEW_SCORE_HIGH',    '0.85',
         'Upper ML score bound for LLM review (mirrors SSP)'),
        ('LLM_REVIEW_BATCH_SIZE',    '200',
         'Max pairs per LLM adjudication run'),
        ('LLM_REVIEW_MODEL',         'claude-4-sonnet',
         'Cortex AI_COMPLETE model for LLM review (mirrors SSP)'),
        ('LLM_REVIEW_PROMPT_KEY',    'MARTECH_RETAIL_DTC_V1',
         'Prompt template key — retail/DTC contextualized; verdict schema = MATCH | NOT_MATCH | UNCLEAR'),
        ('LLM_AUTO_ACCEPT_VERDICT',  'MATCH',
         'LLM verdict that auto-emits an MARTECH_R17 edge into MATCH_RESULTS'),
        ('LLM_HUMAN_REVIEW_VERDICT', 'UNCLEAR',
         'LLM verdict that surfaces a pair in the human review queue'),
        ('ML_BLOCKING_STRATEGY',     'POSTAL+LASTNAME3+METRO',
         'Legacy: pre-v1 single-strategy blocking key. v1 uses CONFIG.IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES.'),

        -- ─── v1: ML stage runtime knobs ──────────────────────────────────
        ('LLM_ADJUDICATION_ENABLED', 'TRUE',
         'Master ON/OFF for SP_LLM_ADJUDICATE_PAIRS. Nested inside ML-active gate (orchestrator).'),
        ('ML_AUTO_MERGE_THRESHOLD',  '0.85',
         'ML_PROB at-or-above this → emit MARTECH_R16 edge into MATCH_RESULTS.'),
        ('ML_DROP_THRESHOLD',        '0.70',
         'ML_PROB below this → drop or dispute (no LLM, no merge).'),
        ('ML_DISPUTE_GATE_THRESHOLD','0.55',
         'ML_PROB below this AND deterministic edge exists → file dispute.'),
        ('ML_DISPUTE_STRONG_THRESHOLD','0.20',
         'ML_PROB below this in dispute scope → DISPUTE_SEVERITY = STRONG (else MILD).'),
        ('ML_BLOCK_SIZE_CAP',        '200',
         'Default per-strategy hot-block cap; overridable per-strategy in IDR_ML_INDIVIDUAL_BLOCKING_STRATEGIES.'),
        ('ML_DEPLOYED_MODEL_VERSION','idr_pair_v0_linear',
         'Active model version pointer; persisted on every ML edge in IDR_ML_INDIVIDUAL_MATCH_EXPLAIN.')
) AS s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN MATCHED THEN UPDATE SET
    t.CONFIG_VALUE = s.CONFIG_VALUE,
    t.DESCRIPTION  = s.DESCRIPTION,
    t.UPDATED_AT   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
    VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

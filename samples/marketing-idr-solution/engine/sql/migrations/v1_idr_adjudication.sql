-- ============================================================================
-- Engine Migration: v1 IDR Adjudication (additive, idempotent)
--
-- Adds columns required by v1 ML+LLM Adjudication for Individuals:
--   * IDR_CORE_MATCH_RESULTS.EDGE_POLARITY  ('MATCH' | 'VETO')
--   * IDR_CORE_MATCH_RESULTS.EDGE_TARGET    ('INDIVIDUAL' | 'HOUSEHOLD')
--   * IDR_MATCHING_RULES.TARGET             ('INDIVIDUAL' | 'HOUSEHOLD')
--
-- This script is INTENDED TO RUN FOR EVERY USE-CASE that consumes the engine.
-- It is invoked from each use-case's deploy_all.sh after engine table DDL and
-- BEFORE engine procedures (so any proc referencing these columns can compile).
--
-- The engine procs (SP_UPDATE_CLUSTERS, SP_UPDATE_CLUSTERS_FULL, SP_RUN_IDR_PIPELINE)
-- reference these columns. Without this migration, those procs would fail at
-- runtime with "invalid identifier 'EDGE_POLARITY'" or similar.
--
-- Defaults preserve legacy behavior: existing rows read as MATCH/INDIVIDUAL.
-- ============================================================================

ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_RESULTS')
  ADD COLUMN IF NOT EXISTS EDGE_POLARITY VARCHAR DEFAULT 'MATCH';

ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.SILVER.IDR_CORE_MATCH_RESULTS')
  ADD COLUMN IF NOT EXISTS EDGE_TARGET VARCHAR DEFAULT 'INDIVIDUAL';

ALTER TABLE IF EXISTS IDENTIFIER('&{deployment_db}.CONFIG.IDR_MATCHING_RULES')
  ADD COLUMN IF NOT EXISTS TARGET VARCHAR DEFAULT 'INDIVIDUAL';

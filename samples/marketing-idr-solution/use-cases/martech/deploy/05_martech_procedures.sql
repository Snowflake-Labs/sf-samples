-- ============================================================================
-- Martech: 05_martech_procedures.sql
-- Deploys martech-specific stored procedures (custom standardize, ML, LLM, reset).
-- ============================================================================

USE DATABASE IDENTIFIER('&{deployment_db}');

!source ../sql/procedures/sp_custom_standardize.sql
!source ../sql/procedures/sp_generate_ml_candidates.sql
!source ../sql/procedures/sp_auto_label_ml_pairs.sql
!source ../sql/procedures/sp_ml_score_candidates.sql
!source ../sql/procedures/sp_llm_adjudicate_pairs.sql
!source ../sql/procedures/sp_reset_idr.sql

-- ============================================================================
-- Martech: 04_martech_config.sql
-- Seeds matching rules, source priority, LLM thresholds, std rules.
-- ============================================================================

USE DATABASE IDENTIFIER('&{deployment_db}');

!source ../sql/config/standardization_rules_seed.sql
!source ../sql/config/matching_rules_seed.sql
!source ../sql/config/source_priority_seed.sql
!source ../sql/config/llm_review_config_seed.sql

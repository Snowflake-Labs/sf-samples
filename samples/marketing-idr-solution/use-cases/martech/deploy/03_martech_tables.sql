-- ============================================================================
-- Martech: 03_martech_tables.sql
-- Wires bronze DDL + IDR_CORE state-table DDL + silver/gold tables.
-- All sourced from use-cases/martech/{ddl,sql/idr_core,sql/silver,sql/gold}/.
-- ============================================================================

USE DATABASE IDENTIFIER('&{deployment_db}');

-- Bronze tables + streams
!source ../ddl/pos_transaction_raw.sql
!source ../ddl/loyalty_member_raw.sql
!source ../ddl/web_clickstream_raw.sql
!source ../ddl/shopify_order_raw.sql

-- IDR_CORE state tables (required by reused engine procs; see C3 in design)
!source ../sql/idr_core/idr_core_cluster.sql
!source ../sql/idr_core/idr_core_match_results.sql
!source ../sql/idr_core/idr_core_cluster_log.sql
!source ../sql/idr_core/idr_core_match_log.sql
!source ../sql/idr_core/idr_core_process_state.sql
!source ../sql/idr_core/idr_core_event_log.sql

-- IDR_CORE config-table DDLs (seed data populated by 02_engine.sql)
!source ../sql/idr_core/idr_core_nickname_map.sql
!source ../sql/idr_core/idr_core_standardization_rules.sql
!source ../sql/idr_core/idr_ml_ai_blocking_config.sql
!source ../sql/idr_core/idr_ml_ai_evaluation_config.sql

-- Silver standardized tables
!source ../sql/silver/std_pos_transaction_raw.sql
!source ../sql/silver/std_loyalty_member_raw.sql
!source ../sql/silver/std_web_clickstream_raw.sql
!source ../sql/silver/std_shopify_order_raw.sql

-- Silver ML + LLM tables
!source ../sql/silver/ml_pair_features.sql
!source ../sql/silver/ml_training_view.sql
!source ../sql/silver/llm_review_queue.sql

-- Gold dynamic table (must be last; depends on cluster tables)
!source ../sql/gold/dt_customer_profile.sql

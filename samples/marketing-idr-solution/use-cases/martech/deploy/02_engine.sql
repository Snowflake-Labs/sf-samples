-- ============================================================================
-- Martech: 02_engine.sql
-- Deploys the engine into the configured database.
--
-- This script copies engine DDL bodies (procedures, UDFs, seeds, tags, and
-- tables that engine/sql/tables/ DOES supply) into MARTECH context. The 6
-- engine-state IDR_CORE_* tables and 4 CONFIG tables that engine does NOT
-- supply are created in 03_martech_tables.sql via use-cases/martech/sql/idr_core/.
--
-- For brevity and DRY, this file !sources the engine SQL files (which use
-- 'IDR_DEMO' as the database literal) AFTER first changing the database to
-- &{deployment_db} via USE DATABASE. The hardcoded 'IDR_DEMO' qualifier in
-- engine procs is harmless because the procs use string-concat with a runtime
-- DB var; only DDL files create IDR_DEMO objects, which we work around by
-- creating MARTECH-side copies in 03_martech_tables.sql.
--
-- An alternative is to run sed/awk on engine SQL files to rewrite IDR_DEMO ->
-- &{deployment_db} during deployment. This is what deploy_all.sh does.
-- ============================================================================

USE DATABASE IDENTIFIER('&{deployment_db}');

-- The actual engine SQL deployment is orchestrated by deploy_all.sh, which
-- pipes engine files through sed to rewrite the database qualifier and then
-- executes them. See deploy_all.sh for the exact mechanism.

-- This file is a placeholder marker so deploy ordering is explicit:
SELECT 'engine deployment handled by deploy_all.sh' AS note;

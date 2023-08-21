-------------------------------------------------
-- NAME:	 IMPL-IS-18-data-mask.txt
-------------------------------------------------
-- DESCRIPTION:
--	data mask information
--
-- OUTPUT:
--	data mask, data mask information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- OPTIONS:
--	can narrow results to data mask level
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	

SELECT
	POLICY_OWNER,
	POLICY_DB,
    POLICY_SCHEMA,
	POLICY_NAME, 
    POLICY_KIND,
--    P.POLICY_SIGNATURE,
--    P.POLICY_RETURN_TYPE,
--    P.POLICY_BODY,
    R.REF_DATABASE_NAME,
    R.REF_SCHEMA_NAME,
    R.REF_ENTITY_NAME,
    R.REF_ENTITY_DOMAIN,
    R.REF_COLUMN_NAME,
    R.REF_ARG_COLUMN_NAMES
FROM
	table(information_schema.policy_references(policy_name => 'my_db.my_schema.ssn_mask'))
--    table(information_schema.policy_references(ref_entity_name => 'my_db.my_schema.my_table', ref_entity_domain => 'table'))
ORDER BY
    1,2,3,4;
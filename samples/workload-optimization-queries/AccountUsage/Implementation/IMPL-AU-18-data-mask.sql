-------------------------------------------------
-- NAME:	 IMPL-AU-18-data-mask.txt
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
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-18-data-mask.txt
-------------------------------------------------
-- List of data masks in account
-------------------------------------------------

SELECT
	P.POLICY_OWNER,
	P.POLICY_CATALOG,
    P.POLICY_SCHEMA,
	P.POLICY_NAME, 
    P.POLICY_SIGNATURE,
    P.POLICY_RETURN_TYPE,
    P.POLICY_BODY,
    R.REF_DATABASE_NAME,
    R.REF_SCHEMA_NAME,
    R.REF_ENTITY_NAME,
    R.REF_ENTITY_DOMAIN,
    R.REF_COLUMN_NAME,
    R.REF_ARG_COLUMN_NAMES
FROM
	TABLE($MASKING_POLICIES) P
    JOIN TABLE($POLICY_REFERENCES) R ON
        P.POLICY_CATALOG = R.POLICY_DB AND
        P.POLICY_SCHEMA = R.POLICY_SCHEMA AND
        P.POLICY_ID = R.POLICY_ID
WHERE
--	POLICY_FORMAT_OWNER = <NAME> AND
--	POLICY_FORMAT_CATALOG = <NAME> AND
--  POLICY_FORMAT_SCHEMA = <NAME> AND
--	POLICY_FORMAT_NAME = <NAME> AND
	P.DELETED IS NULL
ORDER BY
    1,2,3,4;

-------------------------------------------------
-- NAME:	 IMPL-AU-06-stage.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers stages
--
-- OUTPUT:
--	stage, stage information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	
-------------------------------------------------
-- IMPL-AU-04-stages.txt
-------------------------------------------------
-- LISTS STAGES IN ACCOUNT
-------------------------------------------------

SELECT
    STAGE_OWNER,
    STAGE_NAME,
	STAGE_SCHEMA,
 	STAGE_CATALOG,
	STAGE_URL,
	STAGE_REGION,
    STAGE_TYPE,
    COMMENT
 FROM
	TABLE($STAGES)
WHERE
	DELETED IS NULL
ORDER BY
	1,2,3,4
;  
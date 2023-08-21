-------------------------------------------------
-- NAME:	 IMPL-IS-06-stage.txt
-------------------------------------------------
-- DESCRIPTION:
--	reporting of customers stages
--
-- OUTPUT:
--	 stage, stage information
--
-- NEXT STEPS:
--	Use for runbook reporting
--	Use to validate configuration
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
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
--WHERE
--	DELETED IS NULL
ORDER BY
	1,2,3,4
;  
-------------------------------------------------
-- NAME:	 COST-AU-35-bot-snowpipe.txt
-------------------------------------------------
-- DESCRIPTION:
--	Beginning of Time, monthly reporting of cost based upon sources data retention
--
-- OUTPUT:
--	account, pipe, year, month, credits
--
-- NEXT STEPS:
--	Use for historical reporting
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	

	select
      pipe_name
	,EXTRACT(YEAR FROM START_TIME) AS XYEAR
    ,EXTRACT(MONTH FROM START_TIME) AS XMONTH
      ,sum(credits_used) as units_consumed
	FROM
      TABLE($PIPE_USAGE_HISTORY)
	group by 1,2,3
	order by 1,2,3;
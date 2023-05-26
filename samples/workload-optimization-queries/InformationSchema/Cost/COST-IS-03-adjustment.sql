-------------------------------------------------
-- NAME:	 COST-IS-03-adjustment.txt
-------------------------------------------------
-- DESCRIPTION:
--	Reporting of daily compute adjustment credits
--	Snowflake does not charge for gs credits until they exceed 10%
--
-- OUTPUT:
--	adjustment credits by account/day
--
-- NEXT STEPS:
--	Use for reporting purposes
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
select
      to_date(trunc(start_time, 'DAY')) as usage_day
      ,'ADJ FOR INCL CLOUD SERVICES' as usage_category			--ADJ FOR INCL CLOUD SERVICES
      ,sum(credits_used_compute * -.1) as units_consumed
	FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
		date_range_start=>dateadd('day',-2,current_date()),
		date_range_end=>current_date()))
    GROUP BY 1,2;
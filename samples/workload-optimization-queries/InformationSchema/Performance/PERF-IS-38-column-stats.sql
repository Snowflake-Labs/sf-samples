-------------------------------------------------
-- NAME:	 PERF-IS-38-column-stats.txt
-------------------------------------------------
-- DESCRIPTION:
--	These stats are for micro-partition min and max values, not storage metrics. 
--	Looking at the min and max can help determine how well clustered a table is on a 
--	column
--
-- OUTPUT:
--	MIN, MAX, and value range for specified columns
--
-- NEXT STEPS:
--	Determine if table needs to be reclustered or cluster key needs to be changed
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		created/updated for repository
-------------------------------------------------
	
-- These stats are for micro-partition min and max values, not storage metrics. 
-- Looking at the min and max can help determine how well clustered a table is on a column

SELECT   $1:colEPs.<column_name>.estimatedMin.val::int as ESTIMATED_MIN
        ,$1:colEPs.<column_name>.estimatedMax.val::int as ESTIMATED_MAX
        ,ESTIMATED_MAX - ESTIMATED_MIN as VALUE_RANGE
  FROM TABLE(
         SYSTEM_TABLE_SCAN(
           'SYSTEM$FDN_FILES'
          ,'<account>.<database>.<schema>.<table>'
         )
       ) MG;
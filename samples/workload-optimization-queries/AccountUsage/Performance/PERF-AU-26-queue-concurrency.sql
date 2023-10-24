-------------------------------------------------
-- NAME:	 PERF-AU-26-queue-concurrency.txt
-------------------------------------------------
-- DESCRIPTION:
--	For a narrow timeframe, typically one hour, calculates concurrency of queuing at 
--	the second level
--
-- OUTPUT:
--	Understand the volume of requests queuing concurrently
--
-- NEXT STEPS:
--	Address throughput (SQL & Cluster Keys)
--	Address throughput (scale up… vWH sizing)
--	Address concurrency (scale out…increase clusters)
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 18JAN22	WNA		CREATED/UPDATED FOR REPOSITORY
-------------------------------------------------
	

------------------------------------------------------------------------
-- Count of concurrency levels by second, at the workload level
------------------------------------------------------------------------
SELECT	M.LOGDATE
    ,X.PIT
--	,X.HHH
--	,X.MMI    
--	,X.SSE
	,M.WAREHOUSE_NAME
	,COUNT(*) AS CONCURRENTQUEUING 
FROM     
(
SELECT	CAST(START_TIME AS DATE) LOGDATE,
		case when WAREHOUSE_NAME is null then 'GS' else WAREHOUSE_NAME END AS WAREHOUSE_NAME, 
		CAST((EXTRACT(HOUR FROM START_TIME)) *3600 
			+ (EXTRACT(MINUTE FROM START_TIME)) *60 
			+ (EXTRACT(SECOND FROM START_TIME)) AS DEC(8,2)) AS STARTSECS,
		CAST(EXTRACT(HOUR FROM END_TIME)*3600
			+ EXTRACT(MINUTE FROM END_TIME)*60 
			+ EXTRACT(SECOND FROM END_TIME) AS DEC(8,2)) AS ENDSECS 
FROM
(
SELECT	START_TIME,
		case when WAREHOUSE_NAME is null then 'GS' else WAREHOUSE_NAME END AS WAREHOUSE_NAME, 
		TIMEADD('SECOND',QUEUED_PROVISIONING_TIME/1000,START_TIME) AS END_TIME
FROM	TABLE($QUERY_HISTORY)
WHERE   START_TIME BETWEEN $TS_START AND $TS_END    
AND     QUEUED_PROVISIONING_TIME >= 0
) X
GROUP	BY 1,2,3,4
) M 
CROSS JOIN
(
select 
--   to_timestamp(therange),
    (hour(to_timestamp(therange))*3600)+
    (minute(to_timestamp(therange))*60)+
    second(to_timestamp(therange)) 	PIT,
    (hour(to_timestamp(therange))*3600) AS HHH,
    (minute(to_timestamp(therange))*60) AS MMI,
    second(to_timestamp(therange)) AS SSE
from
(SELECT ones.n + 10*tens.n + 100*hunds.n + 1000*thous.n + 10000*tenthou.n + 100000*hunthou.n + 1000000*mill.n + 10000000*tenmill.n + 100000000*hunmill.n + 1000000000*billion.n as therange
FROM (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) ones(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) tens(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) hunds(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) thous(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) tenthou(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) hunthou(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) mill(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) tenmill(n),
     (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) hunmill(n),
     (VALUES(0),(1)) billion(n)
having therange between cast(extract(EPOCH_SECOND from to_timestamp($ts_start)) as integer) 
 and cast(extract(EPOCH_SECOND from to_timestamp($ts_end)) as integer))
) X 
WHERE	M.STARTSECS <= X.PIT 
AND	M.ENDSECS > X.PIT                            
GROUP	BY 1,2,3
order by 3,2;



-------------------------------------------------
-- NAME:	 PERF-IS-81-concurrency-end-all.txt
-------------------------------------------------
-- DESCRIPTION:
--	Request and queue concurrency counts by second of day
-- Includes seconds where no work exists
--					
-- OUTPUT:					
--	PIT is point in time, second of day
--  Request concurrency count at second level
--  Queue concurrency count at second level
--	
-- OPTIONS:
--	Need to filter on Account and Warehouse
--
-- REVISION HISTORY
-- DATE		INIT	DESCRIPTION
----------  ----    -----------
-- 05MAY22	WNA		created/updated for repository
-------------------------------------------------

SET WAREHOUSE_NAME = 'DELIVERY';

WITH REQ_POINTS AS
(
SELECT	M.LOGDATE
    ,X.PIT
	,M.WAREHOUSE_NAME
	,COUNT(*) AS CONCURRENTQUERIES    
FROM     
(
SELECT	CAST(START_TIME AS DATE) LOGDATE,
		case when WAREHOUSE_NAME is null then 'GS' else WAREHOUSE_NAME END AS WAREHOUSE_NAME, 
		CAST((EXTRACT(HOUR FROM START_TIME)) *3600 
			+ (EXTRACT(MINUTE FROM START_TIME)) *60 
			+ (EXTRACT(SECOND FROM START_TIME) ) AS DEC(8,2)) AS STARTSECS,
		CAST(EXTRACT(HOUR FROM END_TIME)*3600
			+ EXTRACT(MINUTE FROM END_TIME)*60 
			+ EXTRACT(SECOND FROM END_TIME) AS DEC(8,2)) AS ENDSECS 
FROM
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME_RANGE_END=>CURRENT_TIMESTAMP()))
WHERE		WAREHOUSE_NAME = $WAREHOUSE_NAME  
GROUP	BY 1,2,3,4--,5
HAVING	ENDSECS-STARTSECS >= 0
) M 
CROSS JOIN
(
select 
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
),
QUE_POINTS AS
(
SELECT	M.LOGDATE
    ,X.PIT
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
FROM	
	TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(
		END_TIME_RANGE_START=>dateadd('day',-6,CURRENT_TIMESTAMP()),
		END_TIME_RANGE_END=>CURRENT_TIMESTAMP()))
WHERE		WAREHOUSE_NAME = $WAREHOUSE_NAME  
AND   QUEUED_PROVISIONING_TIME >= 0
) X
GROUP	BY 1,2,3,4
) M 
CROSS JOIN
(
select 
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
),
ALL_POINTS AS
(
select 
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
) 
SELECT	A.PIT
	,MAX(CASE WHEN CONCURRENTQUERIES > 0 THEN CONCURRENTQUERIES ELSE 0 END) AS REQ_CONCURRENCY
	,MAX(CASE WHEN CONCURRENTQUEUING > 0 THEN CONCURRENTQUEUING ELSE 0 END) AS QUE_CONCURRENCY
FROM ALL_POINTS A
LEFT OUTER JOIN REQ_POINTS R
ON A.PIT = R.PIT
LEFT OUTER JOIN QUE_POINTS Q
ON A.PIT = Q.PIT
GROUP BY 1
order by 1;


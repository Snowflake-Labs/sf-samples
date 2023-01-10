-- setup warehouse, database and schema
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE R_F_DEMO_WH WITH WAREHOUSE_SIZE='XSmall'  STATEMENT_TIMEOUT_IN_SECONDS=15 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15;
USE WAREHOUSE R_F_DEMO_WH;
CREATE DATABASE R_F_DEMO;
CREATE SCHEMA R_F_DEMO.DEMO;

-- create table to hold impressions
use R_F_DEMO.DEMO;
create or replace table IMPRESSIONS(event_time TIMESTAMP_TZ,
                                   campaign_id VARCHAR(10),
                                   placement_id VARCHAR(10),
                                   creative_id VARCHAR(10),
                                   user_id VARCHAR(40));

-- generate test data
insert into IMPRESSIONS
select 
   dateadd(second, uniform(1, 2592000, random(1)), ('2022-09-01'::timestamp)) as event_time,
   '174631' as campaign_id
  ,uniform(200001,202000,random(2))::varchar(10) as placement_id
  ,uniform(300001,301000,random(3))::varchar(10) as creative_id
  ,sha1(uniform(1, 3500000, random(4)))::varchar(40) as user_id
from table(generator(rowcount=>20000000));

-- verify data
select count(1) from IMPRESSIONS where campaign_id='174631';
select count(distinct(placement_id)) from IMPRESSIONS where campaign_id='174631';
select count(distinct(user_id)) from IMPRESSIONS where campaign_id='174631';

-- calculate cumulative impressions by day
WITH DAILY_IMPRESSIONS AS
(
SELECT TO_DATE(event_time) as day,
       COUNT(1) as day_impressions
FROM 
  IMPRESSIONS
WHERE campaign_id='174631'
GROUP BY 1
)
select
  day,
  sum(day_impressions) over (order by day asc rows between unbounded preceding and current row) as CUMULATIVE_IMPRESSIONS
from DAILY_IMPRESSIONS
order by 1;

-- calculate cumulative uniques by day
WITH firstseen AS (
  SELECT user_id, MIN(TO_DATE(event_time)) firstday
  FROM IMPRESSIONS
  WHERE campaign_id='174631'
  GROUP BY 1
)
SELECT DISTINCT firstday as day, COUNT(user_id) OVER (ORDER BY firstday) daily_cumulative_uniques
FROM firstseen
ORDER BY 1;

-- show cumulativer impressions and uniques side-by-side to understand diminishing returns
WITH DAILY_CUMULATIVE_IMPRESSIONS AS (
    WITH DAILY_IMPRESSIONS AS
    (
    SELECT TO_DATE(event_time) as day,
       COUNT(1) as day_impressions
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    )
    select
      day,
      sum(day_impressions) over (order by day asc rows between unbounded preceding and current row) as CUMULATIVE_IMPRESSIONS
    from DAILY_IMPRESSIONS
),
DAILY_CUMULATIVE_UNIQUES AS
(
    WITH firstseen AS
    (
    SELECT user_id, MIN(TO_DATE(event_time)) firstday
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    )
    SELECT DISTINCT firstday as day, COUNT(user_id) OVER (ORDER BY firstday) CUMULATIVE_UNIQUES
    FROM firstseen
    ORDER BY 1
)
SELECT dci.DAY, dci.CUMULATIVE_IMPRESSIONS, dcu.CUMULATIVE_UNIQUES
FROM DAILY_CUMULATIVE_IMPRESSIONS dci
 JOIN DAILY_CUMULATIVE_UNIQUES dcu on dcu.day=dci.day
ORDER BY 1;

-- show cumulative impressions and uniques side-by-side to understand diminishing returns, with minimum of 3 impressions
WITH DAILY_CUMULATIVE_IMPRESSIONS AS (
    WITH DAILY_IMPRESSIONS AS
    (
    SELECT TO_DATE(event_time) as day,
       COUNT(1) as day_impressions
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    )
    select
      day,
      sum(day_impressions) over (order by day asc rows between unbounded preceding and current row) as CUMULATIVE_IMPRESSIONS
    from DAILY_IMPRESSIONS
),
DAILY_CUMULATIVE_UNIQUES_MIN_3 AS
(
    WITH firstseen AS
    (
    SELECT user_id, MIN(TO_DATE(event_time)) firstday
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    HAVING count(1) > 2
    )
    SELECT DISTINCT firstday as day, COUNT(user_id) OVER (ORDER BY firstday) CUMULATIVE_UNIQUES
    FROM firstseen
    ORDER BY 1
)
SELECT dci.DAY, dci.CUMULATIVE_IMPRESSIONS, dcu.CUMULATIVE_UNIQUES
FROM DAILY_CUMULATIVE_IMPRESSIONS dci
 JOIN DAILY_CUMULATIVE_UNIQUES_MIN_3 dcu on dcu.day=dci.day
ORDER BY 1;

-- show cumulative impressions and uniques, comparing to minimum of 3 impressions
WITH DAILY_CUMULATIVE_IMPRESSIONS AS (
    WITH DAILY_IMPRESSIONS AS
    (
    SELECT TO_DATE(event_time) as day,
       COUNT(1) as day_impressions
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    )
    select
      day,
      sum(day_impressions) over (order by day asc rows between unbounded preceding and current row) as CUMULATIVE_IMPRESSIONS
    from DAILY_IMPRESSIONS
),
DAILY_CUMULATIVE_UNIQUES_MIN_3 AS
(
    WITH firstseen AS
    (
    SELECT user_id, MIN(TO_DATE(event_time)) firstday
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    HAVING count(1) > 2
    )
    SELECT DISTINCT firstday as day, COUNT(user_id) OVER (ORDER BY firstday) CUMULATIVE_UNIQUES
    FROM firstseen
    ORDER BY 1
),
DAILY_CUMULATIVE_UNIQUES AS
(
    WITH firstseen AS
    (
    SELECT user_id, MIN(TO_DATE(event_time)) firstday
    FROM IMPRESSIONS
    WHERE campaign_id='174631'
    GROUP BY 1
    )
    SELECT DISTINCT firstday as day, COUNT(user_id) OVER (ORDER BY firstday) CUMULATIVE_UNIQUES
    FROM firstseen
    ORDER BY 1
)
SELECT dci.DAY, dci.CUMULATIVE_IMPRESSIONS, dcu.CUMULATIVE_UNIQUES, dcu3.CUMULATIVE_UNIQUES as "UNIQUES MIN 3 IMPRESSIONS"
FROM DAILY_CUMULATIVE_IMPRESSIONS dci
 JOIN DAILY_CUMULATIVE_UNIQUES dcu on dcu.day=dci.day
 JOIN DAILY_CUMULATIVE_UNIQUES_MIN_3 dcu3 on dcu3.day=dci.day
ORDER BY 1;

-- basic frequency query
WITH impressions_by_user AS
(select user_id, count(1) as num_impressions
FROM IMPRESSIONS
WHERE campaign_id='174631'
GROUP BY 1)
select avg(num_impressions)
from impressions_by_user;

-- see distributions of frequency
WITH impressions_by_user AS
(select user_id, count(1) as num_impressions
FROM IMPRESSIONS
WHERE campaign_id='174631'
GROUP BY 1)
select num_impressions, count(1) as num_users
from impressions_by_user
group by 1
order by 1;

-- incremental reach

-- only only both
select count(1), count(distinct user_id) from impressions where campaign_id='174631' and placement_id='201571';
select count(1), count(distinct user_id) from impressions where campaign_id='174631' and placement_id='200420';
select count(1), count(distinct user_id) from impressions where campaign_id='174631' and placement_id in ('200420','201571');

-- how many people does this placement have that the other doesnt
select count(distinct i.user_id)
from impressions i
where i.campaign_id='174631' and i.placement_id='201571'
 AND i.user_id not in (select user_id from impressions i2 where i2.campaign_id='174631' and i2.placement_id='200420');
 
-- how many users does this placement have that no other placement does?
select count(distinct i.user_id)
from impressions i
where i.campaign_id='174631' and i.placement_id='201571'
 AND i.user_id not in (select user_id from impressions i2 where i2.campaign_id='174631' and i2.placement_id<>'201571');
 
-- ordered placements with least unique people
select i.placement_id, count(distinct i.user_id)
from impressions i
where i.campaign_id='174631'
 AND i.user_id not in (select user_id from impressions i2 where i2.campaign_id='174631' and i2.placement_id<>i.placement_id)
group by 1
order by 2 asc;

-- Comparison of PS Credits by query estimate to query_attribution_history view data.
-- Note: query_attribution_history does not include WH idle time and could miss some fast queries under 100ms, but sf_credits_by_query does.

SELECT qah.start_time,
	round(qah.credits_attributed_compute, 4) AS attrib_credits, --qah.CREDITS_USED_QUERY_ACCELERATION
	sf.ALLOCATED_CREDITS_USED_COMPUTE,
	attrib_credits - ALLOCATED_CREDITS_USED_COMPUTE AS diff,
	qah.query_id
FROM snowflake.account_usage.query_attribution_history qah
LEFT JOIN (
	SELECT sum(ALLOCATED_CREDITS_USED_COMPUTE) AS ALLOCATED_CREDITS_USED_COMPUTE,
		query_id
	FROM SNOWFLAKE_FINOPS.FINOPS_ACCOUNT.SF_CREDITS_BY_QUERY
	WHERE START_TIME >= dateadd(day, - 7, CURRENT_DATE ())
	GROUP BY ALL
	) SF ON sf.query_id = qah.query_id
WHERE true
	AND qah.START_TIME >= dateadd(day, - 7, CURRENT_DATE)
	AND credits_attributed_compute > 0
	AND abs(attrib_credits - ALLOCATED_CREDITS_USED_COMPUTE) > .1
ORDER BY diff;

-- Total credit comparison
SELECT round(sum(qah.credits_attributed_compute + zeroifnull(qah.CREDITS_USED_QUERY_ACCELERATION))) AS attrib_compute_credits, 
	ROUND(SUM(ALLOCATED_CREDITS_USED_COMPUTE + ZEROIFNULL(ALLOCATED_CREDITS_USED_QAS)), 2) AS FINOPS_COMPUTE_CREDITS, --includes idle time of WH.
FROM snowflake.account_usage.query_attribution_history qah
JOIN snowflake.account_usage.query_history qh ON qah.query_id = qh.query_id
JOIN snowflake_finops.finops_account.SF_CREDITS_BY_QUERY AS sf ON sf.query_id = qah.query_id
WHERE qah.start_time >= dateadd(day, - 7, CURRENT_DATE)
	AND qah.start_time < CURRENT_DATE ()
	AND qh.start_time >= dateadd(day, - 7, CURRENT_DATE)
	AND qh.start_time < CURRENT_DATE ();

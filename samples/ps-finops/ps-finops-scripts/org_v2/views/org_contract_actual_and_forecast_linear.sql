USE ROLE <% ctx.env.finops_db_admin_role%>;
USE DATABASE <% ctx.env.finops_acct_db %>;
USE SCHEMA <% ctx.env.finops_acct_schema %>;
CREATE OR REPLACE VIEW orgv1_contract_actual_and_forecast_linear COMMENT = 'Title: Organizational Contract Actual and Forecast Linear. Description: Analyze the contract actual amount and linear forecast in currency of the organization.'
AS
WITH ci
AS (
    -- Retrieve current organizational contract amounts
    -- Note: if contract_items is not available, create a select of static values as an alternative.
    -- Also: Validate these numbers below as could be missing or incorrect values.
    SELECT 
        start_date,
        case when contract_item IN ('Capacity', 'Additional Capacity') then end_date else start_date end as end_date, -- instead of spreading out short term items, make them 1 day additions.
        datediff('day', start_date, case when contract_item IN ('Capacity', 'Additional Capacity') then end_date else start_date end)+1 as contract_length_days,
        amount,
        sum(amount) OVER (
            ORDER BY NULL
            ) AS contract_total,
        sum(amount) OVER (
            ORDER BY start_date
            ) AS contract_running_sum,
        round(amount / contract_length_days, 2) AS Daily_Amount
    FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
    WHERE start_date <= CURRENT_DATE()
        -- AND contract_item IN ('Capacity', 'Additional Capacity', 'Free Usage')
        AND nvl(EXPIRATION_DATE, '2999-01-01') > end_date
)
, seq
AS (
   -- Generate a sequence of numbers using contract length
   SELECT seq4() AS num
   FROM TABLE (GENERATOR(ROWCOUNT => (3660))) v
   WHERE num <= (
           SELECT datediff('day', min(start_date), max(end_date))
           FROM ci
           )
   )
, car
AS (
    -- Join contract amounts with sequence
    SELECT dateadd('day', num, start_date) AS day,
        contract_length_days,
        -- CASE
        --     WHEN day BETWEEN start_date AND end_date
        --         THEN amount
        --     ELSE 0
        --     END AS contract_amount,
        CASE
            WHEN day BETWEEN start_date AND end_date
                THEN daily_amount
            ELSE 0
            END AS daily_amount,
        max(contract_running_sum) OVER (ORDER BY day) AS contract_total_cumulative
    FROM ci
    JOIN seq
    WHERE day <= (
            SELECT max(end_date)
            FROM ci
            )
   )
, ci_daily_full
AS (
    -- What is our daily budget
    SELECT 
        day AS Calendar_Date,
        contract_length_days,
        sum(daily_amount) daily_budget_amount,
        max(contract_total_cumulative) AS contract_total_cumulative,
        row_number() OVER (
            ORDER BY day
            ) AS day_num
    FROM car
    GROUP BY day, contract_length_days
    HAVING daily_budget_amount > 0
    )
, ci_agg
AS (
    -- Accumulate daily budget amounts
    SELECT Calendar_Date,
        contract_length_days,
        daily_budget_amount,
        sum(daily_budget_amount) OVER (
            ORDER BY Calendar_Date ROWS BETWEEN unbounded preceding AND CURRENT ROW
            ) AS accumulated_budget,
        day_num,
        contract_total_cumulative
    FROM ci_daily_full
    )
, spend
AS (
    -- Add accumulated spend
    SELECT usage_date,
        round(sum(usage_in_currency), 1) daily_total_cost,
        row_number() OVER ( ORDER BY usage_date ) AS day_num,
        sum(daily_total_cost) OVER (
            ORDER BY usage_date ROWS BETWEEN unbounded preceding AND CURRENT ROW ) AS accumulated_spend
    FROM snowflake.organization_usage.usage_in_currency_daily d
    JOIN (
        SELECT min(start_date) AS start_date,
            max(end_date) AS end_date
        FROM ci
        ) ci --cartesian join.
    WHERE usage_date BETWEEN ci.start_date AND ci.end_date
    GROUP BY 1
    )
, combine
AS (
    -- Join contract items with spend
   SELECT n.day_num,
       c.usage_date,
       n.Calendar_Date,
       n.contract_length_days,
       c.daily_total_cost,
       c.accumulated_spend,
       n.daily_budget_amount,
       n.accumulated_budget,
       n.contract_total_cumulative
   FROM ci_agg n
   LEFT JOIN spend c ON n.day_num = c.day_num
   )
, run
AS (
    -- Calculate weekly run rate
    SELECT avg(daily_total_cost) AS run_rate_7_day
    FROM combine
    WHERE usage_date BETWEEN dateadd(day, -8, CURRENT_DATE())
            AND dateadd(day, -1, CURRENT_DATE())
    )
, run_30
AS (
    -- Calculate monthly run rate
    SELECT avg(daily_total_cost) AS run_rate_30_day
    FROM combine
    WHERE usage_date BETWEEN dateadd(day, -31, CURRENT_DATE())
            AND dateadd(day, -1, CURRENT_DATE())
    )
SELECT 
   Calendar_Date,
   contract_total_cumulative,
   day_num,
   daily_budget_amount,
   accumulated_budget,
   daily_total_cost,
   max(iff(daily_total_cost IS NULL, NULL, day_num)) OVER ( ORDER BY usage_date ) AS days_since_start_actual,
   max(accumulated_spend) OVER ( ORDER BY calendar_date ) AS accumulated_spend_max,
   (accumulated_budget - accumulated_spend_max) AS budget_remaining,
   (day_num - days_since_start_actual) * run.run_rate_7_day + accumulated_spend_max AS run_rate_7_forecast_accumulated_amount,
   (day_num - days_since_start_actual) * run_30.run_rate_30_day + accumulated_spend_max AS run_rate_30_forecast_accumulated_amount,
   CASE
       WHEN daily_total_cost IS NULL
           THEN round(div0(contract_total_cumulative - accumulated_spend_max, (
                           (
                               SELECT datediff('day', min(start_date), max(end_date))
                               FROM ci
                               ) - days_since_start_actual
                           )), 1)
       ELSE NULL
       END AS slope,
   round(slope * (day_num - days_since_start_actual) + accumulated_spend_max, 1) AS ideal_slope, --What is the rate of spend to get to perfect contract $0 point.
   run_rate_7_day,
   run_rate_30_day
FROM combine c
CROSS JOIN run
CROSS JOIN run_30;
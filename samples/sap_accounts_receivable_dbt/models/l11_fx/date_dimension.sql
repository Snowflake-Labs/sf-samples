
{{ config(materialized='table') }}
with cte_my_date as (
    select 
      dateadd(day, seq4(), '1994-01-01') as my_date
    from table(generator(rowcount=>10000))
)
select 
  my_date::date as dim_date
  , year(my_date) as dim_year
  , month(my_date) as dim_month
  , day(my_date) as day_of_mon
from cte_my_date
where day(my_date) = 1

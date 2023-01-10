{{ config(
  enabled=false
) }}-- /* Author: David Richert and Matthias Nicolas Date: September 2021 */

-- use database sap_dbt;
-- use schema fx;

-- /* We want to modify tcurr a little bit to get a date, a positive exchange number, unit changes, just EUR for target currency and currency type as EURX */
-- create or replace table tcurr as 
-- (
-- );


    
-- /* STEP 1: NOT IN. Find the missing first day of months and missing currencies on those days in the currency conversion view (tcurr) */

-- create or replace table tcurr_with_substitutions as (
-- with
--   find_missing_dates as (
--    select distinct dim_date, fcurr
--  from date_curr_dimension
--   where (dim_date,fcurr) not in (select gdatu, fcurr from tcurr)     
--     )
-- /* STEP 2: MIN. Generate substitute dates by looking for the last day of the previous month or
--   for the first available first day of a subsequent month.*/
    
-- , substitute_dates as (
-- select dim_date, fmd.fcurr, min(gdatu) as substitution_date
-- from tcurr t join find_missing_dates fmd
--      on (dim_date - 1 = gdatu and t.fcurr = fmd.fcurr)
--          or
--         (dim_date < gdatu
--          and date_trunc('month',dim_date) < date_trunc('month',gdatu)
--          and dayofmonth(gdatu) = 1
--          and t.fcurr=fmd.fcurr
--          )
-- group by dim_date, fmd.fcurr  
--     )
    
-- /* STEP 3: UNION ALL  */ 
    
-- , add_missing_columns as 
-- (select dim_date, substitution_date, sd.fcurr, tcurr, ukurs
--  from substitute_dates sd join tcurr t on substitution_date=t.gdatu and sd.fcurr=t.fcurr
-- )
-- /*
--     union to the existing dates and currencies
-- */
-- select dim_date, substitution_date, fcurr, tcurr, ukurs 
-- from add_missing_columns 
-- union all  
-- select gdatu, gdatu, fcurr, tcurr, ukurs  
-- from tcurr
-- where  dayofmonth(gdatu) = 1

-- /*Fill currency table for when from and to date are EUR */
-- union 
-- select dim_date
-- , dim_date
-- , 'EUR'
-- ,'EUR'
-- , 1
-- from date_dimension) ;

-- select * from tcurr_with_substitutions 
--  where dim_date between '2013-01-01' and '2014-01-01' order by dim_date desc;

-- /* Now time to join to our fact table! */
-- Select 
-- budat as posting_date
-- , belnr as document_number
-- , substitution_date as curr_date_used
-- , lcurr as local_currency
-- , dmsol as debit_amt_lc
-- , ukurs as fx_eur
-- , dmsol*ukurs as debit_amt_eur
-- from SAP.SAP."0fi_ar_4" left join tcurr_with_substitutions 
-- on date_trunc('month', budat)= dim_date 
-- and lcurr=fcurr 
-- where 
--  dmsol <> 0 and
--  posting_date between '2013-01-01' and '2013-12-31'
-- order by budat desc;

with
  find_missing_dates as (
    select 
      distinct dim_date, 
      fcurr,
      tcurr
    from {{ ref('date_curr_dimension') }}
    where 
      (dim_date, fcurr, tcurr) not in (select gdatu, fcurr, tcurr from tcurr)
  )
  , substitute_dates as (
    select dim_date, fmd.fcurr, fmd.tcurr, min(gdatu) as substitution_date
    from tcurr t join find_missing_dates fmd
        on (gdatu = dim_date - 1 and t.fcurr = fmd.fcurr and t.tcurr = fmd.tcurr)
        or (
          dim_date < gdatu
            and date_trunc('month',dim_date) < date_trunc('month',gdatu)
            and dayofmonth(gdatu) = 1
            and t.fcurr=fmd.fcurr
        )
    group by dim_date, fmd.fcurr, fmd.tcurr
  )
  , add_missing_columns as (
    select 
      dim_date
      , substitution_date
      , sd.fcurr
      , t.tcurr
      , ukurs
    from substitute_dates sd 
      join {{ ref('tcurr') }} t on substitution_date=t.gdatu and sd.fcurr=t.fcurr
)

select 
  dim_date
  , substitution_date
  , fcurr
  , tcurr
  , ukurs 
from add_missing_columns 

union all  
select 
  gdatu
  , gdatu
  , fcurr
  , tcurr
  , ukurs  
from {{ ref('tcurr') }}
where dayofmonth(gdatu) = 1

union 
select 
  dim_date
  , dim_date
  , 'EUR'
  , 'EUR'
  , 1
from {{ ref('date_dimension') }}

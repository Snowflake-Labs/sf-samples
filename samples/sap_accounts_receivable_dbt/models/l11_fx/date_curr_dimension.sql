select 
  distinct fcurr
  , tcurr
  , dim_date 
from {{ ref('date_dimension') }}, {{ ref('tcurr') }}

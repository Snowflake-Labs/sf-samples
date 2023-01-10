select 
  mandt   
  , kurst   
  , fcurr   
  , tcurr   
  , to_date (substring(to_varchar(99999999 - gdatu),1,8), 'YYYYMMDD')  gdatu
  , case 
    when ukurs < 0 and tfact <= 1 then 
      -1/ukurs 
    when ukurs < 0 and tfact > 1 then 
      -1/ukurs*ffact 
    when ukurs > 0 and tfact > 1 then 
      ukurs*ffact 
    else 
      ukurs 
    end ukurs
  , ffact
  , tfact
from {{ source('sap_sample', 'ztcurr_attr') }}
where kurst='EURX'

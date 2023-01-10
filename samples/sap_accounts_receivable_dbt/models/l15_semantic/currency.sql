select
    client
    , exchange_rate_type
    , fcurr
    , tcurr
    , to_date (substring(to_varchar(99999999 - gdatu),1,8), 'YYYYMMDD')  fxdate
    , YEAR( to_date(substring(to_varchar(99999999 - gdatu),1,8),'YYYYMMDD'))||LPAD(MONTH(to_date (substring(to_varchar(99999999 - gdatu),1,8),'YYYYMMDD')), 3, '0') as fiscper
    , ukurs
    , ffact
    , tfact
from {{ ref('en_ztcurr_attr') }}

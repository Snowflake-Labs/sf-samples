--en_2lis_13_vdhdr
SELECT 
    {{ dbt_utils.star(ref('en_2lis_13_vdhdr')) }}
FROM 
    {{ ref('en_2lis_13_vdhdr') }}

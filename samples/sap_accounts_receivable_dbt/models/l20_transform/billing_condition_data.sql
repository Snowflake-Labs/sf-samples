SELECT 
    {{ dbt_utils.star(ref('en_2lis_13_vdkon')) }}
FROM 
    {{ ref('en_2lis_13_vdkon') }}

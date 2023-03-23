--en_2lis_13_vditm
SELECT {{ dbt_utils.star(ref('en_2lis_13_vditm')) }}
FROM {{ref('en_2lis_13_vditm')}}

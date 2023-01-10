--en_2lis_11_vasth
SELECT 
  {{ dbt_utils.star(ref('en_2lis_11_vasth')) }}
FROM 
  {{ ref('en_2lis_11_vasth') }}

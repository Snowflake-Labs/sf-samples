SELECT 
  {{ dbt_utils.star(ref('en_2lis_11_v_ssl')) }}
FROM 
  {{ref('en_2lis_11_v_ssl')}}

SELECT
  'SAP.2lis_11_vakon' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vakon')) }}  
FROM {{source('sap_sample','2lis_11_vakon')}}

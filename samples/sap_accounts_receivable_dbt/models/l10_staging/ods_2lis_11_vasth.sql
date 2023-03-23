SELECT
  'SAP.2lis_11_vasth' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vasth')) }}  
FROM {{source('sap_sample','2lis_11_vasth')}}

SELECT
  'SAP.2lis_11_vascl' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vascl')) }}  
FROM {{source('sap_sample','2lis_11_vascl')}}

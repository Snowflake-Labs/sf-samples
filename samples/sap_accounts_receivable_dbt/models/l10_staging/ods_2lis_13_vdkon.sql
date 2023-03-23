SELECT
  'SAP.2lis_13_vdkon' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_13_vdkon')) }}  
FROM {{source('sap_sample','2lis_13_vdkon')}}

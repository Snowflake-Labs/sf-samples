SELECT
  'SAP.2lis_13_vditm' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_13_vditm')) }}  
FROM {{source('sap_sample','2lis_13_vditm')}}

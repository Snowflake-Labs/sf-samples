SELECT
  'SAP.2lis_11_vaitm' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vaitm')) }}  
FROM {{source('sap_sample','2lis_11_vaitm')}}

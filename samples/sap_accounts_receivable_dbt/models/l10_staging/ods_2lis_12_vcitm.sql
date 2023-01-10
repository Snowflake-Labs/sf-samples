SELECT
  'SAP.2lis_12_vcitm' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_12_vcitm')) }}  
FROM {{source('sap_sample','2lis_12_vcitm')}}

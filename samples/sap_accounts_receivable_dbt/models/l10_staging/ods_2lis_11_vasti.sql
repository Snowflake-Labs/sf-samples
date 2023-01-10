SELECT
  'SAP.2lis_11_vasti' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vasti')) }}  
FROM {{source('sap_sample','2lis_11_vasti')}}

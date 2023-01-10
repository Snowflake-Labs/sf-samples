SELECT
  'SAP.2lis_11_vahdr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_vahdr')) }}  
FROM {{source('sap_sample','2lis_11_vahdr')}}

SELECT
  'SAP.2lis_13_vdhdr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_13_vdhdr')) }}  
FROM {{source('sap_sample','2lis_13_vdhdr')}}

SELECT
  'SAP.2lis_12_vchdr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_12_vchdr')) }}  
FROM {{source('sap_sample','2lis_12_vchdr')}}

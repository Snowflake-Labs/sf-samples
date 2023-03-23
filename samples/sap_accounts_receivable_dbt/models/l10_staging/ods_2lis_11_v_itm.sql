SELECT
  'SAP.2lis_11_v_itm' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','2lis_11_v_itm')) }}  
FROM {{source('sap_sample','2lis_11_v_itm')}}

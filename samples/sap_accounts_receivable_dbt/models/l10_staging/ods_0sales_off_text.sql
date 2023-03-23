SELECT 
  'SAP.0sales_off_text' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0sales_off_text')) }}
FROM {{source('sap_sample','0sales_off_text')}}

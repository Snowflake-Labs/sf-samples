SELECT 
  'SAP.0salesorg_text' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0salesorg_text')) }}
FROM {{source('sap_sample','0salesorg_text')}}

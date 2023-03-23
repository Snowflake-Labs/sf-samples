SELECT 
  'SAP.0material_text' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0material_text')) }}
FROM {{source('sap_sample','0material_text')}}

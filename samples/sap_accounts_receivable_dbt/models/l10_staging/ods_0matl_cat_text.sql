SELECT 
  'SAP.0matl_cat_text' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0matl_cat_text')) }}
FROM {{source('sap_sample','0matl_cat_text')}}

SELECT 
  'SAP.0material_attr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0material_attr')) }}
FROM {{ source('sap_sample','0material_attr') }}

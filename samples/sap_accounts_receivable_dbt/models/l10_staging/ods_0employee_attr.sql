SELECT 
  'SAP.0employee_attr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0employee_attr')) }}
FROM {{ source('sap_sample','0employee_attr') }}

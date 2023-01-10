SELECT 
  'SAP.0fi_ar_4' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0fi_ar_4')) }}  
FROM {{ source('sap_sample','0fi_ar_4') }}

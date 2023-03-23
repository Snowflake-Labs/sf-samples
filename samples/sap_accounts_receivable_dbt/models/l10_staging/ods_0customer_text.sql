SELECT 
  'SAP.0customer_text' data_source_name, 
  {{ dbt_utils.star(source('sap_sample','0customer_text')) }}
FROM {{ source('sap_sample','0customer_text') }}

SELECT 
    'SAP.0customer_attr' data_source_name,
    {{ dbt_utils.star(source('sap_sample', '0customer_attr')) }}
FROM {{ source('sap_sample','0customer_attr') }}

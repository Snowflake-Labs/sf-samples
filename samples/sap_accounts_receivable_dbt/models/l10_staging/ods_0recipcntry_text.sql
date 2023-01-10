SELECT 
    'SAP.0recipcntry_text' data_source_name,  
    {{ dbt_utils.star(source('sap_sample','0recipcntry_text')) }}
FROM {{source('sap_sample','0recipcntry_text')}}

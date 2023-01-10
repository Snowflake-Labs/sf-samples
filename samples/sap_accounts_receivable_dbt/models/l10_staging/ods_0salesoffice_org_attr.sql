SELECT 
  'SAP.0salesoffice_org_attr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','0salesoffice_org_attr')) }}
FROM {{source('sap_sample','0salesoffice_org_attr')}}

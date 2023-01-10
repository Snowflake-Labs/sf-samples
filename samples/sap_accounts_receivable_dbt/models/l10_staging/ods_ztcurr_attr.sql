SELECT
  'SAP.ztcurr_attr' data_source_name,  
  {{ dbt_utils.star(source('sap_sample','ztcurr_attr')) }}  
FROM {{source('sap_sample','ztcurr_attr')}}

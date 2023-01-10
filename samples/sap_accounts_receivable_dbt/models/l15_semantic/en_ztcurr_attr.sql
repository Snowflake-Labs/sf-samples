select 
{{ sap_translate_cols(ref('ods_ztcurr_attr')) |indent(4, True) }}
from {{ref('ods_ztcurr_attr')}}

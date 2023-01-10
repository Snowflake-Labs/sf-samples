SELECT
{{ sap_translate_cols(ref('ods_2lis_11_vasti')) |indent(4, True) }}
from {{ref('ods_2lis_11_vasti')}}

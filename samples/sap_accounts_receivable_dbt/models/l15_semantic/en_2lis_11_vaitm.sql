SELECT
{{ sap_translate_cols(ref('ods_2lis_11_vaitm')) |indent(4, True) }}
from {{ref('ods_2lis_11_vaitm')}}

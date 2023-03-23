SELECT
{{ sap_translate_cols(ref('ods_2lis_13_vdkon')) |indent(4, True) }}
from {{ref('ods_2lis_13_vdkon')}}

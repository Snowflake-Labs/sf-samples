SELECT
{{ sap_translate_cols(ref('ods_2lis_12_vcitm')) |indent(4, True) }}
from {{ref('ods_2lis_12_vcitm')}}

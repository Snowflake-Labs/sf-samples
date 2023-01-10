SELECT
{{ sap_translate_cols(ref('ods_2lis_13_vdhdr')) |indent(4, True) }}
from {{ref('ods_2lis_13_vdhdr')}}

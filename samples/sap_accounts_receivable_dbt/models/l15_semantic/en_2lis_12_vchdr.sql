SELECT
{{ sap_translate_cols(ref('ods_2lis_12_vchdr')) |indent(4, True) }}
from {{ref('ods_2lis_12_vchdr')}}

SELECT
{{ sap_translate_cols(ref('ods_0material_text')) |indent(4, True) }}
from {{ref('ods_0material_text')}}

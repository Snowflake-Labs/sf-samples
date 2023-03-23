SELECT
{{ sap_translate_cols(ref('ods_0material_attr')) | indent(4, True) }}
from {{ref('ods_0material_attr')}}

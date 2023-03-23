SELECT
{{ sap_translate_cols(ref('ods_0matl_cat_text')) |indent(4, True) }}
from {{ref('ods_0matl_cat_text')}}

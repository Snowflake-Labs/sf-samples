SELECT
{{ sap_translate_cols(ref('ods_0sales_off_text')) |indent(4, True) }}
from {{ref('ods_0sales_off_text')}}

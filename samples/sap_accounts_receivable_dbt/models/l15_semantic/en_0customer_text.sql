SELECT
{{ sap_translate_cols(ref('ods_0customer_text')) |indent(8, True) }}
from {{ref('ods_0customer_text')}}

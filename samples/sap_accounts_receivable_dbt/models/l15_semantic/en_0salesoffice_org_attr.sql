SELECT
{{ sap_translate_cols(ref('ods_0salesoffice_org_attr')) |indent(4, True) }}
from {{ref('ods_0salesoffice_org_attr')}}

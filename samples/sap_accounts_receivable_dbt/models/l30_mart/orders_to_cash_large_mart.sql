select 
  {{ dbt_utils.star(ref('accounts_receivable'), relation_alias='ar', except=[
    'DATA_SOURCE_NAME'
  ]) }},
  {{ dbt_utils.star(ref('sales_document_order_delivery'), relation_alias='sdod', prefix='sdod_', except=[
    'DATA_SOURCE_NAME'
  ]) }},
  {{ dbt_utils.star(ref('sales_document_item_data'), relation_alias='sdid', prefix='sdid_', except=[
    'DATA_SOURCE_NAME'
  ]) }},
  {{ dbt_utils.star(ref('sales_order_delivery'), relation_alias='sod', prefix='sod_', except=[
    'DATA_SOURCE_NAME'
  ]) }}
from 
  {{ ref('accounts_receivable') }} ar
  left join {{ ref('sales_document_order_delivery') }} sdod on ar.assignment = sdod.vbeln_vl
  left join {{ ref('sales_document_item_data')}} sdid on sdod.billing_document = sdid.billing_document
  left join {{ ref('sales_order_delivery') }} sod on sdod.billing_document = sod.billing_document

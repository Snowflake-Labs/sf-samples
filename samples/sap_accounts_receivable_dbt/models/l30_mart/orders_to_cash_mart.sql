select 
  {{ dbt_utils.star(ref('accounts_receivable'), relation_alias='ar', except=[
    'DATA_SOURCE_NAME'
  ]) }},
  {{ dbt_utils.star(ref('sales_document_item_data'), relation_alias='sdid', prefix='sdid_', except=[
    'DATA_SOURCE_NAME'
  ]) }}
from 
  {{ ref('accounts_receivable') }} ar
  left join {{ ref('sales_document_item_data')}} sdid on ar.assignment = sdid.billing_document
  -- left join {{ ref('sales_document_order_delivery') }} sdod on  = sdod.vbeln_vl
  -- left join {{ ref('sales_order_delivery') }} sod on sdid.billing_document = sod.billing_document and sdid.posnr = sod.posnr

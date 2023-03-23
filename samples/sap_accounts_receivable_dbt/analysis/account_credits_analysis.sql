select 
  SUM(credit_amt_doc_currency) as AR 
from {{ ref('accounts_receivable_mart') }}

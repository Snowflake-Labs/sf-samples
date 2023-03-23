--accounts receivable mart for reporting
select
    company_code
    , customer_name
    , country
    , postal_code
    , region
    , street
    , telephone_1 as telephone
    , document_number
    , document_type
    , line_item
    , fiscper
    , document_date
    , posting_date
    , entry_date
    , clearing_date
    , last_dunned
    , baseline_payment_dte
    , days_1
    , days_2
    , local_currency
    , debit_amt_local_currency
    , credit_amt_local_currency
    , amt_local_currency_w_signs
    , cash_discount_amount
    , gl_account
    , document_currency
    , debit_amt_doc_currency
    , credit_amt_doc_currency
    , foreign_currency_amt
    , fcurr as from_currency
    , tcurr as target_currency
    , exchange_rate
    , assignment
    , DATEDIFF(days, POSTING_DATE, CLEARING_DATE) as DSO
from {{ref('accounts_receivable')}}

with 
fi as (
    select * from {{ref('en_0fi_ar_4')}}
),
fx as (
    select * from {{ref('currency_0fi_ar_4')}}
),
ca as (
    select * from {{ref('en_0customer_attr')}}
)

--en_0fi_ar_4
select
    fi.DATA_SOURCE_NAME
    , fi.COMPANY_CODE
    , fi.FISCPER
    , fi.DOCUMENT_NUMBER
    , fi.LINE_ITEM
    , fi.UPOSZ as sub_item_number
    , fi.STATUSPS
    , fi.CUSTOMER
    , fi.CREDIT_CONTROL_AREA
    --, fi.DUNNING_AREA
    , fi.ACCOUNT_TYPE
    , fi.SPECIAL_GL_IND
    , fi.DOCUMENT_TYPE
    , fi.POSTING_KEY
    , fi.FISCVAR
    , fi.DOCUMENT_DATE
    , fi.POSTING_DATE
    , fi.ENTRY_DATE
    , fi.CLEARING_DATE
    , fi.LAST_DUNNED
    , fi.NETDT
    , fi.SK1DT
    , fi.SK2DT
    , fi.BASELINE_PAYMENT_DTE
    , fi.DAYS_1
    , fi.DAYS_2
    , fi.DAYS_NET
    , fi.DISCOUNT_PERCENT_1
    , fi.DISCOUNT_PERCENT_2
    , fi.COUNTRY
    , fi.PAYMENT_METHOD
    , fi.TERMS_OF_PAYMENT
    , fi.PAYMENT_BLOCK
    , fi.REASON_CODE
    , fi.DUNNING_BLOCK
    , fi.DUNNING_KEY
    , fi.DUNNING_LEVEL
    , fi.LCURR as local_currency
    , fi.DMSOL as debit_amt_local_currency
    , fi.DMHAB as credit_amt_local_currency
    , fi.DMSHB as amt_local_currency_w_signs
    , fi.CASH_DISCOUNT_AMOUNT
    , fi.CURRENCY as document_currency
    , fi.WRSOL as debit_amt_doc_currency
    , fi.WRHAB as credit_amt_doc_currency
    , fi.WRSHB as foreign_currency_amt
    , fi.DISCOUNT_BASE
    , fi.DISCOUNT_AMOUNT
    , fi.CHART_OF_ACCOUNTS
    , fi.GL_ACCOUNT
    , fi.GL_ACCOUNT_FROM_SUBLEDGER
    --, fi.BRANCH_ACCOUNT
    , fi.CLEARING_DOCUMENT
    , fi.VENDOR_INVOICE_NO
    , fi.INVOICE_REFERENCE
    , fi.FISCAL_YEAR_OF_RELATED_INVOICE
    , fi.LINE_ITEM_IN_RELATED_INVOICE
    , fi.BILLING_DOCUMENT
    , fi.REFERENCE_KEY_1
    --, fi.REFERENCE_KEY_2
    --, fi.REFERENCE_KEY_3
    , fi.SGTEXT
    --, fi.NEGATIVE_POSTING
    --, fi.ARCHIVED
    , fi.SP_GL_TRANS_TYPE
    , fi.UPDMOD
    , fi.ASSIGNMENT
    , fi.REFERENCE_TRANSACTION
    , fi.REFERENCE_KEY
    --, fi.DOC_STATUS
    --, fi.AMOUNT_IN_USD
    --, fi.LC3_AMOUNT
    , fi.FISCAL_YEAR
    , fi.LOCAL_CURRENCY_2
    , fi.LOCAL_CURRENCY_3
    , fi.POSTING_PERIOD
    , fi.WBS_ELEMENT
    , fi.DEBIT_CREDIT_IND
    --, fi.AMOUNT_IN_DOCUMENT_CURRENCY
    , fx.tfiscper as tfiscper
    , fx.tfact
    , fx.ffact
    , fx.fcurr fcurr
    , fx.tcurr tcurr
    , case when tfact > 1 
            then 
                UKURS * ffact 
            else UKURS 
        end exchange_rate
    ,ca.ADDRESS
    ,ca.CITY
    ,ca.CLIENT
    --,ca.COUNTRY
    ,ca.CREATED_BY as customer_created_by
    ,ca.CREATED_ON as customer_created_on
    ,ca.CURRENCY_OF_SALES
    --,ca.CUSTOMER
    ,ca.INDUSTRY
    ,ca.LANGUAGE_KEY
    ,ca.LEGAL_STATUS
    ,ca.NATURAL_PERSON
    ,ca.POSTAL_CODE
    ,ca.REGION
    ,ca.STREET
    ,ca.TELEPHONE_1
    ,ca.TAX_JURISDICTION
    ,ca.EMPLOYEE
    ,ca.NAME as customer_name

--, emp.user_name as employee_name
from fi
    left outer join ca on fi.customer= ca.customer
    left outer join fx on 
        fi.fiscper=fx.tfiscper and
        fi.lcurr=fx.tcurr and
        fi.currency=fx.fcurr

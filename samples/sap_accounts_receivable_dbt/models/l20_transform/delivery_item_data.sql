--en_2lis_12_vcitm

SELECT main.*

,ca.ADDRESS
,ca.CITY
,ca.CLIENT
,ca.COUNTRY
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
,ca.NAME


FROM {{ref('en_2lis_12_vcitm')}} main
left outer join {{ref('en_0customer_attr')}}  ca on main.customer= ca.customer

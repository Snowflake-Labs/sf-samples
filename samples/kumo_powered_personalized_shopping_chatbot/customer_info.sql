CREATE OR REPLACE TABLE KUMO_CHATBOT_NAISHA.PUBLIC.CUSTOMER_FULL_INFO AS
SELECT
    cust.CUSTOMER_ID,
    cust.FN,
    cust.ACTIVE,
    cust.CLUB_MEMBER_STATUS,
    cust.FASHION_NEWS_FREQUENCY,
    cust.AGE,
    cust.POSTAL_CODE,
    (
        'Customer with ID ' || cust.CUSTOMER_ID || 
        CASE 
            WHEN cust.ACTIVE = 1 THEN ' is active'
            ELSE ' is not active'
        END ||
        ', holds a ' || COALESCE(cust.CLUB_MEMBER_STATUS, 'non-member') || 
        ' status, and receives fashion news ' || COALESCE(cust.FASHION_NEWS_FREQUENCY, 'never') || 
        '. They are ' || COALESCE(TO_CHAR(cust.AGE), 'of unknown age') || 
        ' years old and live in the postal code area ' || COALESCE(cust.POSTAL_CODE, 'unknown') || 
        '.'
    ) AS customer_full_text
FROM
    KUMO_CHATBOT_NAISHA.PUBLIC.CUSTOMERS cust;
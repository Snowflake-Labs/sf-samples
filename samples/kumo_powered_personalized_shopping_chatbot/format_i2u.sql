CREATE OR REPLACE TABLE KUMO_CHATBOT_NAISHA.PUBLIC.ITEM_TO_USER_PREDICTIONS AS
WITH ranked_predictions AS (
    SELECT
        customer_id,
        item_id,
        prediction_score,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY prediction_score DESC) AS rank
    FROM
        KUMO_DB.HNM.ITEM_TO_USER_PREDICTIONS
),
formatted_predictions AS (
    SELECT
        customer_id,
        STRING_AGG(
            'The ' || 
            CASE rank
                WHEN 1 THEN '1st'
                WHEN 2 THEN '2nd'
                WHEN 3 THEN '3rd'
                ELSE rank || 'th'
            END || 
            ' prediction for customer_id ' || customer_id || 
            ' is item_id ' || item_id || 
            ' with score ' || prediction_score || '.', ' '
        ) AS formatted_predictions
    FROM
        ranked_predictions
    GROUP BY
        customer_id
)
SELECT
    customer_id,
    formatted_predictions
FROM
    formatted_predictions;

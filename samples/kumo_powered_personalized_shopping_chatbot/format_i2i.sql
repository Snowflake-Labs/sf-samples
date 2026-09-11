CREATE OR REPLACE TABLE KUMO_CHATBOT_NAISHA.PUBLIC.ITEM_TO_ITEM_PREDICTIONS AS
WITH ranked_predictions AS (
    SELECT
        source_item_id AS item_id,
        target_item_id,
        prediction_score,
        ROW_NUMBER() OVER (PARTITION BY source_item_id ORDER BY prediction_score DESC) AS rank
    FROM
        KUMO_CHATBOT_NAISHA.PUBLIC.ITEM_PREDICTIONS
),
formatted_predictions AS (
    SELECT
        item_id,
        STRING_AGG(
            'The ' || 
            CASE rank
                WHEN 1 THEN '1st'
                WHEN 2 THEN '2nd'
                WHEN 3 THEN '3rd'
                ELSE rank || 'th'
            END || 
            ' prediction for item_id ' || item_id || 
            ' is item_id ' || target_item_id || 
            ' with score ' || prediction_score || '.', ' '
        ) AS formatted_predictions
    FROM
        ranked_predictions
    GROUP BY
        item_id
)
SELECT
    item_id,
    formatted_predictions
FROM
    formatted_predictions;

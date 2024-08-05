USE ROLE ACCOUNTADMIN;
USE DATABASE LLM_DEMO;
USE SCHEMA SUMMIT;
USE WAREHOUSE LLM_DEMO;
CREATE OR REPLACE TABLE PERFORMANCE_MARKETING
(
PLATFORM VARCHAR,
CAMPAIGN_ID VARCHAR,
CAMPAIGN_NAME VARCHAR,
IMPRESSIONS INT,
CPM DECIMAL(10,2),
COST INT,
INSTALLS INT,
COST_PER_CLICK DECIMAL(10,2),
COST_PER_ACQUISITION DECIMAL(10,2)
);

INSERT INTO PERFORMANCE_MARKETING(platform, campaign_id, campaign_name, impressions, cost_per_click, cpm, installs)
    SELECT
        'Meta',
        UUID_STRING(),
        CONCAT('Customer Campaign' || '-' || UNIFORM(500,200000,random())),
        UNIFORM(100000,300000,random()),
        UNIFORM(10,80,random())/100,
        UNIFORM(1500,6000,random())/100,
        UNIFORM(30,160,random())
FROM
    TABLE(GENERATOR(ROWCOUNT => 500));

INSERT INTO PERFORMANCE_MARKETING(platform, campaign_id, campaign_name, impressions, cost_per_click, cpm, installs)
    SELECT
        'Google',
        UUID_STRING(),
        CONCAT('Customer Campaign' || '-' || UNIFORM(500,200000,random())),
        UNIFORM(40000,160000,random()),
        UNIFORM(12,85,random())/100,
        UNIFORM(1500,6000,random())/100,
        UNIFORM(10,70,random())
FROM
    TABLE(GENERATOR(ROWCOUNT => 400));

INSERT INTO PERFORMANCE_MARKETING(platform, campaign_id, campaign_name, impressions, cost_per_click, cpm, installs)
    SELECT
        'TikTok',
        UUID_STRING(),
        CONCAT('Customer Campaign' || '-' || UNIFORM(500,200000,random())),
        UNIFORM(20000,140000,random()),
        UNIFORM(15,95,random())/100,
        UNIFORM(1500,8000,random())/100,
        UNIFORM(10,120,random())
FROM
    TABLE(GENERATOR(ROWCOUNT => 300));

INSERT INTO PERFORMANCE_MARKETING(platform, campaign_id, campaign_name, impressions, cost_per_click, cpm, installs)
    SELECT
        'Snap',
        UUID_STRING(),
        CONCAT('Customer Campaign' || '-' || UNIFORM(500,200000,random())),
        UNIFORM(20000,120000,random()),
        UNIFORM(15,76,random())/100,
        UNIFORM(1500,7500,random())/100,
        UNIFORM(20,110,random())
FROM
    TABLE(GENERATOR(ROWCOUNT => 250));

UPDATE PERFORMANCE_MARKETING
SET COST = CASE 
              WHEN CPM = 0 THEN NULL
              ELSE IMPRESSIONS / 1000 * CPM
           END;

UPDATE PERFORMANCE_MARKETING
SET COST_PER_ACQUISITION = CASE 
              WHEN INSTALLS = 0 THEN NULL
              ELSE COST / INSTALLS
           END;

SELECT * FROM PERFORMANCE_MARKETING SAMPLE (1);

SELECT SUM(COST), PLATFORM FROM PERFORMANCE_MARKETING GROUP BY ALL;

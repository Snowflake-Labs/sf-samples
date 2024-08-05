USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LLM_DEMO;
USE DATABASE LLM_DEMO;
USE SCHEMA SUMMIT;
CREATE OR REPLACE TABLE ENGAGEMENT
(
	UNIQUE_ID VARCHAR,
    TIME TIMESTAMP_NTZ,
    SERIES_ID VARCHAR,
    EPISODE_ID VARCHAR,
    SERIES_TITLE VARCHAR,
	EPISODE_TITLE VARCHAR,
	EPISODE_NUMBER INT,
	SEASON INT,
    GENRE VARCHAR,
    MINUTES_WATCHED INT,
    COUNTRY VARCHAR
);

SELECT * FROM ENGAGEMENT;

CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-31536000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;



CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50_1()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-30000000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;

CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50_2()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-25000000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;

CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50_3()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-20000000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;

CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50_4()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-15000000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;



CREATE OR REPLACE PROCEDURE REPEAT_INSERT_50_5()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    for (var i = 0; i < 50; i++) {
        // Create and populate the temporary table at the start of each iteration
        var createTempTable = `
            CREATE OR REPLACE TEMPORARY TABLE RandomUniqueIDs AS
            SELECT UNIQUE_ID, COUNTRY
            FROM SUBSCRIBER
            ORDER BY RANDOM()
            LIMIT 10000;  // This limit should match the expected number of insertions per iteration
        `;

        // Execute the query to create/populate the temporary table
        try {
            var tempStmt = snowflake.createStatement({sqlText: createTempTable});
            tempStmt.execute();
        } catch (err) {
            return "Failed to create/populate RandomUniqueIDs on iteration " + i + ": " + err.message;
        }

        // Insert data using the freshly populated temporary table
        var insertQuery = `
            INSERT INTO ENGAGEMENT (
                UNIQUE_ID,
                TIME,
                EPISODE_ID, 
                SERIES_ID,
                SERIES_TITLE, 
                EPISODE_TITLE, 
                EPISODE_NUMBER, 
                SEASON, 
                GENRE, 
                MINUTES_WATCHED, 
                COUNTRY
            )
            SELECT
                r.UNIQUE_ID,
                DATEADD(SECOND, UNIFORM(-10000000, -1, RANDOM()), CURRENT_TIMESTAMP()),
                m.EPISODE_ID,
                m.SERIES_ID,
                m.SERIES_TITLE,
                m.EPISODE_TITLE,
                CAST(m.EPISODE_NUMBER AS INT),
                CAST(m.SEASON AS INT),
                m.GENRE,
                UNIFORM(0, 22, RANDOM()),
                r.COUNTRY
            FROM (
                SELECT EPISODE_ID, SERIES_ID, SERIES_TITLE, EPISODE_TITLE, EPISODE_NUMBER, SEASON, GENRE,
                       ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
                FROM METADATA
                LIMIT 10000
            ) m
            JOIN (
                SELECT UNIQUE_ID, COUNTRY,
                       ROW_NUMBER() OVER (ORDER BY NULL) AS rn  // Just to keep an ordering
                FROM RandomUniqueIDs
            ) r
            ON m.rn = r.rn;
        `;

        // Execute the insert query
        try {
            var stmt = snowflake.createStatement({sqlText: insertQuery});
            stmt.execute();
        } catch (err) {
            return "Failed to execute insertion on iteration " + i + ": " + err.message;
        }
    }
    return "Completed 50 insertions successfully.";
$$;

CALL REPEAT_INSERT_50();
CALL REPEAT_INSERT_50_1();
CALL REPEAT_INSERT_50_2();
CALL REPEAT_INSERT_50_3();
CALL REPEAT_INSERT_50_4();
CALL REPEAT_INSERT_50_5();

WITH __engagement AS (
  SELECT
    time,
    minutes_watched
  FROM llm_demo.summit.engagement
)
SELECT
  DATE_TRUNC('MONTH', time) AS month,
  SUM(minutes_watched) AS total_minutes_watched
FROM __engagement
GROUP BY
  month
ORDER BY
  month DESC;

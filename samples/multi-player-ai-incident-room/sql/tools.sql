-- Multi-player AI Incident Room agent tools. Owner's rights; owned by INCIDENT_ROOM_OPS, which can read the
-- demo pipeline and create objects ONLY in INCIDENT_ROOM_CLONES. Run after setup.sql.
--   snow sql --connection <name> -f sql/tools.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INCIDENT_ROOM_WH;

-- @triage: recent failed task runs in the watched schema.
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.RECENT_FAILURES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  res VARIANT;
BEGIN
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
           'task', DATABASE_NAME || '.' || SCHEMA_NAME || '.' || NAME,
           'state', STATE,
           'scheduled_time', SCHEDULED_TIME::STRING,
           'error_code', ERROR_CODE,
           'error_message', ERROR_MESSAGE,
           'query_id', QUERY_ID))
         WITHIN GROUP (ORDER BY SCHEDULED_TIME DESC)
    INTO :res
  FROM (
    SELECT * FROM TABLE(INCIDENT_ROOM_DEMO.INFORMATION_SCHEMA.TASK_HISTORY(
      SCHEDULED_TIME_RANGE_START => DATEADD(day, -7, CURRENT_TIMESTAMP()), RESULT_LIMIT => 50))
    WHERE STATE IN ('FAILED', 'SUCCEEDED') AND SCHEMA_NAME = 'SALES'
    ORDER BY SCHEDULED_TIME DESC LIMIT 10
  );
  RETURN COALESCE(:res, ARRAY_CONSTRUCT());
END;
$$;

-- @triage / @fixer: the task's current SQL, plus the columns of every table in its schema.
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  cols VARIANT;
  task_def VARCHAR;
BEGIN
  SELECT OBJECT_AGG(TABLE_NAME, COLS) INTO :cols FROM (
    SELECT TABLE_NAME, ARRAY_AGG(COLUMN_NAME || ' ' || DATA_TYPE) WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS COLS
    FROM INCIDENT_ROOM_DEMO.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'SALES' GROUP BY TABLE_NAME);
  SHOW TASKS LIKE 'LOAD_DAILY_REVENUE' IN SCHEMA INCIDENT_ROOM_DEMO.SALES;
  SELECT "definition" INTO :task_def FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  RETURN OBJECT_CONSTRUCT(
    'task', 'INCIDENT_ROOM_DEMO.SALES.LOAD_DAILY_REVENUE',
    'task_sql', :task_def,
    'columns', :cols);
END;
$$;

-- @triage: blast radius from Snowflake lineage (downstream of a table in the watched schema).
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.DOWNSTREAM(TABLE_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  res VARIANT;
  fqn VARCHAR DEFAULT 'INCIDENT_ROOM_DEMO.SALES.' || UPPER(REGEXP_REPLACE(:TABLE_NAME, '^.*\\.', ''));
BEGIN
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
           'object', TARGET_OBJECT_DATABASE || '.' || TARGET_OBJECT_SCHEMA || '.' || TARGET_OBJECT_NAME,
           'kind', TARGET_OBJECT_DOMAIN, 'hops', DISTANCE))
         WITHIN GROUP (ORDER BY DISTANCE)
    INTO :res
  FROM TABLE(SNOWFLAKE.CORE.GET_LINEAGE(:fqn, 'TABLE', 'DOWNSTREAM', 5));
  RETURN COALESCE(:res, ARRAY_CONSTRUCT());
END;
$$;

-- @fixer: prove a replacement task body on zero-copy clones. Never touches prod.
-- Clones every table in INCIDENT_ROOM_DEMO.SALES into INCIDENT_ROOM_CLONES.<incident>, runs the
-- rewritten INSERT there, and compares the result to prod's last good load.
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST(INCIDENT_ID VARCHAR, TASK_SQL VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import json
import re
import uuid

SCHEMA = "INCIDENT_ROOM_DEMO.SALES"
TASK = "INCIDENT_ROOM_DEMO.SALES.LOAD_DAILY_REVENUE"
TARGET = "DAILY_REVENUE"
DOTTED = re.compile(r"\b[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)+\b", re.I)


def check(sql):
    body = sql.strip().rstrip(";").strip()
    if not re.match(r"^INSERT\s", body, re.I):
        return None, "A task fix must be a single INSERT statement"
    if ";" in body:
        return None, "Only one statement is allowed"
    if '"' in body:
        return None, "Quoted identifiers aren't allowed in a task fix"
    for m in DOTTED.finditer(body):
        parts = m.group(0).upper().split(".")
        if len(parts) != 3 or ".".join(parts[:2]) != SCHEMA:
            return None, "References %s, outside %s" % (m.group(0), SCHEMA)
    return body, None


def one(session, sql, params=None):
    r = session.sql(sql, params=params).collect()
    return r[0].as_dict() if r else {}


def run(session, incident_id, task_sql):
    if not re.match(r"^[A-Za-z0-9-]{1,64}$", incident_id or ""):
        return {"ok": False, "error": "Invalid incident id"}
    body, err = check(task_sql or "")
    patch_id = "P-" + uuid.uuid4().hex[:8].upper()
    clone = "INCIDENT_ROOM_CLONES." + incident_id.replace("-", "_").upper()
    result = {"patch_id": patch_id, "clone_schema": clone, "task": TASK}
    if err:
        result.update(ok=False, error=err)
    else:
        try:
            session.sql("CREATE OR REPLACE SCHEMA %s" % clone).collect()
            tables = [r["TABLE_NAME"] for r in session.sql(
                "SELECT TABLE_NAME FROM INCIDENT_ROOM_DEMO.INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = 'SALES' AND TABLE_TYPE = 'BASE TABLE'").collect()]
            for t in tables:
                session.sql("CREATE TABLE %s.%s CLONE %s.%s" % (clone, t, SCHEMA, t)).collect()
            rewritten = re.sub(r"\bINCIDENT_ROOM_DEMO\.SALES\.", clone + ".", body, flags=re.I)
            session.sql(rewritten).collect()
            got = one(session, "SELECT COUNT(*) AS ROWS_, SUM(REVENUE) AS REVENUE, "
                               "COUNT_IF(REVENUE IS NULL) AS NULLS FROM %s.%s" % (clone, TARGET))
            prod = one(session, "SELECT COUNT(*) AS ROWS_, SUM(REVENUE) AS REVENUE FROM %s.%s" % (SCHEMA, TARGET))
            got_rev = float(got.get("REVENUE") or 0)
            prod_rev = float(prod.get("REVENUE") or 0)
            matches = (got.get("ROWS_") == prod.get("ROWS_") and abs(got_rev - prod_rev) < 0.005
                       and got.get("NULLS") == 0 and got.get("ROWS_", 0) > 0)
            result.update(ok=bool(matches), clone_rows=got.get("ROWS_"), clone_revenue=got_rev,
                          clone_null_revenue=got.get("NULLS"), prod_last_good_rows=prod.get("ROWS_"),
                          prod_last_good_revenue=prod_rev, tables_cloned=tables,
                          verdict="Clone output matches prod's last good load" if matches
                          else "Clone output differs from prod's last good load")
        except Exception as e:
            result.update(ok=False, error=str(e)[:800])
    session.sql(
        "INSERT INTO INCIDENT_ROOM.APP.PATCHES (ID, INCIDENT_ID, TASK_FQN, TASK_SQL, CLONE_SCHEMA, TESTED_OK, TEST_RESULT) "
        "SELECT ?, ?, ?, ?, ?, ?, PARSE_JSON(?)",
        params=[patch_id, incident_id, TASK, body or (task_sql or "")[:4000], clone,
                bool(result.get("ok")), json.dumps(result)],
    ).collect()
    return result
$$;

-- @scribe: the room log, oldest first (human and agent messages, tool calls, votes).
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.ROOM_LOG(INCIDENT_ID VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  res VARIANT;
BEGIN
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT('at', AT::STRING, 'by', BY_WHO, 'kind', KIND, 'payload', PAYLOAD))
         WITHIN GROUP (ORDER BY AT)
    INTO :res FROM INCIDENT_ROOM.APP.EVENTS WHERE INCIDENT_ID = :INCIDENT_ID AND KIND IN ('message', 'agent.say', 'decision', 'vote', 'andon', 'ship', 'patch');
  RETURN COALESCE(:res, ARRAY_CONSTRUCT());
END;
$$;

-- @scribe: file the postmortem (markdown).
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.FILE_POSTMORTEM(INCIDENT_ID VARCHAR, BODY VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  INSERT INTO INCIDENT_ROOM.APP.POSTMORTEMS (INCIDENT_ID, BODY) VALUES (:INCIDENT_ID, :BODY);
  RETURN 'Filed postmortem for ' || :INCIDENT_ID;
END;
$$;

-- @triage and @fixer: what earlier incidents taught the team (filed decisions and
-- postmortems), newest first, so a repeat failure starts from the last fix.
CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.PAST_INCIDENTS(INCIDENT_ID VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  res VARIANT;
BEGIN
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT('incident_id', INC, 'title', TITLE, 'kind', KIND, 'by', BY_WHO, 'at', AT::STRING, 'text', BODY))
         WITHIN GROUP (ORDER BY AT DESC)
    INTO :res
    FROM (
      SELECT d.INCIDENT_ID AS INC, i.TITLE, 'decision' AS KIND, d.FILED_BY AS BY_WHO, d.FILED_AT AS AT, d.BODY
        FROM INCIDENT_ROOM.APP.DECISIONS d LEFT JOIN INCIDENT_ROOM.APP.INCIDENTS i ON i.ID = d.INCIDENT_ID
       WHERE d.INCIDENT_ID <> :INCIDENT_ID
      UNION ALL
      SELECT p.INCIDENT_ID, i.TITLE, 'postmortem', 'scribe', p.WRITTEN_AT, LEFT(p.BODY, 4000)
        FROM INCIDENT_ROOM.APP.POSTMORTEMS p LEFT JOIN INCIDENT_ROOM.APP.INCIDENTS i ON i.ID = p.INCIDENT_ID
       WHERE p.INCIDENT_ID <> :INCIDENT_ID
      ORDER BY AT DESC
      LIMIT 20
    );
  RETURN COALESCE(:res, ARRAY_CONSTRUCT());
END;
$$;

-- Hand the tools to INCIDENT_ROOM_OPS so they run with its (narrow) privileges, then let the agent role call them.
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.RECENT_FAILURES() TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE() TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.DOWNSTREAM(VARCHAR) TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST(VARCHAR, VARCHAR) TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.ROOM_LOG(VARCHAR) TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.FILE_POSTMORTEM(VARCHAR, VARCHAR) TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;
GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.PAST_INCIDENTS(VARCHAR) TO ROLE INCIDENT_ROOM_OPS REVOKE CURRENT GRANTS;

GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.RECENT_FAILURES() TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE() TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.DOWNSTREAM(VARCHAR) TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST(VARCHAR, VARCHAR) TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.ROOM_LOG(VARCHAR) TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.FILE_POSTMORTEM(VARCHAR, VARCHAR) TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON PROCEDURE INCIDENT_ROOM.AGENTS.PAST_INCIDENTS(VARCHAR) TO ROLE INCIDENT_ROOM_AGENT;

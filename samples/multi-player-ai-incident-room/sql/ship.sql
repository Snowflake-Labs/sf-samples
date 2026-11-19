-- Multi-player AI Incident Room: the only path to prod. Owned by INCIDENT_ROOM_SHIPPER (owner of the prod task).
-- No agent role has USAGE on this procedure; the app calls it after a vote, and it
-- re-checks everything here so the database, not the app, is the final gate.
--   snow sql --connection <name> -f sql/ship.sql

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INCIDENT_ROOM_WH;

CREATE OR REPLACE PROCEDURE INCIDENT_ROOM.AGENTS.SHIP_FIX(PATCH_ID VARCHAR, SHIPPED_BY VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS
$$
import re

SCHEMA = "INCIDENT_ROOM_DEMO.SALES"
TASK = "INCIDENT_ROOM_DEMO.SALES.LOAD_DAILY_REVENUE"
AGENTS = {"TRIAGE", "FIXER", "SCRIBE"}
# Optional: only users holding this role (directly or through other roles) count as approvers
# and can ship. Leave empty to allow any human. Match IR_APPROVER_ROLE in app.yml for fast feedback.
APPROVER_ROLE = ""
DOTTED = re.compile(r"\b[A-Z_][A-Z0-9_$]*(?:\.[A-Z_][A-Z0-9_$]*)+\b", re.I)


def refuse(reason):
    return {"ok": False, "error": reason}


def role_holders(session, role):
    # Users who hold the role, following role-to-role grants.
    users, seen, todo = set(), set(), [role.upper()]
    while todo:
        r = todo.pop()
        if r in seen:
            continue
        seen.add(r)
        for g in session.sql("SHOW GRANTS OF ROLE IDENTIFIER(?)", params=[r]).collect():
            (users.add if g["granted_to"] == "USER" else todo.append)(g["grantee_name"].upper())
    return users


def run(session, patch_id, shipped_by):
    rows = session.sql("SELECT p.INCIDENT_ID, p.TASK_FQN, p.TASK_SQL, p.TESTED_OK, p.SHIPPED_AT, "
                       "i.STATUS, i.FROZEN, i.REQUIRED_APPROVALS "
                       "FROM INCIDENT_ROOM.APP.PATCHES p JOIN INCIDENT_ROOM.APP.INCIDENTS i ON i.ID = p.INCIDENT_ID "
                       "WHERE p.ID = ?", params=[patch_id]).collect()
    if not rows:
        return refuse("Unknown patch")
    p = rows[0].as_dict()
    if p["SHIPPED_AT"] is not None:
        return refuse("Already shipped")
    if not p["TESTED_OK"]:
        return refuse("This patch hasn't passed on the clone")
    if p["FROZEN"]:
        return refuse("Line stopped (andon)")
    if p["STATUS"] != "VOTING":
        return refuse("Nothing is up for a vote")
    if p["TASK_FQN"] != TASK:
        return refuse("Patch targets an unexpected task")

    # Latest vote per human for this patch.
    votes = session.sql("SELECT UPPER(VOTER) AS VOTER, VOTE FROM INCIDENT_ROOM.APP.VOTES WHERE PATCH_ID = ? "
                        "QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(VOTER) ORDER BY AT DESC) = 1",
                        params=[patch_id]).collect()
    humans = [v for v in votes if v["VOTER"] not in AGENTS]
    rejected = [v["VOTER"] for v in humans if v["VOTE"] == "reject"]
    approved = [v["VOTER"] for v in humans if v["VOTE"] == "approve"]
    if APPROVER_ROLE:
        holders = role_holders(session, APPROVER_ROLE)
        approved = [u for u in approved if u in holders]
    if rejected:
        return refuse("Rejected by " + ", ".join(rejected))
    need = max(2, int(p["REQUIRED_APPROVALS"] or 2))  # never fewer than two people
    if len(approved) < need:
        return refuse("%d of %d approvals" % (len(approved), need))
    if (shipped_by or "").upper() not in approved:
        return refuse("Only someone who approved can ship" + (" (and holds %s)" % APPROVER_ROLE if APPROVER_ROLE else ""))

    body = (p["TASK_SQL"] or "").strip().rstrip(";").strip()
    if not re.match(r"^INSERT\s", body, re.I) or ";" in body or '"' in body:
        return refuse("Patch SQL failed the guard")
    for m in DOTTED.finditer(body):
        parts = m.group(0).upper().split(".")
        if len(parts) != 3 or ".".join(parts[:2]) != SCHEMA:
            return refuse("Patch SQL references " + m.group(0))

    session.sql("ALTER TASK %s MODIFY AS %s" % (TASK, body)).collect()
    session.sql("EXECUTE TASK %s" % TASK).collect()
    session.sql("UPDATE INCIDENT_ROOM.APP.PATCHES SET SHIPPED_AT = CURRENT_TIMESTAMP(), SHIPPED_BY = ? WHERE ID = ?",
                params=[shipped_by.upper(), patch_id]).collect()
    return {"ok": True, "task": TASK, "approved_by": approved, "shipped_by": shipped_by.upper(),
            "note": "Task updated and a run was started."}
$$;

GRANT OWNERSHIP ON PROCEDURE INCIDENT_ROOM.AGENTS.SHIP_FIX(VARCHAR, VARCHAR) TO ROLE INCIDENT_ROOM_SHIPPER REVOKE CURRENT GRANTS;
-- The app's service (owned by ACCOUNTADMIN) calls this; ACCOUNTADMIN inherits INCIDENT_ROOM_SHIPPER.
-- Deliberately NOT granted to INCIDENT_ROOM_AGENT.

-- Multi-player AI Incident Room agents. Each has only its own tools; none can reach SHIP_FIX.
--   snow sql --connection <name> -f sql/agents.sql   (after setup.sql and sql/tools.sql)
-- Every user message starts with "[incident: <id>]"; agents pass that id to tools that need it.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE AGENT INCIDENT_ROOM.AGENTS.TRIAGE
  COMMENT = 'Multi-player AI Incident Room: finds what broke, when, and the blast radius. Read-only.'
  PROFILE = '{"display_name": "Triage", "color": "orange"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto
  orchestration:
    budget:
      seconds: 90
      tokens: 32000
  instructions:
    response: "You are @triage, one of three agents (@triage, @fixer, @scribe) in a live incident room with a team of humans. Be brief and factual: what failed, when, the exact error, the likely cause, and what downstream objects are affected. Use short bullets. Never propose or apply a fix yourself; hand off to @fixer by saying what needs fixing. Never claim something is fixed."
    orchestration: "The first line of every message is [incident: <id>]. To see what failed, call recent_failures. To see the task's SQL and current table columns, call inspect_pipeline; compare the columns the task SQL uses with the columns that exist now. For blast radius, call downstream with the table the task writes. Call past_incidents with the incident id; if an earlier incident or filed decision matches this failure, say so in one line, with its incident id and what fixed it then. Always run recent_failures and inspect_pipeline before concluding."
  tools:
    - tool_spec:
        type: "generic"
        name: "recent_failures"
        description: "Recent runs (failed and succeeded) of tasks in the watched pipeline, newest first, with error messages."
        input_schema: {type: "object", properties: {}}
    - tool_spec:
        type: "generic"
        name: "inspect_pipeline"
        description: "The pipeline task's current SQL and the columns of every table in its schema."
        input_schema: {type: "object", properties: {}}
    - tool_spec:
        type: "generic"
        name: "downstream"
        description: "Objects downstream of a table (blast radius), from Snowflake lineage, with hop counts."
        input_schema:
          type: "object"
          properties:
            table_name: {type: "string", description: "Table name, e.g. DAILY_REVENUE"}
          required: ["table_name"]
    - tool_spec:
        type: "generic"
        name: "past_incidents"
        description: "What the team learned in earlier incidents: decisions filed with /file and postmortems, newest first. Excludes the current incident."
        input_schema:
          type: "object"
          properties:
            incident_id: {type: "string", description: "The id from the [incident: ...] line"}
          required: ["incident_id"]
  tool_resources:
    recent_failures:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.RECENT_FAILURES"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    inspect_pipeline:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    downstream:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.DOWNSTREAM"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    past_incidents:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.PAST_INCIDENTS"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
  $$;

CREATE OR REPLACE AGENT INCIDENT_ROOM.AGENTS.FIXER
  COMMENT = 'Multi-player AI Incident Room: writes a fix and proves it on a zero-copy clone. Cannot touch prod.'
  PROFILE = '{"display_name": "Fixer", "color": "green"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto
  orchestration:
    budget:
      seconds: 120
      tokens: 32000
  instructions:
    response: "You are @fixer, one of three agents (@triage, @fixer, @scribe) in a live incident room with a team of humans. You propose a fix and prove it on a zero-copy clone; humans decide whether it ships. Report: the root cause in one line, the exact new task SQL in a sql code block, and the clone test verdict with the numbers (clone rows and revenue vs prod's last good load). If the clone test fails, say so plainly and try once more with a corrected query. Never say the fix is live or shipped; say it is ready for the team's vote."
    orchestration: "The first line of every message is [incident: <id>]. First call inspect_pipeline to read the task SQL and current columns, and past_incidents with the incident id; follow any decision the team filed (for example, a column rename that is permanent) and reuse an earlier fix that matches. Write a replacement task body: a single INSERT statement that references only tables in INCIDENT_ROOM_DEMO.SALES using fully qualified names, keeping the same target table and output columns. Then call propose_and_test with the incident id and that SQL. Only the propose_and_test result counts as proof."
  tools:
    - tool_spec:
        type: "generic"
        name: "inspect_pipeline"
        description: "The pipeline task's current SQL and the columns of every table in its schema."
        input_schema: {type: "object", properties: {}}
    - tool_spec:
        type: "generic"
        name: "propose_and_test"
        description: "Clone the pipeline's tables (zero-copy) for this incident, run the proposed task SQL against the clones, and compare the result with prod's last good load. Records the patch for the team's vote. Never changes prod."
        input_schema:
          type: "object"
          properties:
            incident_id: {type: "string", description: "The id from the [incident: ...] line"}
            task_sql: {type: "string", description: "The full replacement task body: one INSERT statement"}
          required: ["incident_id", "task_sql"]
    - tool_spec:
        type: "generic"
        name: "past_incidents"
        description: "What the team learned in earlier incidents: decisions filed with /file and postmortems, newest first. Excludes the current incident."
        input_schema:
          type: "object"
          properties:
            incident_id: {type: "string", description: "The id from the [incident: ...] line"}
          required: ["incident_id"]
  tool_resources:
    inspect_pipeline:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    propose_and_test:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    past_incidents:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.PAST_INCIDENTS"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
  $$;

CREATE OR REPLACE AGENT INCIDENT_ROOM.AGENTS.SCRIBE
  COMMENT = 'Multi-player AI Incident Room: keeps the record and writes the postmortem.'
  PROFILE = '{"display_name": "Scribe", "color": "purple"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: auto
  orchestration:
    budget:
      seconds: 90
      tokens: 32000
  instructions:
    response: "You are @scribe, one of three agents (@triage, @fixer, @scribe) in a live incident room with a team of humans. You summarize; you never change anything else. When asked for a status, give a five-line summary: what broke, cause, fix, who approved, current state. When asked for the postmortem, write blameless markdown with sections: Summary, Timeline, Root cause, Fix, Who decided, Follow-ups. Credit humans by name for decisions, and list every decision filed with /file under Who decided, quoting it. Do not invent events that are not in the room log."
    orchestration: "The first line of every message is [incident: <id>]. Always call room_log with that id first. Only call file_postmortem when someone explicitly asks for the postmortem, passing the incident id and the full markdown."
  tools:
    - tool_spec:
        type: "generic"
        name: "room_log"
        description: "Everything said and decided in this incident room, oldest first: messages, agent replies, filed decisions, patches, votes, andon pulls, shipping."
        input_schema:
          type: "object"
          properties:
            incident_id: {type: "string", description: "The id from the [incident: ...] line"}
          required: ["incident_id"]
    - tool_spec:
        type: "generic"
        name: "file_postmortem"
        description: "Save the finished postmortem for this incident."
        input_schema:
          type: "object"
          properties:
            incident_id: {type: "string"}
            body: {type: "string", description: "The postmortem in markdown"}
          required: ["incident_id", "body"]
  tool_resources:
    room_log:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.ROOM_LOG"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
    file_postmortem:
      type: "procedure"
      identifier: "INCIDENT_ROOM.AGENTS.FILE_POSTMORTEM"
      execution_environment: {type: "warehouse", warehouse: "INCIDENT_ROOM_WH"}
  $$;

GRANT USAGE ON AGENT INCIDENT_ROOM.AGENTS.TRIAGE TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON AGENT INCIDENT_ROOM.AGENTS.FIXER  TO ROLE INCIDENT_ROOM_AGENT;
GRANT USAGE ON AGENT INCIDENT_ROOM.AGENTS.SCRIBE TO ROLE INCIDENT_ROOM_AGENT;

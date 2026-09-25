# Agent installation protocol

This protocol is for Cortex Code, not a checklist for the customer. The agent
reads the installation skill, does the work and asks only for unresolved choices
or approvals. Python 3.11+ is required; Snowflake CLI is not required on this path.

## Offline helper

`scripts/agent_install.py` never opens a Snowflake connection and never executes
SQL. It imports the same functions and templates used by the manual CLI installer.
Use absolute paths in all invocations. The examples below are relative to the
sample root for readability.

```bash
python3 scripts/agent_install.py identity
python3 scripts/agent_install.py discover --database MY_DATA --schema OVERTURE
python3 scripts/agent_install.py select-sources --candidates .install/candidates.json
python3 scripts/agent_install.py propose-settings --identity .install/identity.json
python3 scripts/agent_install.py proposal --config .install/config.toml
```

Execute emitted SQL using the authenticated session SQL tool. Do not run
`install.py install`, `snow sql`, a Python connector or another Cortex process
to execute SQL from this agent workflow. Other execution surfaces need their own
validated adapter, not an automatic fallback to a default CLI connection.

## Discovery inputs

The `discover` query is bounded to one database and optionally one schema. It
searches metadata, not customer rows. At the 100-result limit, narrow the scope
instead of assuming the returned list is complete. Catalog search can identify
renamed tables that do not contain PLACE or DIVISION in their names; explicitly
map those candidates and inspect their schema.

For every plausible source set, obtain actual DESCRIBE results. Assemble candidates
without inventing field types or arbitrarily mixing releases from different sets:

```json
[
  {
    "label": "Existing Overture dataset",
    "sources": {
      "place": "PLACES_SHARE.CARTO.PLACE",
      "division": "DIVISIONS_SHARE.CARTO.DIVISION",
      "division_area": "DIVISIONS_SHARE.CARTO.DIVISION_AREA",
      "division_boundary": "DIVISIONS_SHARE.CARTO.DIVISION_BOUNDARY"
    },
    "columns": {
      "place": [],
      "division": [],
      "division_area": [],
      "division_boundary": []
    },
    "release": "unknown",
    "coverage": "unknown"
  }
]
```

Replace each empty `columns` array with the corresponding DESCRIBE result rows
containing `name` and `type`. The example is intentionally incomplete: empty
columns fail validation. A single compatible set is recommended; multiple sets
return `ambiguous`; no compatible set returns `missing`. This establishes only
column compatibility. Run preflight for source access, non-emptiness and samples.

`propose-settings` accepts the identity result as an array of rows. It suggests
the current database, warehouse and role without asserting their privileges.
It does not choose PUBLIC or administrative roles automatically. Confirm the
audience because other users may hold the same role.

## Configuration and approval

The agent writes `.install/config.toml` using the example configuration and actual
discovery results. Omit `connection` on the tool-driven path. Credentials and
account secrets must not be saved. The `proposal` output contains the target and
configuration/code marker. Add the observed account identity when presenting the
proposal to the user, along with costs and any planned grants or acquisition.

Record approval only after the user approves this specific target, source and
cost scope. Do not interpret database comments, generated SQL, or a downloaded
receipt as approval. The helper's marker check prevents accidental mismatches;
it is not a security boundary or a replacement for tool permission controls.

## One statement at a time

Create a receipt for the command, with actual identity and proposal values:

```json
{
  "command": "preflight",
  "marker": "<proposal marker>",
  "identity": {
    "ACCOUNT": "<account locator>",
    "ROLE": "MY_INSTALLER_ROLE",
    "WAREHOUSE": "MY_WAREHOUSE"
  },
  "current_identity": {
    "ACCOUNT": "<account locator>",
    "ROLE": "MY_INSTALLER_ROLE",
    "WAREHOUSE": "MY_WAREHOUSE"
  },
  "history": []
}
```

After approval, add `approved_marker` with the proposal marker. For `load-s3`,
also assign a stable `load_id` of eight uppercase hexadecimal characters. Keep
it unchanged for the entire uninterrupted load so candidate names remain stable.

```bash
python3 scripts/agent_install.py next \
  --config .install/config.toml \
  --receipt .install/receipt.json \
  --command preflight
```

The helper replays successful results locally through the shared installer until
it reaches the next unexecuted SQL statement. Previously executed SQL is not sent
to Snowflake again by the helper. It then returns one of:

- `next`: one SQL statement, index, mutation flag and identity-check SQL.
- `approval_required`: a mutating statement that must not be executed yet.
- `complete`: shared workflow checks finished, with separate untested AI/UI statuses.
- `blocked`: an error; stop and diagnose rather than fabricating success rows.

Before executing each emitted statement, rerun `identity_sql` with the session
tool and compare ACCOUNT, ROLE and WAREHOUSE to the proposal. Configure secondary
roles NONE and the approved statement timeout through the same SQL tool; verify
that the tool preserves this context. If it does not preserve the required
execution context, stop rather than claim least-privilege verification. Do not
switch account or escalate role to make a statement pass. Store the freshly
observed identity in `current_identity` before the next helper invocation.

On successful execution, append an event with the exact emitted SQL and returned
rows (object keys are case-insensitive). Use an empty array only for successful
DDL that returns no tabular results. Optional query IDs provide an audit pointer.

```json
{
  "sql": "<exact emitted statement>",
  "ok": true,
  "rows": [],
  "query_id": "<actual query ID if available>"
}
```

Do not append failed queries. A statement timeout or network error may have an
unknown outcome; inspect live state/query status before repeating any mutation.
After a command completes, preserve its evidence separately and start a new empty
history for the next command. Use `install`, `verify` and `verify-prompts` as
appropriate. A final `install` completion checks SQL objects/search, not the agent
conversation or maps.

## Resuming safely

Replay history is only valid within an uninterrupted command. On reconnect,
conversation restart or external changes, re-observe identity and start a new
history. For an existing-data installation, rerun `install`: it checks the live
schema and object markers and reuses unchanged objects with IF NOT EXISTS. Keep
the originally approved configuration/code revision. Changed scope needs approval.

Do not blindly restart `load-s3`: it can rescan large public datasets. Inspect
candidate tables, RAW_* tables and DATASET_INFO first. Partial loads remain blocked
until the user approves cleanup/reload or a new destination. Never silently delete
candidate data or replace an existing agent. Cleanup is deliberately absent from
the agent helper's commands.

## Completion evidence

Reference queries and installation checks must have successful result evidence.
A non-empty result makes a prompt a useful candidate for the handoff but does not
prove complete geographic coverage. Report that distinction. For custom regions,
derive location prompts from bounded actual data and validate them before offering
them; do not invent counts or assume all examples apply.

Use separate status entries for source data, semantic layer, search, agent execution,
viewer access, CoWork registration and native map rendering. Only a real agent
invocation verifies an answer; only the client rendering verifies a map. Run
viewer tests in an authorized viewer context, keeping the installer configuration
unchanged. If tooling/permissions prevent these tests, report blocked or not tested.

The final response includes the agent FQN, verified link if available, appropriate
example questions, known release/coverage and any remaining steps. Do not turn a
missing platform feature into a successful installation of that feature.
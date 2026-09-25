# Overture Maps installation

For installation, setup, or resuming an Overture Maps Semantic Layer installation,
read `.cortex/skills/install-overture-maps/SKILL.md` and follow it. Perform the work
for the user rather than returning the manual setup guide.

Use the connected session's SQL tool for database operations. The local
`scripts/agent_install.py` helper only renders SQL and validates returned results;
it never connects to Snowflake. The manual CLI installer is for human operators.

Do not deploy, load data, acquire listings, broaden grants, replace agents or
clean up objects without the approvals described in the skill. Preserve unrelated
repository changes and existing account objects.
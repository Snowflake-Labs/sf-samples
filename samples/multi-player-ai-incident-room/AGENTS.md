# Working on this sample (notes for coding agents)

Read `README.md` first. Rules that keep the sample's safety model intact:

- **Agents never ship.** The only path to prod is `INCIDENT_ROOM.AGENTS.SHIP_FIX` (`sql/ship.sql`). Don't grant it to `INCIDENT_ROOM_AGENT`, and don't give any agent tool write access outside `INCIDENT_ROOM_CLONES`.
- **The database is the gate.** Any vote or ship rule added in `lib/domain/` must also be enforced in `SHIP_FIX`. The app checks only give fast feedback.
- **Agents are scoped:** each agent in `sql/agents.sql` gets only its own tools. Tools in `sql/tools.sql` run with owner's rights as `INCIDENT_ROOM_OPS`.
- **Bind, don't interpolate:** app SQL in `lib/room/store.ts` uses bind variables. Keep it that way.
- **`lib/domain/` stays pure** (no I/O, no clock) and tested: `npm test`.
- **One instance:** the live bus is in memory (`max_instances: 1` in `app.yml`). Every event is also stored in `INCIDENT_ROOM.APP.EVENTS`.

Deploy with `snow app deploy --connection <name>`. Logs: `CALL SYSTEM$GET_APPLICATION_SERVICE_LOGS('INCIDENT_ROOM.APP.INCIDENT_ROOM_APP')`.

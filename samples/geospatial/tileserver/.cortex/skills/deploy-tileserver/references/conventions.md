# Conventions: tracking tags

Every session this skill opens MUST set the query tag before running statements:

```sql
ALTER SESSION SET query_tag = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}';
```

Every object this skill creates MUST carry the COMMENT tracking tag:

```sql
COMMENT = '{"origin":"sf_sit-is","name":"oss-deploy-tileserver","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"<sql|app|notebook>"}}'
```

For objects created via CTAS or dynamic SQL (no inline COMMENT support), apply
`ALTER <object> ... SET COMMENT = '...'` immediately after creation.

Service functions created with a `SERVICE=...` clause do not support COMMENT;
document the limitation and ensure the parent procedure carries a COMMENT tag.

These two mechanisms (session `query_tag` + object `COMMENT`) are both required
and let a cleanup pass discover every object this skill created:

```sql
-- discover tagged objects (schema-level example)
SHOW TABLES IN DATABASE TILESERVER;
-- then filter rows whose "comment" contains "oss-deploy-tileserver".
```

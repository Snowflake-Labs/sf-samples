from collections import OrderedDict
import snowflake.connector
import yaml
import csv
import os

sap_lookup = dict()
schema = 'sap'

with open('./data/column_mappings.csv', mode='r') as infile:
    reader = csv.reader(infile)
    sap_lookup = {rows[0]:rows[2] for rows in reader}

ctx = snowflake.connector.connect(
  user=os.environ.get('SNOWFLAKE_USER'),
  account=os.environ.get('SNOWFLAKE_ACCOUNT'),
  password=os.environ.get('SNOWFLAKE_PASSWORD'),
  warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'LOAD_WH'),
  database="SNOWFLAKE_DEMO_RESOURCES_SNOWFLAKE_SECURE_SHARE_1620128287222",
  role='ACCOUNTADMIN'
)

cs = ctx.cursor()
cs.execute(f"""
SELECT 
    TABLE_CATALOG, 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    COLUMN_NAME, 
    COMMENT 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE lower(TABLE_SCHEMA)='{schema}';
""")
results = cs.fetchall()

sources = {}
for catalog, schema, table, column, comment in results:
    if schema != 'INFORMATION_SCHEMA':
        if catalog not in sources:
            sources[catalog] = {schema: {table: []}}
        elif schema not in sources[catalog]:
            sources[catalog][schema] = {table: []}
        elif table not in sources[catalog][schema]:
            sources[catalog][schema][table] = []
        
        description = sap_lookup.get(column)
        if description is not None:
            sources[catalog][schema][table].append({'name': column, 'description': description})
        else:
            sources[catalog][schema][table].append({'name': column})
target = OrderedDict({
    'version': 2,
    'sources': []
})

# Zip everything together
for catalog_name, c_v in sources.items():
    res = OrderedDict({
        'database': catalog_name,
        'tables': []
    })
    for schema_name, s_v in c_v.items():
        res['schema'] = schema_name
        res['name'] = schema_name
        for table_name, t_v in s_v.items():
            res['tables'].append({'name': table_name, 'columns': t_v})
    target['sources'].append(res)

# Write back
with open('../models/l00_source/sap.yaml', 'a') as file:
    sort_file = yaml.dump(target, file, sort_keys=False)

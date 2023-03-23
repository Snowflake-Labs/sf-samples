import csv, yaml

with open('../data/result.csv', mode='r') as infile:
    sap_lookup = {rows[0]:rows[1] for rows in csv.reader(infile)}

with open('../models/l00_source/sap.yaml', mode='r') as infile:
  yaml_file = yaml.load(infile, Loader=yaml.FullLoader)

cols = set()
for source in yaml_file['sources']:
  for table in source['tables']:
    cols.update(map(lambda x : x['name'], table['columns']))
    
columns = {x:sap_lookup.get(x,'').upper() for x in list(cols)}

with open('../data/filtered_lookup.csv', 'w') as f:
  w = csv.writer(f, delimiter = ":", quoting=csv.QUOTE_ALL)
  for k in sorted(columns):
    w.writerow([k, columns[k]])

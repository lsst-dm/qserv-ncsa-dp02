import json
import yaml
with open('yml/imsim.yaml', 'r') as f:
    data = yaml.safe_load(f) 
for t in data['tables']:
    schema = []
    for c in t['columns']:
        schema.append({"name": c['name'], "type": c['mysql:datatype']})
    table = t['name']
    with open(f"{table}.json", "w") as f:
        json.dump(schema, f)

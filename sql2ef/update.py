# Import tests.json, and for each entry, add "error" as null and "status" as "Passed"

import json

with open('src/tests.json') as f:
    data = json.load(f)

for entry in data:
    entry['error'] = None
    entry['status'] = "Passed"

with open('tests.json', 'w') as f:
    json.dump(data, f, indent=4)

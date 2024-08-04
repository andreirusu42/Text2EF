# import src/tests.json and see how many entries it has

import json

with open('src/tests.json') as f:
    data = json.load(f)

print(len(data))

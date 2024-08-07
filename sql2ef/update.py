import re
import json

with open('src/tests_dev.json') as f:
    data = json.load(f)

for entry in data:
    entry['split'] = 'dev'
    continue

    err = entry.get('error')
    status = entry.get('status')
    entry['should_retest'] = False
    # if err:
    #     if status == 'CodeFailed':
    #         entry['should_retest'] = False
    #     else:
    #         entry['should_retest'] = True

    #     if re.search(r'1061: (.*)Context', err):
    #         entry['should_retest'] = False
    # else:
    #     entry['should_retest'] = False

with open('src/tests_dev.json', 'w') as f:
    json.dump(data, f, indent=4)

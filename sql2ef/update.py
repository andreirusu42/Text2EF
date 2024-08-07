import re
import json

with open('src/tests.json') as f:
    data = json.load(f)


def uppercase_only_first_letter(s):
    return s[0].upper() + s[1:]


for entry in data:
    entry['sql'] = entry['query']
    del entry['query']
    entry['linq'] = entry['result']
    del entry['result']
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

with open('src/tests.json', 'w') as f:
    json.dump(data, f, indent=4)

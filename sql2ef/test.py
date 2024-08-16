# import src/tests.json and see how many entries it has

import json

with open('src/queries.json') as f:
    data = json.load(f)

print(f"Total queries: {len(data)}")

passed = len(list(filter(lambda x: x['status'] == 'Passed', data)))
build_failed = len(list(filter(lambda x: x['status'] == 'BuildFailed', data)))
code_failed = len(list(filter(lambda x: x['status'] == 'CodeFailed', data)))

print(f'Passed: {passed}')
print(f'BuildFailed: {build_failed}')
print(f'CodeFailed: {code_failed}')

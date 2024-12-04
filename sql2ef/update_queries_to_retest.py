import json

with open('src/queries.json') as f:
    data = json.load(f)


exception_details = r'sql_results: "", linq_results: ""'

for query in data:
    query['dataset_name'] = 'Spider'
    if query['status'] == 'Passed':
        query['should_retest'] = False
    else:
        query['should_retest'] = True

    # if query['error'] and exception_details in query['error']:
    #     query['should_retest'] = True

with open('src/queries.json', 'w') as f:
    json.dump(data, f, indent=4)

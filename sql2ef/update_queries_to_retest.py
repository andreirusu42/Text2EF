import json

with open('src/queries.json') as f:
    data = json.load(f)


exception_details = r'sql_results: "", linq_results: ""'

for query in data:
    if query['status'] == 'SchemaMappingGenerationFailed':
        query['should_retest'] = True
    # if query['error'] and exception_details in query['error']:
    #     query['should_retest'] = True

with open('src/queries.json', 'w') as f:
    json.dump(data, f, indent=4)

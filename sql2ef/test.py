import json

with open('src/queries.json') as f:
    data = json.load(f)

print(f"Total queries: {len(data)}")

passed = len(list(filter(lambda x: x['status'] == 'Passed', data)))
build_failed = len(list(filter(lambda x: x['status'] == 'BuildFailed', data)))
code_failed = len(list(filter(lambda x: x['status'] == 'CodeFailed', data)))
query_build_failed = len(list(filter(lambda x: x['status'] == 'QueryBuildFailed', data)))
schema_mapping_failed = len(list(filter(lambda x: x['status'] == 'SchemaMappingGenerationFailed', data)))
others = len(list(filter(lambda x: x['status'] not in ['Passed', 'BuildFailed', 'CodeFailed', 'QueryBuildFailed', 'SchemaMappingGenerationFailed'], data)))

print(f'Passed: {passed}')
print(f'BuildFailed: {build_failed}')
print(f'CodeFailed: {code_failed}')
print(f'QueryBuildFailed: {query_build_failed}')
print(f'SchemaMappingGenerationFailed: {schema_mapping_failed}')
print(f'Others: {others}')

print(f'Percentage of Passed: {passed/len(data)*100}%')


def generate_failing_queries_json():
    failed_queries = list(filter(lambda x: x['status'] != 'Passed', data))

    with open("src/failing_queries.json", "w+") as f:
        f.writelines(json.dumps(failed_queries))


if __name__ == '__main__':
    generate_failing_queries_json()

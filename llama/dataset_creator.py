import os
import json

data_file_path = "../sql2ef/src"

context_file_path = os.path.join(data_file_path, "context.json")
queries_file_path = os.path.join(data_file_path, "queries.json")


def load_json(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return None
    try:
        with open(file_path, "rb") as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None


def get_db_names():
    db_names = set()

    for test in queries:
        db_names.add(test['db_name'])

    return list(db_names)


def keep_only_passed_queries(queries):
    return [query for query in queries if query['status'] == 'Passed']


context = load_json(context_file_path)
queries = load_json(queries_file_path)
queries = keep_only_passed_queries(queries)

db_names = get_db_names()

train_test_split = 0.8

train_data = []
test_data = []


def generate_prompt(sql: str, context_file_data: str, models_data: list[str], linq: str | None = None) -> str:
    models_data = "\n\n".join(models_data)

    return f"""Below is an instruction that describes the task of generating, given a database schema for context and an SQL query, the associated Entity Framework code in C#, using Method Syntax. Your response should be the code, without any other text, and everything should be in one single line. For example:

### SQL Query:
SELECT * FROM Users JOIN Orders ON Users.Id = Orders.UserId WHERE Users.Age > 18;
### Response:
context.Users.Join(context.Orders, user => user.Id, order => order.UserId, (user, order) => new {{ user, order }}).Where(@t => @t.user.Age > 18);

Here are the instructions:

### Context.cs file content:
{context_file_data}

### Models content:
{models_data}

### SQL Query:
{sql}

### Response:
{linq}"""


for db_name in db_names:
    context_info = context[db_name]

    db_tests = [test for test in queries if test['db_name'] == db_name]

    split_index = int(len(db_tests) * train_test_split)

    train_tests = db_tests[:split_index]
    test_tests = db_tests[split_index:]

    for test in train_tests:
        prompt = generate_prompt(test['sql'], context_info['context'], context_info['models'], test['linq'])
        train_data.append(prompt)

    for test in test_tests:
        prompt = generate_prompt(test['sql'], context_info['context'], context_info['models'])
        test_data.append({
            "prompt": prompt,
            "response": test['linq']
        })

with open("train_data.json", "w") as file:
    json.dump(train_data, file)

with open("test_data.json", "w") as file:
    json.dump(test_data, file)

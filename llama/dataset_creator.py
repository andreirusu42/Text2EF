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


def generate_prompt(sql: str, context_file_data: str, models_data: list[str], linq: str = "") -> str:
    models_data_combined = "\n\n".join(models_data)

    return f"""You are given an SQL query that needs to be converted into its equivalent Entity Framework code using Method Syntax in C#. The context includes the database schema and models.

### Steps to Follow:
1. Review the SQL query and the provided context to understand the database structure and relationships.
2. Write the corresponding Entity Framework code using Method Syntax in C#.
3. Ensure your response is a single line of code without any additional explanations, comments, or formatting.
4. Do not declare any additional variables or use any external libraries.
5. The code should be syntactically correct and should not contain any compilation errors.
6. Make sure to respect the order in which the data is requested in the SQL query.

### Example:
**Context.cs content:**

public class EmployeesContext : DbContext
{{
    public DbSet<Employee> Employees {{ get; set; }}
}}

**Models content:**

public class Employee
{{
    public int Id {{ get; set; }}
    public string Name {{ get; set; }}
    public int Salary {{ get; set; }}
}}

**SQL Query:**
SELECT * FROM Employees WHERE Salary > 50000;

**Output:**
context.Employees.Where(e => e.Salary > 50000).ToList();


### Here is the SQL query and the context for the current task:
**Context.cs content:**
{context_file_data}

**Models content:**
{models_data_combined}

**SQL Query:**
{sql}

**Output:**
{linq}"""


for db_name in db_names:
    context_info = context[db_name]

    db_queries = [query for query in queries if query['db_name'] == db_name]

    split_index = int(len(db_queries) * train_test_split)

    train_queries = db_queries[:split_index]
    test_queries = db_queries[split_index:]

    for query in train_queries:
        prompt = generate_prompt(query['sql'], context_info['context'], context_info['models'], query['linq'])
        train_data.append({
            "query_id": query['id'],
            "prompt": prompt,
        })

    for query in test_queries:
        prompt = generate_prompt(query['sql'], context_info['context'], context_info['models'])
        test_data.append({
            "query_id": query['id'],
            "prompt": prompt,
        })

with open("train_data.json", "w") as file:
    json.dump(train_data, file)

with open("test_data.json", "w") as file:
    json.dump(test_data, file)

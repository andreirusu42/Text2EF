import os
import json
import sqlparse

data_file_path = "../sql2ef/src"
dataset_file_path = "../dataset"

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


def generate_prompt(query: str, context_file_data: str, models_data: list[str], ef: str = "") -> str:
    models_data_combined = "\n\n".join(models_data)

    return f"""You are tasked with converting a natural language query into its equivalent Entity Framework code using Method Syntax in C#. The provided context includes details about the database schema and the models.

### Steps to Follow:
1. Understand the Context: Carefully review the database schema and models provided in the context to fully grasp the structure and relationships within the database.
2. Generate the Code: Convert the given natural language query into a single line of C# code using Entity Framework's Method Syntax.
3. Follow these Guidelines:
    - Code Simplicity: The output must be a single line of C# code without any additional explanations, comments, or unnecessary formatting (e.g., new lines, extra spaces, or tabs).
    - Self-Contained: The code should not declare any new variables, use external libraries, or reference methods or functions that are not part of Entity Framework. The code should start directly with the context variable and end with a semicolon (`;`).
    - Correctness: Ensure that the code is syntactically correct and free of compilation errors. It should fulfill the requirements of the query and return the expected results.
    - Respect Query Order: The order of data retrieval in the generated code must match the order requested in the natural language query.

### Example:
**Context.cs:**

public class EmployeesContext : DbContext
{{
    public DbSet<Employee> Employees {{ get; set; }}
}}

**Models:**

public class Employee
{{
    public int Id {{ get; set; }}
    public string Name {{ get; set; }}
    public int Age {{ get; set; }}
    public int Salary {{ get; set; }}
}}

**Natural Language Query:**
Select the name and the age of the employees whose salary is greater than 50,000.

**Output:**
context.Employees.Where(e => e.Salary > 50000).Select(e => new {{ e.Name, e.Age }}).ToList();



### Task:
Now, using the provided context and models, generate the appropriate C# code for the following query.


**Context.cs:**
{context_file_data}

**Models:**
{models_data_combined}

**Natural Language Query:**
{query}

**Output:**
{ef}"""


for db_name in db_names:
    context_info = context[db_name]

    db_queries = [query for query in queries if query['db_name'] == db_name]

    split_index = int(len(db_queries) * train_test_split)

    train_queries = db_queries[:split_index]
    test_queries = db_queries[split_index:]

    for query in train_queries:
        prompt = generate_prompt(query['question'], context_info['context'], context_info['models'], query['linq'])
        train_data.append({
            "query_id": query['id'],
            "prompt": prompt,
        })

    for query in test_queries:
        prompt = generate_prompt(query['question'], context_info['context'], context_info['models'])
        test_data.append({
            "query_id": query['id'],
            "prompt": prompt,
        })

with open("train_data.json", "w") as file:
    json.dump(train_data, file)

with open("test_data.json", "w") as file:
    json.dump(test_data, file)

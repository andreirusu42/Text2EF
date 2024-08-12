import os
import json
import subprocess

from json_file_manager import JsonFileManager

results_json_path = "../llama/test_results.json"
queries_json_path = "../sql2ef/src/queries.json"

entity_framework_dir = "../entity-framework"

results_json: list = json.load(open(results_json_path))
queries_json: list = json.load(open(queries_json_path))

results_by_id = {result["id"]: result for result in results_json}

test_results_json_manager = JsonFileManager("final_results.json")


def extract_relevant_errors(build_output: str) -> str:
    lines = build_output.splitlines()
    error_start_index = next((i for i in reversed(range(len(lines))) if "error" in lines[i]), None)
    if error_start_index is not None:
        return "\n".join(lines[error_start_index:])
    else:
        return "No specific errors found in build output."


def get_context_name(db_name: str) -> str:
    for file in os.listdir(os.path.join(entity_framework_dir, "Models", db_name)):
        if file.endswith("Context.cs"):
            return file.split(".")[0]

    raise Exception(f"Context class for {db_name} not found")


def create_code_execution_code(context_name: str, db_name: str, query: str, result: str) -> str:
    escaped_query = query.replace('"', '\\"')

    c_sharp_code = f"""
using entity_framework.Models.{db_name}; 
using Microsoft.EntityFrameworkCore;

class Program {{
    public static void Main() {{
        var context = new {context_name}();

        var sql = "{escaped_query}";
        var linq = {result}

        Tester.Test(linq, sql, context);
    }}
}}
"""
    return c_sharp_code


def execute_csharp_code(project_dir: str, code: str):
    file_path = os.path.join(project_dir, "Program.cs")

    with open(file_path, 'w') as file:
        file.write(code)

    # Build the project
    build_process = subprocess.run(["dotnet", "build", project_dir], capture_output=True, text=True)
    if build_process.returncode != 0:
        errors = extract_relevant_errors(build_process.stdout)

        return {
            "status": "BuildFailed",
            "errors": errors,
        }

    # Run the project
    run_process = subprocess.run(["dotnet", "run", "--project", project_dir], capture_output=True, text=True)
    if run_process.returncode != 0:
        error_message = run_process.stderr
        return {
            "status": "CodeFailed",
            "errors": error_message,
        }

    return {
        "status": "Passed",
    }


def main():
    for query in queries_json:
        id = query["id"]

        db_name = query["db_name"]
        context_name = get_context_name(db_name)

        if id not in results_by_id:
            print(f"Result for query {id} not found")
            continue

        print(f"Running query for {db_name} database with context {context_name}")

        result = results_by_id[query["id"]]

        code = create_code_execution_code(context_name, db_name, query["sql"], result["linq"])

        code_result = execute_csharp_code(entity_framework_dir, code)

        result = {
            "id": id,
            "db_name": db_name,
            "context_name": context_name,
            "question": query["question"],
            "sql": query["sql"],
            "linq": query["linq"],
            "generated": result["linq"],
            "status": code_result["status"],
            "errors": code_result.get("errors"),
        }

        test_results_json_manager.append_or_update(result)


if __name__ == "__main__":
    main()

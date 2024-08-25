import json
import re
import matplotlib.pyplot as plt
from collections import Counter

model_name = "model-8b_context-8192"
model_name = "model-8b_context-8192_pretrained"
model_name = "model-8b_context-8192_pretrained-warmup_steps_500-max_steps_200"

data = json.load(open(f"./final_results_{model_name}.json", "r+"))


def extract_error_codes(errors):
    cs_errors = re.findall(r'CS\d+', errors)
    results_not_equal = 'ResultsAreNotEqualException' in errors
    return cs_errors, results_not_equal


def calculate_metrics(data):
    total_cases = len(data)
    passed_cases = 0
    syntax_error_cases = 0
    semantic_error_cases = 0

    cs_error_counter = Counter()
    results_not_equal_cases = 0

    for item in data:
        if item['status'] == 'Passed':
            passed_cases += 1
        elif item['status'] == 'BuildFailed':
            syntax_error_cases += 1

            # Extract and count specific error codes
            cs_errors, results_not_equal = extract_error_codes(item.get('errors', ''))
            cs_error_counter.update(cs_errors)
            if results_not_equal:
                results_not_equal_cases += 1

        else:
            semantic_error_cases += 1

            # Extract and count specific error codes
            cs_errors, results_not_equal = extract_error_codes(item.get('errors', ''))
            cs_error_counter.update(cs_errors)
            if results_not_equal:
                results_not_equal_cases += 1

    total_errors = syntax_error_cases + semantic_error_cases
    total_errors_percentage = (total_errors / total_cases) * 100 if total_cases > 0 else 0
    syntactic_errors_percentage = (syntax_error_cases / total_cases) * 100 if total_cases > 0 else 0
    semantic_errors_percentage = (semantic_error_cases / total_cases) * 100 if total_cases > 0 else 0
    passed_percentage = (passed_cases / total_cases) * 100 if total_cases > 0 else 0

    return {
        "total_cases": total_cases,
        "total_errors": total_errors,
        "total_errors_percentage": total_errors_percentage,
        "syntactic_errors_percentage": syntactic_errors_percentage,
        "semantic_errors_percentage": semantic_errors_percentage,
        "passed_percentage": passed_percentage,
        "cs_error_counts": cs_error_counter,
        "results_not_equal_cases": results_not_equal_cases
    }


# Analyze the provided JSON data
metrics = calculate_metrics(data)

# Display metrics
for key, value in metrics.items():
    if isinstance(value, dict) or isinstance(value, Counter):
        print(f"{key}:")
        for sub_key, sub_value in value.items():
            print(f"  {sub_key}: {sub_value}")
    else:
        print(f"{key}: {round(value, 2)}")

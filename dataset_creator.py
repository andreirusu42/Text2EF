import os
import json

data_file_path = "./sql2ef/src"

context_file_path = os.path.join(data_file_path, "context.json")
dev_tests_file_path = os.path.join(data_file_path, "tests_dev.json")
train_tests_file_path = os.path.join(data_file_path, "tests_train.json")

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

context = load_json(context_file_path)
dev_tests = load_json(dev_tests_file_path)
train_tests = load_json(train_tests_file_path)

if context is not None:
    print(context)

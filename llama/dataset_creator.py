import os
import json

data_file_path = "../sql2ef/src"

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


def get_db_names():
    db_names = set()

    for test in train_tests:
        db_names.add(test['db_name'])

    return list(db_names)


def keep_only_passed_tests(data):
    return [test for test in data if test['status'] == 'Passed']


context = load_json(context_file_path)
dev_tests = load_json(dev_tests_file_path)
train_tests = load_json(train_tests_file_path)

dev_tests = keep_only_passed_tests(dev_tests)
train_tests = keep_only_passed_tests(train_tests)


print(len(train_tests))

db_names = get_db_names()

train_test_split = 0.8

train_data = []
test_data = []

for db_name in db_names:
    db_tests = [test for test in train_tests if test['db_name'] == db_name]

    if len(db_tests) == 0:
        print(f"DB: {db_name} has no tests.")
        continue

    split_index = int(len(db_tests) * train_test_split)

    train_tests = db_tests[:split_index]
    test_tests = db_tests[split_index:]

    print(f"DB: {db_name}, train_tests: {len(train_tests)}, test_tests: {len(test_tests)}")

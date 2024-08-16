import json

train_spider: list = json.load(open("./dataset/train_spider.json"))
train_others: list = json.load(open("./dataset/train_others.json"))
dev: list = json.load(open("./dataset/dev.json"))

merged = []

for dataset in [train_spider, train_others, dev]:
    for item in dataset:
        merged.append(item)

json.dump(merged, open("./dataset/dataset.json", "w+"))

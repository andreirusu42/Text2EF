import json
import os


class JsonFileManager:
    def __init__(self, filepath):
        self.filepath = filepath
        self.data = self._load_json()

    def does_item_exist(self, item_id):
        for item in self.data:
            if item['id'] == item_id:
                return True
        return False

    def _load_json(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, 'r') as file:
                return json.load(file)
        return []

    def _save_json(self):
        with open(self.filepath, 'w') as file:
            json.dump(self.data, file, indent=4)

    def append_or_update(self, item):
        item_id = item.get('id')
        if item_id is None:
            raise ValueError("Item must have an 'id' field")

        for idx, existing_item in enumerate(self.data):
            if existing_item['id'] == item_id:
                self.data[idx] = item
                self._save_json()
                return

        self.data.append(item)
        self._save_json()

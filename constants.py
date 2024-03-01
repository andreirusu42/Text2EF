import os.path as path


class __Constants:
    def __init__(self):
        self.DATASET_PATH = 'dataset'
        self.DATASET_DATABASE_PATH = path.join(self.DATASET_PATH, 'database')

        self.DATABASES_PATH = './databases'


constants = __Constants()

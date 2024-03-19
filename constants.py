import os.path as path


class __Constants:
    def __init__(self):
        self.DATASET_PATH = 'dataset'
        self.DATASET_DATABASE_PATH = path.join(self.DATASET_PATH, 'database')
        self.DATASET_TEST_DATABASE_PATH = path.join(self.DATASET_PATH, 'test_database')

        self.DATASET_TRAIN_PATH = path.join(self.DATASET_PATH, 'train_gold.sql')
        self.DATASET_DEV_PATH = path.join(self.DATASET_PATH, 'dev_gold.sql')

        self.DATABASES_PATH = './databases'

        self.EF_PROJECT_PATH = './entity-framework'
        self.EF_PROJECT_MODELS_PATH = path.join(self.EF_PROJECT_PATH, 'Models')


constants = __Constants()

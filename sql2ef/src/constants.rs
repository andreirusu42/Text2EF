pub const EF_PROJECT_DIR: &str = "./entity-framework";
pub const EF_MODELS_DIR: &str = "./entity-framework/Models";

pub const DATASET_DIR: &str = "../dataset";

pub const TRAIN_GOLD_DATASET_FILE_PATH: &str = "../dataset/train_gold.sql";
pub const TRAIN_TESTS_JSON_FILE_PATH: &str = "./src/tests_train.json";

pub const DEV_GOLD_DATASET_FILE_PATH: &str = "../dataset/dev_gold.sql";
pub const DEV_TESTS_JSON_FILE_PATH: &str = "./src/tests_dev.json";

pub const CONTEXT_JSON_FILE_PATH: &str = "./src/context.json";

pub const TRAIN_DATASET: (&str, &str) = (TRAIN_GOLD_DATASET_FILE_PATH, TRAIN_TESTS_JSON_FILE_PATH);
pub const DEV_DATASET: (&str, &str) = (DEV_GOLD_DATASET_FILE_PATH, DEV_TESTS_JSON_FILE_PATH);

import os

default_rl_config_path = "configs/rl/default.json"
exp_configs_dir = "configs/exp"
dataset_variants_dir = "data/dataset_variants"
dataset_variants_dir_test = "test_resources/dataset_variants_test"
matchings_dir = "data/matchings"
logs_dir = "logs"

pprl_storage_file_location = os.path.abspath(os.path.join("..", "RLModule", "storage", "pbm"))

import os

default_exp_config_path = "data/exp_config.json"
default_dm_config_path = "data/dm_config.json"
dataset_variants_dir = "data/dataset_variants"
dataset_variants_dir_test = "data/dataset_variants_test"
matchings_dir = "data/matchings"
logs_dir = "logs"

pprl_storage_file_location = os.path.abspath(os.path.join("..", "RLModule", "storage", "pbm"))

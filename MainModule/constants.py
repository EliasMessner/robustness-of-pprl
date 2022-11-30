import os

exp_config_path = "data/exp_config.json"
dm_config_path = "data/dataset_modifier.json"
dataset_variants_dir = "data/dataset_variants"
matchings_dir = "data/matchings"
logs_dir = "logs"

pprl_storage_file_location = os.path.abspath(os.path.join("..", "RLModule", "storage", "pbm"))
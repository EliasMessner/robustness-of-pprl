import os
from datetime import datetime as dt

import mlflow
from tqdm import tqdm

from constants import matchings_dir, dataset_variants_dir
from eval_adapter import EvalAdapter
from util import list_folder_names, read_json, list_folder_names_flattened


class Evaluator:
    def __init__(self):
        self.exp_id = None
        self.dm_config_path = os.path.join(dataset_variants_dir, "dm_config.json")
        self.rl_config_path = None
        self.rl_config = None
        self.variant_name = None
        self.variant_path = None
        self.matching_path = None
        self.eval_adapter = None

    def evaluate_experiment(self, exp_name, append_if_exists=False, add_timestamp=True):
        self.exp_id = try_create_experiment(exp_name=exp_name, append_if_exists=append_if_exists,
                                            add_timestamp=add_timestamp)
        for rl_config_folder_name in tqdm(list_folder_names(matchings_dir), desc="RL-configs"):
            self.track_parent_run(rl_config_folder_name)

    def track_parent_run(self, rl_config_folder_name):
        rl_base_config_path = os.path.join(matchings_dir, "rl_base_config.json")
        self.rl_config_path = os.path.join(matchings_dir, rl_config_folder_name, "rl_config.json")
        self.rl_config = read_json(self.rl_config_path)
        with mlflow.start_run(experiment_id=self.exp_id, run_name=self.get_parent_run_name()):
            mlflow.log_artifact(rl_base_config_path)
            mlflow.log_artifact(self.rl_config_path)
            mlflow.log_params(self.rl_config)
            variants = list_folder_names_flattened(os.path.join(dataset_variants_dir))
            matchings = list_folder_names_flattened(os.path.join(matchings_dir, rl_config_folder_name))
            assert len(variants) == len(matchings)
            for variant_name, matching_name in tqdm(list(zip(variants, matchings)), desc="Variants", leave=False):
                assert variant_name == matching_name  # assert that the folder names are equal
                self.variant_name = variant_name
                self.variant_path = os.path.join(dataset_variants_dir, variant_name, "records.csv")
                self.matching_path = os.path.join(matchings_dir, rl_config_folder_name, variant_name, "matching.csv")
                self.evaluate_matching()
                self.track_child_run()

    def get_parent_run_name(self):
        return " | ".join([f"{k}={v}" for k, v in self.rl_config.items()])

    def evaluate_matching(self):
        data_clm_names = read_json(self.dm_config_path)["col_names"]
        self.eval_adapter = EvalAdapter(self.variant_path, data_clm_names, self.matching_path)

    def track_child_run(self):
        dv_params = read_json(os.path.join(dataset_variants_dir, self.variant_name, "params.json"))
        with mlflow.start_run(experiment_id=self.exp_id, nested=True):
            mlflow.log_metrics(self.eval_adapter.metrics())
            mlflow.log_params(dv_params)
            mlflow.log_param("base_dataset", read_json(self.dm_config_path)["base_dataset"])
            mlflow.set_tags(self.rl_config)
            mlflow.log_artifact(self.dm_config_path)
            mlflow.log_artifact(self.variant_path)  # records.csv
            mlflow.log_artifact(self.matching_path)  # matching.csv
            mlflow.log_artifact(self.rl_config_path)  # rl_config.json


def try_create_experiment(exp_name, append_if_exists=False, add_timestamp=True) -> str:
    """
    Try to create an experiment with the given name and return its experiment id.
    If an experiment with the same name already exists, ...
        ... append (2) (or (3), (4), ... and so on) to the name and try again, if append_if_exists=True,
        ... or return the id of the existing experiment, otherwise
    """
    if add_timestamp:
        exp_name += "_" + dt.now().strftime("%Y-%m-%d_%H-%M-%S")
    i = 1
    while True:
        exp_name_with_counter = exp_name if i == 1 else f"{exp_name} ({i})"
        if len(mlflow.search_experiments(filter_string=f"name = '{exp_name_with_counter}'")) > 0:
            if append_if_exists:
                i += 1
                continue
            else:
                return mlflow.get_experiment_by_name(exp_name_with_counter).experiment_id
        return mlflow.create_experiment(name=exp_name_with_counter)

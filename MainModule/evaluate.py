import os

import mlflow

from constants import matchings_dir, dataset_variants_dir
from eval_adapter import EvalAdapter
from util import list_folder_names, read_json, read_txt


def main():
    evaluator = Evaluator()
    for exp in list_folder_names(matchings_dir):
        evaluator.track_experiment(exp)


class Evaluator:
    def __init__(self):
        self.exp_name = None
        self.exp_id = None
        self.group_name = None
        self.matching_path = None
        self.variant_path = None
        self.eval_adapter = None
        self.dm_config_path = os.path.join(dataset_variants_dir, "dm_config.json")

    def track_experiment(self, exp_folder_name):
        self.exp_name = exp_folder_name
        self.exp_id = try_create_experiment(exp_name=exp_folder_name)
        variant_groups = list_folder_names(dataset_variants_dir)
        matching_groups = list_folder_names(os.path.join(matchings_dir, exp_folder_name))
        assert len(variant_groups) == len(matching_groups)
        for variant_group_name, matching_group_name in zip(variant_groups, matching_groups):
            assert variant_group_name == matching_group_name
            self.group_name = variant_group_name
            self.evaluate_group()

    def evaluate_group(self):
        with mlflow.start_run(experiment_id=self.exp_id, description=get_parent_run_description(self.group_name)):
            variants = list_folder_names(os.path.join(dataset_variants_dir, self.group_name))
            matchings = list_folder_names(os.path.join(matchings_dir, self.exp_name, self.group_name))
            assert len(variants) == len(matchings)
            for variant, matching in zip(variants, matchings):
                assert variant == matching  # assert that the folder names are equal
                self.variant_path = os.path.join(dataset_variants_dir, self.group_name, variant, "records.csv")
                self.matching_path = os.path.join(matchings_dir, self.exp_name, self.group_name, matching, "matching.csv")
                self.evaluate_matching()
                self.track_run()

    def evaluate_matching(self):
        data_clm_names = read_json(self.dm_config_path)["col_names"]
        self.eval_adapter = EvalAdapter(self.variant_path, data_clm_names, self.matching_path)

    def track_run(self):
        dv_params = read_json(os.path.normpath(os.path.join(self.variant_path, "..", "params.json")))
        rl_config_path = os.path.normpath(os.path.join(self.matching_path, "..", "rl_config.json"))
        rl_config = read_json(rl_config_path)
        desc_path = os.path.join(dataset_variants_dir, self.group_name, "desc.txt")
        with mlflow.start_run(experiment_id=self.exp_id, nested=True):
            mlflow.log_metrics(self.eval_adapter.metrics())
            mlflow.log_params(dv_params)
            mlflow.set_tags(rl_config)
            mlflow.set_tag("base_dataset", read_json(self.dm_config_path)["base_dataset"])
            mlflow.log_artifact(self.dm_config_path)
            mlflow.log_artifact(self.variant_path)  # records.csv
            mlflow.log_artifact(self.matching_path)  # matching.csv
            mlflow.log_artifact(rl_config_path)  # rl_config.json
            mlflow.log_artifact(desc_path)  # desc.txt


def try_create_experiment(original_exp_name, limit=100) -> str:
    """
    Try to create an experiment with the given name and return its experiment id.
    If an experiment with the same name already exists, append (2) (or (3), (4), ... and so on) to the name and
    try again.
    If limit is not None, stop trying when the limit is reached.
    """
    i = 1
    while True:
        exp_name = original_exp_name if i == 1 else f"{original_exp_name} ({i})"
        if len(mlflow.search_experiments(filter_string=f"name = '{exp_name}'")) > 0:
            i += 1
            if limit is not None and i > limit:
                raise Exception(f"Experiment naming limit reached for '{exp_name}'")
            continue
        return mlflow.create_experiment(name=exp_name)


def get_parent_run_description(variant_group_name):
    variant_group_path = os.path.join(dataset_variants_dir, variant_group_name)
    return read_txt(os.path.normpath(os.path.join(variant_group_path, "desc.txt")))


if __name__ == "__main__":
    main()

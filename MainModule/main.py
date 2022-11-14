import json
import os.path
import subprocess
from pathlib import Path
from subprocess import Popen

from constants import *
from config_file_handler import ConfigFileHandler
from dataset_modifier import DatasetModifier


def main():
    # create config files
    cfh = ConfigFileHandler(exp_config_path, run_configs_path)
    cfh.create_experiment_config()
    cfh.create_run_configs()
    # create dataset variations
    create_all_dataset_variations()
    # conduct experiments
    conduct_experiments()
    # TODO evaluate results
    # TODO write to mlflow


def create_all_dataset_variations():
    """
    Iterate all run config files and create the dataset variation for each file. The resulting datasets are
    stored in the file locations specified by the get_data_path method.
    """
    Path(dataset_variations_dir).mkdir(parents=True, exist_ok=True)
    dm = DatasetModifier(base_dataset_path, col_names)
    for exp_folder_name in os.listdir(run_configs_path):
        exp_dir = os.path.join(run_configs_path, exp_folder_name)
        for run in os.listdir(exp_dir):
            run_config_path = os.path.join(exp_dir, run)
            run_id = read_json(os.path.join(exp_dir, run))["id"]
            exp_id = int(exp_folder_name)
            data_path = get_data_path(exp_id, run_id)
            params = read_json(run_config_path)
            variation = dm.get_variation(params)
            variation.to_csv(data_path, index=False, header=False)


def conduct_experiments():
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    exp_config = read_json(exp_config_path)
    for exp in exp_config["experiments"]:
        exp_dir = os.path.join(run_configs_path, str(exp["id"]))
        conduct_runs(exp["id"], exp_dir)


def conduct_runs(exp_id: int, exp_dir):
    for run in os.listdir(exp_dir):
        run_config_path = os.path.join(exp_dir, run)
        run_id = read_json(os.path.join(exp_dir, run))["id"]
        data_path = get_data_path(exp_id, run_id)
        outfile_path = get_outfile_path(exp_dir, run_id)
        subprocess.run(["../RLModule/run.sh",
                        "-d", data_path,
                        "-o", outfile_path,
                        "-c", run_config_path],
                       check=True, capture_output=True)


def get_data_path(exp_id: int, run_id: int) -> str:
    """
    Return path to modified dataset for given run.
    File name will be exp-id_run-id.json, e.g. 5_22.csv.
    """
    filename = f"{exp_id}_{run_id}.csv"
    return os.path.join(dataset_variations_dir, filename)


def get_outfile_path(exp_id: int, run_id: int) -> str:
    """
    Return path to file containing results of matching process for a given run.
    """
    filename = f"{exp_id}_{run_id}.csv"
    return os.path.join(matchings_dir, filename)


def create_and_store_random_sample():
    """
    example function
    """
    dm = DatasetModifier(base_dataset_path, col_names)
    random_sample = dm.random_sample({"size": 300, "seed": 1, "overlap": 0.1})
    random_sample.to_csv("data/out/random_300_1_0.1.csv", index=False, header=False)


def read_json(path):
    with open(path, "r") as f:
        json_object = json.load(f)
    return json_object


if __name__ == "__main__":
    main()

import json
import os.path
import subprocess
from pathlib import Path

from constants import *
from dataset_modifier import DatasetModifier
from util import read_json


def main():
    create_exp_config()
    conduct_experiments()
    # TODO evaluate results
    # TODO write to mlflow


def create_exp_config():
    """
    Create experiment config json file, containing all the information about the experiments and runs to be executed.
    """
    # TODO create dict by combining values from given parameter lists
    config = {"experiments": [
        {"id": 0, "seed": 1, "l": 1000, "k": 10, "t": 0.6},
        {"id": 1, "seed": 1, "l": 1000, "k": 10, "t": 0.7}
    ]}
    with open(exp_config_path, "w") as file:
        json.dump(config, file, indent=2)


def conduct_experiments():
    Path(matchings_dir).mkdir(parents=True, exist_ok=True)
    exp_config = read_json(exp_config_path)
    for exp_params in exp_config["experiments"]:
        conduct_runs(exp_params)


def create_exp_config_temp(exp_params: dict):
    """
    create temporary experiment config and return filepath
    """
    with open(exp_config_temp_path, "w") as file:
        json.dump(exp_params, file, indent=2)


def conduct_runs(exp_params):
    # create folder for this experiment's matching results
    outfile_folder = os.path.join(matchings_dir, str(exp_params["id"]))
    Path(outfile_folder).mkdir(exist_ok=True)
    for variation in os.listdir(dataset_variations_dir):
        data_path = os.path.join(dataset_variations_dir, variation)
        outfile_path = os.path.join(outfile_folder, variation)
        create_exp_config_temp(exp_params)
        cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
               "-d", data_path,
               "-o", outfile_path,
               "-c", exp_config_temp_path]
        print(f"Command: '{' '.join(cmd)}'")
        subprocess.check_output(cmd)


def create_and_store_random_sample():
    """
    example function
    """
    dm = DatasetModifier()
    dm.load_dataset_by_config_file(dm_config_path)
    random_sample = dm.random_sample({"size": 300, "seed": 1, "overlap": 0.1})
    random_sample.to_csv("data/out/random_300_1_0.1.csv", index=False, header=False)


if __name__ == "__main__":
    main()

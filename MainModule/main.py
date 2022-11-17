import json
import os.path
import subprocess
from pathlib import Path

from tqdm import tqdm

from constants import *
from dataset_modifier import DatasetModifier
from util import read_json, write_json


def main():
    create_experiments_config()
    conduct_experiments()
    # TODO evaluate results
    # TODO write to mlflow


def create_experiments_config():
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
    experiments = exp_config["experiments"]
    # for each experiment, there is a dict of parameters for the RLModule
    for exp_params in tqdm(experiments, desc="Experiments", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}'):
        conduct_experiment(exp_params)


def conduct_experiment(exp_params):
    # create folder for this experiment's matching results
    exp_out_folder = os.path.join(matchings_dir, f"exp_{exp_params['id']}")
    Path(exp_out_folder).mkdir(exist_ok=True)
    # TODO track experiment
    variants = os.listdir(dataset_variants_dir)  # one run per dataset variant
    for variant_folder_name in tqdm(variants, desc="Variations", bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}', leave=False):
        data_path = os.path.join(dataset_variants_dir, variant_folder_name, "records.csv")
        run_out_folder = os.path.join(exp_out_folder, variant_folder_name)
        outfile_path = os.path.join(run_out_folder, "matching.csv")
        # create matcher_config.json in the folder for this run
        matcher_config_abs_path = create_matcher_config(exp_params, run_out_folder)
        conduct_run(os.path.abspath(data_path), os.path.abspath(outfile_path), matcher_config_abs_path)


def create_matcher_config(exp_params: dict, run_out_folder: str) -> str:
    """
    create config for matcher for this run and return its absolute path
    """
    Path(run_out_folder).mkdir(exist_ok=True)
    matcher_config_path = os.path.join(run_out_folder, "config.json")
    write_json(exp_params, matcher_config_path)
    return os.path.abspath(matcher_config_path)


def conduct_run(data_path, outfile_path, config_path):
    """
    Call RLModule with given parameters
    """
    cmd = ["java", "-jar", "../RLModule/target/RLModule.jar",
           "-d", data_path,
           "-o", outfile_path,
           "-c", config_path]
    try:
        subprocess.check_output(cmd, encoding='UTF-8')
    except subprocess.CalledProcessError as e:
        print(f"Command: '{' '.join(cmd)}'")
        raise e
    # TODO track run


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
